"""Generate separate local fixtures and digest-pinned Kubernetes application manifests.

The production renderer integrates with an existing secured Dapr control plane,
HA PostgreSQL, workflow state backend and pre-provisioned broker. It does not
turn the single-node local containers into a production HA cluster.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import ipaddress
import json
import os
from pathlib import Path
import re
import secrets
from urllib.parse import urlsplit

from .configuration import https_issuer, https_origin
from .domain import PermanentError, require, token

IMAGES = {"python": "python:3.13.5-slim-bookworm", "dapr": "daprio/daprd:1.18.3",
          "control": "daprio/dapr:1.18.3", "postgres": "postgres:17-alpine", "redis": "redis:7.4-alpine",
          "rabbitmq": "rabbitmq:4.1-management-alpine", "nats": "nats:2.14.6-alpine",
          "natscli": "natsio/nats-box:0.19.0", "nginx": "nginx:1.28-alpine"}


def topology(tenant="demo", source="workflow-worker"):
    token(tenant)
    token(source)
    command = f"a2a.v1.{tenant}.command.agent.{source}"
    event, dlq = f"a2a.v1.{tenant}.event.task", f"a2a.v1.{tenant}.dlq"
    return {
        "api": {"role": "api", "consumer": "tasks", "subscribe": None, "publish": [command]},
        "workflow": {"role": "workflow", "consumer": "tasks", "subscribe": command, "publish": [event, dlq]},
        "observer-a": {"role": "observer", "consumer": "observer-a", "subscribe": event, "publish": [dlq]},
        "observer-b": {"role": "observer", "consumer": "observer-b", "subscribe": event, "publish": [dlq]},
        "operator": {"role": "operator", "consumer": "tasks", "subscribe": dlq, "publish": [command, event]},
    }


def _broker_endpoint(endpoint, scheme):
    """Keep credentials/query data out of generated component ConfigMaps."""
    require(isinstance(endpoint, str) and endpoint
            and all(32 < ord(c) < 127 for c in endpoint), "BROKER_TLS_REQUIRED")
    try:
        parsed = urlsplit(endpoint)
        require(parsed.scheme == scheme and bool(parsed.hostname)
                and parsed.username is None and parsed.password is None
                and parsed.path in {"", "/"} and "?" not in endpoint and "#" not in endpoint,
                "BROKER_TLS_REQUIRED")
        require(parsed.port is None or 1 <= parsed.port <= 65535, "BROKER_TLS_REQUIRED")
        return parsed
    except ValueError:
        raise PermanentError("BROKER_TLS_REQUIRED") from None

def component(broker, name, *, tenant="demo", source="workflow-worker", endpoint=None, production=False):
    require(broker in {"jetstream", "rabbitmq"}, "INVALID_BROKER")
    roles = topology(tenant, source)
    require(name in roles, "INVALID_ROLE")
    role, appid = roles[name], "a2amq-" + name
    topics = sorted({t for r in roles.values() for t in r["publish"]})
    meta = [
        {"name": "allowedTopics", "value": ",".join(topics)},
        {"name": "protectedTopics", "value": ",".join(topics)},
        {"name": "publishingScopes", "value": appid + "=" + ",".join(role["publish"])},
        {"name": "subscriptionScopes", "value": appid + "=" + (role["subscribe"] or "")},
    ]
    def value(k, v):
        meta.append({"name": k, "value": str(v)})
    def secret(k, key):
        meta.append({"name": k, "secretKeyRef": {"name": appid + "-broker" if production else key, "key": key}})
    if broker == "jetstream":
        endpoint = endpoint or "nats://nats:4222"
        _broker_endpoint(endpoint, "tls" if production else "nats")
        value("natsURL", endpoint)
        if production:
            secret("jwt", "jwt")
            secret("seedKey", "seed")
        else:
            secret("token", "nats-token")
        value("name", appid)
        value("streamName", "A2A_MQ")
        if role["subscribe"]:
            value("durableName", f"a2a-{tenant}-{name}-v1")
            value("queueGroupName", f"a2a-{tenant}-{name}-v1")
            for k, v in {"ackPolicy": "explicit", "deliverPolicy": "all", "ackWait": "60s",
                         "maxDeliver": "-1", "maxAckPending": "32"}.items():
                value(k, v)
    else:
        endpoint = endpoint or "amqp://rabbitmq:5672"
        parsed = _broker_endpoint(endpoint, "amqps" if production else "amqp")
        value("protocol", parsed.scheme)
        value("hostname", parsed.netloc)
        if production:
            secret("username", "username")
        else:
            value("username", "a2amq")
        secret("password", "rabbit-password" if not production else "password")
        value("consumerID", appid)
        for k, v in {"durable": "true", "deletedWhenUnused": "false", "autoAck": "false",
                     "deliveryMode": "2", "publisherConfirm": "true", "requeueInFailure": "true",
                     "prefetchCount": "32", "exchangeKind": "fanout", "enableDeadLetter": "false"}.items():
            value(k, v)
        if production:
            secret("caCert", "ca.crt")
            secret("clientCert", "tls.crt")
            secret("clientKey", "tls.key")
    result = {"apiVersion": "dapr.io/v1alpha1", "kind": "Component", "metadata": {"name": "bus-" + name},
              "scopes": [appid], "spec": {"type": "pubsub." + broker, "version": "v1", "metadata": meta}}
    if not production:
        result["auth"] = {"secretStore": "local-secrets"}
    return result


def broker_plan(tenant="demo", source="workflow-worker"):
    roles = topology(tenant, source)
    topics = sorted({t for r in roles.values() for t in r["publish"]})
    queues = [{"name": "a2amq-" + name + "-v1", "topic": r["subscribe"], "durable": True,
               "arguments": {"x-queue-type": "quorum"}}
              for name, r in roles.items() if r["subscribe"]]
    parking = f"a2a.v1.{tenant}.parking"
    topics.append(parking)
    queues.append({"name": "a2amq-parking-v1", "topic": parking, "durable": True,
                   "arguments": {"x-queue-type": "quorum"}})
    policies = []
    for q in queues:
        definition = {"max-length": 1000000, "max-length-bytes": 1073741824, "overflow": "reject-publish"}
        if q["name"] != "a2amq-parking-v1":
            definition.update({"delivery-limit": 50, "dead-letter-strategy": "at-least-once",
                "dead-letter-exchange": parking if q["name"] == "a2amq-operator-v1" else f"a2a.v1.{tenant}.dlq"})
        policies.append({"name": q["name"], "pattern": "^" + re.escape(q["name"]) + "$",
                         "apply-to": "quorum_queues", "priority": 10, "definition": definition})
    return {"exchanges": [{"name": t, "type": "fanout", "durable": True} for t in topics], "queues": queues,
            "policies": policies,
            "stream": {"name": "A2A_MQ", "subjects": [f"a2a.v1.{tenant}.>"], "retention": "limits",
                "storage": "file", "num_replicas": 1, "max_age": 604800000000000,
                "max_bytes": 1073741824, "max_msgs": 1000000, "max_msg_size": 1048576, "discard": "new"}}


def _write(root, name, content, *, private=False):
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = content if isinstance(content, str) else json.dumps(content, indent=2) + "\n"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600 if private else 0o644)
    with os.fdopen(fd, "w") as stream:
        stream.write(raw)


def _local_keys(root):
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    signing = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "A2A-MQ disposable localhost")])
    now = datetime.now(timezone.utc)
    cert = x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key()) \
        .serial_number(x509.random_serial_number()).not_valid_before(now-timedelta(minutes=5)) \
        .not_valid_after(now+timedelta(days=7)) \
        .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost"),
                     x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), critical=False) \
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True).sign(key, hashes.SHA256())
    def pem(k):
        return k.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                               serialization.NoEncryption()).decode()
    _write(root, "secrets/tls.key", pem(key), private=True)
    _write(root, "secrets/tls.crt", cert.public_bytes(serialization.Encoding.PEM).decode())
    _write(root, "secrets/jwt-private.pem", pem(signing), private=True)
    _write(root, "keys.json", {"local": signing.public_key().public_bytes(serialization.Encoding.PEM,
                                               serialization.PublicFormat.SubjectPublicKeyInfo).decode()})


def compose(output: Path, *, broker: str, source_root: Path, images: dict | None = None):
    require(not output.exists(), "OUTPUT_ALREADY_EXISTS")
    images = {**IMAGES, **(images or {})}
    require(set(images) == set(IMAGES) and all(isinstance(v, str) and v and ":latest" not in v
            for v in images.values()), "INVALID_IMAGE_CONFIG")
    require((source_root / "deploy/Dockerfile.full").is_file(), "SOURCE_ROOT_REQUIRED")
    plan, roles = broker_plan(), topology()
    require(broker in {"jetstream", "rabbitmq"}, "INVALID_BROKER")
    output.mkdir(parents=True, mode=0o700)
    _local_keys(output)
    passwords = {k: secrets.token_hex(32) for k in ("postgres", "redis", "nats", "rabbit")}
    uid, gid = os.getuid(), os.getgid()
    _write(output, ".gitignore", "*\n")
    _write(output, "secrets/postgres-password", passwords["postgres"], private=True)
    _write(output, "secrets/postgres-dsn", f"host=postgres port=5432 dbname=a2amq user=a2amq password={passwords['postgres']} sslmode=disable", private=True)
    _write(output, "secrets/redis.conf", f"bind 0.0.0.0\nport 6379\nappendonly yes\nrequirepass {passwords['redis']}\ndir /data\n", private=True)
    _write(output, "secrets/redis.env", "REDISCLI_AUTH=" + passwords["redis"] + "\n", private=True)
    _write(output, "secrets/rabbit.env", "RABBITMQ_DEFAULT_USER=a2amq\nRABBITMQ_DEFAULT_PASS=" + passwords["rabbit"] + "\n", private=True)
    _write(output, "secrets/rabbit-password", passwords["rabbit"], private=True)
    _write(output, "secrets/nats.conf", "port: 4222\njetstream { store_dir: /data }\nauthorization { token: "
           + json.dumps(passwords["nats"]) + " }\n", private=True)
    _write(output, "secrets/nats.env", "NATS_TOKEN=" + passwords["nats"] + "\n", private=True)
    _write(output, "broker-plan.json", plan)
    _write(output, "stream.json", plan["stream"])
    app_image = "a2amq-implementation:local"
    services = {
        "postgres": {"image": images["postgres"], "environment": {"POSTGRES_DB": "a2amq", "POSTGRES_USER": "a2amq",
                     "POSTGRES_PASSWORD_FILE": "/run/secrets/password"},
                     "volumes": ["pg-data:/var/lib/postgresql/data", "./secrets/postgres-password:/run/secrets/password:ro"],
                     "healthcheck": {"test": ["CMD-SHELL", "pg_isready -U a2amq -d a2amq"], "interval": "2s", "timeout": "3s", "retries": 60}},
        "redis": {"image": images["redis"], "user": f"{uid}:{gid}", "entrypoint": ["redis-server"],
                  "command": ["/config/redis.conf"], "env_file": ["./secrets/redis.env"],
                  "volumes": ["redis-data:/data", "./secrets/redis.conf:/config/redis.conf:ro"]},
        "placement": {"image": images["control"], "command": ["./placement", "--port", "50005", "--log-level", "warn"]},
        "scheduler": {"image": images["control"], "command": ["./scheduler", "--port", "50006", "--etcd-data-dir", "/data"],
                      "volumes": ["scheduler-data:/data"]},
    }
    if broker == "jetstream":
        services["nats"] = {"image": images["nats"], "user": f"{uid}:{gid}", "entrypoint": ["nats-server"],
            "command": ["-c", "/config/nats.conf"], "volumes": ["nats-data:/data", "./secrets/nats.conf:/config/nats.conf:ro"]}
        _write(output, "secrets/nats-token", passwords["nats"], private=True)
        services["broker-init"] = {"image": app_image, "build": {"context": str(source_root.resolve()),
            "dockerfile": "deploy/Dockerfile.full", "args": {"PYTHON_IMAGE": images["python"]}},
            "user": f"{uid}:{gid}", "depends_on": ["nats"],
            "entrypoint": ["python", "-m", "a2a_mq.provision"],
            "command": ["--nats-url", "nats://nats:4222", "--password-file", "/run/secrets/password",
                        "--plan", "/config/plan.json", "--development"],
            "volumes": ["./secrets/nats-token:/run/secrets/password:ro", "./broker-plan.json:/config/plan.json:ro"]}
    else:
        services["rabbitmq"] = {"image": images["rabbitmq"], "env_file": ["./secrets/rabbit.env"],
                                 "volumes": ["rabbit-data:/var/lib/rabbitmq"]}
        services["broker-init"] = {"image": app_image, "build": {"context": str(source_root.resolve()),
            "dockerfile": "deploy/Dockerfile.full", "args": {"PYTHON_IMAGE": images["python"]}},
            "user": f"{uid}:{gid}", "depends_on": ["rabbitmq"],
            "entrypoint": ["python", "-m", "a2a_mq.provision"],
            "command": ["--rabbit-url", "http://rabbitmq:15672", "--password-file", "/run/secrets/password",
                        "--plan", "/config/plan.json", "--development"],
            "volumes": ["./secrets/rabbit-password:/run/secrets/password:ro", "./broker-plan.json:/config/plan.json:ro"]}
    services["volume-init"] = {"image": app_image, "build": {"context": str(source_root.resolve()),
        "dockerfile": "deploy/Dockerfile.full", "args": {"PYTHON_IMAGE": images["python"]}},
        "user": "0:0", "entrypoint": ["python", "-c"],
        "command": [f"import os; [os.chown(p,{uid},{gid}) for p in ('/redis','/nats','/scheduler')]"],
        "volumes": ["redis-data:/redis", "nats-data:/nats", "scheduler-data:/scheduler"]}
    for service in ("redis", "scheduler", "nats"):
        if service in services:
            services[service]["depends_on"] = {"volume-init": {"condition": "service_completed_successfully"}}
    ports = {"api": 18000, "workflow-a": 18011, "workflow-b": 18012, "observer-a": 18001, "observer-b": 18002, "operator": 18003}
    for name, role in roles.items():
        appid = "a2amq-" + name
        app_token, dapr_token = secrets.token_hex(32), secrets.token_hex(32)
        _write(output, f"secrets/{name}-app", app_token, private=True)
        _write(output, f"secrets/{name}-dapr", dapr_token, private=True)
        _write(output, f"secrets/{name}.env", f"APP_API_TOKEN={app_token}\nDAPR_API_TOKEN={dapr_token}\n", private=True)
        _write(output, f"secrets/{name}-dapr.json", {"redis-password": passwords["redis"],
               "rabbit-password": passwords["rabbit"], "nats-token": passwords["nats"]}, private=True)
        _write(output, f"components/{name}/bus.yaml", component(broker, name))
        _write(output, f"components/{name}/secrets.yaml", {"apiVersion": "dapr.io/v1alpha1", "kind": "Component",
            "metadata": {"name": "local-secrets"}, "scopes": [appid],
            "spec": {"type": "secretstores.local.file", "version": "v1",
                "metadata": [{"name": "secretsFile", "value": "/run/secrets/dapr.json"}]}})
        _write(output, f"components/{name}/resiliency.yaml", {"apiVersion": "dapr.io/v1alpha1", "kind": "Resiliency",
            "metadata": {"name": "retry-" + name}, "scopes": [appid], "spec": {
                "policies": {"retries": {"bounded": {"policy": "constant", "duration": "1s", "maxRetries": 5}}},
                "targets": {"components": {"bus-" + name: {"inbound": {"retry": "bounded"}}}}}})
        if name == "workflow":
            _write(output, f"components/{name}/state.yaml", {"apiVersion": "dapr.io/v1alpha1", "kind": "Component",
                "metadata": {"name": "workflow-state"}, "scopes": [appid], "auth": {"secretStore": "local-secrets"},
                "spec": {"type": "state.redis", "version": "v1", "metadata": [
                    {"name": "redisHost", "value": "redis:6379"}, {"name": "redisPassword", "secretKeyRef": {
                        "name": "redis-password", "key": "redis-password"}}, {"name": "actorStateStore", "value": "true"}]}})
        for instance in (["workflow-a", "workflow-b"] if name == "workflow" else [name]):
            env = {"A2A_ROLE": role["role"], "A2A_TENANT": "demo", "A2A_CONSUMER": role["consumer"],
                "A2A_SOURCE": "workflow-worker", "A2A_COMPONENT": "bus-" + name,
                "A2A_POSTGRES_DSN_FILE": "/run/secrets/postgres-dsn", "A2A_DEVELOPMENT": "1",
                "APP_API_TOKEN_FILE": "/run/secrets/app", "DAPR_API_TOKEN_FILE": "/run/secrets/dapr",
                "A2A_PUBLIC_URL": "https://localhost:18443", "A2A_JWT_ISSUER": "https://local-idp.example",
                "A2A_JWT_KEYS_FILE": "/config/keys.json", "A2A_ENABLE_MCP": "1" if name == "api" else "0"}
            if broker == "rabbitmq" and role["subscribe"]:
                env["A2A_SUBSCRIPTION_QUEUE"] = appid + "-v1"
            services[instance] = {"image": app_image, "build": {"context": str(source_root.resolve()),
                "dockerfile": "deploy/Dockerfile.full", "args": {"PYTHON_IMAGE": images["python"]}},
                "user": f"{uid}:{gid}", "init": True, "environment": env,
                "depends_on": {"postgres": {"condition": "service_healthy"},
                               "broker-init": {"condition": "service_completed_successfully"}},
                "ports": [f"127.0.0.1:{ports[instance]}:8000"], "read_only": True,
                "tmpfs": ["/tmp"], "cap_drop": ["ALL"], "security_opt": ["no-new-privileges:true"],
                "volumes": [f"./secrets/{name}-app:/run/secrets/app:ro", f"./secrets/{name}-dapr:/run/secrets/dapr:ro",
                    "./secrets/postgres-dsn:/run/secrets/postgres-dsn:ro", "./keys.json:/config/keys.json:ro"],
                "stop_grace_period": "90s"}
            services[instance + "-dapr"] = {"build": {"context": str(source_root.resolve()),
                "dockerfile": "deploy/Dockerfile.sidecar", "args": {"DAPR_IMAGE": images["dapr"]}},
                "image": "a2amq-sidecar:local", "network_mode": "service:" + instance,
                "env_file": [f"./secrets/{name}.env"],
                "depends_on": {instance: {"condition": "service_started"}, "placement": {"condition": "service_started"},
                               "scheduler": {"condition": "service_started"}, "redis": {"condition": "service_started"}},
                "volumes": [f"./components/{name}:/components:ro", f"./secrets/{name}-dapr.json:/run/secrets/dapr.json:ro"],
                "command": ["--app-id", appid, "--app-port", "8000", "--app-protocol", "http",
                    "--dapr-http-port", "3500", "--dapr-grpc-port", "50001", "--resources-path", "/components",
                    "--dapr-listen-addresses", "127.0.0.1", "--placement-host-address", "placement:50005",
                    "--scheduler-host-address", "scheduler:50006", "--log-level", "warn", "--app-max-concurrency", "8"]}
    _write(output, "nginx.conf", '''events {}
http { access_log off; server { listen 8443 ssl; server_name localhost;
ssl_certificate /certs/tls.crt; ssl_certificate_key /certs/tls.key;
client_max_body_size 512k; client_body_timeout 10s;
location / { proxy_pass http://api:8000; proxy_http_version 1.1; proxy_set_header Host $http_host;
proxy_buffering off; proxy_read_timeout 3700s; proxy_set_header Connection ""; }
} }
''')
    services["gateway"] = {"image": images["nginx"], "ports": ["127.0.0.1:18443:8443"], "depends_on": ["api"],
        "volumes": ["./nginx.conf:/etc/nginx/nginx.conf:ro", "./secrets/tls.crt:/certs/tls.crt:ro", "./secrets/tls.key:/certs/tls.key:ro"]}
    _write(output, "compose.yaml", {"name": "a2amq-full-" + broker, "services": services,
        "volumes": {v: {} for v in ("pg-data", "redis-data", "scheduler-data", "nats-data", "rabbit-data")}})
    _write(output, "manifest.json", {"broker": broker, "profile": "disposable-local-not-production", "images": images,
        "ownerUid": uid, "publicUrl": "https://localhost:18443", "source": str(source_root.resolve()),
        "roles": roles, "ports": ports, "requiresLiveValidation": True})
    return output / "compose.yaml"


K8S_DNS_LABEL = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
K8S_DNS_HOST = re.compile(
    r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*$"
)


def _k8s_label(value: str) -> str:
    require(isinstance(value, str) and K8S_DNS_LABEL.fullmatch(value) is not None,
            "INVALID_KUBERNETES_NAME")
    return value


def _ingress_host(value: str | None) -> str:
    require(isinstance(value, str) and 0 < len(value) <= 253
            and K8S_DNS_HOST.fullmatch(value) is not None, "INVALID_INGRESS_HOST")
    try:
        ipaddress.ip_address(value)
    except ValueError:
        return value
    raise PermanentError("INVALID_INGRESS_HOST")


def _redis_hosts(value: str) -> str:
    require(isinstance(value, str) and 0 < len(value) <= 4096
            and not any(c.isspace() for c in value), "INVALID_REDIS_HOST")
    hosts = value.split(",")
    require(1 <= len(hosts) <= 32 and all(hosts), "INVALID_REDIS_HOST")
    try:
        for entry in hosts:
            parsed = urlsplit("//" + entry)
            require(bool(parsed.hostname) and parsed.port is not None
                    and 1 <= parsed.port <= 65535
                    and parsed.username is None and parsed.password is None
                    and parsed.path == "" and not parsed.query and not parsed.fragment,
                    "INVALID_REDIS_HOST")
    except ValueError:
        raise PermanentError("INVALID_REDIS_HOST") from None
    return value


def kubernetes(config: dict) -> list[dict]:
    """App-side production resources. Secrets/infrastructure are deliberately external."""
    required = {"namespace", "image", "broker", "brokerEndpoint", "publicUrl", "issuer", "redisHost", "egressCIDRs"}
    require(isinstance(config, dict) and required <= set(config) <= required | {"cryptoConfigMap", "otlpEndpoint"}, "INVALID_PRODUCTION_CONFIG")
    namespace = _k8s_label(config["namespace"])
    require(isinstance(config["image"], str)
            and re.fullmatch(r"[^\s]+@sha256:[0-9a-f]{64}", config["image"]) is not None,
            "DIGEST_PINNED_IMAGE_REQUIRED")
    public = https_origin(config["publicUrl"])
    https_issuer(config["issuer"])
    require(isinstance(config["egressCIDRs"], list) and config["egressCIDRs"]
            and len(config["egressCIDRs"]) <= 64
            and all(isinstance(c, str) for c in config["egressCIDRs"]),
            "EXPLICIT_EGRESS_REQUIRED")
    try:
        networks = [ipaddress.ip_network(c) for c in config["egressCIDRs"]]
    except (TypeError, ValueError):
        raise PermanentError("EXPLICIT_EGRESS_REQUIRED") from None
    require(all(n.prefixlen > 0 for n in networks), "EXPLICIT_EGRESS_REQUIRED")
    redis_host = _redis_hosts(config["redisHost"])
    redis_ports = {urlsplit("//" + entry).port or 6379 for entry in redis_host.split(",")}
    broker = _broker_endpoint(
        config["brokerEndpoint"], "tls" if config["broker"] == "jetstream" else "amqps"
    )
    broker_port = broker.port or (4222 if config["broker"] == "jetstream" else 5671)
    egress_ports = {80, 443, 5432, 6379, 4222, 5671, 50001, 50002, 50005, 50006,
                    broker_port, *redis_ports}
    docs, roles = [{"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": namespace}}], topology(namespace)
    host = _ingress_host(urlsplit(public).hostname)
    for name, role in roles.items():
        appid = "a2amq-" + name
        bus = component(config["broker"], name, tenant=namespace, endpoint=config["brokerEndpoint"], production=True)
        bus["metadata"]["namespace"] = namespace
        docs.append(bus)
        labels = {"app": appid, "part-of": "a2amq"}
        docs.append({"apiVersion": "v1", "kind": "ServiceAccount",
                     "metadata": {"name": appid, "namespace": namespace},
                     "automountServiceAccountToken": False})
        projected = [
            {"secret": {"name": appid + "-app-token", "items": [{"key": "token", "path": "app"}]}},
            {"secret": {"name": appid + "-dapr-token", "items": [{"key": "token", "path": "dapr"}]}},
            {"secret": {"name": "a2amq-database", "items": [{"key": "dsn", "path": "postgres-dsn"}]}},
            {"configMap": {"name": "a2amq-identity", "items": [{"key": "keys.json", "path": "keys.json"}]}}]
        copied_names = ["app", "dapr", "postgres-dsn", "keys.json"]
        if config.get("cryptoConfigMap"):
            _k8s_label(config["cryptoConfigMap"])
            projected.extend([
                {"secret": {"name": appid + "-vault-token", "items": [{"key": "token", "path": "vault-token"}]}},
                {"configMap": {"name": config["cryptoConfigMap"], "items": [{"key": "crypto.json", "path": "crypto.json"}]}}])
            copied_names += ["vault-token", "crypto.json"]
        annotations = {"dapr.io/enabled": "true", "dapr.io/app-id": appid, "dapr.io/app-port": "8000",
            "dapr.io/app-token-secret": appid + "-app-token", "dapr.io/api-token-secret": appid + "-dapr-token",
            "dapr.io/disable-builtin-k8s-secret-store": "true",
            "dapr.io/config": "a2amq-secure", "dapr.io/sidecar-listen-addresses": "127.0.0.1",
            "dapr.io/app-max-concurrency": "8", "dapr.io/sidecar-cpu-request": "100m",
            "dapr.io/sidecar-memory-request": "128Mi", "dapr.io/sidecar-cpu-limit": "1",
            "dapr.io/sidecar-memory-limit": "512Mi", "dapr.io/volume-mounts-ro": "broker-ca:/broker-ca",
            "dapr.io/env": "SSL_CERT_FILE=/broker-ca/ca.crt"}
        env = {"A2A_ROLE": role["role"], "A2A_TENANT": namespace, "A2A_CONSUMER": role["consumer"],
            "A2A_COMPONENT": "bus-" + name, "A2A_PUBLIC_URL": public, "A2A_JWT_ISSUER": config["issuer"],
            "A2A_JWT_KEYS_FILE": "/credentials/keys.json", "A2A_POSTGRES_DSN_FILE": "/credentials/postgres-dsn",
            "APP_API_TOKEN_FILE": "/credentials/app", "DAPR_API_TOKEN_FILE": "/credentials/dapr",
            "A2A_ENABLE_MCP": "1" if name == "api" else "0"}
        if config.get("cryptoConfigMap"):
            env["A2A_CRYPTO_CONFIG"] = "/credentials/crypto.json"
        if config.get("otlpEndpoint"):
            otlp = https_origin(config["otlpEndpoint"], "OTLP_HTTPS_REQUIRED")
            env["A2A_OTLP_ENDPOINT"] = otlp
            env["A2A_OTLP_CA_FILE"] = "/broker-ca/ca.crt"
            egress_ports.add(urlsplit(otlp).port or 443)
        if config["broker"] == "rabbitmq" and role["subscribe"]:
            env["A2A_SUBSCRIPTION_QUEUE"] = appid + "-v1"
        security = {"runAsNonRoot": True, "runAsUser": 10001, "runAsGroup": 10001,
                    "allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}}
        copy = "from pathlib import Path; import os; " \
               "[(Path('/credentials',n).write_bytes(Path('/projected',n).read_bytes())," \
               "os.chmod('/credentials/'+n,0o600)) for n in " + repr(tuple(copied_names)) + "]"
        container = {"name": "app", "image": config["image"], "securityContext": security,
            "ports": [{"containerPort": 8000, "name": "http"}], "env": [{"name": k, "value": v} for k, v in env.items()],
            "resources": {"requests": {"cpu": "100m", "memory": "256Mi"}, "limits": {"cpu": "2", "memory": "1Gi"}},
            "volumeMounts": [{"name": "credentials", "mountPath": "/credentials", "readOnly": True},
                             {"name": "tmp", "mountPath": "/tmp"}, {"name": "broker-ca", "mountPath": "/broker-ca", "readOnly": True}],
            "livenessProbe": {"httpGet": {"path": "/health/live", "port": "http"}, "initialDelaySeconds": 40},
            "readinessProbe": {"exec": {"command": ["python", "-m", "a2a_mq.provision", "--readiness"]},
                               "periodSeconds": 10, "timeoutSeconds": 10},
            "startupProbe": {"httpGet": {"path": "/health/live", "port": "http"}, "failureThreshold": 30, "periodSeconds": 5}}
        docs.append({"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": appid, "namespace": namespace},
            "spec": {"replicas": 2 if name in {"api", "workflow"} else 1, "selector": {"matchLabels": labels},
                "template": {"metadata": {"labels": labels, "annotations": annotations}, "spec": {
                    "serviceAccountName": appid, "automountServiceAccountToken": False,
                    "terminationGracePeriodSeconds": 90,
                    "securityContext": {"fsGroup": 10001, "seccompProfile": {"type": "RuntimeDefault"}},
                    "initContainers": [{"name": "copy-credentials", "image": config["image"], "securityContext": security,
                        "command": ["python", "-c", copy], "volumeMounts": [
                            {"name": "projected", "mountPath": "/projected", "readOnly": True},
                            {"name": "credentials", "mountPath": "/credentials"}]}],
                    "containers": [container], "volumes": [
                        {"name": "projected", "projected": {"defaultMode": 288, "sources": projected}},
                        {"name": "credentials", "emptyDir": {"medium": "Memory", "sizeLimit": "1Mi"}},
                        {"name": "tmp", "emptyDir": {"sizeLimit": "64Mi"}},
                        {"name": "broker-ca", "configMap": {"name": "a2amq-ca-bundle"}}],
                    "topologySpreadConstraints": [{"maxSkew": 1, "topologyKey": "kubernetes.io/hostname",
                        "whenUnsatisfiable": "ScheduleAnyway", "labelSelector": {"matchLabels": labels}}]}}}})
        docs.append({"apiVersion": "v1", "kind": "Service", "metadata": {"name": appid, "namespace": namespace},
                     "spec": {"selector": labels, "ports": [{"port": 8000, "targetPort": "http"}]}})
        if name in {"api", "workflow"}:
            docs.append({"apiVersion": "policy/v1", "kind": "PodDisruptionBudget", "metadata": {"name": appid, "namespace": namespace},
                         "spec": {"minAvailable": 1, "selector": {"matchLabels": labels}}})
    docs.append({"apiVersion": "dapr.io/v1alpha1", "kind": "Configuration", "metadata": {"name": "a2amq-secure", "namespace": namespace},
                 "spec": {"mtls": {"enabled": True, "workloadCertTTL": "24h", "allowedClockSkew": "15m"}}})
    docs.append({"apiVersion": "dapr.io/v1alpha1", "kind": "Component", "metadata": {"name": "workflow-state", "namespace": namespace},
        "scopes": ["a2amq-workflow"], "spec": {"type": "state.redis", "version": "v1", "metadata": [
            {"name": "redisHost", "value": redis_host}, {"name": "enableTLS", "value": "true"},
            {"name": "actorStateStore", "value": "true"}, {"name": "redisPassword", "secretKeyRef": {
                "name": "a2amq-workflow-state", "key": "password"}}]}})
    docs.append({"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy", "metadata": {"name": "a2amq-boundaries", "namespace": namespace},
        "spec": {"podSelector": {"matchLabels": {"part-of": "a2amq"}}, "policyTypes": ["Ingress", "Egress"],
            "ingress": [{"from": [{"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": namespace}}}],
                         "ports": [{"protocol": "TCP", "port": 50002}]},
                        {"from": [{"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "ingress-nginx"}}}],
                         "ports": [{"protocol": "TCP", "port": 8000}]}],
            "egress": [{"to": [{"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "kube-system"}}}],
                        "ports": [{"protocol": "UDP", "port": 53}, {"protocol": "TCP", "port": 53}]},
                       {"to": [{"ipBlock": {"cidr": str(n)}} for n in networks],
                        "ports": [{"protocol": "TCP", "port": p} for p in sorted(egress_ports)]}]}})
    docs.append({"apiVersion": "networking.k8s.io/v1", "kind": "Ingress", "metadata": {"name": "a2amq", "namespace": namespace,
        "annotations": {"nginx.ingress.kubernetes.io/ssl-redirect": "true", "nginx.ingress.kubernetes.io/proxy-buffering": "off",
                        "nginx.ingress.kubernetes.io/proxy-read-timeout": "3700", "nginx.ingress.kubernetes.io/enable-access-log": "false"}},
        "spec": {"ingressClassName": "nginx", "tls": [{"hosts": [host], "secretName": "a2amq-ingress-tls"}],
            "rules": [{"host": host, "http": {"paths": [{"path": "/", "pathType": "Prefix", "backend": {
                "service": {"name": "a2amq-api", "port": {"number": 8000}}}}]}}]}})
    return docs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="profile", required=True)
    local = sub.add_parser("compose")
    local.add_argument("--output", type=Path, required=True)
    local.add_argument("--broker", choices=["jetstream", "rabbitmq"], required=True)
    local.add_argument("--source", type=Path, default=Path.cwd())
    local.add_argument("--images", type=Path)
    prod = sub.add_parser("kubernetes")
    prod.add_argument("--config", type=Path, required=True)
    prod.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.profile == "compose":
        print(compose(args.output, broker=args.broker, source_root=args.source,
                      images=json.loads(args.images.read_text()) if args.images else None))
    else:
        values = kubernetes(json.loads(args.config.read_text()))
        require(not args.output.exists(), "OUTPUT_ALREADY_EXISTS")
        with args.output.open("x") as file:
            json.dump({"apiVersion": "v1", "kind": "List", "items": values}, file, indent=2)


if __name__ == "__main__":
    main()
