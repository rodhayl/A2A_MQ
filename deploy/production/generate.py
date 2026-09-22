#!/usr/bin/env python3
"""Generate secret-free Kubernetes/Dapr manifests for the three production roles."""
from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
import re
from urllib.parse import urlsplit

from a2a_mq.broker_profiles import assert_portability_contract, platform_profiles
from a2a_mq.domain import require, token

IMAGE = re.compile(r"^[A-Za-z0-9._/:@-]+@sha256:[0-9a-f]{64}$")


def https_url(value: str) -> str:
    parsed = urlsplit(value)
    require(parsed.scheme == "https" and bool(parsed.hostname) and not parsed.username
            and not parsed.password and not parsed.query and not parsed.fragment,
            "PUBLIC_HTTPS_URL_REQUIRED")
    return value.rstrip("/")


def env(name: str, value: str) -> dict:
    return {"name": name, "value": value}


def secret_env(name: str, secret: str, key: str = "token") -> dict:
    return {"name": name, "valueFrom": {"secretKeyRef": {"name": secret, "key": key}}}


def deployment(*, namespace: str, role: str, app_id: str, image: str,
               tenant: str, component: str, factory: str, replicas: int,
               public_url: str, issuer: str, jwt_kid: str,
               incoming_dapr: bool) -> dict:
    annotations = {
        "dapr.io/enabled": "true",
        "dapr.io/app-id": app_id,
        "dapr.io/api-token-secret": "dapr-api-token",
        "dapr.io/disable-builtin-k8s-secret-store": "true",
        "dapr.io/log-as-json": "true",
    }
    if incoming_dapr:
        annotations.update({"dapr.io/app-port": "8000", "dapr.io/app-token-secret": "app-api-token"})
    variables = [
        env("A2A_TENANT", tenant),
        env("A2A_PUBSUB_COMPONENT", component),
        secret_env("A2A_POSTGRES_DSN", "a2amq-postgres", "dsn"),
        env("DAPR_HTTP_ENDPOINT", "http://127.0.0.1:3500"),
        secret_env("DAPR_API_TOKEN", "dapr-api-token"),
    ]
    if role == "api":
        variables += [
            env("A2A_CONSUMER", "workflow-worker"),
            env("A2A_DLQ_CONSUMER", "operations"),
            env("A2A_PUBLIC_URL", public_url),
            env("A2A_MCP_RESOURCE_URL", public_url + "/mcp"),
            env("A2A_JWT_KID", jwt_kid),
            env("A2A_JWT_ISSUER", issuer),
            env("A2A_JWT_PUBLIC_KEY_FILE", "/etc/a2amq/jwt/public.pem"),
        ]
    elif role == "worker":
        variables += [env("A2A_CONSUMER", "workflow-worker"),
                      env("A2A_SOURCE", "workflow-worker"),
                      secret_env("APP_API_TOKEN", "app-api-token")]
    else:
        variables += [env("A2A_DLQ_CONSUMER", "operations"),
                      secret_env("APP_API_TOKEN", "app-api-token")]
    volumes: list[dict] = []
    mounts: list[dict] = []
    if role == "api":
        volumes.append({"name": "jwt", "configMap": {"name": "a2amq-jwt-public-key",
                                                         "items": [{"key": "public.pem", "path": "public.pem"}]}})
        mounts.append({"name": "jwt", "mountPath": "/etc/a2amq/jwt", "readOnly": True})
    command = ["python", "-m", "uvicorn", factory, "--factory", "--host", "0.0.0.0",
               "--port", "8000", "--no-access-log", "--limit-concurrency", "256"]
    probe = {"httpGet": {"path": "/health/ready", "port": 8000},
             "periodSeconds": 10, "timeoutSeconds": 3, "failureThreshold": 6}
    live = {"httpGet": {"path": "/health/live", "port": 8000},
            "periodSeconds": 20, "timeoutSeconds": 3, "failureThreshold": 3}
    return {
        "apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": {"name": app_id, "namespace": namespace,
                     "labels": {"app.kubernetes.io/name": "a2a-mq", "app.kubernetes.io/component": role}},
        "spec": {"replicas": replicas, "selector": {"matchLabels": {"app": app_id}},
                 "template": {"metadata": {"labels": {"app": app_id}, "annotations": annotations},
                              "spec": {"automountServiceAccountToken": False,
                                       "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}},
                                       "containers": [{"name": "app", "image": image,
                                                       "imagePullPolicy": "IfNotPresent", "command": command,
                                                       "ports": [{"containerPort": 8000, "name": "http"}],
                                                       "env": variables, "volumeMounts": mounts,
                                                       "readinessProbe": probe, "livenessProbe": live,
                                                       "securityContext": {"runAsNonRoot": True, "runAsUser": 10001,
                                                                           "allowPrivilegeEscalation": False,
                                                                           "readOnlyRootFilesystem": True,
                                                                           "capabilities": {"drop": ["ALL"]}},
                                                       "resources": {"requests": {"cpu": "100m", "memory": "128Mi"},
                                                                     "limits": {"cpu": "1", "memory": "512Mi"}}}],
                                       "volumes": volumes}}}
    }


def generate(*, namespace: str, tenant: str, broker: str, image: str,
             public_url: str, issuer: str, jwt_kid: str, public_key: str) -> dict:
    namespace, tenant, jwt_kid = map(token, (namespace, tenant, jwt_kid))
    require(IMAGE.fullmatch(image) is not None, "IMAGE_DIGEST_REQUIRED")
    public_url, issuer = https_url(public_url), https_url(issuer)
    require("BEGIN PUBLIC KEY" in public_key and "PRIVATE KEY" not in public_key
            and len(public_key) <= 65536, "INVALID_IDENTITY_KEY")
    profiles = platform_profiles(broker=broker, tenant=tenant)
    items: list[dict] = []
    for profile in profiles.values():
        assert_portability_contract(profile)
        component = copy.deepcopy(profile.component)
        component["metadata"]["namespace"] = namespace
        items.append(component)
    items.append({
        "apiVersion": "dapr.io/v1alpha1", "kind": "Component",
        "metadata": {"name": "workflow-state", "namespace": namespace},
        "spec": {"type": "state.postgresql", "version": "v2", "metadata": [
            {"name": "connectionString", "secretKeyRef": {"name": "a2amq-workflow-state",
                                                               "key": "connection-string"}},
            {"name": "actorStateStore", "value": "true"},
            {"name": "tablePrefix", "value": "a2amq_workflow_"},
        ]},
        "scopes": ["a2amq-worker"],
    })
    items.append({"apiVersion": "v1", "kind": "ConfigMap",
                  "metadata": {"name": "a2amq-jwt-public-key", "namespace": namespace},
                  "data": {"public.pem": public_key}})
    roles = {
        "api": ("a2amq-api", "a2a_mq.production_env:create_api_from_env", 2, False),
        "worker": ("a2amq-worker", "a2a_mq.production_env:create_worker_from_env", 2, True),
        "dlq": ("a2amq-dlq", "a2a_mq.production_env:create_dlq_from_env", 2, True),
    }
    for role, (app_id, factory, replicas, incoming) in roles.items():
        items.append(deployment(namespace=namespace, role=role, app_id=app_id, image=image,
                                tenant=tenant, component=profiles[role].component_name,
                                factory=factory, replicas=replicas, public_url=public_url,
                                issuer=issuer, jwt_kid=jwt_kid, incoming_dapr=incoming))
        if replicas > 1:
            items.append({"apiVersion": "policy/v1", "kind": "PodDisruptionBudget",
                          "metadata": {"name": app_id, "namespace": namespace},
                          "spec": {"minAvailable": 1, "selector": {"matchLabels": {"app": app_id}}}})
    items.append({"apiVersion": "v1", "kind": "Service",
                  "metadata": {"name": "a2amq-api", "namespace": namespace},
                  "spec": {"selector": {"app": "a2amq-api"},
                           "ports": [{"name": "http", "port": 80, "targetPort": 8000}]}})
    return {"apiVersion": "v1", "kind": "List", "items": items}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", default="a2amq")
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--broker", choices=("jetstream", "rabbitmq"), required=True)
    parser.add_argument("--image", required=True, help="immutable image reference ending in @sha256:<64 hex>")
    parser.add_argument("--public-url", required=True)
    parser.add_argument("--issuer", required=True)
    parser.add_argument("--jwt-kid", required=True)
    parser.add_argument("--jwt-public-key", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    require(not args.output.exists() and args.output.parent.is_dir(), "INVALID_OUTPUT")
    public_key = args.jwt_public_key.read_text(encoding="ascii")
    manifest = generate(namespace=args.namespace, tenant=args.tenant, broker=args.broker,
                        image=args.image, public_url=args.public_url, issuer=args.issuer,
                        jwt_kid=args.jwt_kid, public_key=public_key)
    args.output.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
