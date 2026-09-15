"""Generate the disposable, loopback-only Compose fixture using only stdlib."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
TENANT = "demo"
COMMAND = "a2a.v1.demo.command.skill.echo"
EVENT = "a2a.v1.demo.event.task"
DLQ = "a2a.v1.demo.dlq"


def render() -> dict[str, dict]:
    outputs: dict[str, dict] = {}
    stream = {"name": "A2A_MQ", "subjects": ["a2a.v1.demo.>"], "retention": "limits",
              "storage": "file", "num_replicas": 1, "max_age": 86400000000000,
              "max_bytes": 268435456, "max_msgs": 100000, "max_msg_size": 1048576, "discard": "new"}
    outputs["generated/stream.json"] = stream
    roles = {"producer": (None, [COMMAND]), "worker-echo": (COMMAND, [EVENT, DLQ]),
             "observer-a": (EVENT, [DLQ]), "observer-b": (EVENT, [DLQ])}
    for role, (topic, publishes) in roles.items():
        metadata = {"natsURL": "nats://nats:4222", "name": role, "streamName": "A2A_MQ",
                    "publishingScopes": role + "=" + ",".join(publishes),
                    "subscriptionScopes": role + "=" + (topic or "")}
        if topic:
            metadata.update(durableName="a2a-demo-" + role + "-v1", queueGroupName="a2a-demo-" + role + "-v1",
                            ackPolicy="explicit", deliverPolicy="all", ackWait="60s", maxDeliver="20", maxAckPending="32")
        outputs[f"generated/{role}/bus.yaml"] = {
            "apiVersion": "dapr.io/v1alpha1", "kind": "Component", "metadata": {"name": "bus"},
            "scopes": [role], "spec": {"type": "pubsub.jetstream", "version": "v1",
                                       "metadata": [{"name": k, "value": v} for k, v in metadata.items()]}}
        outputs[f"generated/{role}/resiliency.yaml"] = {
            "apiVersion": "dapr.io/v1alpha1", "kind": "Resiliency", "metadata": {"name": "bus-retries"},
            "scopes": [role], "spec": {"policies": {"retries": {"bounded": {
                "policy": "constant", "duration": "1s", "maxRetries": 20}}},
                "targets": {"components": {"bus": {"inbound": {"retry": "bounded"}}}}}}
    services = {
        "nats": {"image": "nats:2.14.6-alpine", "command": ["-js", "-sd", "/data", "-m", "8222"],
                 "ports": ["127.0.0.1:4222:4222", "127.0.0.1:8222:8222"], "volumes": ["nats-data:/data"]},
        "init": {"image": "natsio/nats-box:0.19.0", "depends_on": ["nats"], "entrypoint": ["/bin/sh", "-ec"],
                 "volumes": ["./generated/stream.json:/config/stream.json:ro"],
                 "command": ["i=0; until nats -s nats://nats:4222 account info >/dev/null 2>&1; do "
                             "i=$$((i+1)); [ $$i -lt 30 ] || exit 1; sleep 1; done; "
                             "nats -s nats://nats:4222 stream info A2A_MQ >/dev/null 2>&1 || "
                             "nats -s nats://nats:4222 stream add --config /config/stream.json"]},
    }
    volumes = {"nats-data": {}, "worker-state": {}, "observer-a-state": {}, "observer-b-state": {}, "producer-state": {}}
    apps = {"producer": ("producer", 18000), "worker-a": ("worker-echo", 18011),
            "worker-b": ("worker-echo", 18012), "observer-a": ("observer-a", 18001), "observer-b": ("observer-b", 18002)}
    for name, (identity, port) in apps.items():
        kind = "worker" if name.startswith("worker") else "observer" if name.startswith("observer") else "producer"
        volume = "worker-state" if kind == "worker" else name + "-state"
        services[name] = {
            "build": {"context": "..", "dockerfile": "deploy/Dockerfile"}, "image": "a2a-mq:local",
            "environment": {"A2A_ROLE": kind, "A2A_TENANT": TENANT, "A2A_CONSUMER": identity,
                            "A2A_DATABASE": "/state/tasks.sqlite"},
            "ports": [f"127.0.0.1:{port}:8000"] + (["127.0.0.1:3500:3500"] if name == "producer" else []),
            "volumes": [volume + ":/state"],
            "healthcheck": {"test": ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health/live', timeout=2)"],
                            "interval": "2s", "timeout": "3s", "retries": 30},
        }
        services[name + "-dapr"] = {
            "image": "daprio/daprd:1.18.2", "network_mode": "service:" + name,
            "depends_on": {name: {"condition": "service_healthy"}, "init": {"condition": "service_completed_successfully"}},
            "volumes": [f"./generated/{identity}:/components:ro"],
            "command": ["./daprd", "--app-id", identity, "--app-port", "8000", "--app-protocol", "http",
                        "--dapr-http-port", "3500", "--resources-path", "/components", "--log-level", "warn",
                        "--dapr-listen-addresses", "0.0.0.0", "--app-max-concurrency", "4"],
        }
    outputs["compose.yaml"] = {"name": "a2a-mq-local", "services": services, "volumes": volumes}
    return outputs


def generate(root: Path = ROOT) -> None:
    for relative, value in render().items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, indent=2) + "\n")


if __name__ == "__main__":
    generate()
