"""Production-oriented Dapr Pub/Sub component generators for supported brokers.

They make backend-specific durability/consumer semantics explicit. Generating a
component is not evidence that the broker has passed the live parity matrix.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .domain import PermanentError, require, token


def _positive(value: int, code: str, maximum: int) -> int:
    require(type(value) is int and 1 <= value <= maximum, code)
    return value


def _secret(name: str, key: str) -> dict[str, Any]:
    return {"secretKeyRef": {"name": token(name), "key": token(key)}}


def _metadata(items: list[tuple[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for name, value in items:
        entry = {"name": name}
        if isinstance(value, dict) and "secretKeyRef" in value:
            entry.update(value)
        else:
            entry["value"] = str(value).lower() if isinstance(value, bool) else str(value)
        result.append(entry)
    return result


@dataclass(frozen=True)
class BrokerProfile:
    broker: str
    component: dict[str, Any]
    consumer: str
    app_ids: tuple[str, ...]

    @property
    def component_name(self) -> str:
        return self.component["metadata"]["name"]


def rabbitmq_component(*, component: str, consumer: str, app_ids: tuple[str, ...],
                       secret: str = "rabbitmq", prefetch: int = 32) -> BrokerProfile:
    """RabbitMQ: durable queues, manual ACK, persistent publish, confirms and DLQ."""
    component, consumer, secret = token(component), token(consumer), token(secret)
    require(app_ids and all(token(app) for app in app_ids), "INVALID_ROUTE")
    prefetch = _positive(prefetch, "INVALID_PREFETCH", 65535)
    metadata = _metadata([
        ("host", _secret(secret, "host")), ("consumerID", consumer),
        ("durable", True), ("deletedWhenUnused", False), ("autoAck", False),
        ("deliveryMode", 2), ("requeueInFailure", True), ("prefetchCount", prefetch),
        ("concurrencyMode", "parallel"), ("publisherConfirm", True),
        ("enableDeadLetter", True), ("caCert", _secret(secret, "ca-cert")),
        ("clientCert", _secret(secret, "client-cert")), ("clientKey", _secret(secret, "client-key")),
    ])
    body = {"apiVersion": "dapr.io/v1alpha1", "kind": "Component", "metadata": {"name": component},
            "spec": {"type": "pubsub.rabbitmq", "version": "v1", "metadata": metadata},
            "scopes": list(app_ids)}
    return BrokerProfile("rabbitmq", body, consumer, app_ids)


def jetstream_component(*, component: str, consumer: str, app_ids: tuple[str, ...],
                        stream: str = "a2a", secret: str = "nats",
                        max_ack_pending: int = 256, replicas: int = 3) -> BrokerProfile:
    """JetStream: explicit durable/queue identity with retained all-delivery semantics."""
    component, consumer, stream, secret = map(token, (component, consumer, stream, secret))
    require(app_ids and all(token(app) for app in app_ids), "INVALID_ROUTE")
    max_ack_pending = _positive(max_ack_pending, "INVALID_BACKPRESSURE", 1000000)
    replicas = _positive(replicas, "INVALID_REPLICAS", 5)
    metadata = _metadata([
        ("natsURL", _secret(secret, "url")), ("token", _secret(secret, "token")),
        ("streamName", stream), ("durableName", consumer), ("queueGroupName", consumer),
        ("ackPolicy", "explicit"), ("deliverPolicy", "all"), ("ackWait", "30s"),
        ("maxDeliver", 10), ("backOff", "1s,5s,30s,2m"),
        ("maxAckPending", max_ack_pending), ("replicas", replicas), ("memoryStorage", False),
        ("tls_client_cert", _secret(secret, "client-cert")),
        ("tls_client_key", _secret(secret, "client-key")),
    ])
    body = {"apiVersion": "dapr.io/v1alpha1", "kind": "Component", "metadata": {"name": component},
            "spec": {"type": "pubsub.jetstream", "version": "v1", "metadata": metadata},
            "scopes": list(app_ids)}
    return BrokerProfile("jetstream", body, consumer, app_ids)


def production_profiles(*, broker: str, tenant: str,
                        workers: tuple[str, ...] = ("worker-1", "worker-2"),
                        observers: tuple[str, ...] = ("observer-a", "observer-b")) -> list[BrokerProfile]:
    """Give replicas one consumer identity and independent observers distinct identities."""
    tenant = token(tenant)
    require(broker in {"rabbitmq", "jetstream"}, "UNSUPPORTED_BROKER")
    require(len(workers) >= 1 and len(observers) >= 1, "INVALID_TOPOLOGY")
    factory = rabbitmq_component if broker == "rabbitmq" else jetstream_component
    result = [factory(component=f"bus-{tenant}-workers", consumer=f"{tenant}-workers", app_ids=workers)]
    for app in observers:
        token(app)
        result.append(factory(component=f"bus-{tenant}-{app}", consumer=f"{tenant}-{app}", app_ids=(app,)))
    return result


def assert_portability_contract(profile: BrokerProfile) -> None:
    """Fail closed when a profile loses required at-least-once semantics."""
    values = {item["name"]: item for item in profile.component["spec"]["metadata"]}
    if profile.broker == "rabbitmq":
        required = {"durable": "true", "deletedWhenUnused": "false", "autoAck": "false",
                    "deliveryMode": "2", "requeueInFailure": "true", "publisherConfirm": "true",
                    "enableDeadLetter": "true", "consumerID": profile.consumer}
    elif profile.broker == "jetstream":
        required = {"durableName": profile.consumer, "queueGroupName": profile.consumer,
                    "ackPolicy": "explicit", "deliverPolicy": "all", "memoryStorage": "false"}
    else:
        raise PermanentError("UNSUPPORTED_BROKER")
    for name, expected in required.items():
        require(values.get(name, {}).get("value") == expected, "BROKER_PROFILE_UNSAFE")
