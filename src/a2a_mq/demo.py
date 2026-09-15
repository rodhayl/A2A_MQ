"""Phase-2 transport demo. Phase 3 replaces volatile processing with SQL/outbox."""
import os

from .domain import Envelope, require
from .reference import echo_events
from .transport import DaprPublisher, subscriber


def create_app():
    role = os.getenv("A2A_ROLE", "worker")
    require(role in {"worker", "observer", "producer"}, "INVALID_ROLE")
    tenant = os.getenv("A2A_TENANT", "demo")
    consumer = os.getenv("A2A_CONSUMER", "worker-echo")
    bus = DaprPublisher(os.getenv("DAPR_HTTP_ENDPOINT", "http://127.0.0.1:3500"),
                        api_token=os.getenv("DAPR_API_TOKEN", ""))
    observed: dict[str, dict] = {}

    async def handle(envelope: Envelope):
        if role == "worker":
            envelope.check_expiry()
            for event in echo_events(envelope, consumer):
                await bus.publish(event)
        else:
            e = envelope.as_dict()
            require(e["operation"] == "task.snapshot", "UNSUPPORTED_OPERATION")
            if len(observed) >= 1000:
                observed.pop(next(iter(observed)))
            observed[e["messageId"]] = {"id": e["messageId"], "taskId": e["taskId"], "sequence": e["sequence"]}

    topic = None if role == "producer" else f"a2a.v1.{tenant}." + (
        "command.skill.echo" if role == "worker" else "event.task")
    app = subscriber(handle, bus, tenant=tenant, consumer=consumer, topic=topic,
                     app_token=os.getenv("APP_API_TOKEN", ""))

    @app.get("/internal/observations")
    async def observations():
        return list(observed.values())

    return app
