"""The DLQ collector validates Dapr metadata and never recursively dead-letters poison."""
import base64
import hashlib

import httpx
import pytest

from a2a_mq.central_dlq import CentralDLQ
from a2a_mq.dlq_service import create_dlq_collector
from a2a_mq.domain import PermanentError, canonical, command
from a2a_mq.identity import Principal
from a2a_mq.storage import SQLiteStore
from a2a_mq.transport import cloud_event


class Bus:
    component = "bus"
    def __init__(self): self.closed = False
    async def ready(self): return True
    async def close(self): self.closed = True


class Publisher:
    def __init__(self): self.items = []
    async def publish(self, envelope): self.items.append(envelope)


def event(original: bytes):
    return {
        "specversion": "1.0", "id": "dead-1", "source": "urn:a2a-mq:worker-a",
        "type": "a2a-mq.deadletter", "datacontenttype": "application/json",
        "pubsubname": "bus", "topic": "a2a.v1.demo.dlq",
        "data": {"reason": "INVALID_PAYLOAD", "consumer": "worker-a", "truncated": False,
                 "capturedBytesSha256": hashlib.sha256(original).hexdigest(),
                 "originalBase64": base64.b64encode(original).decode("ascii")},
    }


async def test_verified_deadletter_is_persisted_with_origin(tmp_path):
    store = SQLiteStore(tmp_path / "state.db", tenant="demo", consumer="operations", clock=lambda: 1000.0)
    ops = CentralDLQ(store)
    bus = Bus(); app = create_dlq_collector(store, bus, ops, app_token="fixture-token")
    original = b'{"arbitrary":"poison"}'
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as client:
            assert (await client.post("/events", json=event(original))).status_code == 401
            response = await client.post("/events", json=event(original),
                                         headers={"dapr-api-token": "fixture-token"})
            assert response.json() == {"status": "SUCCESS"}
            assert (await client.get("/health/ready", headers={"dapr-api-token": "fixture-token"})).status_code == 200
    admin = Principal("operator", "demo", frozenset({"a2a:admin"}))
    rows, _ = ops.list_dlq(admin)
    assert len(rows) == 1 and rows[0]["originConsumer"] == "worker-a"
    assert ops.dlq_payload(admin, rows[0]["entryId"]) == original and bus.closed


async def test_collector_replays_real_dapr_cloudevent_bytes(tmp_path):
    store = SQLiteStore(tmp_path / "state.db", tenant="demo", consumer="operations")
    ops = CentralDLQ(store)
    bus = Bus(); app = create_dlq_collector(store, bus, ops, app_token="fixture-token")
    original = command("hello", tenant="demo", skill="echo")
    delivered = {**cloud_event(original), "pubsubname": "bus", "topic": original.route}
    raw = canonical(delivered).encode()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://app") as client:
            response = await client.post("/events", json=event(raw),
                                         headers={"dapr-api-token": "fixture-token"})
            assert response.json() == {"status": "SUCCESS"}

    admin = Principal("operator", "demo", frozenset({"a2a:admin"}))
    entry = ops.list_dlq(admin)[0][0]["entryId"]
    publisher = Publisher()
    replayed = await ops.replay_dlq(admin, entry, publisher)
    assert replayed.wire == original.wire
    assert [item.wire for item in publisher.items] == [original.wire]


@pytest.mark.parametrize("case", ["truncated", "topic-mismatch", "identifier-mismatch"])
async def test_central_replay_rejects_unsafe_original_delivery(tmp_path, case):
    store = SQLiteStore(tmp_path / "state.db", tenant="demo", consumer="operations")
    ops = CentralDLQ(store)
    admin = Principal("operator", "demo", frozenset({"a2a:admin"}))
    original = command("hello", tenant="demo", skill="echo")
    delivered = {**cloud_event(original), "pubsubname": "bus", "topic": original.route}
    truncated = case == "truncated"
    if case == "topic-mismatch":
        delivered["topic"] = "a2a.v1.demo.command.skill.other"
    if case == "identifier-mismatch":
        delivered["id"] = "different-id"
    raw = canonical(delivered).encode()
    entry = ops.record_dlq(raw, "INVALID_PAYLOAD", origin_consumer="worker-a",
                           truncated=truncated)
    publisher = Publisher()
    with pytest.raises(PermanentError, match="DLQ_NOT_REPLAYABLE"):
        await ops.replay_dlq(admin, entry, publisher)
    assert publisher.items == []


async def test_corrupt_deadletter_is_dropped_without_recursive_dlq(tmp_path):
    store = SQLiteStore(tmp_path / "state.db", tenant="demo", consumer="operations", clock=lambda: 1000.0)
    ops = CentralDLQ(store)
    bus = Bus(); app = create_dlq_collector(store, bus, ops, app_token="fixture-token")
    bad = event(b"x"); bad["data"]["capturedBytesSha256"] = "0" * 64
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as client:
            response = await client.post("/events", json=bad, headers={"dapr-api-token": "fixture-token"})
            assert response.json() == {"status": "SUCCESS"}
    admin = Principal("operator", "demo", frozenset({"a2a:admin"}))
    assert ops.list_dlq(admin)[0] == []
