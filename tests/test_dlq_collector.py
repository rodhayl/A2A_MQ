"""The DLQ collector validates Dapr metadata and never recursively dead-letters poison."""
import base64
import hashlib

import httpx

from a2a_mq.central_dlq import CentralDLQ
from a2a_mq.dlq_service import create_dlq_collector
from a2a_mq.identity import Principal
from a2a_mq.storage import SQLiteStore


class Bus:
    component = "bus"
    def __init__(self): self.closed = False
    async def ready(self): return True
    async def close(self): self.closed = True


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
