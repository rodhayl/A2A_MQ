"""Cooperative HTTP deadlines, resource cleanup and exact callback metadata."""
import asyncio
from contextlib import asynccontextmanager
import json

import httpx
import pytest

from a2a_mq.domain import PermanentError, RetryableError, command
from a2a_mq.transport import DaprPublisher, cloud_event, subscriber


@asynccontextmanager
async def app_client(handler, **options):
    sent = []
    def respond(request):
        if request.method == "POST":
            sent.append(json.loads(request.content))
        return httpx.Response(204)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as outbound:
        bus = DaprPublisher(client=outbound)
        e = command("local fixture")
        app = subscriber(handler, bus, tenant="demo", consumer="worker", topic=e.route, **options)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as client:
            yield client, {**cloud_event(e), "topic": e.route, "pubsubname": "bus"}, sent


class UnreadBody(httpx.AsyncByteStream):
    def __init__(self):
        self.closed = False
    async def __aiter__(self):
        raise AssertionError("status-only requests must not buffer response bodies")
        yield b""  # pragma: no cover
    async def aclose(self):
        self.closed = True


@pytest.mark.parametrize("operation", ["publish", "ready"])
async def test_status_only_requests_close_without_reading_bodies(operation):
    body = UnreadBody()
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, stream=body))) as client:
        bus = DaprPublisher(client=client)
        if operation == "publish":
            await bus.publish(command("x"))
        else:
            assert await bus.ready()
        assert body.closed


@pytest.mark.parametrize("operation", ["publish", "ready"])
async def test_total_outbound_deadline_includes_header_wait(operation):
    async def slow(request):
        await asyncio.sleep(0.2)
        return httpx.Response(204)
    async with httpx.AsyncClient(transport=httpx.MockTransport(slow)) as client:
        bus = DaprPublisher(client=client, timeout=0.02)
        async with asyncio.timeout(2):
            if operation == "publish":
                with pytest.raises(RetryableError, match="PUBLISH_UNAVAILABLE"):
                    await bus.publish(command("x"))
            else:
                assert not await bus.ready()


@pytest.mark.parametrize("value", [True, False, None, "1", float("nan"), float("inf"), 0, -1, 61])
def test_invalid_publisher_timeout_is_permanent(value):
    with pytest.raises(PermanentError, match="INVALID_TIMEOUT"):
        DaprPublisher(timeout=value)


@pytest.mark.parametrize("value", [True, 1.5, "1", None, 0, 129])
async def test_invalid_concurrency_is_permanent(value):
    async def handle(e):
        pass
    with pytest.raises(PermanentError, match="INVALID_CONCURRENCY"):
        async with app_client(handle, concurrency=value):
            pass


@pytest.mark.parametrize("field,value", [("source", "urn:a2a-mq:other"), ("type", "a2a-mq.task.snapshot")])
async def test_callback_metadata_must_match_envelope(field, value):
    called = []
    async def handle(e):
        called.append(e)
    async with app_client(handle) as (client, event, sent):
        event[field] = value
        response = await client.post("/events", json=event)
        assert response.json() == {"status": "SUCCESS"}
        assert not called
        assert sent[0]["data"]["reason"] == "IDENTIFIER_MISMATCH"


async def test_handler_deadline_retries_and_releases_slot():
    calls = 0
    cleaned = asyncio.Event()
    async def handle(e):
        nonlocal calls
        calls += 1
        if calls == 1:
            try:
                await asyncio.Event().wait()
            finally:
                cleaned.set()
    async with app_client(handle, callback_timeout=0.02, concurrency=1) as (client, event, sent):
        async with asyncio.timeout(2):
            assert (await client.post("/events", json=event)).json() == {"status": "RETRY"}
            assert cleaned.is_set()
            assert (await client.post("/events", json=event)).json() == {"status": "SUCCESS"}
        assert not sent


async def test_callback_budget_includes_body_read():
    called = []
    async def handle(e):
        called.append(e)
    async def body():
        yield b"{"
        await asyncio.sleep(0.2)
        yield b"}"
    async with app_client(handle, callback_timeout=0.02) as (client, event, sent):
        async with asyncio.timeout(2):
            response = await client.post("/events", content=body(), headers={"content-type": "application/json"})
        assert response.json() == {"status": "RETRY"}
        assert not called and not sent


async def test_callback_budget_includes_semaphore_wait():
    started = asyncio.Event()
    calls = 0
    async def handle(e):
        nonlocal calls
        calls += 1
        started.set()
        await asyncio.Event().wait()
    async with app_client(handle, callback_timeout=0.03, concurrency=1) as (client, event, sent):
        async with asyncio.timeout(2):
            first = asyncio.create_task(client.post("/events", json=event))
            await started.wait()
            second = asyncio.create_task(client.post("/events", json=event))
            responses = await asyncio.gather(first, second)
        assert all(r.json() == {"status": "RETRY"} for r in responses)
        assert calls <= 2 and not sent


async def test_readiness_deadline():
    async def handle(e):
        pass
    async def ready():
        await asyncio.Event().wait()
        return True
    async with app_client(handle, callback_timeout=0.02, readiness=ready) as (client, event, sent):
        async with asyncio.timeout(2):
            assert (await client.get("/health/ready")).status_code == 503


async def test_lifespan_exception_still_closes_owned_client():
    async def handle(e):
        pass
    bus = DaprPublisher()
    app = subscriber(handle, bus, tenant="demo", consumer="worker", topic=None)
    try:
        with pytest.raises(RuntimeError, match="fixture shutdown"):
            async with app.router.lifespan_context(app):
                raise RuntimeError("fixture shutdown")
        assert bus.client is None or bus.client.is_closed
    finally:
        await bus.close()


@pytest.mark.parametrize("value", [True, None, "1", 0, -1, float("nan"), float("inf"), 301])
async def test_invalid_callback_deadline_is_permanent(value):
    async def handle(e):
        pass
    with pytest.raises(PermanentError, match="INVALID_TIMEOUT"):
        async with app_client(handle, callback_timeout=value):
            pass


async def test_admission_timeout_never_starts_handler(monkeypatch):
    gate = asyncio.Semaphore(0)
    monkeypatch.setattr(asyncio, "Semaphore", lambda capacity: gate)
    called = []
    async def handle(e):
        called.append(e)
    async with app_client(handle, callback_timeout=0.02) as (client, event, sent):
        async with asyncio.timeout(2):
            response = await client.post("/events", json=event)
        assert response.json() == {"status": "RETRY"}
        assert not called and not sent
        gate.release()
        assert (await client.post("/events", json=event)).json() == {"status": "SUCCESS"}
        assert len(called) == 1


async def test_external_cancellation_is_not_success_or_quarantine():
    entered = asyncio.Event()
    cleaned = asyncio.Event()
    async def handle(e):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaned.set()
    async with app_client(handle) as (client, event, sent):
        task = asyncio.create_task(client.post("/events", json=event))
        try:
            async with asyncio.timeout(2):
                await entered.wait()
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            assert cleaned.is_set() and not sent
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


async def test_duplicate_app_credentials_are_rejected_before_processing():
    called = []
    async def handle(e):
        called.append(e)
    async with app_client(handle, app_token="fixture-token") as (client, event, sent):
        response = await client.post("/events", json=event,
                                     headers=[("dapr-api-token", "fixture-token"),
                                              ("dapr-api-token", "fixture-token")])
        assert response.status_code == 401 and not called and not sent
        response = await client.post("/events", json=event, headers={"dapr-api-token": "fixture-token"})
        assert response.json() == {"status": "SUCCESS"} and len(called) == 1


async def test_owned_publisher_client_is_lazy_singleton_and_terminally_closed(monkeypatch):
    created = []
    real_client = httpx.AsyncClient

    def factory(**kwargs):
        client = real_client(transport=httpx.MockTransport(lambda request: httpx.Response(204)))
        created.append(client)
        return client

    monkeypatch.setattr(httpx, "AsyncClient", factory)
    bus = DaprPublisher()
    assert bus.client is None and created == []
    assert await bus.ready()
    assert len(created) == 1 and bus.client is created[0]
    assert await bus.ready()
    assert len(created) == 1
    await bus.close()
    assert created[0].is_closed and bus.client is None
    assert not await bus.ready()
    assert len(created) == 1


async def test_injected_publisher_client_remains_caller_owned_after_close():
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(204))) as client:
        bus = DaprPublisher(client=client)
        await bus.close()
        assert not client.is_closed
        assert await bus.ready()
