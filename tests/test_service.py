"""Actual callback boundary + SQLite + Dapr HTTP publisher contract integration."""
import asyncio
from contextlib import asynccontextmanager
import json
import threading

import httpx
import pytest

from a2a_mq.domain import Envelope, PermanentError
from a2a_mq.service import ServiceSettings, create_app, from_env
from a2a_mq.storage import SQLiteStore
from a2a_mq.transport import DaprPublisher, cloud_event
from test_runtime import until
from test_storage import NOW, counts, request, result

TOKEN = "local-test-token-123456789"


@asynccontextmanager
async def client_for(store, *, role="worker", response_status=204, callback_timeout=30):
    sent = []
    status = [response_status]
    def respond(req):
        if req.method == "POST":
            sent.append(json.loads(req.content))
        return httpx.Response(status[0])
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as outbound:
        bus = DaprPublisher(client=outbound)
        app = create_app(store, bus, settings=ServiceSettings(app_token=TOKEN, role=role,
                         callback_timeout=callback_timeout, relay_interval=0.01))
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app",
                                         headers={"dapr-api-token": TOKEN}) as client:
                yield client, app, sent, status


def callback(event):
    return {**cloud_event(event), "pubsubname": "bus", "topic": event.route}


async def test_callback_commits_during_outage_and_relay_recovers(store):
    async with client_for(store, response_status=503) as (client, app, sent, status):
        response = await client.post("/events", json=callback(request()))
        assert response.json() == {"status": "SUCCESS"}
        assert counts(store) == dict(inbox=1, effects=1, tasks=1, outbox=1)
        await until(lambda: bool(sent))
        assert len(store.pending_outbox()) == 1
        assert (await client.get("/health/ready")).status_code == 503
        status[0] = 204
        await until(lambda: not store.pending_outbox())
        assert (await client.get("/health/ready")).status_code == 200
        assert (await client.post("/events", json=callback(request()))).json() == {"status": "SUCCESS"}
    assert len(sent) >= 2
    assert all(event == sent[0] for event in sent)


async def test_busy_callback_retries_without_quarantine(store):
    store.claim(request(), "other")
    async with client_for(store) as (client, app, sent, status):
        response = await client.post("/events", json=callback(request()))
        assert response.json() == {"status": "RETRY"}
        assert not sent
        assert counts(store)["effects"] == 0


async def test_bad_command_quarantined_only_after_confirmation(store):
    d = request().as_dict()
    d["payload"]["message"]["parts"] = [{"data": {"foo": "bar"}}]
    event = Envelope.from_dict(d)
    async with client_for(store) as (client, app, sent, status):
        assert (await client.post("/events", json=callback(event))).json() == {"status": "SUCCESS"}
        assert sent[-1]["type"] == "a2a-mq.deadletter"
        assert sent[-1]["data"]["reason"] == "ECHO_REQUIRES_TEXT"
    assert counts(store)["effects"] == 0


async def test_authentication_before_callback_and_queries(store):
    async with client_for(store) as (client, app, sent, status):
        for path in ["/internal/tasks", "/internal/tasks/x", "/dapr/subscribe"]:
            assert (await client.get(path, headers={"dapr-api-token": "wrong"})).status_code == 401
        response = await client.post("/events", json=callback(request()), headers={"dapr-api-token": "wrong"})
        assert response.status_code == 401 and counts(store)["inbox"] == 0
        assert (await client.get("/health/live", headers={"dapr-api-token": "wrong"})).status_code == 200
        assert (await client.get("/health/ready", headers={"dapr-api-token": "wrong"})).status_code == 200
        response = await client.get("/internal/tasks", headers=[("dapr-api-token", TOKEN), ("dapr-api-token", TOKEN)])
        assert response.status_code == 401
        assert not sent


async def test_authenticated_query_paging_filters_and_fixed_scope(store):
    for n in range(5):
        store.apply_snapshot(result(task_id=f"task-{n}", context=f"ctx-{n % 2}"))
    other = SQLiteStore(store.path, tenant="other", consumer="workers", clock=lambda: NOW)
    other.apply_snapshot(result(request(tenant="other"), task_id="secret"))
    async with client_for(store, role="observer") as (client, app, sent, status):
        response = await client.get("/internal/tasks", params={"limit": 2})
        assert response.status_code == 200 and response.headers["cache-control"] == "no-store"
        page = response.json()
        assert len(page["tasks"]) == 2 and page["nextCursor"] == "task-1"
        second = (await client.get("/internal/tasks", params={"after": page["nextCursor"], "limit": 2})).json()
        assert [t["id"] for t in second["tasks"]] == ["task-2", "task-3"]
        assert len((await client.get("/internal/tasks", params={"context_id": "ctx-0"})).json()["tasks"]) == 3
        assert (await client.get("/internal/tasks/secret")).status_code == 404
        assert (await client.get("/internal/tasks/task-0")).json()["id"] == "task-0"
        for params in [{"tenant": "other"}, {"consumer": "other"}, {"state": "bad"}, {"limit": 201}]:
            assert (await client.get("/internal/tasks", params=params)).status_code == 422
        assert not sent


async def test_observer_callback_and_restart_preserves_projection(store):
    async with client_for(store, role="observer") as (client, app, sent, status):
        assert (await client.post("/events", json=callback(result()))).json() == {"status": "SUCCESS"}
        assert (await client.get("/internal/tasks/task-1")).status_code == 200
    reopened = SQLiteStore(store.path, tenant="demo", consumer="workers", clock=lambda: NOW)
    async with client_for(reopened, role="observer") as (client, app, sent, status):
        assert (await client.get("/internal/tasks/task-1")).status_code == 200
        assert (await client.post("/events", json=callback(result()))).json() == {"status": "SUCCESS"}
        assert not sent
    assert counts(store)["inbox"] == 1


async def test_callback_timeout_keeps_sql_commit_recoverable_and_health_responsive(store, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    original = store.commit_claim
    def blocked(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        return original(*args, **kwargs)
    monkeypatch.setattr(store, "commit_claim", blocked)
    async with client_for(store, callback_timeout=0.05) as (client, app, sent, status):
        task = asyncio.create_task(client.post("/events", json=callback(request())))
        try:
            await until(entered.is_set)
            async with asyncio.timeout(1):
                assert (await client.get("/health/live")).status_code == 200
            assert (await task).json() == {"status": "RETRY"}
            assert counts(store)["effects"] == 0
        finally:
            release.set()
        await until(lambda: counts(store)["effects"] == 1)
        assert (await client.post("/events", json=callback(request()))).json() == {"status": "SUCCESS"}
    assert counts(store)["effects"] == 1


async def test_shutdown_closes_owned_bus_even_when_lifespan_body_raises(store):
    bus = DaprPublisher()
    app = create_app(store, bus, settings=ServiceSettings(app_token=TOKEN))
    with pytest.raises(RuntimeError, match="fixture"):
        async with app.router.lifespan_context(app):
            db = app.state.runtime.database
            raise RuntimeError("fixture")
    assert app.state.runtime is None and bus._closed and (bus.client is None or bus.client.is_closed)
    assert db._closed and not db._pending


async def test_no_lifespan_is_not_ready_and_queries_fail_closed(store):
    bus = DaprPublisher()
    app = create_app(store, bus, settings=ServiceSettings(app_token=TOKEN))
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app",
                                     headers={"dapr-api-token": TOKEN}) as client:
            assert (await client.get("/internal/tasks")).status_code == 503
            assert (await client.post("/events", json=callback(request()))).json() == {"status": "RETRY"}
    finally:
        await bus.close()


@pytest.mark.parametrize("value", ["", "short", " " * 16, "\n" * 16, "x" * 257, None])
def test_service_requires_valid_app_token(value):
    with pytest.raises(PermanentError, match="APP_TOKEN_REQUIRED"):
        ServiceSettings(app_token=value)


@pytest.mark.parametrize("field,value", [("role", "producer"), ("role", []), ("target_kind", []),
                                         ("callback_timeout", float("nan")), ("concurrency", True),
                                         ("storage_capacity", 0), ("relay_batch_size", 201),
                                         ("relay_interval", 0)])
def test_invalid_settings_rejected(field, value):
    with pytest.raises(PermanentError):
        ServiceSettings(app_token=TOKEN, **{field: value})


def test_token_not_exposed_in_settings_repr():
    assert TOKEN not in repr(ServiceSettings(app_token=TOKEN))


def test_factory_requires_token_and_explicit_storage_before_creation(tmp_path, monkeypatch):
    path = tmp_path / "state.db"
    monkeypatch.setenv("A2A_SQLITE_PATH", str(path))
    monkeypatch.delenv("APP_API_TOKEN", raising=False)
    with pytest.raises(PermanentError, match="APP_TOKEN_REQUIRED"):
        from_env()
    assert not path.exists()
    monkeypatch.setenv("APP_API_TOKEN", TOKEN)
    monkeypatch.delenv("A2A_SQLITE_PATH")
    with pytest.raises(PermanentError, match="STORAGE_PATH_REQUIRED"):
        from_env()
