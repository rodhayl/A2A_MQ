"""Composition roots own background loops and drain resources deterministically."""
import asyncio
from contextlib import asynccontextmanager
import threading

import httpx
import pytest
from starlette.applications import Starlette

from a2a_mq.domain import PermanentError
from a2a_mq.production import create_api_platform, create_workflow_worker
from a2a_mq.storage import SQLiteStore


class Bus:
    component = "bus"

    def __init__(self):
        self.closed = False
        self.published = []

    async def publish(self, envelope):
        self.published.append(envelope.wire)

    async def ready(self):
        return True

    async def close(self):
        self.closed = True


class Runtime:
    def __init__(self):
        self.started = self.stopped = False
        self.names = []

    def register_workflow(self, fn, *, name=None):
        self.names.append(name)

    def register_activity(self, fn, *, name=None):
        self.names.append(name)

    def start(self):
        self.started = True

    def shutdown(self):
        self.stopped = True


class BlockingRuntime(Runtime):
    def __init__(self):
        super().__init__()
        self.release = threading.Event()

    def start(self):
        self.release.wait(5)
        self.started = True


class FailingRuntime(Runtime):
    def start(self):
        raise RuntimeError("workflow start failed")


class Client:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def bridge(api, verifier, *, public_url):
    app = Starlette()
    app.state.public_url = public_url
    return app


def mcp(api, verifier, *, resource_url, profile):
    marker = {"entered": False, "exited": False, "paths": []}
    mcp.marker = marker

    @asynccontextmanager
    async def manager():
        marker["entered"] = True
        yield
        marker["exited"] = True

    async def app(scope, receive, send):
        from starlette.responses import JSONResponse
        marker["paths"].append(scope["path"])
        await JSONResponse({"path": scope["path"]})(scope, receive, send)

    class SessionManager:
        def run(self):
            return manager()

    class Server:
        session_manager = SessionManager()

    assert profile == "workflow"
    return Server(), app


@pytest.mark.parametrize("public_url,mcp_url,code", [
    ("https://agent.example/base", None, "PUBLIC_HTTPS_URL_REQUIRED"),
    ("https://agent.example", "https://agent.example/not-mcp", "MCP_HTTPS_RESOURCE_REQUIRED"),
    ("https://agent.example", "https://agent.example/mcp?x=1", "MCP_HTTPS_RESOURCE_REQUIRED"),
])
def test_api_platform_rejects_invalid_public_paths_before_composition(public_url,mcp_url,code):
    with pytest.raises(PermanentError, match=code):
        create_api_platform(
            object(), object(), Bus(),
            a2a_verifier=object(), mcp_verifier=object(),
            public_url=public_url, mcp_resource_url=mcp_url,
            bridge_factory=bridge, mcp_factory=mcp,
        )


async def test_api_platform_enters_mcp_lifespan_and_drains_relays(tmp_path):
    path = tmp_path / "shared.db"
    tasks = SQLiteStore(path, tenant="demo", consumer="workflow")
    dead = SQLiteStore(path, tenant="demo", consumer="operations")
    bus = Bus()
    app = create_api_platform(
        tasks, dead, bus,
        a2a_verifier=object(), mcp_verifier=object(),
        public_url="https://agent.example",
        bridge_factory=bridge, mcp_factory=mcp,
    )
    async with app.router.lifespan_context(app):
        assert mcp.marker["entered"] and not mcp.marker["exited"] and not bus.closed
        import httpx
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="https://agent.example") as client:
            assert (await client.post("/mcp", json={})).json()["path"] == "/mcp"
            assert (await client.get("/.well-known/oauth-protected-resource/mcp")).json()["path"] \
                == "/.well-known/oauth-protected-resource/mcp"
    assert mcp.marker["exited"] and bus.closed
    assert mcp.marker["paths"] == ["/mcp", "/.well-known/oauth-protected-resource/mcp"]


async def test_api_liveness_fails_when_command_relay_dies(tmp_path):
    path = tmp_path / "api-dead-relay.db"
    tasks = SQLiteStore(path, tenant="demo", consumer="workflow")
    dead = SQLiteStore(path, tenant="demo", consumer="operations")
    bus = Bus()
    app = create_api_platform(
        tasks, dead, bus,
        a2a_verifier=object(), mcp_verifier=object(),
        public_url="https://agent.example",
        bridge_factory=bridge, mcp_factory=mcp,
    )

    async def stop_immediately(*args, **kwargs):
        return None

    app.state.command_relay.run = stop_immediately
    async with app.router.lifespan_context(app):
        await asyncio.sleep(0)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="https://agent.example") as client:
            live = await client.get("/health/live")
            ready = await client.get("/health/ready")
            assert live.status_code == 503 and live.json() == {"status":"failed"}
            assert ready.status_code == 503
    assert bus.closed


async def test_worker_http_lifespan_does_not_block_on_workflow_runtime_start(tmp_path):
    store = SQLiteStore(tmp_path / "worker-blocked.db", tenant="demo", consumer="workflow")
    bus = Bus()
    runtime = BlockingRuntime()
    client_instance = Client()
    app = create_workflow_worker(
        store, bus, app_token="fixture-token",
        runtime_factory=lambda: runtime, client_factory=lambda: client_instance,
        workflow_sdk=object(), reconcile_interval=.01, relay_interval=.01,
    )
    async with app.router.lifespan_context(app):
        assert not runtime.started
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://app") as client:
            assert (await client.get("/health/live")).status_code == 200
            assert (await client.get("/health/ready")).status_code == 503
            runtime.release.set()
            async with asyncio.timeout(2):
                while not app.state.a2amq_worker_state["workflow_ready"]:
                    await asyncio.sleep(.01)
            assert (await client.get("/health/ready")).status_code == 200
    assert runtime.stopped and bus.closed and client_instance.closed
    assert app.state.a2amq_worker_state["workflow_ready"] is False


async def test_worker_liveness_fails_when_critical_runtime_task_dies(tmp_path):
    store = SQLiteStore(tmp_path / "worker-failed.db", tenant="demo", consumer="workflow")
    bus = Bus()
    runtime = FailingRuntime()
    app = create_workflow_worker(
        store, bus, app_token="fixture-token",
        runtime_factory=lambda: runtime, client_factory=Client,
        workflow_sdk=object(), reconcile_interval=.01, relay_interval=.01,
    )
    async with app.router.lifespan_context(app):
        async with asyncio.timeout(2):
            while not app.state.a2amq_worker_state["tasks"][0].done():
                await asyncio.sleep(.01)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://app") as client:
            live = await client.get("/health/live")
            ready = await client.get("/health/ready")
            assert live.status_code == 503 and live.json() == {"status":"failed"}
            assert ready.status_code == 503
    assert runtime.stopped and bus.closed


async def test_worker_official_sdk_is_pinned_to_colocated_grpc(tmp_path):
    store = SQLiteStore(tmp_path / "worker-colocated.db", tenant="demo", consumer="workflow")
    bus = Bus()
    created = {}

    class OfficialRuntime(Runtime):
        def __init__(self, *, host=None, port=None):
            super().__init__()
            created["runtime"] = (host, port)

    class OfficialClient(Client):
        def __init__(self, *, host=None, port=None):
            super().__init__()
            created["client"] = (host, port)

    class SDK:
        WorkflowRuntime = OfficialRuntime
        DaprWorkflowClient = OfficialClient

    app = create_workflow_worker(
        store, bus, app_token="fixture-token",
        workflow_host="127.0.0.1", workflow_port="51001",
        workflow_sdk=SDK, reconcile_interval=.01, relay_interval=.01,
    )
    async with app.router.lifespan_context(app):
        async with asyncio.timeout(2):
            while not app.state.a2amq_worker_state["workflow_ready"]:
                await asyncio.sleep(.01)
    assert created == {
        "runtime": ("127.0.0.1", "51001"),
        "client": ("127.0.0.1", "51001"),
    }


async def test_worker_starts_official_runtime_shape_and_shuts_down(tmp_path):
    store = SQLiteStore(tmp_path / "worker.db", tenant="demo", consumer="workflow")
    bus = Bus()
    runtime = Runtime()
    app = create_workflow_worker(
        store, bus, app_token="fixture-token",
        runtime_factory=lambda: runtime, client_factory=Client,
        workflow_sdk=object(), reconcile_interval=.01, relay_interval=.01,
    )
    async with app.router.lifespan_context(app):
        async with asyncio.timeout(2):
            while not app.state.a2amq_worker_state["workflow_ready"]:
                await asyncio.sleep(.01)
        assert runtime.started and "a2amq-reference-v1" in runtime.names
    assert runtime.stopped and bus.closed
