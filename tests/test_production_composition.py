"""Composition roots own background loops and drain resources deterministically."""
from contextlib import asynccontextmanager

from starlette.applications import Starlette

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


class Client:
    pass


def bridge(api, verifier, *, public_url):
    app = Starlette()
    app.state.public_url = public_url
    return app


def mcp(api, verifier, *, resource_url, path):
    marker = {"entered": False, "exited": False}

    @asynccontextmanager
    async def lifespan(app):
        marker["entered"] = True
        yield
        marker["exited"] = True

    app = Starlette(lifespan=lifespan)
    app.state.marker = marker
    assert path == "/"
    return app


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
    mcp_app = next(route.app for route in app.routes if getattr(route, "path", None) == "/mcp")
    async with app.router.lifespan_context(app):
        assert mcp_app.state.marker["entered"] and not bus.closed
    assert mcp_app.state.marker["exited"] and bus.closed


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
        assert runtime.started and "a2amq-reference-v1" in runtime.names
    assert runtime.stopped and bus.closed
