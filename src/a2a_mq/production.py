"""Production composition roots for public API and Dapr Workflow worker roles.

The factories accept explicit dependencies so process topology, storage and secret
loading remain deployment concerns. No global singleton owns accepted work.
"""
from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlsplit

from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

from .bridge import create_bridge
from .central_dlq import CentralDLQ
from .domain import PermanentError, RetryableError, require, token
from .mcp_server import create_mcp as create_mcp_http
from .operations import Metrics, OperationsManager
from .ops_http import create_operations_app
from .runtime import AsyncSQLite, OutboxRelay
from .task_api import TaskAPI
from .tasks import TaskLedger
from .workflows import ActionRelay, DaprActions, register


def _public_https(value: str, code: str = "PUBLIC_HTTPS_URL_REQUIRED") -> str:
    parsed = urlsplit(value)
    require(isinstance(value, str) and parsed.scheme == "https" and bool(parsed.hostname)
            and not parsed.username and not parsed.password and not parsed.query
            and not parsed.fragment and len(value) <= 2048, code)
    return value.rstrip("/")


def _close_store(store) -> None:
    closer = getattr(store, "close", None)
    if callable(closer):
        closer()


async def _finish(task: asyncio.Task | None, *, timeout: float = 15) -> None:
    if task is None:
        return
    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
    except TimeoutError:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    except Exception:
        pass


class _ASGIEndpoint:
    """Preserve the public path when routing a three-argument ASGI application."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        await self.app(scope, receive, send)


def create_api_platform(task_store, dlq_store, bus, *, a2a_verifier, mcp_verifier,
                        public_url: str, mcp_resource_url: str | None = None,
                        backup_dir: str | Path | None = None,
                        database_capacity: int = 16, relay_interval: float = .5,
                        bridge_factory: Callable[..., Any] = create_bridge,
                        mcp_factory: Callable[..., Any] = create_mcp_http) -> Starlette:
    """Public A2A + MCP + operations plane over shared durable task storage."""
    public_url = _public_https(public_url)
    mcp_resource = _public_https(mcp_resource_url or public_url + "/mcp")
    require(task_store.scope.tenant == dlq_store.scope.tenant, "STORAGE_TENANT_MISMATCH")
    require(type(database_capacity) is int and 1 <= database_capacity <= 128, "INVALID_STORAGE_CAPACITY")
    require(type(relay_interval) in {int, float} and .01 <= relay_interval <= 60,
            "INVALID_RELAY_INTERVAL")

    task_ledger = TaskLedger(task_store)
    task_db = AsyncSQLite(task_store, capacity=database_capacity)
    dlq_db = task_db if dlq_store is task_store else AsyncSQLite(dlq_store, capacity=max(4, database_capacity // 2))
    api = TaskAPI(task_ledger, task_db)
    task_ops = OperationsManager(task_ledger, metrics=Metrics())
    dlq_ops = CentralDLQ(dlq_store, metrics=Metrics())
    relay = ActionRelay(task_ledger, task_db, publisher=bus)

    bridge = bridge_factory(api, a2a_verifier, public_url=public_url)
    mcp_server, mcp_app = mcp_factory(
        api, mcp_verifier, resource_url=mcp_resource, profile="workflow"
    )
    ops_app = create_operations_app(task_ops, task_db, bus, a2a_verifier,
                                    dlq_ops=dlq_ops, dlq_database=dlq_db,
                                    backup_dir=backup_dir)

    state: dict[str, Any] = {"relay_task": None, "stopping": False}

    async def live(request):
        relay_task = state["relay_task"]
        failed = (not state["stopping"] and relay_task is not None and relay_task.done())
        return JSONResponse({"status": "failed" if failed else "alive"},
                            status_code=503 if failed else 200)

    async def ready(request):
        relay_task = state["relay_task"]
        if state["stopping"] or relay_task is None or relay_task.done():
            return JSONResponse({"status": "unavailable"}, status_code=503)
        try:
            async with asyncio.timeout(5):
                storage = await task_db.call(task_ledger.running, limit=1)
                outbound = await bus.ready()
            del storage
            return JSONResponse({"status": "ready" if outbound else "unavailable"},
                                status_code=200 if outbound else 503)
        except Exception:
            return JSONResponse({"status": "unavailable"}, status_code=503)

    @asynccontextmanager
    async def lifespan(app: Starlette):
        stop = asyncio.Event()
        state["stopping"] = False
        relay_task: asyncio.Task | None = None
        try:
            async with AsyncExitStack() as stack:
                await stack.enter_async_context(mcp_server.session_manager.run())
                relay_task = asyncio.create_task(relay.run(stop, interval=relay_interval),
                                                 name="a2amq-command-relay")
                state["relay_task"] = relay_task
                yield
                state["stopping"] = True
                stop.set()
                await _finish(relay_task)
        finally:
            state["stopping"] = True
            state["relay_task"] = None
            try:
                if dlq_db is not task_db:
                    await dlq_db.aclose()
                await task_db.aclose()
            finally:
                try:
                    await bus.close()
                finally:
                    if dlq_store is not task_store:
                        await asyncio.to_thread(_close_store, dlq_store)
                    await asyncio.to_thread(_close_store, task_store)

    app = Starlette(routes=[
        Route("/health/live", live, methods=["GET"]),
        Route("/health/ready", ready, methods=["GET"]),
        Route("/mcp", _ASGIEndpoint(mcp_app), methods=["GET", "POST", "DELETE", "OPTIONS"]),
        Route("/.well-known/oauth-protected-resource/mcp", _ASGIEndpoint(mcp_app),
              methods=["GET", "OPTIONS"]),
        Mount("/ops", app=ops_app),
        Mount("/", app=bridge),
    ], lifespan=lifespan)
    app.state.task_ledger = task_ledger
    app.state.task_api = api
    app.state.task_operations = task_ops
    app.state.dlq_operations = dlq_ops
    app.state.command_relay = relay
    return app


def create_workflow_worker(store, bus, *, app_token: str, source: str = "workflow-worker",
                           database_capacity: int = 16, callback_concurrency: int = 8,
                           callback_timeout: float = 30, relay_interval: float = .5,
                           reconcile_interval: float = 1,
                           runtime_factory: Callable[[], Any] | None = None,
                           client_factory: Callable[[], Any] | None = None,
                           workflow_sdk: Any | None = None):
    """Dapr callback + official Workflow runtime + control/outbox/reconcile loops."""
    from .transport import subscriber

    source = token(source)
    ledger = TaskLedger(store, source=source)
    database = AsyncSQLite(store, capacity=database_capacity)
    state: dict[str, Any] = {"tasks": [], "runtime": None, "started": False,
                             "workflow_ready": False, "stopping": False}

    async def handle(envelope):
        actions = state.get("actions")
        if actions is None or not state["workflow_ready"]:
            raise RetryableError("WORKFLOW_UNAVAILABLE")
        await database.call(actions.ensure_started, envelope)

    async def readiness() -> bool:
        if not state["started"] or not state["workflow_ready"] or state["stopping"]:
            return False
        if any(task.done() for task in state["tasks"]):
            return False
        try:
            await database.call(ledger.running, limit=1)
            return True
        except Exception:
            return False

    async def liveness() -> bool:
        # Connecting to daprd is not a liveness failure. A completed critical
        # background task while the worker is otherwise running is.
        return (state["started"] and not state["stopping"]
                and not any(task.done() for task in state["tasks"]))

    async def reconcile(actions, stop: asyncio.Event) -> None:
        cursor = None
        while not stop.is_set():
            try:
                ids = await database.call(ledger.unconfirmed, limit=50, after=cursor)
                for task_id in ids:
                    try:
                        await database.call(actions.reconcile, task_id)
                    except Exception:
                        pass
                cursor = ids[-1] if len(ids) == 50 else None
            except Exception:
                cursor = None
            try:
                await asyncio.wait_for(stop.wait(), timeout=reconcile_interval)
            except TimeoutError:
                pass

    async def run_runtime(runtime, stop: asyncio.Event) -> None:
        """Let the HTTP app bind while the Workflow worker connects to daprd."""
        start = asyncio.create_task(asyncio.to_thread(runtime.start),
                                    name="a2amq-workflow-runtime-start")
        try:
            await asyncio.shield(start)
            state["workflow_ready"] = True
            await stop.wait()
        finally:
            state["workflow_ready"] = False
            if not start.done():
                start.cancel()
            await asyncio.gather(start, return_exceptions=True)

    @asynccontextmanager
    async def lifespan(app):
        runtime = client = None
        stop = asyncio.Event()
        tasks: list[asyncio.Task] = []
        state["stopping"] = False
        try:
            if runtime_factory is None or client_factory is None:
                try:
                    import dapr.ext.workflow as dapr_workflow
                except ImportError:
                    raise PermanentError("WORKFLOW_EXTRA_REQUIRED") from None
                runtime = (runtime_factory or dapr_workflow.WorkflowRuntime)()
                register(runtime, ledger, sdk=workflow_sdk or dapr_workflow)
                client = (client_factory or dapr_workflow.DaprWorkflowClient)()
            else:
                runtime = runtime_factory()
                register(runtime, ledger, sdk=workflow_sdk)
                client = client_factory()
            actions = DaprActions(ledger, client)
            controls = ActionRelay(ledger, database, actions=actions)
            outbox = OutboxRelay(database, bus)
            state.update(runtime=runtime, client=client, actions=actions,
                         controls=controls, outbox=outbox, started=True)
            tasks = [
                asyncio.create_task(run_runtime(runtime, stop), name="a2amq-workflow-runtime"),
                asyncio.create_task(controls.run(stop, interval=relay_interval), name="a2amq-control-relay"),
                asyncio.create_task(outbox.run(stop, interval=relay_interval), name="a2amq-result-relay"),
                asyncio.create_task(reconcile(actions, stop), name="a2amq-workflow-reconcile"),
            ]
            state["tasks"] = tasks
            yield
        finally:
            state["stopping"] = True
            state["started"] = False
            stop.set()
            if runtime is not None:
                try:
                    # Shutdown first so a Workflow start blocked on daprd readiness
                    # cannot hold application shutdown indefinitely.
                    await asyncio.to_thread(runtime.shutdown)
                except Exception:
                    pass
            for task in tasks:
                await _finish(task)
            if client is not None:
                close = getattr(client, "close", None)
                if callable(close):
                    try:
                        await asyncio.to_thread(close)
                    except Exception:
                        pass
            try:
                await database.aclose()
            finally:
                try:
                    await bus.close()
                finally:
                    await asyncio.to_thread(_close_store, store)
            state["tasks"] = []
            state["workflow_ready"] = False
            state["runtime"] = None
            for key in ("client", "actions", "controls", "outbox"):
                state.pop(key, None)

    topic = f"a2a.v1.{store.scope.tenant}.command.agent.{source}"
    app = subscriber(handle, bus, tenant=store.scope.tenant, consumer=store.scope.consumer,
                     topic=topic, app_token=app_token, concurrency=callback_concurrency,
                     readiness=readiness, liveness=liveness, lifespan=lifespan,
                     callback_timeout=callback_timeout)
    app.state.a2amq_worker_state = state
    app.state.a2amq_ledger = ledger
    return app
