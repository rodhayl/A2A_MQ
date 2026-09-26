"""Optional single-host Workflow tier built on the lightweight local composition.

This profile keeps API + Workflow execution in one Python process and one daprd
sidecar. Dapr owns durable waits, timers, activities and external events. SQLite
remains the application DB; Dapr Workflow uses a separate state.sqlite component.
"""
from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import asdict, dataclass
import os
from typing import Any

from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

from .configuration import read_file, verifier
from .deadletters import DeadLetters, ReplayService
from .dlq_callback import create_dlq_callback
from .domain import Envelope, RetryableError, canonical, require
from .local_lite import (ASGIEndpoint, PathRewrite, LocalLiteSettings, _close_local_resources,
                         _integer, _no_lifespan)
from .maintenance import Operations
from .operations_http import create_operations
from .recovery import AdmissionRepair
from .queue_lab import create_queue_lab
from .runtime import AsyncSQLite, OutboxRelay
from .storage import SQLiteStore
from .task_api import TaskAPI
from .tasks import TaskLedger
from .transport import DaprPublisher, _AppTokenMiddleware, cloud_event, subscriber
from .workflows import ActionRelay, DaprActions


@dataclass(frozen=True)
class LocalWorkflowSettings(LocalLiteSettings):
    dapr_grpc_port: int = 50001
    relay_interval: float = 0.25

    def __post_init__(self):
        super().__post_init__()
        require(type(self.dapr_grpc_port) is int and 1 <= self.dapr_grpc_port <= 65535,
                "INVALID_DAPR_PORT")

    @classmethod
    def from_env(cls, env=None):
        env = os.environ if env is None else env
        payload = asdict(LocalLiteSettings.from_env(env))
        if "A2A_RELAY_INTERVAL" not in env:
            payload["relay_interval"] = 0.25
        return cls(**payload, dapr_grpc_port=_integer(env, "DAPR_GRPC_PORT", 50001, 1, 65535))


async def _reconcile(ledger, database, actions, stop, interval):
    cursor = None
    while not stop.is_set():
        try:
            ids = await database.call(ledger.unconfirmed, limit=50, after=cursor)
            for task_id in ids:
                if stop.is_set():
                    break
                try:
                    await database.call(actions.reconcile, task_id)
                except Exception:
                    pass
            cursor = ids[-1] if len(ids) == 50 else None
        except Exception:
            pass
        try:
            await asyncio.wait_for(stop.wait(), interval)
        except TimeoutError:
            pass


async def _runtime_worker(runtime, status, stop):
    """Start the official worker without blocking ASGI port binding."""
    try:
        await asyncio.to_thread(runtime.start)
        status["workflow_ready"] = True
        await stop.wait()
    finally:
        status["workflow_ready"] = False


def create_app(settings: LocalWorkflowSettings):
    app_token = read_file(settings.app_token_file, secret=True, limit=512).strip()
    dapr_token = read_file(settings.dapr_token_file, secret=True, limit=512).strip()
    require(16 <= len(app_token) <= 256 and 16 <= len(dapr_token) <= 256, "TOKEN_LENGTH_INVALID")

    store = SQLiteStore(settings.sqlite_path, tenant=settings.tenant, consumer=settings.consumer)
    database = AsyncSQLite(store, capacity=settings.database_capacity)
    bus = DaprPublisher(settings.dapr_http, component=settings.component, api_token=dapr_token,
                        timeout=settings.rpc_timeout)
    ledger = TaskLedger(store, source=settings.source, max_active=settings.max_active,
                        max_records=settings.max_records)
    api = TaskAPI(ledger, database, wait_timeout=settings.callback_timeout)
    identity = verifier(settings)

    status: dict[str, Any] = {"running": False, "workflow_ready": False, "jobs": [], "actions": None, "runtime": None}

    async def accept(envelope: Envelope):
        if not status["running"] or not status["workflow_ready"] or status["actions"] is None:
            raise RetryableError("SERVICE_NOT_RUNNING")
        await database.call(status["actions"].ensure_started, envelope)

    async def ready_value() -> bool:
        if (not status["running"] or not status["workflow_ready"]
                or any(job.done() for job in status["jobs"])):
            return False
        try:
            await database.call(store.list_tasks, limit=1)
            if not await bus.ready():
                return False
            runtime = status["runtime"]
            return bool(runtime is not None and await asyncio.to_thread(
                runtime.wait_for_worker_ready, timeout=.1))
        except Exception:
            return False

    callback = subscriber(accept, bus, tenant=settings.tenant, consumer=settings.consumer,
                          topic=settings.command_route, app_token=app_token,
                          concurrency=settings.concurrency, readiness=ready_value,
                          lifespan=_no_lifespan, callback_timeout=settings.callback_timeout)

    operations = Operations(ledger)
    archive = DeadLetters(store, component="bus-dlq")
    dlq_callback = create_dlq_callback(archive, database, app_token=app_token,
                                       active=lambda: status["running"],
                                       timeout=settings.callback_timeout,
                                       concurrency=settings.concurrency)
    replay = ReplayService(archive, database, bus,
                           allowed_routes=frozenset({settings.command_route, settings.event_route}))

    async def demo_deadletter(principal, task_id: str):
        await api.get(principal, task_id)
        envelope = await database.call(ledger.command, task_id)
        await bus.quarantine(
            canonical(cloud_event(envelope)).encode(),
            settings.tenant,
            settings.consumer,
            "QUEUE_LAB_DEMO",
        )

    ops_app = create_operations(api, identity, operations, archive, replay, database,
                                timeout=settings.callback_timeout)

    from .bridge import create_bridge
    bridge = create_bridge(api, identity, public_url=settings.public_url, profile="workflow",
                           allow_insecure_loopback=True)

    mcp_server = mcp_app = None
    if settings.mcp_enabled:
        from .mcp_server import create_mcp
        resource_url = settings.public_url + "/mcp"
        mcp_identity = verifier(settings, audience=resource_url)
        mcp_server, mcp_app = create_mcp(api, mcp_identity, resource_url=resource_url,
                                         profile="workflow", allow_insecure_loopback=True)

    async def live(request):
        failed = status["running"] and any(job.done() for job in status["jobs"])
        return JSONResponse({"status": "failed" if failed else "alive"},
                            status_code=503 if failed else 200,
                            headers={"Cache-Control": "no-store"})

    async def ready(request):
        ok = await ready_value()
        return JSONResponse({"status": "ready" if ok else "unavailable"},
                            status_code=200 if ok else 503,
                            headers={"Cache-Control": "no-store"})

    async def subscriptions(request):
        return JSONResponse([
            {"pubsubname": settings.component, "topic": settings.command_route, "route": "/events",
             "deadLetterTopic": f"a2a.v1.{settings.tenant}.dlq"},
            {"pubsubname": "bus-dlq", "topic": f"a2a.v1.{settings.tenant}.dlq",
             "route": "/dlq-events"},
        ])

    discovery = _AppTokenMiddleware(Starlette(routes=[Route("/dapr/subscribe", subscriptions)]), app_token)
    routes = [
        Route("/dapr/subscribe", ASGIEndpoint(discovery), methods=["GET"]),
        Route("/events", ASGIEndpoint(callback), methods=["POST"]),
        Route("/dlq-events", ASGIEndpoint(PathRewrite(dlq_callback, "/events")), methods=["POST"]),
        Route("/health/live", live),
        Route("/health/ready", ready),
        Mount("/operations", ops_app),
        Mount("/lab", create_queue_lab(
            settings,
            profile="local-workflow",
            verifier=identity,
            demo_deadletter=demo_deadletter,
        )),
    ]
    if mcp_app is not None:
        routes.extend([
            Route("/mcp", ASGIEndpoint(mcp_app), methods=["GET", "POST", "DELETE", "OPTIONS"]),
            Route("/.well-known/oauth-protected-resource/mcp", ASGIEndpoint(mcp_app),
                  methods=["GET", "OPTIONS"]),
        ])
    routes.append(Mount("/", bridge))

    @asynccontextmanager
    async def lifespan(app):
        stop = asyncio.Event()
        runtime = client = None
        jobs = []
        async with AsyncExitStack() as stack:
            stack.push_async_callback(_close_local_resources, database, bus)
            if mcp_server is not None:
                await stack.enter_async_context(mcp_server.session_manager.run())
            try:
                from .application import official_workflow
                runtime, client = official_workflow(settings, ledger, dapr_token)
                actions = DaprActions(ledger, client)
                status["runtime"], status["actions"] = runtime, actions
                jobs = [
                    asyncio.create_task(_runtime_worker(runtime, status, stop),
                                        name="a2amq-local-workflow-runtime"),
                    asyncio.create_task(ActionRelay(ledger, database, publisher=bus, actions=actions)
                                        .run(stop, settings.relay_interval),
                                        name="a2amq-local-workflow-actions"),
                    asyncio.create_task(OutboxRelay(database, bus)
                                        .run(stop, interval=settings.relay_interval),
                                        name="a2amq-local-workflow-outbox"),
                    asyncio.create_task(AdmissionRepair(ledger).run(database, stop),
                                        name="a2amq-local-workflow-admission-repair"),
                    asyncio.create_task(_reconcile(ledger, database, actions, stop, 1.0),
                                        name="a2amq-local-workflow-reconcile"),
                    asyncio.create_task(replay.run(stop), name="a2amq-local-workflow-replay"),
                ]
                status["jobs"], status["running"] = jobs, True
                yield
            finally:
                status["running"] = False
                stop.set()
                if runtime is not None:
                    try:
                        await asyncio.to_thread(runtime.shutdown)
                    except Exception:
                        pass
                for job in jobs:
                    if not job.done():
                        job.cancel()
                if jobs:
                    await asyncio.gather(*jobs, return_exceptions=True)
                status["jobs"], status["actions"], status["runtime"] = [], None, None
                if client is not None:
                    try:
                        await asyncio.to_thread(client.close)
                    except Exception:
                        pass

    app = Starlette(routes=routes, lifespan=lifespan)
    app.state.a2amq_api = api
    app.state.a2amq_ledger = ledger
    app.state.a2amq_profile = "local-workflow"
    return app


def from_env():
    """Uvicorn factory for the optional local Workflow tier."""
    return create_app(LocalWorkflowSettings.from_env())
