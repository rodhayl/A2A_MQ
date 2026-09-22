"""Lightweight single-host A2A bridge: SQLite + Dapr Pub/Sub, no Workflow engine.

The local-lite profile intentionally supports only immediate deterministic text
transforms. Durable waits/timers/approval remain in the Workflow profile instead
of being reimplemented as a second scheduler.
"""
from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
import os
from urllib.parse import urlsplit

from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

from .bridge import create_bridge
from .configuration import https_issuer, read_file, verifier
from .deadletters import DeadLetters, ReplayService
from .dlq_callback import create_dlq_callback
from .domain import Envelope, PermanentError, TERMINAL, load_json, require, state_of, token
from .maintenance import Operations
from .operations_http import create_operations
from .recovery import AdmissionRepair
from .runtime import AsyncSQLite, OutboxRelay
from .storage import SQLiteStore
from .task_api import TaskAPI
from .tasks import Action, TaskLedger
from .transport import DaprPublisher, _AppTokenMiddleware, subscriber
from .workflow_options import options, text_parts
from .workflows import ActionRelay


class ASGIEndpoint:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        await self.app(scope, receive, send)


class PathRewrite:
    """Route one outer callback path into a small Dapr callback app."""

    def __init__(self, app, path: str):
        self.app, self.path = app, path

    async def __call__(self, scope, receive, send):
        scope = {**scope, "path": self.path, "raw_path": self.path.encode()}
        await self.app(scope, receive, send)


def _loopback_url(value: str) -> str:
    try:
        parsed = urlsplit(value)
        require(parsed.scheme in {"http", "https"}
                and parsed.hostname in {"127.0.0.1", "localhost", "::1"}
                and parsed.path in {"", "/"} and not parsed.username and not parsed.password
                and not parsed.query and not parsed.fragment, "LOCAL_LOOPBACK_URL_REQUIRED")
        if parsed.port is not None:
            require(1 <= parsed.port <= 65535, "LOCAL_LOOPBACK_URL_REQUIRED")
    except (TypeError, ValueError):
        raise PermanentError("LOCAL_LOOPBACK_URL_REQUIRED") from None
    return value.rstrip("/")


def _integer(env, name: str, default: int, low: int, high: int) -> int:
    raw = env.get(name, str(default))
    require(isinstance(raw, str) and raw.isascii() and raw.isdigit() and len(raw) <= 9,
            "INVALID_CONFIGURATION")
    value = int(raw)
    require(low <= value <= high, "INVALID_CONFIGURATION")
    return value


def _boolean(env, name: str) -> bool:
    raw = env.get(name, "0")
    require(raw in {"0", "1"}, "INVALID_CONFIGURATION")
    return raw == "1"


@dataclass(frozen=True)
class LocalLiteSettings:
    sqlite_path: str
    app_token_file: str
    dapr_token_file: str
    jwt_keys_file: str
    issuer: str
    public_url: str = "http://127.0.0.1:18000"
    tenant: str = "demo"
    consumer: str = "local"
    source: str = "local-worker"
    component: str = "bus"
    dapr_http: str = "http://127.0.0.1:3500"
    mcp_enabled: bool = False
    concurrency: int = 2
    database_capacity: int = 4
    max_active: int = 128
    max_records: int = 10000
    callback_timeout: float = 30
    rpc_timeout: float = 5
    relay_interval: float = .1

    def __post_init__(self):
        require(bool(self.sqlite_path), "STORAGE_PATH_REQUIRED")
        require(bool(self.app_token_file) and bool(self.dapr_token_file) and bool(self.jwt_keys_file),
                "TOKEN_FILES_REQUIRED")
        for value in (self.tenant, self.consumer, self.source, self.component):
            token(value)
        _loopback_url(self.public_url)
        _loopback_url(self.dapr_http)
        https_issuer(self.issuer)
        require(type(self.mcp_enabled) is bool, "INVALID_CONFIGURATION")
        require(type(self.concurrency) is int and 1 <= self.concurrency <= 16, "INVALID_CONCURRENCY")
        require(type(self.database_capacity) is int and 1 <= self.database_capacity <= 32,
                "INVALID_STORAGE_CAPACITY")
        require(type(self.max_active) is int and 1 <= self.max_active <= 10000, "INVALID_QUOTA")
        require(type(self.max_records) is int and self.max_active <= self.max_records <= 1000000,
                "INVALID_QUOTA")
        for value, low, high in ((self.callback_timeout, .01, 300), (self.rpc_timeout, .01, 60),
                                 (self.relay_interval, .01, 10)):
            require(type(value) in {int, float} and low <= value <= high, "INVALID_TIMEOUT")

    @property
    def command_route(self) -> str:
        return f"a2a.v1.{self.tenant}.command.agent.{self.source}"

    @property
    def event_route(self) -> str:
        return f"a2a.v1.{self.tenant}.event.task"

    @classmethod
    def from_env(cls, env=None):
        env = os.environ if env is None else env
        return cls(
            sqlite_path=env.get("A2A_SQLITE_PATH", ""),
            app_token_file=env.get("APP_API_TOKEN_FILE", ""),
            dapr_token_file=env.get("DAPR_API_TOKEN_FILE", ""),
            jwt_keys_file=env.get("A2A_JWT_KEYS_FILE", ""),
            issuer=env.get("A2A_JWT_ISSUER", "https://local-a2amq.invalid"),
            public_url=env.get("A2A_PUBLIC_URL", "http://127.0.0.1:18000"),
            tenant=env.get("A2A_TENANT", "demo"),
            consumer=env.get("A2A_CONSUMER", "local"),
            source=env.get("A2A_SOURCE", "local-worker"),
            component=env.get("A2A_COMPONENT", "bus"),
            dapr_http=env.get("DAPR_HTTP_ENDPOINT", "http://127.0.0.1:3500"),
            mcp_enabled=_boolean(env, "A2A_ENABLE_MCP"),
            concurrency=_integer(env, "A2A_CONCURRENCY", 2, 1, 16),
            database_capacity=_integer(env, "A2A_DATABASE_CAPACITY", 4, 1, 32),
            max_active=_integer(env, "A2A_MAX_ACTIVE", 128, 1, 10000),
            max_records=_integer(env, "A2A_MAX_RECORDS", 10000, 1, 1000000),
        )


class LocalLiteTaskAPI(TaskAPI):
    """Same A2A surface with Workflow-only features rejected explicitly."""

    @staticmethod
    def _validate_local_request(request) -> None:
        require(isinstance(request, dict) and isinstance(request.get("message"), dict), "INVALID_REQUEST")
        require(not request["message"].get("taskId"), "WORKFLOW_FEATURE_UNAVAILABLE")
        policy = options(request)
        require(not policy["inputRequired"] and not policy["authRequired"]
                and policy["delaySeconds"] == 0, "WORKFLOW_FEATURE_UNAVAILABLE")

    async def send(self, principal, request):
        self._validate_local_request(request)
        return await super().send(principal, request)

    async def send_snapshots(self, principal, request):
        self._validate_local_request(request)
        async for item in super().send_snapshots(principal, request):
            yield item

    async def approve(self, principal, task_id, request_id, approved):
        raise PermanentError("WORKFLOW_FEATURE_UNAVAILABLE")


class LocalLiteActions:
    """Immediate durable executor; deliberately not a second Workflow engine."""

    def __init__(self, ledger: TaskLedger):
        self.ledger = ledger

    def _state(self, task_id: str) -> str:
        with self.ledger.store._connection() as db:
            return state_of(self.ledger._task(db, task_id))

    def _complete_start(self, action: Action) -> None:
        if self._state(action.task_id) in TERMINAL:
            self.ledger.finish(action.task_id)
            self.ledger.delivered(action)
            return
        command = self.ledger.command(action.task_id)
        payload = command.as_dict()["payload"]
        policy = options(payload)
        require(not policy["inputRequired"] and not policy["authRequired"]
                and policy["delaySeconds"] == 0, "WORKFLOW_FEATURE_UNAVAILABLE")
        parts = text_parts(payload["message"])
        self.ledger.step(action.task_id, "local-working", "WORKING")
        transformed = [value.upper() if policy["transform"] == "upper" else value for value in parts]
        artifacts = [{"artifactId": "result", "parts": [{"text": value} for value in transformed]}]
        self.ledger.step(action.task_id, "local-completed", "COMPLETED", artifacts=artifacts)
        self.ledger.finish(action.task_id)
        self.ledger.delivered(action)

    def _cancel(self, action: Action) -> None:
        self.ledger.verify_control(action.task_id, "cancel", load_json(action.body))
        if self._state(action.task_id) not in TERMINAL:
            self.ledger.step(action.task_id, "local-canceled", "CANCELED")
        self.ledger.finish(action.task_id)
        self.ledger.delivered(action)

    def dispatch(self, action: Action) -> None:
        require(isinstance(action, Action), "INVALID_CONTROL")
        if action.kind == "start":
            self._complete_start(action)
            return
        if action.kind == "cancel":
            self._cancel(action)
            return
        raise PermanentError("WORKFLOW_FEATURE_UNAVAILABLE")


@asynccontextmanager
async def _no_lifespan(app):
    yield


async def _close_local_resources(database: AsyncSQLite, bus: DaprPublisher) -> None:
    """Close app-owned async resources even when startup fails before yielding."""
    try:
        await database.aclose()
    finally:
        await bus.close()


def create_app(settings: LocalLiteSettings):
    app_token = read_file(settings.app_token_file, secret=True, limit=512).strip()
    dapr_token = read_file(settings.dapr_token_file, secret=True, limit=512).strip()
    require(16 <= len(app_token) <= 256 and 16 <= len(dapr_token) <= 256, "TOKEN_LENGTH_INVALID")

    store = SQLiteStore(settings.sqlite_path, tenant=settings.tenant, consumer=settings.consumer)
    database = AsyncSQLite(store, capacity=settings.database_capacity)
    bus = DaprPublisher(settings.dapr_http, component=settings.component, api_token=dapr_token,
                        timeout=settings.rpc_timeout)
    ledger = TaskLedger(store, source=settings.source, max_active=settings.max_active,
                        max_records=settings.max_records)
    api = LocalLiteTaskAPI(ledger, database, wait_timeout=settings.callback_timeout)
    actions = LocalLiteActions(ledger)
    identity = verifier(settings)

    async def accept(envelope: Envelope):
        await database.call(ledger.accept, envelope)

    running = {"value": False, "jobs": []}

    async def ready_value() -> bool:
        if not running["value"] or any(job.done() for job in running["jobs"]):
            return False
        try:
            await database.call(store.list_tasks, limit=1)
            return await bus.ready()
        except Exception:
            return False

    callback = subscriber(accept, bus, tenant=settings.tenant, consumer=settings.consumer,
                          topic=settings.command_route, app_token=app_token,
                          concurrency=settings.concurrency, readiness=ready_value,
                          lifespan=_no_lifespan, callback_timeout=settings.callback_timeout)

    operations = Operations(ledger)
    archive = DeadLetters(store, component="bus-dlq")
    dlq_callback = create_dlq_callback(archive, database, app_token=app_token,
                                       active=lambda: running["value"],
                                       timeout=settings.callback_timeout,
                                       concurrency=settings.concurrency)
    replay = ReplayService(archive, database, bus,
                           allowed_routes=frozenset({settings.command_route, settings.event_route}))
    ops_app = create_operations(api, identity, operations, archive, replay, database,
                                timeout=settings.callback_timeout)
    bridge = create_bridge(api, identity, public_url=settings.public_url, profile="local-lite",
                           allow_insecure_loopback=True)

    mcp_server = mcp_app = None
    if settings.mcp_enabled:
        from .mcp_server import create_mcp
        resource_url = settings.public_url + "/mcp"
        mcp_identity = verifier(settings, audience=resource_url)
        mcp_server, mcp_app = create_mcp(api, mcp_identity, resource_url=resource_url,
                                         profile="local-lite", allow_insecure_loopback=True)

    async def live(request):
        failed = running["value"] and any(job.done() for job in running["jobs"])
        return JSONResponse({"status": "failed" if failed else "alive"}, status_code=503 if failed else 200,
                            headers={"Cache-Control": "no-store"})

    async def ready(request):
        ok = await ready_value()
        return JSONResponse({"status": "ready" if ok else "unavailable"}, status_code=200 if ok else 503,
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
    ]
    if mcp_app is not None:
        routes.extend([
            Route("/mcp", ASGIEndpoint(mcp_app), methods=["GET", "POST", "DELETE", "OPTIONS"]),
            Route("/.well-known/oauth-protected-resource/mcp", ASGIEndpoint(mcp_app), methods=["GET", "OPTIONS"]),
        ])
    routes.append(Mount("/", bridge))

    @asynccontextmanager
    async def lifespan(app):
        stop = asyncio.Event()
        async with AsyncExitStack() as stack:
            stack.push_async_callback(_close_local_resources, database, bus)
            if mcp_server is not None:
                await stack.enter_async_context(mcp_server.session_manager.run())
            jobs = [
                asyncio.create_task(
                    ActionRelay(ledger, database, publisher=bus, actions=actions).run(
                        stop, settings.relay_interval
                    ),
                    name="a2amq-local-actions",
                ),
                asyncio.create_task(OutboxRelay(database, bus).run(stop, interval=settings.relay_interval),
                                    name="a2amq-local-outbox"),
                asyncio.create_task(AdmissionRepair(ledger).run(database, stop),
                                    name="a2amq-local-admission-repair"),
                asyncio.create_task(replay.run(stop), name="a2amq-local-replay"),
            ]
            running["jobs"], running["value"] = jobs, True
            try:
                yield
            finally:
                running["value"] = False
                stop.set()
                for job in jobs:
                    if not job.done():
                        job.cancel()
                await asyncio.gather(*jobs, return_exceptions=True)
                running["jobs"] = []

    app = Starlette(routes=routes, lifespan=lifespan)
    app.state.a2amq_api = api
    app.state.a2amq_ledger = ledger
    app.state.a2amq_profile = "local-lite"
    return app


def from_env():
    """Uvicorn factory for the three-process local profile: app + daprd + NATS."""
    return create_app(LocalLiteSettings.from_env())
