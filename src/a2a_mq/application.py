"""Role-based assembly of the implemented SQL, Dapr, A2A, MCP and operations adapters.

Only Dapr schedules workflows. Background tasks below dispatch durable SQL records
or reconcile Dapr results; none acts as a competing orchestration engine.
"""
from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
import logging
import os
from typing import Callable

import httpx
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

from . import configuration
from .configuration import Settings, read_file
from .deadletters import DeadLetters, ReplayService
from .dlq_callback import create_dlq_callback
from .domain import PermanentError, RetryableError, canonical, load_json, require
from .identity import Principal
from .maintenance import Operations
from .operations_http import create_operations
from .recovery import AdmissionRepair
from .runtime import AsyncSQLite, OutboxRelay
from .task_api import TaskAPI
from .tasks import TaskLedger
from .transport import DaprPublisher, _AppTokenMiddleware, subscriber
from .vault import FileToken, TransitKey, VaultTransit
from .workflows import ActionRelay, DaprActions, register

LOG = logging.getLogger("a2a_mq.application")


def official_workflow(settings, ledger, api_token):
    """Configure deadlines BEFORE SDK import; reject an already cached mismatch."""
    os.environ["DAPR_API_TOKEN"] = api_token
    os.environ["DAPR_API_TIMEOUT_SECONDS"] = str(settings.rpc_timeout)
    from dapr.conf import settings as dapr_settings
    from dapr.ext.workflow import DaprWorkflowClient, WorkflowRuntime
    from dapr.ext.workflow.logger import LoggerOptions
    from .telemetry import MetadataHandler
    require(dapr_settings.DAPR_API_TOKEN == api_token
            and float(dapr_settings.DAPR_API_TIMEOUT_SECONDS) == settings.rpc_timeout,
            "DAPR_SETTINGS_ALREADY_LOADED")
    logger = LoggerOptions(log_handler=MetadataHandler())
    runtime = WorkflowRuntime(host="127.0.0.1", port=str(settings.dapr_grpc_port), logger_options=logger,
        maximum_concurrent_activity_work_items=settings.concurrency,
        maximum_concurrent_orchestration_work_items=settings.concurrency,
        maximum_thread_pool_workers=settings.concurrency, worker_ready_timeout=30)
    try:
        register(runtime, ledger)
        client = DaprWorkflowClient(host="127.0.0.1", port=str(settings.dapr_grpc_port), logger_options=logger)
    except BaseException:
        runtime.shutdown()
        raise
    return runtime, client


def official_bridge(api, verifier, *, public_url):
    from .bridge import create_bridge
    return create_bridge(api, verifier, public_url=public_url)


def official_mcp(api, verifier, *, resource_url):
    from .mcp_server import create_mcp
    return create_mcp(api, verifier, resource_url=resource_url)


@dataclass
class Dependencies:
    """Explicit test seam. The environment entry point always uses real adapters."""
    store: Callable = configuration.store
    workflow: Callable = official_workflow
    bridge: Callable = official_bridge
    mcp: Callable = official_mcp
    bus: Callable = DaprPublisher
    verifier: Callable = configuration.verifier


async def _sleep(stop, seconds):
    try:
        await asyncio.wait_for(stop.wait(), seconds)
    except TimeoutError:
        pass


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
                    LOG.warning('{"event":"workflow_reconcile_retry"}')
            cursor = ids[-1] if len(ids) == 50 else None
        except Exception:
            LOG.warning('{"event":"workflow_reconcile_unavailable"}')
        await _sleep(stop, interval)


async def _metrics(operations, database, telemetry, stop):
    principal = Principal("internal-metrics", operations.store.scope.tenant, frozenset({"a2a:operate"}))
    while not stop.is_set():
        try:
            telemetry.update_backlog(await database.call(operations.backlog, principal))
        except Exception:
            telemetry.update_backlog({})
        await _sleep(stop, 10)


async def _protectors(settings, cleanup):
    if not settings.crypto_file:
        return {}
    from .protection import EnvelopeProtector
    config = load_json(read_file(settings.crypto_file))
    required = {"vaultUrl", "tokenFile", "command", "event"}
    allowed = required | {"caFile", "mount", "namespace"}
    require(isinstance(config, dict) and required <= set(config) <= allowed,
            "INVALID_CRYPTO_CONFIG")
    for kind in ("command", "event"):
        item = config.get(kind)
        require(isinstance(item, dict) and set(item) == {"receiver", "active", "keys"}
                and isinstance(item["keys"], list), "INVALID_CRYPTO_CONFIG")
        for key in item["keys"]:
            key_required = {"id", "name", "writeVersion"}
            key_allowed = key_required | {"minReadVersion"}
            require(isinstance(key, dict) and key_required <= set(key) <= key_allowed,
                    "INVALID_CRYPTO_CONFIG")
    token_provider = FileToken(config["tokenFile"])
    protectors = {}
    for kind, route in (("command", settings.command_route), ("event", settings.event_route)):
        item = config.get(kind)
        keys = []
        for key in item["keys"]:
            keys.append(TransitKey(key["id"], key["name"], settings.tenant, item["receiver"],
                                   key["writeVersion"], key.get("minReadVersion", 1)))
        import ssl
        context = ssl.create_default_context(cafile=config.get("caFile") or None)
        client = httpx.AsyncClient(verify=context, trust_env=False, follow_redirects=False)
        cleanup.push_async_callback(client.aclose)
        provider = VaultTransit(config["vaultUrl"], token_provider, keys, item["active"],
            mount=config.get("mount", "transit"), namespace=config.get("namespace", ""), client=client)
        protectors[route] = EnvelopeProtector(provider, tenant=settings.tenant, receiver=item["receiver"])
    # Validate the mounted credential only after every static crypto/Vault setting
    # has passed its canonical constructor checks. The provider still reads the
    # file again for each Vault call so rotation remains live.
    token_provider()
    return protectors


class ServiceApplication:
    """Fail closed outside lifespan, including after a partial startup failure."""
    def __init__(self, settings: Settings, *, dependencies: Dependencies | None = None):
        self.settings, self.dependencies = settings, dependencies or Dependencies()
        self.running, self.delegate, self.jobs = False, None, []
        self.workflow_ready = False
        self.bus = self.database = self.runtime = None
        self.app = Starlette(lifespan=self.lifespan, routes=[
            Route("/health/live", self.live), Route("/health/ready", self.ready), Mount("/", self.dispatch)])

    async def __call__(self, scope, receive, send):
        await self.app(scope, receive, send)

    async def live(self, request):
        failed = self.running and any(t.done() for t in self.jobs)
        return JSONResponse({"status": "failed" if failed else "alive"}, status_code=503 if failed else 200,
                            headers={"Cache-Control": "no-store"})

    async def ready(self, request):
        valid = False
        try:
            values = request.headers.getlist("dapr-api-token")
            import hmac
            valid = len(values) == 1 and hmac.compare_digest(values[0].encode(), self.app_token.encode())
        except (AttributeError, UnicodeError):
            pass
        if not valid:
            return JSONResponse({"error": "UNAUTHORIZED"}, status_code=401)
        ok = self.running and not any(t.done() for t in self.jobs)
        ok = ok and (self.settings.role != "workflow" or self.workflow_ready)
        if ok:
            try:
                async with asyncio.timeout(self.settings.callback_timeout):
                    await self.database.call(self.database.store.list_tasks, limit=1)
                    ok = await self.bus.ready()
                    if ok and self.runtime is not None:
                        ok = await asyncio.to_thread(self.runtime.wait_for_worker_ready, timeout=.1)
            except Exception:
                ok = False
        return JSONResponse({"status": "ready" if ok else "unavailable"}, status_code=200 if ok else 503,
                            headers={"Cache-Control": "no-store"})

    async def dispatch(self, scope, receive, send):
        if not self.running or self.delegate is None:
            if scope["type"] == "http":
                await JSONResponse({"error": "SERVICE_NOT_RUNNING"}, status_code=503)(scope, receive, send)
            elif scope["type"] == "websocket":
                await send({"type": "websocket.close", "code": 1013})
            return
        await self.delegate(scope, receive, send)

    async def run_runtime(self, runtime, stop):
        # Dapr may wait for the HTTP app port. Do not wait for Dapr's worker
        # stream before allowing Uvicorn to bind that port (startup deadlock).
        start = asyncio.create_task(asyncio.to_thread(runtime.start))
        try:
            await asyncio.shield(start)
            self.workflow_ready = True
            await stop.wait()
        finally:
            self.workflow_ready = False
            if not start.done():
                try:
                    await asyncio.shield(start)
                except Exception:
                    pass

    @asynccontextmanager
    async def lifespan(self, app):
        require(not self.running and self.delegate is None, "SERVICE_ALREADY_RUNNING")
        settings, deps = self.settings, self.dependencies
        stop = asyncio.Event()
        self.jobs = []
        async with AsyncExitStack() as cleanup:
            self.app_token = FileToken(settings.app_token_file)()
            api_token = FileToken(settings.dapr_token_file)()
            require(16 <= len(self.app_token) <= 256 and 16 <= len(api_token) <= 256, "TOKEN_LENGTH_INVALID")
            store = deps.store(settings)
            if hasattr(store, "close"):
                cleanup.push_async_callback(asyncio.to_thread, store.close)
            self.database = database = AsyncSQLite(store, capacity=settings.database_capacity)
            cleanup.push_async_callback(database.aclose)
            self.bus = bus = deps.bus(settings.dapr_http, component=settings.component, api_token=api_token,
                                      timeout=settings.rpc_timeout)
            cleanup.push_async_callback(bus.close)
            ledger = TaskLedger(store, source=settings.source, max_active=settings.max_active,
                                max_records=settings.max_records)
            self.ledger = ledger
            self.api = api = TaskAPI(ledger, database, wait_timeout=settings.callback_timeout)
            self.operations = operations = Operations(ledger)
            archive = DeadLetters(store, component=settings.component)
            protectors = await _protectors(settings, cleanup)
            publisher = bus
            if protectors:
                from .secure_transport import ProtectedPublisher
                publisher = ProtectedPublisher(bus, database, protectors)
            telemetry = None
            if settings.otlp_endpoint:
                from .telemetry import OTLP
                export = OTLP(settings.otlp_endpoint, service_name="a2amq-" + settings.role,
                              ca_file=settings.otlp_ca_file or None)
                cleanup.push_async_callback(asyncio.to_thread, export.close)
                telemetry = export.telemetry
                publisher = telemetry.publisher(publisher)

            async def decode(ce):
                header = ce.get("data", {}).get("protected", {})
                route = header.get("route") if isinstance(header, dict) else None
                require(route in protectors, "ENCRYPTION_ROUTE_NOT_AUTHORIZED")
                return await protectors[route].open(canonical(ce["data"]).encode(), expected_route=route)

            replay = ReplayService(archive, database, bus,
                allowed_routes=frozenset({settings.command_route, settings.event_route}),
                decoder=decode if protectors else None)
            coroutines = []
            try:
                if settings.role in {"api", "operator"}:
                    verifier = deps.verifier(settings)
                    ops = create_operations(api, verifier, operations, archive, replay, database,
                                            timeout=settings.callback_timeout)
                    async def no_subscriptions(request):
                        return JSONResponse([])
                    empty_discovery = _AppTokenMiddleware(Starlette(routes=[
                        Route("/dapr/subscribe", no_subscriptions)]), self.app_token)
                    routes = [Mount("/operations", ops)]
                    if settings.role == "api":
                        routes.append(Route("/dapr/subscribe", empty_discovery))
                    if settings.role == "api":
                        if settings.mcp_enabled:
                            resource_url = settings.public_url.rstrip("/") + "/mcp"
                            mcp_verifier = deps.verifier(settings, audience=resource_url)
                            server, mcp_app = deps.mcp(api, mcp_verifier, resource_url=resource_url)
                            await cleanup.enter_async_context(server.session_manager.run())
                            routes.extend([Route("/mcp", ASGIEndpoint(mcp_app), methods=["GET", "POST", "DELETE", "OPTIONS"]),
                                Route("/.well-known/oauth-protected-resource/mcp", ASGIEndpoint(mcp_app), methods=["GET", "OPTIONS"])])
                        routes.append(Mount("/", deps.bridge(api, verifier, public_url=settings.public_url)))
                        coroutines.append(ActionRelay(ledger, database, publisher=publisher).run(stop, settings.relay_interval))
                        coroutines.append(AdmissionRepair(ledger).run(database, stop))
                    else:
                        callback = create_dlq_callback(archive, database, app_token=self.app_token,
                            active=lambda: self.running, timeout=settings.callback_timeout, concurrency=settings.concurrency)
                        routes.append(Mount("/", callback))
                        coroutines.append(replay.run(stop))
                    self.delegate = Starlette(routes=routes)
                else:
                    if settings.role == "workflow":
                        runtime, client = deps.workflow(settings, ledger, api_token)
                        self.runtime = runtime
                        cleanup.push_async_callback(asyncio.to_thread, client.close)
                        cleanup.push_async_callback(database.aclose)
                        # Register shutdown before start: startup failure also tears down threads.
                        cleanup.push_async_callback(asyncio.to_thread, runtime.shutdown)
                        coroutines.append(self.run_runtime(runtime, stop))
                        actions = DaprActions(ledger, client)

                        async def handler(envelope):
                            if not self.running or not self.workflow_ready:
                                raise RetryableError("SERVICE_NOT_RUNNING")
                            await database.call(actions.ensure_started, envelope)
                        topic = settings.command_route
                        coroutines.extend([ActionRelay(ledger, database, actions=actions).run(stop, settings.relay_interval),
                            OutboxRelay(database, publisher).run(stop, interval=settings.relay_interval),
                            _reconcile(ledger, database, actions, stop, settings.relay_interval)])
                    else:
                        from .projections import ProjectionPolicy
                        projection = ProjectionPolicy(store, max_records=settings.max_records)

                        async def handler(envelope):
                            if not self.running:
                                raise RetryableError("SERVICE_NOT_RUNNING")
                            await database.call(projection.apply, envelope)
                        topic = settings.event_route
                    self.delegate = subscriber(telemetry.handler(handler) if telemetry else handler, bus,
                        tenant=settings.tenant, consumer=settings.consumer, topic=topic, app_token=self.app_token,
                        callback_timeout=settings.callback_timeout, concurrency=settings.concurrency,
                        protector=protectors.get(topic))
                    if settings.subscription_queue:
                        self.delegate = SubscriptionMetadata(self.delegate, settings.subscription_queue)
                    # Observer queries are internal token-protected projection queries, not public owner APIs.
                    if settings.role == "observer":
                        async def projected(request):
                            raw = request.query_params.get("limit", "50")
                            try:
                                require(raw.isascii() and raw.isdigit() and len(raw) <= 3, "INVALID_PAGE_SIZE")
                                require(set(request.query_params) <= {"limit", "after"}, "INVALID_QUERY")
                                async with asyncio.timeout(settings.callback_timeout):
                                    tasks, cursor = await database.call(store.list_tasks, limit=int(raw),
                                        after=request.query_params.get("after"))
                            except PermanentError as error:
                                return JSONResponse({"error": error.code}, status_code=422,
                                                    headers={"Cache-Control": "no-store"})
                            except (RetryableError, TimeoutError):
                                return JSONResponse({"error": "STORAGE_UNAVAILABLE"}, status_code=503,
                                                    headers={"Cache-Control": "no-store"})
                            return JSONResponse({"tasks": tasks, "nextCursor": cursor}, headers={"Cache-Control": "no-store"})
                        self.delegate = Starlette(routes=[Route("/internal/tasks", _AppTokenMiddleware(
                            Starlette(routes=[Route("/internal/tasks", projected)]), self.app_token)), Mount("/", self.delegate)])
                if settings.role == "operator" and settings.subscription_queue:
                    self.delegate = SubscriptionMetadata(self.delegate, settings.subscription_queue)
                if telemetry:
                    coroutines.append(_metrics(operations, database, telemetry, stop))
                self.jobs = [asyncio.create_task(coro) for coro in coroutines]
                coroutines.clear()
                self.running = True
                yield
            finally:
                self.running = False
                stop.set()
                for coro in coroutines:
                    coro.close()
                for job in self.jobs:
                    job.cancel()
                if self.jobs:
                    await asyncio.gather(*self.jobs, return_exceptions=True)
                self.delegate = None
        self.runtime = None


class ASGIEndpoint:
    """Force Starlette Route to treat a three-argument callable as ASGI, not a view."""
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        await self.app(scope, receive, send)


class SubscriptionMetadata:
    """Attach the explicit RabbitMQ quorum queue identity to programmatic discovery."""
    def __init__(self, app, queue):
        self.app, self.queue = app, queue

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["path"] != "/dapr/subscribe":
            return await self.app(scope, receive, send)
        start, chunks = None, []
        async def capture(message):
            nonlocal start
            if message["type"] == "http.response.start":
                start = message
            elif message["type"] == "http.response.body":
                chunks.append(message.get("body", b""))
        await self.app(scope, receive, capture)
        if start is not None and start["status"] == 200:
            values = load_json(b"".join(chunks))
            require(isinstance(values, list), "INVALID_SUBSCRIPTIONS")
            for item in values:
                item["metadata"] = {**item.get("metadata", {}), "queueName": self.queue, "queueType": "quorum"}
            return await JSONResponse(values)(scope, receive, send)
        if start is not None:
            await send(start)
            await send({"type": "http.response.body", "body": b"".join(chunks)})


def from_env():
    return ServiceApplication(Settings.from_env())
