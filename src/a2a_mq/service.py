"""Authenticated single-host durable reference service; not the public A2A binding."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
import os
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse

from .domain import Envelope, PermanentError, RetryableError, require, token
from .runtime import AsyncSQLite, DurableEchoWorker, OutboxRelay, SnapshotObserver
from .storage import SQLiteStore
from .transport import DaprPublisher, subscriber


@dataclass(frozen=True)
class ServiceSettings:
    app_token: str = field(repr=False)
    role: str = "worker"
    target_kind: str = "skill"
    target: str = "echo"
    source: str | None = None
    concurrency: int = 4
    storage_capacity: int = 8
    callback_timeout: float = 30
    relay_interval: float = 1
    relay_batch_size: int = 50

    def __post_init__(self) -> None:
        require(isinstance(self.app_token, str) and 16 <= len(self.app_token) <= 256
                and all(33 <= ord(c) <= 126 for c in self.app_token), "APP_TOKEN_REQUIRED")
        require(isinstance(self.role, str) and self.role in {"worker", "observer"}, "INVALID_ROLE")
        require(isinstance(self.target_kind, str) and self.target_kind in {"skill", "agent"}, "INVALID_ROUTE")
        token(self.target)
        if self.source is not None:
            token(self.source)
        if self.role == "worker" and self.target_kind == "agent" and self.source is not None:
            require(self.source == self.target, "AGENT_SOURCE_MISMATCH")
        require(type(self.concurrency) is int and 1 <= self.concurrency <= 128, "INVALID_CONCURRENCY")
        require(type(self.storage_capacity) is int and 1 <= self.storage_capacity <= 128,
                "INVALID_STORAGE_CAPACITY")
        require(type(self.callback_timeout) in {int, float} and 0 < self.callback_timeout <= 300,
                "INVALID_TIMEOUT")
        require(type(self.relay_interval) in {int, float} and 0.01 <= self.relay_interval <= 60,
                "INVALID_RELAY_INTERVAL")
        require(type(self.relay_batch_size) is int and 1 <= self.relay_batch_size <= 200,
                "INVALID_PAGE_SIZE")


@dataclass
class _Runtime:
    database: AsyncSQLite
    handler: DurableEchoWorker | SnapshotObserver
    relay_task: asyncio.Task[None] | None


def create_app(store: SQLiteStore, bus: DaprPublisher, *, settings: ServiceSettings) -> FastAPI:
    """Lifespan owns the database lane and bus; every query uses one bound scope."""
    topic = (f"a2a.v1.{store.scope.tenant}.event.task" if settings.role == "observer" else
             f"a2a.v1.{store.scope.tenant}.command.{settings.target_kind}.{settings.target}")

    def active() -> _Runtime:
        runtime = app.state.runtime
        if runtime is None:
            raise RetryableError("SERVICE_NOT_RUNNING")
        return runtime

    async def handle(envelope: Envelope) -> None:
        await active().handler.handle(envelope)

    async def ready() -> bool:
        runtime = active()
        if settings.role == "worker" and (runtime.relay_task is None or runtime.relay_task.done()):
            return False
        await runtime.database.call(store.list_tasks, limit=1)
        return True

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        database = AsyncSQLite(store, capacity=settings.storage_capacity)
        relay_task = None
        stop = asyncio.Event()
        try:
            handler: DurableEchoWorker | SnapshotObserver
            if settings.role == "worker":
                source = settings.source if settings.source is not None else (
                    settings.target if settings.target_kind == "agent" else store.scope.consumer)
                handler = DurableEchoWorker(database, target_kind=settings.target_kind,
                                            target=settings.target, source=source)
                relay = OutboxRelay(database, bus, batch_size=settings.relay_batch_size)
                relay_task = asyncio.create_task(relay.run(stop, interval=settings.relay_interval))
            else:
                handler = SnapshotObserver(database)
            app.state.runtime = _Runtime(database, handler, relay_task)
            yield
        finally:
            app.state.runtime = None
            stop.set()
            if relay_task is not None:
                relay_task.cancel()
                await asyncio.gather(relay_task, return_exceptions=True)
            try:
                await database.aclose()
            finally:
                await bus.close()

    app = subscriber(handle, bus, tenant=store.scope.tenant, consumer=store.scope.consumer,
                     topic=topic, app_token=settings.app_token, concurrency=settings.concurrency,
                     callback_timeout=settings.callback_timeout, readiness=ready, lifespan=lifespan)
    app.state.runtime = None

    @app.exception_handler(RetryableError)
    async def unavailable(request: Request, error: RetryableError):
        return JSONResponse({"error": error.code}, status_code=503, headers={"Cache-Control": "no-store"})

    @app.exception_handler(PermanentError)
    async def invalid(request: Request, error: PermanentError):
        return JSONResponse({"error": error.code}, status_code=422, headers={"Cache-Control": "no-store"})

    async def query(function, **kwargs) -> Any:
        try:
            async with asyncio.timeout(settings.callback_timeout):
                return await active().database.call(function, **kwargs)
        except TimeoutError:
            raise RetryableError("QUERY_TIMEOUT") from None

    @app.get("/internal/tasks")
    async def list_tasks(request: Request, context_id: str | None = None, state: str | None = None,
                         after: str | None = None, limit: int = Query(50, ge=1, le=200)):
        require(set(request.query_params) <= {"context_id", "state", "after", "limit"}, "INVALID_QUERY")
        tasks, cursor = await query(store.list_tasks, context_id=context_id, state=state, after=after, limit=limit)
        return JSONResponse({"tasks": tasks, "nextCursor": cursor}, headers={"Cache-Control": "no-store"})

    @app.get("/internal/tasks/{task_id:path}")
    async def get_task(task_id: str, request: Request):
        require(not request.query_params, "INVALID_QUERY")
        task = await query(store.get_task, task_id=task_id)
        return JSONResponse(task if task is not None else {"error": "TASK_NOT_FOUND"},
                            status_code=200 if task is not None else 404,
                            headers={"Cache-Control": "no-store"})

    return app


def from_env() -> FastAPI:
    """Use with uvicorn a2a_mq.service:from_env --factory on a trusted local host."""
    settings = ServiceSettings(app_token=os.getenv("APP_API_TOKEN", ""),
                               role=os.getenv("A2A_ROLE", "worker"),
                               target_kind=os.getenv("A2A_TARGET_KIND", "skill"),
                               target=os.getenv("A2A_TARGET", "echo"),
                               source=os.getenv("A2A_SOURCE"))
    path = os.getenv("A2A_SQLITE_PATH", "")
    require(bool(path), "STORAGE_PATH_REQUIRED")
    tenant = token(os.getenv("A2A_TENANT", "demo"))
    consumer = token(os.getenv("A2A_CONSUMER", "worker-echo"))
    store = SQLiteStore(path, tenant=tenant, consumer=consumer)
    bus = DaprPublisher(os.getenv("DAPR_HTTP_ENDPOINT", "http://127.0.0.1:3500"),
                        api_token=os.getenv("DAPR_API_TOKEN", ""))
    return create_app(store, bus, settings=settings)
