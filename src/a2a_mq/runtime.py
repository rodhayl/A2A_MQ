"""Bounded asynchronous integration for the single-host SQL echo reference.

Only deterministic echo + transactional SQL effects run here. This is not a
workflow scheduler or an executor for arbitrary external tools.
"""
from __future__ import annotations

import asyncio
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
import logging
from typing import Any, ParamSpec, Protocol, TypeVar
from uuid import uuid4

from .domain import TERMINAL, Envelope, RetryableError, canonical, require, state_of, token
from .reference import echo_events
from .storage import SQLiteStore, MAX_PAGE

P = ParamSpec("P")
T = TypeVar("T")
LOG = logging.getLogger("a2a_mq.runtime")


class Publisher(Protocol):
    async def publish(self, envelope: Envelope) -> None: ...


class AsyncSQLite:
    """A bounded, single-thread database lane, owned by one asyncio event loop.

    Capacity counts actual submitted futures, including those whose caller was
    cancelled while SQLite was still running. Cancellation never means rollback.
    Call aclose() in application lifespan shutdown to drain started operations.
    """

    def __init__(self, store: SQLiteStore, *, capacity: int = 8):
        require(type(capacity) is int and 1 <= capacity <= 128, "INVALID_STORAGE_CAPACITY")
        self.store = store
        self._capacity = capacity
        self._pending: set[Future[Any]] = set()
        self._pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="a2amq-sqlite")
        self._loop: asyncio.AbstractEventLoop | None = None
        self._closed = False
        self._shutdown: asyncio.Task[None] | None = None

    def _bind(self) -> asyncio.AbstractEventLoop:
        loop = asyncio.get_running_loop()
        require(self._loop is None or self._loop is loop, "STORAGE_EVENT_LOOP_MISMATCH")
        self._loop = loop
        return loop

    async def call(self, function: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
        loop = self._bind()
        if self._closed:
            raise RetryableError("STORAGE_CLOSED")
        if len(self._pending) >= self._capacity:
            raise RetryableError("STORAGE_OVERLOADED")
        # No await between checking capacity and submission: admission is atomic
        # on this event loop. A worker callback releases capacity only on completion.
        future = self._pool.submit(function, *args, **kwargs)
        self._pending.add(future)
        future.add_done_callback(lambda done: loop.call_soon_threadsafe(self._pending.discard, done))
        try:
            return await asyncio.wrap_future(future, loop=loop)
        except asyncio.CancelledError:
            future.cancel()  # Cancels queued work only; running SQL may still commit.
            raise

    async def aclose(self) -> None:
        self._bind()
        self._closed = True
        if self._shutdown is None:
            self._shutdown = asyncio.create_task(
                asyncio.to_thread(self._pool.shutdown, wait=True, cancel_futures=True)
            )
        await asyncio.shield(self._shutdown)


class DurableEchoWorker:
    """ACK-ready only after a full terminal result is durable in the local store."""

    def __init__(self, database: AsyncSQLite, *, target_kind: str = "skill",
                 target: str = "echo", source: str | None = None,
                 lease_seconds: float = 30):
        require(isinstance(target_kind, str) and target_kind in {"skill", "agent"}, "INVALID_ROUTE")
        require(type(lease_seconds) in {int, float} and 0 < lease_seconds <= 3600, "INVALID_LEASE")
        self.database = database
        self.source = token(source if source is not None else database.store.scope.consumer)
        self.topic = f"a2a.v1.{database.store.scope.tenant}.command.{target_kind}.{token(target)}"
        if target_kind == "agent":
            require(self.source == target, "AGENT_SOURCE_MISMATCH")
        self.owner = str(uuid4())  # Physical lease owner; never part of stable event IDs.
        self.lease_seconds = lease_seconds

    def _execute(self, request: Envelope) -> None:
        claim = self.database.store.claim(request, self.owner, lease_seconds=self.lease_seconds)
        if claim.status == "busy":
            raise RetryableError("CLAIM_BUSY")
        if claim.status == "completed":
            if claim.result is None or state_of(claim.result.as_dict()["payload"]) not in TERMINAL:
                raise RetryableError("STORAGE_INTEGRITY_ERROR")
            return
        # Emit the complete terminal snapshot only. Intermediate fixture states
        # have not been durably observed and are not presented as workflow progress.
        outcome = echo_events(request, self.source)[-1]
        effect = {"echo": outcome.as_dict()["payload"]["artifacts"][0]["parts"][0]["text"]}
        self.database.store.commit_claim(claim, effect_key=claim.message_id, effect=effect, result=outcome)

    async def handle(self, request: Envelope) -> None:
        require(isinstance(request, Envelope), "INVALID_ENVELOPE")
        require(request.route == self.topic, "UNAUTHORIZED_ROUTE")
        await self.database.call(self._execute, request)


class SnapshotObserver:
    def __init__(self, database: AsyncSQLite):
        self.database = database
        self.topic = f"a2a.v1.{database.store.scope.tenant}.event.task"

    async def handle(self, event: Envelope) -> None:
        require(isinstance(event, Envelope) and event.route == self.topic, "UNAUTHORIZED_ROUTE")
        await self.database.call(self.database.store.apply_snapshot, event)


@dataclass(frozen=True)
class RelayResult:
    scanned: int
    published: int
    failed: int


class OutboxRelay:
    """At-least-once relay; only accepted publications are marked as sent.

    A UUID cursor advances past normal publish failures and wraps each sweep.
    It is an optimization, not an acknowledgment. Restarting may repeat a publish.
    """

    def __init__(self, database: AsyncSQLite, publisher: Publisher, *, batch_size: int = 50,
                 publish_timeout: float = 10):
        require(type(batch_size) is int and 1 <= batch_size <= MAX_PAGE, "INVALID_PAGE_SIZE")
        require(type(publish_timeout) in {int, float} and 0 < publish_timeout <= 300, "INVALID_TIMEOUT")
        self.database, self.publisher = database, publisher
        self.batch_size, self.publish_timeout = batch_size, publish_timeout
        self._cursor: str | None = None
        self._lock = asyncio.Lock()

    async def flush_once(self) -> RelayResult:
        async with self._lock:
            items = await self.database.call(self.database.store.pending_outbox,
                                             limit=self.batch_size, after=self._cursor)
            published = failed = 0
            for item in items:
                try:
                    active = await self.database.call(self.database.store.record_publish_attempt, item)
                    if not active:
                        continue
                    async with asyncio.timeout(self.publish_timeout):
                        await self.publisher.publish(item.envelope)
                    await self.database.call(self.database.store.mark_published, item)
                    published += 1
                except Exception:
                    failed += 1
                    # No task text, credentials, exception text or raw envelope in logs.
                    LOG.warning(canonical({"event": "outbox_retry", "reason": "PUBLISH_OR_MARK_FAILED",
                                           "consumer": item.scope.consumer,
                                           "messageId": item.envelope.as_dict()["messageId"]}))
            self._cursor = (items[-1].envelope.as_dict()["messageId"]
                            if len(items) == self.batch_size else None)
            return RelayResult(len(items), published, failed)

    async def run(self, stop: asyncio.Event, *, interval: float = 1) -> None:
        require(type(interval) in {int, float} and 0.01 <= interval <= 60, "INVALID_RELAY_INTERVAL")
        while not stop.is_set():
            try:
                await self.flush_once()
            except Exception:
                LOG.warning(canonical({"event": "outbox_retry", "reason": "OUTBOX_UNAVAILABLE"}))
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
            except TimeoutError:
                pass
