"""Authorized task operations shared by the official A2A and MCP adapters."""
from __future__ import annotations

import asyncio
import base64
from typing import Any

from .domain import (PermanentError, RetryableError, STATES, TERMINAL, canonical,
                     load_json, require, state_of, timestamp)
from .identity import Principal
from .storage import _key
from .tasks import TaskLedger, digest, stable_id

INTERRUPTED = {"INPUT_REQUIRED", "AUTH_REQUIRED"}


def history(task: dict, length: int | None) -> dict:
    task = load_json(canonical(task))
    if length is not None:
        require(type(length) is int and 0 <= length <= 1000, "INVALID_HISTORY_LENGTH")
        if length == 0:
            task.pop("history", None)
        elif "history" in task:
            task["history"] = task["history"][-length:]
    return task


def configuration(request: dict) -> dict:
    require(isinstance(request.get("message"), dict), "INVALID_MESSAGE")
    value = request.get("configuration", {})
    require(isinstance(value, dict), "INVALID_CONFIGURATION")
    if "taskPushNotificationConfig" in value:
        raise PermanentError("PUSH_NOTIFICATIONS_UNSUPPORTED")
    require(set(value) <= {"historyLength", "acceptedOutputModes", "returnImmediately"}, "INVALID_CONFIGURATION")
    if "returnImmediately" in value:
        require(type(value["returnImmediately"]) is bool, "INVALID_CONFIGURATION")
    history({}, value.get("historyLength"))
    modes = value.get("acceptedOutputModes", [])
    require(isinstance(modes, list) and len(modes) <= 16
            and all(isinstance(mode, str) for mode in modes), "INVALID_CONFIGURATION")
    require(not modes or "text/plain" in modes, "CONTENT_TYPE_UNSUPPORTED")
    return value


class TaskQuery:
    """Stable updated-time descending keyset query; every page reauthorizes scope."""

    def __init__(self, ledger: TaskLedger):
        self.ledger = ledger

    def list(self, principal: Principal, *, page_size: int = 50, page_token: str = "",
             context_id: str | None = None, status: str | None = None,
             status_timestamp_after: str | None = None,
             history_length: int | None = None, include_artifacts: bool = False) -> dict:
        principal.authorize("read", self.ledger.store.scope.tenant)
        require(type(page_size) is int and 1 <= page_size <= 100, "INVALID_PAGE_SIZE")
        require(type(include_artifacts) is bool, "INVALID_QUERY")
        history({}, history_length)
        if context_id is not None:
            _key(context_id)
        if status is not None:
            require(isinstance(status, str) and status in STATES, "INVALID_STATE")
        after_time = None
        if status_timestamp_after is not None:
            require(isinstance(status_timestamp_after, str), "INVALID_TIME")
            after_time = timestamp(status_timestamp_after).timestamp()
        scope_hash = digest([*self.ledger.keys, principal.subject,
                             "a2a:admin" in principal.scopes, context_id, status, after_time])
        cursor = None
        require(isinstance(page_token, str) and len(page_token) <= 2048, "INVALID_PAGE_TOKEN")
        if page_token:
            try:
                raw = base64.b64decode(page_token, altchars=b"-_", validate=True)
                cursor = load_json(raw, 1024)
                require(isinstance(cursor, dict) and set(cursor) == {"scope", "time", "id"}
                        and cursor["scope"] == scope_hash, "INVALID_PAGE_TOKEN")
                require(type(cursor["time"]) in {int, float} and cursor["time"] >= 0, "INVALID_PAGE_TOKEN")
                _key(cursor["id"])
            except (ValueError, TypeError, KeyError):
                raise PermanentError("INVALID_PAGE_TOKEN") from None
        clauses = ["j.tenant=?", "j.consumer=?"]
        values: list[Any] = list(self.ledger.keys)
        if "a2a:admin" not in principal.scopes:
            clauses.append("j.subject=?")
            values.append(principal.subject)
        if context_id is not None:
            clauses.append("t.context_id=?")
            values.append(context_id)
        if status is not None:
            clauses.append("t.state=?")
            values.append(status)
        if after_time is not None:
            clauses.append("j.updated_at>=?")
            values.append(after_time)
        join = (" FROM a2amq_jobs j JOIN a2amq_tasks t ON j.tenant=t.tenant "
                "AND j.consumer=t.consumer AND j.task_id=t.task_id WHERE ")
        where = " AND ".join(clauses)
        with self.ledger.store._transaction() as db:
            total = db.execute("SELECT COUNT(*)" + join + where, values).fetchone()[0]
            if cursor is not None:
                where += " AND (j.updated_at<? OR (j.updated_at=? AND j.task_id<?))"
                values.extend([cursor["time"], cursor["time"], cursor["id"]])
            rows = db.execute("SELECT j.task_id,j.updated_at,t.payload_json" + join + where
                              + " ORDER BY j.updated_at DESC,j.task_id DESC LIMIT ?",
                              (*values, page_size + 1)).fetchall()
        selected = rows[:page_size]
        next_token = ""
        if len(rows) > page_size:
            last = selected[-1]
            next_token = base64.urlsafe_b64encode(canonical({"scope": scope_hash,
                "time": last["updated_at"], "id": last["task_id"]}).encode()).decode()
        tasks = []
        for row in selected:
            task = history(load_json(row["payload_json"]), history_length)
            if not include_artifacts:
                task.pop("artifacts", None)
            tasks.append(task)
        return {"tasks": tasks, "nextPageToken": next_token, "pageSize": page_size, "totalSize": total}


class TaskAPI:
    """Bounded asynchronous facade. A wait timeout does not cancel durable work."""

    def __init__(self, ledger: TaskLedger, database, *, wait_timeout: float = 30,
                 poll_interval: float = .25, stream_timeout: float = 300, max_streams: int = 32):
        require(type(wait_timeout) in {int, float} and 0 < wait_timeout <= 300, "INVALID_TIMEOUT")
        require(type(stream_timeout) in {int, float} and 0 < stream_timeout <= 3600, "INVALID_TIMEOUT")
        require(type(poll_interval) in {int, float} and .005 <= poll_interval <= 10, "INVALID_INTERVAL")
        require(type(max_streams) is int and 1 <= max_streams <= 1024, "INVALID_CAPACITY")
        self.ledger, self.database, self.query = ledger, database, TaskQuery(ledger)
        self.wait_timeout, self.poll_interval = wait_timeout, poll_interval
        self.stream_timeout, self.max_streams, self._streams = stream_timeout, max_streams, 0

    async def _call(self, function, *args, **kwargs):
        try:
            async with asyncio.timeout(self.wait_timeout):
                return await self.database.call(function, *args, **kwargs)
        except TimeoutError:
            raise RetryableError("TASK_API_TIMEOUT") from None

    async def _get(self, principal, task_id):
        principal.authorize("read", self.ledger.store.scope.tenant)
        try:
            return await self._call(self.ledger.get, principal, task_id)
        except PermanentError as error:
            if error.code == "FORBIDDEN":
                raise PermanentError("TASK_NOT_FOUND") from None
            raise

    async def get(self, principal, task_id, *, history_length=None):
        history({}, history_length)
        return history(await self._get(principal, task_id), history_length)

    async def list(self, principal, **options):
        return await self._call(self.query.list, principal, **options)

    async def _wait(self, principal, task_id, *, initial=None, canceled=False, change_only=False):
        try:
            async with asyncio.timeout(self.wait_timeout):
                while True:
                    task = await self._get(principal, task_id)
                    state = state_of(task)
                    changed = initial is None or canonical(task) != canonical(initial)
                    if change_only and changed:
                        return task
                    if canceled and state in TERMINAL:
                        require(state == "CANCELED", "TASK_NOT_CANCELABLE")
                        return task
                    if not canceled and changed and state in TERMINAL | INTERRUPTED:
                        return task
                    await asyncio.sleep(self.poll_interval)
        except TimeoutError:
            raise RetryableError("TASK_WAIT_TIMEOUT") from None

    async def send(self, principal, request):
        require(isinstance(request, dict), "INVALID_REQUEST")
        config = configuration(request)
        immediate = config.get("returnImmediately", False)
        # Submission is authorized by ledger.submit() with a2a:send. A caller that
        # asks this method to wait for later task state must also hold a2a:read,
        # and that check happens before durable admission to avoid a side effect
        # followed by an authorization failure.
        if not immediate:
            principal.authorize("read", self.ledger.store.scope.tenant)
        request = load_json(canonical(request))
        continuation = bool(request.get("message", {}).get("taskId"))
        task = await self._call(self.ledger.submit, principal, request)
        seen = any(m.get("messageId") == request["message"]["messageId"] for m in task.get("history", []))
        if not immediate:
            task = await self._wait(principal, task["id"],
                                    initial=task if continuation and not seen and state_of(task) in INTERRUPTED else None)
        return history(task, config.get("historyLength"))

    async def cancel(self, principal, task_id):
        principal.authorize("cancel", self.ledger.store.scope.tenant)
        task = await self._get(principal, task_id)
        if state_of(task) == "CANCELED":
            return task
        require(state_of(task) not in TERMINAL, "TASK_NOT_CANCELABLE")
        request_id = stable_id(principal.subject, task_id, "cancel")
        await self._call(self.ledger.control, principal, task_id, "cancel", request_id)
        return await self._wait(principal, task_id, canceled=True)

    async def approve(self, principal, task_id, request_id, approved):
        principal.authorize("approve", self.ledger.store.scope.tenant)
        await self._get(principal, task_id)
        return await self._call(self.ledger.control, principal, task_id, "auth", request_id, approved)

    def _reserve_stream(self) -> None:
        if self._streams >= self.max_streams:
            raise RetryableError("STREAM_CAPACITY_EXCEEDED")
        self._streams += 1

    async def _snapshot_items(self, principal, task_id):
        previous = None
        try:
            async with asyncio.timeout(self.stream_timeout):
                while True:
                    task = await self._get(principal, task_id)
                    wire = canonical(task)
                    if wire != previous:
                        yield task
                        previous = wire
                    if state_of(task) in TERMINAL | {"INPUT_REQUIRED"}:
                        return
                    await asyncio.sleep(self.poll_interval)
        except TimeoutError:
            raise RetryableError("STREAM_TIME_LIMIT") from None

    async def snapshots(self, principal, task_id):
        self._reserve_stream()
        inner = self._snapshot_items(principal, task_id)
        try:
            async for task in inner:
                yield task
        finally:
            await inner.aclose()
            self._streams -= 1

    async def send_snapshots(self, principal, request):
        require(isinstance(request, dict), "INVALID_REQUEST")
        config = configuration(request)
        principal.authorize("read", self.ledger.store.scope.tenant)
        # Reserve before the first await/admission so concurrent stream requests
        # cannot all pass a stale capacity check and create durable work that the
        # process then refuses to stream.
        self._reserve_stream()
        inner = None
        try:
            request = load_json(canonical(request))
            task = await self._call(self.ledger.submit, principal, request)
            continuation = bool(request.get("message", {}).get("taskId"))
            seen = any(m.get("messageId") == request["message"]["messageId"] for m in task.get("history", []))
            if continuation and not seen and state_of(task) in INTERRUPTED:
                task = await self._wait(principal, task["id"], initial=task, change_only=True)
            inner = self._snapshot_items(principal, task["id"])
            async for item in inner:
                yield history(item, config.get("historyLength"))
        finally:
            if inner is not None:
                await inner.aclose()
            self._streams -= 1
