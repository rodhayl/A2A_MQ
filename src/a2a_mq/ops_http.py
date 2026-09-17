"""Authenticated administrative HTTP surface for operations already scoped in SQL."""
from __future__ import annotations

import asyncio
import base64
from pathlib import Path
from typing import Any

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Route

from .domain import MAX_BYTES, PermanentError, RetryableError, load_json, require
from .http_boundary import BoundedJSON
from .identity import BearerIdentity, current_principal


def _code(error: PermanentError) -> int:
    if error.code == "UNAUTHENTICATED":
        return 401
    if error.code == "FORBIDDEN":
        return 403
    if error.code.endswith("_NOT_FOUND"):
        return 404
    if error.code.endswith("_CONFLICT") or error.code in {"DLQ_ALREADY_REPLAYED"}:
        return 409
    return 422


def create_operations_app(task_ops, task_database, publisher, verifier, *,
                          dlq_ops=None, dlq_database=None,
                          backup_dir: str | Path | None = None) -> Any:
    """Create an admin-only ASGI app. Paths never select tenant/consumer scope."""
    dlq_ops = dlq_ops or task_ops
    dlq_database = dlq_database or task_database
    backup_root = None if backup_dir is None else Path(backup_dir).absolute()
    if backup_root is not None:
        require(backup_root.is_dir(), "INVALID_BACKUP_PATH")

    async def db_call(database, function, *args, **kwargs):
        try:
            async with asyncio.timeout(60):
                return await database.call(function, *args, **kwargs)
        except TimeoutError:
            raise RetryableError("OPERATIONS_TIMEOUT") from None

    async def json_body(request: Request, *, optional: bool = False):
        raw = await request.body()
        if not raw and optional:
            return {}
        require(bool(raw), "INVALID_JSON")
        return load_json(raw, MAX_BYTES)

    async def stats(request: Request):
        principal = current_principal()
        task = await db_call(task_database, task_ops.stats, principal)
        if dlq_ops is task_ops:
            return JSONResponse(task, headers={"Cache-Control": "no-store"})
        dead = await db_call(dlq_database, dlq_ops.stats, principal)
        task["dlqPending"] = dead["dlqPending"]
        return JSONResponse(task, headers={"Cache-Control": "no-store"})

    async def metrics(request: Request):
        principal = current_principal()
        text = await db_call(task_database, task_ops.prometheus, principal)
        if dlq_ops is not task_ops:
            dead = await db_call(dlq_database, dlq_ops.stats, principal)
            counters = dlq_ops.metrics.snapshot()
            text += "# TYPE a2amq_central_pending_dlq gauge\n"
            text += f"a2amq_central_pending_dlq {dead['dlqPending']}\n"
            for name in ("a2amq_dlq_captured_total", "a2amq_dlq_replay_attempts_total",
                         "a2amq_dlq_replayed_total"):
                exported = name.replace("a2amq_dlq_", "a2amq_central_dlq_", 1)
                text += f"# TYPE {exported} counter\n{exported} {counters[name]:g}\n"
        return PlainTextResponse(text, media_type="text/plain; version=0.0.4; charset=utf-8",
                                 headers={"Cache-Control": "no-store"})

    async def list_dlq(request: Request):
        require(set(request.query_params) <= {"limit", "after", "include_replayed"}, "INVALID_QUERY")
        try:
            limit = int(request.query_params.get("limit", "50"))
        except ValueError:
            raise PermanentError("INVALID_PAGE_SIZE") from None
        after = request.query_params.get("after")
        include = request.query_params.get("include_replayed", "false").lower()
        require(include in {"true", "false"}, "INVALID_QUERY")
        items, cursor = await db_call(dlq_database, dlq_ops.list_dlq, current_principal(),
                                      limit=limit, after=after, include_replayed=include == "true")
        return JSONResponse({"items": items, "nextCursor": cursor}, headers={"Cache-Control": "no-store"})

    async def dlq_payload(request: Request):
        raw = await db_call(dlq_database, dlq_ops.dlq_payload, current_principal(),
                            request.path_params["entry_id"])
        return JSONResponse({"entryId": request.path_params["entry_id"],
                             "base64": base64.b64encode(raw).decode("ascii")},
                            headers={"Cache-Control": "no-store"})

    async def replay(request: Request):
        body = await json_body(request, optional=True)
        require(isinstance(body, dict) and set(body) <= {"force"}, "INVALID_QUERY")
        force = body.get("force", False)
        require(type(force) is bool, "INVALID_QUERY")
        principal = current_principal()
        raw = await db_call(dlq_database, dlq_ops.dlq_payload, principal, request.path_params["entry_id"])
        from .domain import Envelope
        try:
            envelope = Envelope.parse(raw)
        except PermanentError:
            raise PermanentError("DLQ_NOT_REPLAYABLE") from None
        await db_call(dlq_database, dlq_ops.prepare_replay, principal,
                      request.path_params["entry_id"], force=force)
        await publisher.publish(envelope)
        await db_call(dlq_database, dlq_ops.mark_replayed, principal, request.path_params["entry_id"])
        return JSONResponse({"replayed": envelope.as_dict()["messageId"]},
                            headers={"Cache-Control": "no-store"})

    async def retention(request: Request):
        body = await json_body(request)
        require(isinstance(body, dict) and set(body) <= {"retentionSeconds", "limit"}
                and "retentionSeconds" in body, "INVALID_QUERY")
        retired = await db_call(task_database, task_ops.retire, current_principal(),
                                retention_seconds=body["retentionSeconds"], limit=body.get("limit", 100))
        return JSONResponse({"retired": retired, "count": len(retired)}, headers={"Cache-Control": "no-store"})

    async def backup(request: Request):
        require(backup_root is not None, "BACKUP_NOT_CONFIGURED")
        body = await json_body(request)
        require(isinstance(body, dict) and set(body) == {"name"}, "INVALID_QUERY")
        name = body["name"]
        require(isinstance(name, str) and 1 <= len(name) <= 128
                and all(c.isalnum() or c in {"-", "_", "."} for c in name)
                and name not in {".", ".."}, "INVALID_BACKUP_PATH")
        target = (backup_root / name).absolute()
        require(target.parent == backup_root, "INVALID_BACKUP_PATH")
        result = await db_call(task_database, task_ops.backup_sqlite, current_principal(), target)
        return JSONResponse(result, headers={"Cache-Control": "no-store"})

    async def permanent(request: Request, error: PermanentError):
        return JSONResponse({"error": error.code}, status_code=_code(error), headers={"Cache-Control": "no-store"})

    async def retryable(request: Request, error: RetryableError):
        return JSONResponse({"error": error.code}, status_code=503,
                            headers={"Retry-After": "1", "Cache-Control": "no-store"})

    app = Starlette(routes=[
        Route("/stats", stats, methods=["GET"]),
        Route("/metrics", metrics, methods=["GET"]),
        Route("/dlq", list_dlq, methods=["GET"]),
        Route("/dlq/{entry_id}", dlq_payload, methods=["GET"]),
        Route("/dlq/{entry_id}/replay", replay, methods=["POST"]),
        Route("/retention/run", retention, methods=["POST"]),
        Route("/backup", backup, methods=["POST"]),
    ], exception_handlers={PermanentError: permanent, RetryableError: retryable})

    return BearerIdentity(BoundedJSON(app, limit=MAX_BYTES), verifier)
