"""Bearer-authorized operational routes; no raw database or filesystem endpoint."""
from __future__ import annotations

import asyncio

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from .domain import PermanentError, RetryableError, require
from .http_boundary import BoundedJSON
from .identity import BearerIdentity, current_principal


def create_operations(api, verifier, operations, archive, replay, database, *, timeout=30):
    require(type(timeout) in {int, float} and 0 < timeout <= 300, "INVALID_TIMEOUT")

    async def run(function, *args, **kwargs):
        try:
            async with asyncio.timeout(timeout):
                return await database.call(function, *args, **kwargs)
        except TimeoutError:
            raise RetryableError("OPERATION_TIMEOUT") from None

    async def body(request, fields):
        require(not request.query_params, "INVALID_QUERY")
        value = await request.json()
        require(isinstance(value, dict) and set(value) <= fields, "INVALID_OPERATION")
        return value

    async def status(request):
        require(not request.query_params, "INVALID_QUERY")
        return JSONResponse(await run(operations.backlog, current_principal()))

    async def retire(request):
        value = await body(request, {"retentionSeconds", "limit", "dryRun"})
        return JSONResponse(await run(operations.retire, current_principal(),
            retention_seconds=value.get("retentionSeconds", 604800), limit=value.get("limit", 50),
            dry_run=value.get("dryRun", True)))

    async def records(request):
        require(set(request.query_params) <= {"after", "limit"}, "INVALID_QUERY")
        raw = request.query_params.get("limit", "50")
        require(raw.isascii() and raw.isdigit() and len(raw) <= 3, "INVALID_PAGE_SIZE")
        return JSONResponse(await run(archive.list, current_principal(),
                                     after=request.query_params.get("after"), limit=int(raw)))

    async def record(request):
        require(not request.query_params, "INVALID_QUERY")
        return JSONResponse(await run(archive.read, current_principal(), request.path_params["record_id"]))

    async def enqueue(request):
        value = await body(request, {"requestId"})
        require(set(value) == {"requestId"}, "INVALID_REPLAY_REQUEST")
        async with asyncio.timeout(timeout):
            rid = await replay.enqueue(current_principal(), request.path_params["record_id"], value["requestId"])
        return JSONResponse({"replayId": rid, "status": "queued"}, status_code=202)

    async def approve(request):
        value = await body(request, {"requestId", "approved"})
        require(set(value) == {"requestId", "approved"}, "INVALID_APPROVAL")
        task = await api.approve(current_principal(), request.path_params["task_id"],
                                 value["requestId"], value["approved"])
        return JSONResponse({"task": task, "approvalQueued": True}, status_code=202)

    async def failure(request: Request, error):
        if isinstance(error, TimeoutError):
            code, status = "OPERATION_TIMEOUT", 503
        else:
            code = error.code
            status = 503 if isinstance(error, RetryableError) else {
                "UNAUTHENTICATED": 401, "FORBIDDEN": 403, "DLQ_NOT_FOUND": 404,
                "TASK_NOT_FOUND": 404}.get(code, 422)
        return JSONResponse({"error": code}, status_code=status, headers={"Cache-Control": "no-store"})

    app = Starlette(routes=[Route("/status", status), Route("/retention", retire, methods=["POST"]),
        Route("/deadletters", records), Route("/deadletters/{record_id}", record),
        Route("/deadletters/{record_id}/replay", enqueue, methods=["POST"]),
        Route("/tasks/{task_id}/approve", approve, methods=["POST"])],
        exception_handlers={PermanentError: failure, RetryableError: failure, TimeoutError: failure})
    return _NoStore(BearerIdentity(BoundedJSON(app), verifier))


class _NoStore:
    """Operational metadata and archived payloads are never cacheable."""
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        async def guarded(message):
            if message["type"] == "http.response.start":
                headers = [(k,v) for k,v in message.get("headers", []) if k.lower() != b"cache-control"]
                message = {**message, "headers": headers + [(b"cache-control", b"no-store")]}
            await send(message)
        await self.app(scope, receive, guarded)
