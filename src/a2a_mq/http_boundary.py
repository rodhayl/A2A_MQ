"""Bound request bodies before the official SDK deserializes them."""
from __future__ import annotations

import asyncio
import logging

from starlette.responses import JSONResponse

from .domain import MAX_BYTES, PermanentError, load_json, require


class BoundedJSON:
    def __init__(self, app, *, limit=MAX_BYTES, timeout=10):
        require(type(limit) is int and 1 <= limit <= MAX_BYTES * 2, "INVALID_BODY_LIMIT")
        require(type(timeout) in {int, float} and 0 < timeout <= 60, "INVALID_TIMEOUT")
        self.app, self.limit, self.timeout = app, limit, timeout

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def noncache(message):
            if message["type"] == "http.response.start":
                message = {**message, "headers": [(k, v) for k, v in message.get("headers", [])
                             if k.lower() not in {b"cache-control", b"pragma"}]
                             + [(b"cache-control", b"no-store"), (b"pragma", b"no-cache")]}
            await send(message)

        if scope["method"] not in {"POST", "PUT", "PATCH"}:
            await self.app(scope, receive, noncache)
            return
        try:
            headers = scope.get("headers", [])
            types = [v for k, v in headers if k.lower() == b"content-type"]
            lengths = [v for k, v in headers if k.lower() == b"content-length"]
            require(not any(k.lower() == b"content-encoding" for k, _ in headers), "ENCODING_UNSUPPORTED")
            require(len(types) <= 1, "CONTENT_TYPE_UNSUPPORTED")
            require(len(lengths) <= 1, "INVALID_CONTENT_LENGTH")
            if lengths:
                require(len(lengths[0]) <= 10 and lengths[0].isdigit(), "INVALID_CONTENT_LENGTH")
                require(int(lengths[0]) <= self.limit, "MESSAGE_TOO_LARGE")
            body = bytearray()
            async with asyncio.timeout(self.timeout):
                while True:
                    packet = await receive()
                    if packet["type"] == "http.disconnect":
                        return
                    require(packet["type"] == "http.request", "INVALID_REQUEST")
                    data = packet.get("body", b"")
                    require(len(body) + len(data) <= self.limit, "MESSAGE_TOO_LARGE")
                    body.extend(data)
                    if not packet.get("more_body", False):
                        break
            if lengths:
                require(int(lengths[0]) == len(body), "INVALID_CONTENT_LENGTH")
            if body:
                require(bool(types) and types[0].split(b";")[0].strip().lower() == b"application/json",
                        "CONTENT_TYPE_UNSUPPORTED")
                load_json(bytes(body), self.limit)
        except (PermanentError, TimeoutError) as error:
            code = error.code if isinstance(error, PermanentError) else "BODY_TIMEOUT"
            status = 413 if code == "MESSAGE_TOO_LARGE" else 408 if code == "BODY_TIMEOUT" else 400
            await JSONResponse({"error": code}, status_code=status)(scope, receive, noncache)
            return
        delivered = False

        async def replay():
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": bytes(body), "more_body": False}
            return await receive()

        await self.app(scope, replay, noncache)


class MetadataOnlySDKLogs(logging.Filter):
    """Discard SDK exception/parameter text; keep only a fixed diagnostic marker."""

    def filter(self, record):
        record.msg, record.args = "a2amq_upstream_diagnostic", ()
        record.exc_info = record.exc_text = record.stack_info = None
        return True


def sanitize_sdk_logs():
    for name in ("a2a.server.routes.jsonrpc_dispatcher", "a2a.server.routes.rest_dispatcher",
                 "a2a.server.request_handlers.request_handler"):
        logger = logging.getLogger(name)
        if not any(isinstance(f, MetadataOnlySDKLogs) for f in logger.filters):
            logger.addFilter(MetadataOnlySDKLogs())
    try:
        from a2a.utils import error_handlers
        orig = error_handlers._create_error_response
        if getattr(orig, "_a2amq_patched", False) is False:
            def patched(error: Exception):
                if hasattr(error, "status_code"):
                    detail = getattr(error, "detail", str(error))
                    raw_headers = getattr(error, "headers", None)
                    headers = dict(raw_headers) if raw_headers else None
                    return JSONResponse(
                        content={"detail": detail} if isinstance(detail, str) else detail,
                        status_code=getattr(error, "status_code"),
                        headers=headers,
                    )
                return orig(error)
            setattr(patched, "_a2amq_patched", True)
            error_handlers._create_error_response = patched
    except ImportError:
        pass
