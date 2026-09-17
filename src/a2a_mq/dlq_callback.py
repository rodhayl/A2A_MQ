"""Bounded Dapr DLQ archive callback; errors never recurse into the same DLQ."""
import asyncio

from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from .deadletters import MAX_DLQ_BYTES
from .domain import load_json, require
from .transport import _AppTokenMiddleware


def create_dlq_callback(archive, database, *, app_token, active=lambda: True, timeout=30, concurrency=4):
    require(isinstance(app_token, str) and 16 <= len(app_token) <= 256
            and all(33 <= ord(c) <= 126 for c in app_token), "APP_TOKEN_REQUIRED")
    require(type(concurrency) is int and 1 <= concurrency <= 128, "INVALID_CONCURRENCY")
    require(type(timeout) in {int, float} and 0 < timeout <= 300, "INVALID_TIMEOUT")
    gate = asyncio.Semaphore(concurrency)

    async def subscriptions(request):
        return JSONResponse([{"pubsubname": archive.component, "topic": archive.topic, "route": "/events"}])

    async def receive(request):
        try:
            async with asyncio.timeout(timeout), gate:
                require(active(), "SERVICE_NOT_RUNNING")
                types = request.headers.getlist("content-type")
                require(len(types) == 1 and types[0].split(";")[0].strip().lower()
                        in {"application/json", "application/cloudevents+json"}, "INVALID_CONTENT_TYPE")
                require(not request.headers.getlist("content-encoding"), "ENCODING_UNSUPPORTED")
                raw = bytearray()
                async for chunk in request.stream():
                    require(len(raw) + len(chunk) <= MAX_DLQ_BYTES, "MESSAGE_TOO_LARGE")
                    raw.extend(chunk)
                await database.call(archive.capture, load_json(bytes(raw), MAX_DLQ_BYTES))
            return JSONResponse({"status": "SUCCESS"})
        except Exception:
            # No SUCCESS until archive commit, and no recursive quarantine/drop.
            return JSONResponse({"status": "RETRY"})

    app = Starlette(routes=[Route("/dapr/subscribe", subscriptions), Route("/events", receive, methods=["POST"])])
    return _AppTokenMiddleware(app, app_token)
