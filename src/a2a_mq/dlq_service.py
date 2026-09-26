"""Dedicated poison-message collector for Dapr dead-letter CloudEvents."""
from __future__ import annotations

import asyncio
import base64
import hashlib
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .domain import MAX_BYTES, PermanentError, RetryableError, load_json, require, token
from .runtime import AsyncSQLite
from .transport import _AppTokenMiddleware

DLQ_EVENT_LIMIT = MAX_BYTES * 2


def _close_store(store):
    close = getattr(store, "close", None)
    if callable(close):
        close()


def create_dlq_collector(store, bus, operations, *, app_token: str,
                         concurrency: int = 4, callback_timeout: float = 30) -> FastAPI:
    tenant = token(store.scope.tenant)
    topic = f"a2a.v1.{tenant}.dlq"
    require(type(concurrency) is int and 1 <= concurrency <= 128, "INVALID_CONCURRENCY")
    require(type(callback_timeout) in {int, float} and 0 < callback_timeout <= 300, "INVALID_TIMEOUT")
    semaphore = asyncio.Semaphore(concurrency)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        database = AsyncSQLite(store, capacity=max(2, concurrency))
        app.state.database = database
        try:
            yield
        finally:
            app.state.database = None
            try:
                await database.aclose()
            finally:
                try:
                    await bus.close()
                finally:
                    await asyncio.to_thread(_close_store, store)

    app = FastAPI(lifespan=lifespan)
    app.state.database = None
    app.add_middleware(_AppTokenMiddleware, app_token=app_token)

    @app.get("/dapr/subscribe")
    async def subscribe():
        return [{"pubsubname": bus.component, "topic": topic, "route": "/events"}]

    @app.get("/health/live")
    async def live():
        return {"status": "alive"}

    @app.get("/health/ready")
    async def ready():
        database = app.state.database
        if database is None or not await bus.ready():
            return JSONResponse({"status": "unavailable"}, status_code=503)
        try:
            await database.call(lambda: True)
            return JSONResponse({"status": "ready"})
        except Exception:
            return JSONResponse({"status": "unavailable"}, status_code=503)

    @app.post("/events")
    async def receive(request: Request):
        database = app.state.database
        if database is None:
            return {"status": "RETRY"}
        try:
            async with asyncio.timeout(callback_timeout), semaphore:
                raw = bytearray()
                async for chunk in request.stream():
                    require(len(raw) + len(chunk) <= DLQ_EVENT_LIMIT, "MESSAGE_TOO_LARGE")
                    raw.extend(chunk)
                mime = request.headers.get("content-type", "").split(";")[0].strip().lower()
                require(mime in {"application/json", "application/cloudevents+json"}, "INVALID_CONTENT_TYPE")
                event = load_json(bytes(raw), DLQ_EVENT_LIMIT)
                require(isinstance(event, dict) and event.get("specversion") == "1.0"
                        and event.get("type") == "a2a-mq.deadletter"
                        and event.get("datacontenttype") == "application/json"
                        and isinstance(event.get("id"), str), "INVALID_CLOUDEVENT")
                is_allowed = (event.get("pubsubname") == bus.component
                              or (isinstance(event.get("pubsubname"), str)
                                  and (event["pubsubname"].startswith("bus-") or event["pubsubname"] in {"bus", "bus-dlq"})))
                require(is_allowed and event.get("topic") == topic,
                        "UNAUTHORIZED_ROUTE")
                data = event.get("data")
                require(isinstance(data, dict), "INVALID_CLOUDEVENT")
                reason = data.get("reason")
                origin = token(data.get("consumer"))
                require(event.get("source") == "urn:a2a-mq:" + origin, "IDENTIFIER_MISMATCH")
                truncated = data.get("truncated", False)
                require(type(truncated) is bool, "INVALID_CLOUDEVENT")
                encoded = data.get("originalBase64")
                require(isinstance(encoded, str) and len(encoded) <= DLQ_EVENT_LIMIT, "INVALID_DLQ_PAYLOAD")
                try:
                    original = base64.b64decode(encoded, validate=True)
                except (ValueError, TypeError):
                    raise PermanentError("INVALID_DLQ_PAYLOAD") from None
                require(base64.b64encode(original).decode("ascii") == encoded
                        and hashlib.sha256(original).hexdigest() == data.get("capturedBytesSha256"),
                        "INVALID_DLQ_PAYLOAD")
                await database.call(operations.record_dlq, original, reason,
                                    origin_consumer=origin, truncated=truncated)
                return {"status": "SUCCESS"}
        except RetryableError:
            return {"status": "RETRY"}
        except PermanentError:
            # Invalid messages on the DLQ are already poison; never recursively dead-letter them.
            return {"status": "SUCCESS"}
        except TimeoutError:
            return {"status": "RETRY"}
        except Exception:
            return {"status": "RETRY"}

    return app
