"""Dapr HTTP Pub/Sub boundary. Delivery success is not business completion."""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import logging
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any
from urllib.parse import urlsplit
from uuid import NAMESPACE_URL, uuid5

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .domain import MAX_BYTES, Envelope, PermanentError, RetryableError, canonical, load_json, require, token

LOG = logging.getLogger("a2a_mq.transport")
BODY_LIMIT = MAX_BYTES + 16384
Handler = Callable[[Envelope], Awaitable[None]]
Readiness = Callable[[], Awaitable[bool]]


def cloud_event(envelope: Envelope) -> dict[str, Any]:
    e = envelope.as_dict()
    return {
        "specversion": "1.0", "id": e["messageId"], "source": "urn:a2a-mq:" + e["source"],
        "type": "a2a-mq." + e["operation"], "datacontenttype": "application/json",
        "time": e["createdAt"], "data": e,
        **{k: e[k] for k in ("traceparent", "tracestate") if k in e},
    }


class DaprPublisher:
    def __init__(self, url: str = "http://127.0.0.1:3500", component: str = "bus",
                 api_token: str = "", timeout: float = 5.0, client: httpx.AsyncClient | None = None):
        parsed = urlsplit(url)
        require(parsed.scheme in {"http", "https"} and bool(parsed.netloc)
                and not parsed.username and not parsed.password and not parsed.query
                and not parsed.fragment and parsed.path in {"", "/"}, "INVALID_DAPR_URL")
        require(0 < timeout <= 60, "INVALID_TIMEOUT")
        self.url, self.component = url.rstrip("/"), token(component)
        self.timeout = timeout
        self.headers = {"dapr-api-token": api_token} if api_token else {}
        self._owns_client = client is None
        self.client = client or httpx.AsyncClient(follow_redirects=False, trust_env=False)

    async def close(self) -> None:
        if self._owns_client:
            await self.client.aclose()

    async def _post(self, topic: str, event: dict[str, Any]) -> None:
        headers = {**self.headers, "content-type": "application/cloudevents+json"}
        headers.update({k: event[k] for k in ("traceparent", "tracestate") if k in event})
        try:
            response = await self.client.post(
                f"{self.url}/v1.0/publish/{self.component}/{topic}",
                content=canonical(event).encode(), headers=headers,
                timeout=self.timeout, follow_redirects=False,
            )
        except httpx.RequestError:
            raise RetryableError("PUBLISH_UNAVAILABLE") from None
        if response.status_code in {408, 409, 429} or response.status_code >= 500:
            raise RetryableError("PUBLISH_RETRYABLE")
        if not 200 <= response.status_code < 300:
            raise PermanentError("PUBLISH_REJECTED")

    async def publish(self, envelope: Envelope) -> None:
        await self._post(envelope.route, cloud_event(envelope))

    async def quarantine(self, raw: bytes, tenant: str, consumer: str, reason: str,
                         truncated: bool = False) -> None:
        tenant, consumer = token(tenant), token(consumer)
        digest = hashlib.sha256(raw).hexdigest()
        mid = str(uuid5(NAMESPACE_URL, f"a2a-mq/dlq/{tenant}/{consumer}/{digest}/{reason}"))
        event = {
            "specversion": "1.0", "id": mid, "source": "urn:a2a-mq:" + consumer,
            "type": "a2a-mq.deadletter", "datacontenttype": "application/json",
            "data": {"reason": reason, "consumer": consumer, "truncated": truncated,
                     "capturedBytesSha256": digest, "originalBase64": base64.b64encode(raw).decode()},
        }
        await self._post(f"a2a.v1.{tenant}.dlq", event)

    async def ready(self) -> bool:
        try:
            response = await self.client.get(self.url + "/v1.0/healthz/outbound", headers=self.headers,
                                             timeout=self.timeout, follow_redirects=False)
            return 200 <= response.status_code < 300
        except httpx.RequestError:
            return False


def subscriber(handler: Handler, bus: DaprPublisher, *, tenant: str, consumer: str,
               topic: str | None, app_token: str = "", concurrency: int = 4,
               readiness: Readiness | None = None, lifespan: Any = None) -> FastAPI:
    tenant, consumer = token(tenant), token(consumer)
    require(1 <= concurrency <= 128, "INVALID_CONCURRENCY")
    if topic is not None:
        require(topic.startswith(f"a2a.v1.{tenant}.") and "/" not in topic, "INVALID_ROUTE")
    semaphore = asyncio.Semaphore(concurrency)

    @asynccontextmanager
    async def default_lifespan(app: FastAPI):
        yield
        await bus.close()

    app = FastAPI(lifespan=lifespan or default_lifespan)

    @app.middleware("http")
    async def authenticate(request: Request, call_next):
        if app_token and request.url.path != "/health/live":
            supplied = request.headers.get("dapr-api-token", "")
            if not hmac.compare_digest(supplied.encode(), app_token.encode()):
                return JSONResponse({"error": "UNAUTHORIZED"}, status_code=401)
        return await call_next(request)

    @app.get("/dapr/subscribe")
    async def subscriptions():
        if topic is None:
            return []
        return [{"pubsubname": bus.component, "topic": topic, "route": "/events",
                 "deadLetterTopic": f"a2a.v1.{tenant}.dlq"}]

    @app.get("/health/live")
    async def live():
        return {"status": "alive"}

    @app.get("/health/ready")
    async def ready():
        try:
            ok = await bus.ready() and (readiness is None or await readiness())
        except Exception:
            ok = False
        return JSONResponse({"status": "ready" if ok else "unavailable"}, status_code=200 if ok else 503)

    @app.post("/events")
    async def receive(request: Request):
        raw = bytearray()
        truncated = False
        identifiers: dict[str, Any] = {"consumer": consumer}
        try:
            async with semaphore:
                async for chunk in request.stream():
                    remaining = BODY_LIMIT - len(raw)
                    raw.extend(chunk[:remaining])
                    if len(chunk) > remaining:
                        truncated = True
                        raise PermanentError("MESSAGE_TOO_LARGE")
                mime = request.headers.get("content-type", "").split(";")[0].strip().lower()
                require(mime in {"application/json", "application/cloudevents+json"}, "INVALID_CONTENT_TYPE")
                ce = load_json(bytes(raw), BODY_LIMIT)
                require(isinstance(ce, dict) and ce.get("specversion") == "1.0", "INVALID_CLOUDEVENT")
                require(isinstance(ce.get("source"), str) and bool(ce["source"])
                        and isinstance(ce.get("type"), str) and bool(ce["type"]), "INVALID_CLOUDEVENT")
                require(isinstance(ce.get("datacontenttype"), str)
                        and ce["datacontenttype"].split(";")[0] == "application/json", "INVALID_CLOUDEVENT")
                envelope = Envelope.from_dict(ce.get("data"))
                e = envelope.as_dict()
                require(topic is not None and ce.get("pubsubname") == bus.component
                        and ce.get("topic") == topic == envelope.route and e["tenantId"] == tenant,
                        "UNAUTHORIZED_ROUTE")
                require(ce.get("id") == e["messageId"], "IDENTIFIER_MISMATCH")
                identifiers.update({k: e[k] for k in ("messageId", "taskId", "contextId", "correlationId") if k in e})
                await handler(envelope)
            LOG.info(canonical({"event": "ack", **identifiers}))
            return {"status": "SUCCESS"}
        except PermanentError as exc:
            try:
                await bus.quarantine(bytes(raw), tenant, consumer, exc.code, truncated)
            except Exception:
                LOG.warning(canonical({"event": "retry", "reason": "DLQ_UNAVAILABLE", **identifiers}))
                return {"status": "RETRY"}
            LOG.warning(canonical({"event": "quarantined", "reason": exc.code, **identifiers}))
            return {"status": "SUCCESS"}
        except Exception:
            LOG.warning(canonical({"event": "retry", "reason": "HANDLER_UNAVAILABLE", **identifiers}))
            return {"status": "RETRY"}

    return app
