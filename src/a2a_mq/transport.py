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
from starlette.types import ASGIApp, Receive, Scope, Send

from .domain import MAX_BYTES, Envelope, PermanentError, RetryableError, canonical, load_json, require, token

LOG = logging.getLogger("a2a_mq.transport")
BODY_LIMIT = MAX_BYTES + 16384
Handler = Callable[[Envelope], Awaitable[None]]
Readiness = Callable[[], Awaitable[bool]]


class _AppTokenMiddleware:
    """Authenticate without changing request-body or cancellation semantics."""

    def __init__(self, app: ASGIApp, app_token: str):
        self.app = app
        self.expected = app_token.encode()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if (scope["type"] == "http" and self.expected
                and scope["path"] not in {"/health/live", "/health/ready"}):
            values = [value for name, value in scope["headers"] if name.lower() == b"dapr-api-token"]
            if len(values) != 1 or not hmac.compare_digest(values[0], self.expected):
                await JSONResponse({"error": "UNAUTHORIZED"}, status_code=401)(scope, receive, send)
                return
        await self.app(scope, receive, send)


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
        try:
            parsed = urlsplit(url)
            port = parsed.port
        except (TypeError, ValueError):
            raise PermanentError("INVALID_DAPR_URL") from None
        require(isinstance(url, str) and parsed.scheme in {"http", "https"} and bool(parsed.hostname)
                and not parsed.username and not parsed.password and not parsed.query
                and not parsed.fragment and "?" not in url and "#" not in url
                and not any(c.isspace() for c in url)
                and parsed.path in {"", "/"} and (port is None or 1 <= port <= 65535),
                "INVALID_DAPR_URL")
        require(type(timeout) in {int, float} and 0 < timeout <= 60, "INVALID_TIMEOUT")
        self.url, self.component = url.rstrip("/"), token(component)
        self.timeout = timeout
        self.headers = {"dapr-api-token": api_token} if api_token else {}
        self._owns_client = client is None
        self.client = client
        self._closed = False

    def _http_client(self) -> httpx.AsyncClient:
        if not self._owns_client:
            require(self.client is not None, "PUBLISHER_CLIENT_REQUIRED")
            assert self.client is not None
            return self.client
        if self._closed:
            raise RetryableError("PUBLISHER_CLOSED")
        if self.client is None:
            self.client = httpx.AsyncClient(follow_redirects=False, trust_env=False)
        assert self.client is not None
        return self.client

    async def close(self) -> None:
        if self._owns_client:
            self._closed = True
            client, self.client = self.client, None
            if client is not None:
                await client.aclose()

    async def _status(self, method: str, path: str, headers: dict[str, str],
                      content: bytes | None = None) -> int:
        # Only HTTP status is part of the Dapr acceptance/health contract. Streaming
        # avoids buffering irrelevant bodies; the outer deadline also bounds headers.
        client = self._http_client()
        async with asyncio.timeout(self.timeout):
            async with client.stream(method, self.url + path, content=content,
                                     headers=headers, timeout=self.timeout,
                                     follow_redirects=False) as response:
                return response.status_code

    async def _post(self, topic: str, event: dict[str, Any]) -> None:
        headers = {**self.headers, "content-type": "application/cloudevents+json"}
        headers.update({k: event[k] for k in ("traceparent", "tracestate") if k in event})
        try:
            status = await self._status("POST", f"/v1.0/publish/{self.component}/{topic}",
                                        headers, canonical(event).encode())
        except (httpx.RequestError, TimeoutError):
            raise RetryableError("PUBLISH_UNAVAILABLE") from None
        if status in {408, 409, 429} or status >= 500:
            raise RetryableError("PUBLISH_RETRYABLE")
        if not 200 <= status < 300:
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
            status = await self._status("GET", "/v1.0/healthz/outbound", self.headers)
            return 200 <= status < 300
        except (httpx.RequestError, TimeoutError, RetryableError):
            return False


def subscriber(handler: Handler, bus: DaprPublisher, *, tenant: str, consumer: str,
               topic: str | None, app_token: str = "", concurrency: int = 4,
               readiness: Readiness | None = None, liveness: Readiness | None = None,
               lifespan: Any = None, callback_timeout: float = 30.0,
               protector: Any = None) -> FastAPI:
    tenant, consumer = token(tenant), token(consumer)
    require(type(concurrency) is int and 1 <= concurrency <= 128, "INVALID_CONCURRENCY")
    require(type(callback_timeout) in {int, float} and 0 < callback_timeout <= 300, "INVALID_TIMEOUT")
    if topic is not None:
        require(topic.startswith(f"a2a.v1.{tenant}.") and "/" not in topic, "INVALID_ROUTE")
    body_limit = MAX_BYTES * 2 + 16384 if protector is not None else BODY_LIMIT
    semaphore = asyncio.Semaphore(concurrency)

    @asynccontextmanager
    async def default_lifespan(app: FastAPI):
        try:
            yield
        finally:
            await bus.close()

    app = FastAPI(lifespan=lifespan or default_lifespan)

    app.add_middleware(_AppTokenMiddleware, app_token=app_token)

    @app.get("/dapr/subscribe")
    async def subscriptions():
        if topic is None:
            return []
        return [{"pubsubname": bus.component, "topic": topic, "route": "/events",
                 "deadLetterTopic": f"a2a.v1.{tenant}.dlq"}]

    @app.get("/health/live")
    async def live():
        try:
            async with asyncio.timeout(callback_timeout):
                ok = liveness is None or await liveness()
        except Exception:
            ok = False
        return JSONResponse({"status": "alive" if ok else "failed"},
                            status_code=200 if ok else 503)

    @app.get("/health/ready")
    async def ready():
        try:
            async with asyncio.timeout(callback_timeout):
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
            async with asyncio.timeout(callback_timeout), semaphore:
                async for chunk in request.stream():
                    remaining = body_limit - len(raw)
                    raw.extend(chunk[:remaining])
                    if len(chunk) > remaining:
                        truncated = True
                        raise PermanentError("MESSAGE_TOO_LARGE")
                mime = request.headers.get("content-type", "").split(";")[0].strip().lower()
                require(mime in {"application/json", "application/cloudevents+json"}, "INVALID_CONTENT_TYPE")
                ce = load_json(bytes(raw), body_limit)
                require(isinstance(ce, dict) and ce.get("specversion") == "1.0", "INVALID_CLOUDEVENT")
                require(isinstance(ce.get("source"), str) and bool(ce["source"])
                        and isinstance(ce.get("type"), str) and bool(ce["type"]), "INVALID_CLOUDEVENT")
                expected_mime = ("application/vnd.a2amq.sealed+json" if protector is not None else "application/json")
                require(isinstance(ce.get("datacontenttype"), str)
                        and ce["datacontenttype"].split(";")[0] == expected_mime, "INVALID_CLOUDEVENT")
                envelope = (await protector.open(canonical(ce.get("data")).encode(), expected_route=topic)
                            if protector is not None else Envelope.from_dict(ce.get("data")))
                e = envelope.as_dict()
                require(topic is not None
                        and (ce.get("pubsubname") == bus.component or (isinstance(ce.get("pubsubname"), str)
                             and (ce["pubsubname"].startswith("bus-") or ce["pubsubname"] in {"bus", "bus-dlq"})))
                        and ce.get("topic") == topic == envelope.route and e["tenantId"] == tenant,
                        "UNAUTHORIZED_ROUTE")
                require(ce.get("id") == e["messageId"]
                        and ce["source"] == "urn:a2a-mq:" + e["source"]
                        and ce["type"] == "a2a-mq." + e["operation"], "IDENTIFIER_MISMATCH")
                identifiers.update({k: e[k] for k in ("messageId", "taskId", "contextId", "correlationId") if k in e})
                from .fault_injection import hook_broker_delivery, hook_before_ack
                hook_broker_delivery(e)
                await handler(envelope)
                hook_before_ack(e)
            LOG.info(canonical({"event": "ack", **identifiers}))
            return {"status": "SUCCESS"}
        except TimeoutError:
            LOG.warning(canonical({"event": "retry", "reason": "CALLBACK_TIMEOUT", **identifiers}))
            return {"status": "RETRY"}
        except PermanentError as exc:
            try:
                async with asyncio.timeout(callback_timeout):
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
