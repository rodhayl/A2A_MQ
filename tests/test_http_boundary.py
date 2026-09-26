import asyncio
import logging

import httpx
import pytest
from starlette.requests import Request
from starlette.responses import JSONResponse

from a2a_mq.http_boundary import BoundedJSON, MetadataOnlySDKLogs


async def echo(scope, receive, send):
    body = await Request(scope, receive).body()
    await JSONResponse({"bytes": len(body)}, headers={"Cache-Control": "public"})(scope, receive, send)


@pytest.mark.parametrize("content,headers,status", [
    (b'{}', {"Content-Type": "application/json"}, 200),
    (b'', {}, 200),
    (b'{"x":1,"x":2}', {"Content-Type": "application/json"}, 400),
    (b'{"x":NaN}', {"Content-Type": "application/json"}, 400),
    (b'{}', {"Content-Type": "text/plain"}, 400),
    (b'{}', {"Content-Type": "application/json", "Content-Encoding": "gzip"}, 400),
    (b'"' + b'x'*128 + b'"', {"Content-Type": "application/json"}, 413),
])
async def test_body_contract(content, headers, status):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(BoundedJSON(echo, limit=64)), base_url="http://app") as client:
        response = await client.post("/", content=content, headers=headers)
        assert response.status_code == status and response.headers["Cache-Control"] == "no-store"


async def test_streamed_body_timeout():
    async def slow():
        yield b'{'
        await asyncio.sleep(1)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(BoundedJSON(echo, timeout=.01)), base_url="http://app") as client:
        response = await client.post("/", content=slow(), headers={"Content-Type":"application/json"})
        assert response.status_code == 408


def test_sdk_logs_do_not_retain_exception_or_payload():
    record = logging.LogRecord("a2a", logging.ERROR, "test", 1, "private %s", ("secret",),
                               (ValueError, ValueError("secret"), None))
    record.stack_info = "secret-stack"
    assert MetadataOnlySDKLogs().filter(record)
    text = logging.Formatter().format(record)
    assert text == "a2amq_upstream_diagnostic" and "secret" not in text
