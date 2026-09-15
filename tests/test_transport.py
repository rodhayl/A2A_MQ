import asyncio
import json
import logging

import httpx
import pytest

from a2a_mq.domain import Envelope, PermanentError, RetryableError, command
from a2a_mq.reference import echo_events
from a2a_mq.transport import BODY_LIMIT, DaprPublisher, cloud_event, subscriber


def incoming(e):
    return {**cloud_event(e), "topic": e.route, "pubsubname": "bus"}


@pytest.mark.parametrize("status,error", [(204, None), (200, None), (500, RetryableError), (503, RetryableError),
    (408, RetryableError), (409, RetryableError), (429, RetryableError), (400, PermanentError),
    (401, PermanentError), (403, PermanentError), (302, PermanentError)])
async def test_publish_contract(status, error):
    seen = []
    async def respond(request):
        seen.append(request)
        return httpx.Response(status)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        bus = DaprPublisher(client=client, api_token="private-test-token")
        e = command("secret task text")
        if error:
            with pytest.raises(error):
                await bus.publish(e)
        else:
            await bus.publish(e)
            await bus.publish(e)
            assert seen[0].content == seen[1].content
        req = seen[0]
        assert req.url.path == "/v1.0/publish/bus/" + e.route
        assert req.headers["dapr-api-token"] == "private-test-token"
        assert req.headers["content-type"] == "application/cloudevents+json"
        assert json.loads(req.content)["data"] == e.as_dict()


async def test_network_and_readiness():
    def fail(request):
        raise httpx.ConnectError("sensitive upstream body")
    async with httpx.AsyncClient(transport=httpx.MockTransport(fail)) as client:
        bus = DaprPublisher(client=client)
        with pytest.raises(RetryableError, match="PUBLISH_UNAVAILABLE"):
            await bus.publish(command("x"))
        assert not await bus.ready()


@pytest.mark.parametrize("url", ["file:///tmp/x", "http://user:pass@host", "http://host/path", "http://host/?key=x", "http://host/#x"])
def test_bad_sidecar_url(url):
    with pytest.raises(PermanentError):
        DaprPublisher(url)


@pytest.mark.parametrize("failure,dlq_ok,expected", [(None, True, "SUCCESS"), (PermanentError("POISON"), True, "SUCCESS"),
    (PermanentError("POISON"), False, "RETRY"), (RetryableError("BUSY"), True, "RETRY"),
    (RuntimeError("secret prompt"), True, "RETRY")])
async def test_callback_failure_classification(failure, dlq_ok, expected, caplog):
    publications = []
    async def respond(request):
        publications.append(json.loads(request.content))
        return httpx.Response(204 if dlq_ok else 503)
    async def handle(envelope):
        if failure:
            raise failure
    e = command("secret prompt")
    caplog.set_level(logging.INFO, logger="a2a_mq.transport")
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as outbound:
        app = subscriber(handle, DaprPublisher(client=outbound), tenant="demo", consumer="worker", topic=e.route)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as client:
            response = await client.post("/events", json=incoming(e))
            assert response.json() == {"status": expected}
    assert bool(publications) == isinstance(failure, PermanentError)
    assert "secret prompt" not in caplog.text


@pytest.mark.parametrize("change", [lambda ce: ce.update(id="wrong"), lambda ce: ce.update(topic="other"),
    lambda ce: ce.update(pubsubname="other"), lambda ce: ce.update(data={}),
    lambda ce: ce.update(specversion="2.0"), lambda ce: ce.update(datacontenttype="text/plain")])
async def test_invalid_delivery_never_executes(change):
    published, called = [], []
    async def respond(req):
        published.append(json.loads(req.content))
        return httpx.Response(204)
    async def handle(e):
        called.append(e)
    e = command("x")
    ce = incoming(e)
    change(ce)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as outbound:
        app = subscriber(handle, DaprPublisher(client=outbound), tenant="demo", consumer="worker", topic=e.route)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as client:
            assert (await client.post("/events", json=ce)).json()["status"] == "SUCCESS"
    assert not called and len(published) == 1


async def test_token_limit_subscriptions_and_health():
    sent = []
    async def respond(req):
        if req.method == "POST":
            sent.append(json.loads(req.content))
        return httpx.Response(204)
    async def handle(e):
        raise AssertionError("must not execute")
    e = command("x")
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as outbound:
        app = subscriber(handle, DaprPublisher(client=outbound), tenant="demo", consumer="worker", topic=e.route,
                         app_token="token")
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as client:
            assert (await client.post("/events", json=incoming(e))).status_code == 401
            assert (await client.get("/health/live")).status_code == 200
            client.headers["dapr-api-token"] = "token"
            assert (await client.get("/health/ready")).status_code == 200
            subscription = (await client.get("/dapr/subscribe")).json()[0]
            assert subscription["deadLetterTopic"] == "a2a.v1.demo.dlq"
            response = await client.post("/events", content=b"x" * (BODY_LIMIT + 1), headers={"content-type": "application/json"})
            assert response.json()["status"] == "SUCCESS"
            assert sent[0]["data"]["truncated"] is True


async def test_concurrency_bound():
    active = peak = 0
    async def handle(e):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.005)
        active -= 1
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(204))) as outbound:
        e = command("x")
        app = subscriber(handle, DaprPublisher(client=outbound), tenant="demo", consumer="worker", topic=e.route, concurrency=2)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as client:
            results = await asyncio.gather(*[client.post("/events", json=incoming(command("x"))) for _ in range(9)])
            assert all(r.json()["status"] == "SUCCESS" for r in results)
    assert peak == 2


def test_reference_echo_and_trace():
    d = command("hello").as_dict()
    d["traceparent"] = "00-" + "1" * 32 + "-" + "2" * 16 + "-01"
    e = Envelope.from_dict(d)
    events = echo_events(e)
    assert [x.as_dict()["sequence"] for x in events] == [1, 2, 3]
    assert events[-1].as_dict()["payload"]["artifacts"][0]["parts"] == [{"text": "hello"}]
    assert cloud_event(events[-1])["traceparent"] == d["traceparent"]
    assert [x.as_dict()["messageId"] for x in echo_events(e)] == [x.as_dict()["messageId"] for x in events]
