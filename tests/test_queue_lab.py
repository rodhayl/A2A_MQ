import asyncio
import json
from threading import Event

import httpx
from starlette.applications import Starlette

from a2a_mq.domain import Envelope
from a2a_mq.identity import Principal
from a2a_mq.local_lite import LocalLiteSettings, create_app as create_lite
from a2a_mq.local_workflow import LocalWorkflowSettings
from a2a_mq.queue_lab import create_queue_lab
from a2a_mq.transport import cloud_event


def _files(tmp_path):
    app_token = tmp_path / "app-token"
    dapr_token = tmp_path / "dapr-token"
    keys = tmp_path / "keys.json"
    for path, value in ((app_token, "a" * 32), (dapr_token, "d" * 32)):
        path.write_text(value)
        path.chmod(0o600)
    keys.write_text(json.dumps({"local": "unused-fixture-key"}))
    keys.chmod(0o600)
    return app_token, dapr_token, keys


def _lite_settings(tmp_path, **overrides):
    app_token, dapr_token, keys = _files(tmp_path)
    return LocalLiteSettings(
        sqlite_path=str(tmp_path / "local.db"),
        app_token_file=str(app_token),
        dapr_token_file=str(dapr_token),
        jwt_keys_file=str(keys),
        issuer="https://local-a2amq.invalid",
        **overrides,
    )


async def test_queue_lab_assets_are_local_no_store_and_profile_aware(tmp_path):
    settings = _lite_settings(tmp_path)
    app = create_queue_lab(settings, profile="local-lite")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://local"
    ) as client:
        index = await client.get("/")
        css = await client.get("/app.css")
        js = await client.get("/app.js")
        config = await client.get("/config")

    assert index.status_code == css.status_code == js.status_code == config.status_code == 200
    assert "A2A-MQ Queue Lab" in index.text
    assert "/lab/app.css" in index.text and "/lab/app.js" in index.text
    for response in (index, css, js, config):
        assert response.headers["cache-control"] == "no-store"
        assert response.headers["x-content-type-options"] == "nosniff"
        assert "default-src 'self'" in response.headers["content-security-policy"]
    value = config.json()
    assert value["profile"] == "local-lite"
    assert value["tenant"] == "demo"
    assert value["source"] == "local-worker"
    assert value["features"]["workflow"] is False
    assert value["features"]["streaming"] is True
    assert value["features"]["dlqReplay"] is True


async def test_queue_lab_workflow_config_enables_real_workflow_controls(tmp_path):
    app_token, dapr_token, keys = _files(tmp_path)
    settings = LocalWorkflowSettings(
        sqlite_path=str(tmp_path / "workflow.db"),
        app_token_file=str(app_token),
        dapr_token_file=str(dapr_token),
        jwt_keys_file=str(keys),
        issuer="https://local-a2amq.invalid",
    )
    app = create_queue_lab(settings, profile="local-workflow")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://local"
    ) as client:
        value = (await client.get("/config")).json()
    assert value["features"] == {
        "workflow": True,
        "inputRequired": True,
        "authRequired": True,
        "delay": True,
        "streaming": True,
        "dlqReplay": True,
        "agentPause": False,
    }


async def test_local_lite_mounts_queue_lab_without_auth_bypass(tmp_path, monkeypatch):
    settings = _lite_settings(tmp_path)

    class Identity:
        audience = "http://127.0.0.1:18000"
        issuer = "https://local-a2amq.invalid"

    monkeypatch.setattr("a2a_mq.local_lite.verifier", lambda settings, audience=None: Identity())
    monkeypatch.setattr("a2a_mq.local_lite.create_bridge", lambda *a, **k: Starlette())
    app = create_lite(settings)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://local"
    ) as client:
        lab = await client.get("/lab/")
        config = await client.get("/lab/config")
        operations = await client.get("/operations/status")

    assert lab.status_code == 200
    assert config.status_code == 200
    assert operations.status_code == 401


async def test_queue_lab_agent_pause_control_requires_real_bearer_authorization(tmp_path):
    settings = _lite_settings(tmp_path)
    paused = Event()

    class Verifier:
        def verify(self, encoded):
            scopes = {"lab-token": {"a2a:operate"}, "read-token": {"a2a:read"}}.get(encoded)
            if scopes is None:
                from a2a_mq.domain import PermanentError
                raise PermanentError("UNAUTHENTICATED")
            return Principal("owner", "demo", frozenset(scopes))

    app = create_queue_lab(
        settings, profile="local-lite", verifier=Verifier(), agent_pause=paused
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://local"
    ) as client:
        assert (await client.get("/control/agent")).status_code == 401
        forbidden = await client.get(
            "/control/agent", headers={"Authorization": "Bearer read-token"}
        )
        assert forbidden.status_code == 403
        headers = {"Authorization": "Bearer lab-token"}
        invalid = await client.post("/control/agent", headers=headers, json={"paused": "yes"})
        assert invalid.status_code == 422
        initial = await client.get("/control/agent", headers=headers)
        changed = await client.post(
            "/control/agent", headers=headers, json={"paused": True}
        )
        final = await client.get("/control/agent", headers=headers)

    assert initial.json() == {"paused": False}
    assert changed.json() == {"paused": True}
    assert final.json() == {"paused": True}
    assert paused.is_set()


async def test_local_lite_pause_retries_real_dapr_delivery_until_resumed(tmp_path, monkeypatch):
    settings = _lite_settings(tmp_path, callback_timeout=0.05)

    class Verifier:
        audience = "http://127.0.0.1:18000"
        issuer = "https://local-a2amq.invalid"

        def verify(self, encoded):
            if encoded != "lab-token":
                raise AssertionError("unexpected token")
            return Principal(
                "owner",
                "demo",
                frozenset({"a2a:send", "a2a:read", "a2a:cancel", "a2a:operate"}),
            )

    verifier = Verifier()
    monkeypatch.setattr("a2a_mq.local_lite.verifier", lambda settings, audience=None: verifier)
    monkeypatch.setattr("a2a_mq.local_lite.create_bridge", lambda *a, **k: Starlette())
    app = create_lite(settings)
    ledger = app.state.a2amq_ledger
    owner = Principal("owner", "demo", frozenset({"a2a:send", "a2a:read", "a2a:cancel"}))
    task = ledger.submit(owner, {
        "message": {
            "messageId": "paused-delivery",
            "role": "ROLE_USER",
            "parts": [{"text": "hello"}],
        },
        "configuration": {"returnImmediately": True},
    })
    publish = ledger.pending(("publish",))[0]
    envelope = Envelope(publish.body)
    ledger.delivered(publish)
    incoming = {
        **cloud_event(envelope),
        "topic": envelope.route,
        "pubsubname": "bus",
    }

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://local"
    ) as client:
        auth = {"Authorization": "Bearer lab-token"}
        paused = await client.post("/lab/control/agent", headers=auth, json={"paused": True})
        assert paused.status_code == 200 and paused.json() == {"paused": True}

        held = await client.post(
            "/events",
            headers={"dapr-api-token": "a" * 32},
            json=incoming,
        )
        assert held.status_code == 200
        assert held.json() == {"status": "RETRY"}
        assert not [a for a in ledger.pending(("start",)) if a.task_id == task["id"]]

        resumed = await client.post("/lab/control/agent", headers=auth, json={"paused": False})
        assert resumed.status_code == 200 and resumed.json() == {"paused": False}

        accepted = await client.post(
            "/events",
            headers={"dapr-api-token": "a" * 32},
            json=incoming,
        )
        assert accepted.status_code == 200
        assert accepted.json() == {"status": "SUCCESS"}

    assert any(a.task_id == task["id"] for a in ledger.pending(("start",)))


async def test_local_lite_pause_resumes_same_inflight_delivery_without_redelivery(tmp_path, monkeypatch):
    settings = _lite_settings(tmp_path, callback_timeout=0.5)

    class Verifier:
        audience = "http://127.0.0.1:18000"
        issuer = "https://local-a2amq.invalid"

        def verify(self, encoded):
            return Principal("owner", "demo", frozenset({"a2a:operate"}))

    verifier = Verifier()
    monkeypatch.setattr("a2a_mq.local_lite.verifier", lambda settings, audience=None: verifier)
    monkeypatch.setattr("a2a_mq.local_lite.create_bridge", lambda *a, **k: Starlette())
    app = create_lite(settings)
    ledger = app.state.a2amq_ledger
    owner = Principal("owner", "demo", frozenset({"a2a:send", "a2a:read", "a2a:cancel"}))
    task = ledger.submit(owner, {
        "message": {
            "messageId": "inflight-pause",
            "role": "ROLE_USER",
            "parts": [{"text": "hello"}],
        },
        "configuration": {"returnImmediately": True},
    })
    publish = ledger.pending(("publish",))[0]
    envelope = Envelope(publish.body)
    ledger.delivered(publish)
    incoming = {**cloud_event(envelope), "topic": envelope.route, "pubsubname": "bus"}

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://local"
    ) as client:
        auth = {"Authorization": "Bearer lab-token"}
        assert (await client.post(
            "/lab/control/agent", headers=auth, json={"paused": True}
        )).json() == {"paused": True}

        delivery = asyncio.create_task(client.post(
            "/events", headers={"dapr-api-token": "a" * 32}, json=incoming
        ))
        await asyncio.sleep(0.05)
        assert not delivery.done()
        assert not [a for a in ledger.pending(("start",)) if a.task_id == task["id"]]

        assert (await client.post(
            "/lab/control/agent", headers=auth, json={"paused": False}
        )).json() == {"paused": False}
        accepted = await asyncio.wait_for(delivery, timeout=1)

    assert accepted.json() == {"status": "SUCCESS"}
    assert any(a.task_id == task["id"] for a in ledger.pending(("start",)))
