import json
from threading import Event

import httpx
from starlette.applications import Starlette

from a2a_mq.identity import Principal
from a2a_mq.local_lite import LocalLiteSettings, create_app as create_lite
from a2a_mq.local_workflow import LocalWorkflowSettings
from a2a_mq.queue_lab import create_queue_lab


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


def _lite_settings(tmp_path):
    app_token, dapr_token, keys = _files(tmp_path)
    return LocalLiteSettings(
        sqlite_path=str(tmp_path / "local.db"),
        app_token_file=str(app_token),
        dapr_token_file=str(dapr_token),
        jwt_keys_file=str(keys),
        issuer="https://local-a2amq.invalid",
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
            assert encoded == "lab-token"
            return Principal("owner", "demo", frozenset({"a2a:operate"}))

    app = create_queue_lab(
        settings, profile="local-lite", verifier=Verifier(), agent_pause=paused
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://local"
    ) as client:
        assert (await client.get("/control/agent")).status_code == 401
        headers = {"Authorization": "Bearer lab-token"}
        initial = await client.get("/control/agent", headers=headers)
        changed = await client.post(
            "/control/agent", headers=headers, json={"paused": True}
        )
        final = await client.get("/control/agent", headers=headers)

    assert initial.json() == {"paused": False}
    assert changed.json() == {"paused": True}
    assert final.json() == {"paused": True}
    assert paused.is_set()
