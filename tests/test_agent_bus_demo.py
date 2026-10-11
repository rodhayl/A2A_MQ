"""Native demo contracts. These tests do not replace the separate live bus run."""
import base64
import importlib.util
import json
import os
from pathlib import Path
import sys

import pytest


@pytest.fixture
def demo(monkeypatch):
    path = Path(__file__).resolve().parents[1] / "scripts/agent_bus_demo.py"
    monkeypatch.syspath_prepend(str(path.parent))
    spec = importlib.util.spec_from_file_location("agent_bus_demo_tests", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_existing_fixture_refused(demo, tmp_path):
    with pytest.raises(RuntimeError, match="FIXTURE_ALREADY_EXISTS"):
        demo.prepare(tmp_path, 24222)
    assert not list(tmp_path.iterdir())


def test_fixture_roles_topics_private_keys(demo, tmp_path, monkeypatch):
    monkeypatch.setattr(demo, "ports_available", lambda _: None)
    root = tmp_path / "fresh"
    demo.prepare(root, 24222)
    assert root.stat().st_mode & 0o777 == 0o700
    for role in demo.ROLES:
        cfg = demo.role_config(root, role)
        assert set(cfg["keys"]) == ({"command", "event"} if role == "worker" else
                                    {"command"} if role == "producer" else {"event"})
        assert (root / role / "config.json").stat().st_mode & 0o777 == 0o600
        component = json.loads((root / role / "components/bus.yaml").read_text())
        metadata = {x["name"]: x["value"] for x in component["spec"]["metadata"]}
        assert metadata["subscriptionScopes"] == role + "=" + (cfg["topic"] or "")
        if role.startswith("observer"):
            assert metadata["durableName"] == "demo-" + role
    assert demo.role_config(root, "observer-a")["keys"] == demo.role_config(root, "observer-b")["keys"]
    assert demo.role_config(root, "producer")["keys"]["command"] != demo.role_config(root, "worker")["keys"]["event"]


async def sealed_records(demo):
    from a2a_mq.protection import EnvelopeProtector, LocalKey, LocalKeyRing
    e = demo.command("UNIQUE-PLAINTEXT-CANARY", tenant=demo.TENANT)
    protector = EnvelopeProtector(LocalKeyRing([LocalKey("key", demo.TENANT, "receiver", os.urandom(32))], "key"),
                                  tenant=demo.TENANT, receiver="receiver")
    result = []
    for i, envelope in enumerate([e, *demo.echo_events(e)], 1):
        ce = demo.cloud_event(envelope)
        ce.update(data=json.loads(await protector.seal(envelope)), datacontenttype=demo.SEALED_MIME)
        result.append({"sequence": i, "subject": envelope.route, "event": ce})
    return e, result


async def test_wire_requires_actual_sealed_bytes(demo):
    envelope, records = await sealed_records(demo)
    result = demo.inspect_protected(records, envelope)
    assert len(result) == 4
    assert {x["topic"] for x in result} == {demo.COMMAND, demo.EVENT}
    assert all("sealedSha256" in x for x in result)


@pytest.mark.parametrize("change,code", [
    ("plaintext", "PLAINTEXT_CANARY_ON_BROKER"),
    ("mime", "BROKER_PAYLOAD_NOT_SEALED"),
    ("topic", "BROKER_PAYLOAD_NOT_SEALED"),
    ("missing", "BROKER_MESSAGE_NOT_UNIQUE"),
    ("duplicate", "BROKER_MESSAGE_NOT_UNIQUE"),
])
async def test_wire_refuses_false_proof(demo, change, code):
    envelope, records = await sealed_records(demo)
    if change == "plaintext":
        records[0]["event"]["data"]["leak"] = "UNIQUE-PLAINTEXT-CANARY"
    elif change == "mime":
        records[0]["event"]["datacontenttype"] = "application/json"
    elif change == "topic":
        records[0]["subject"] = demo.EVENT
    elif change == "missing":
        records.pop()
    else:
        records.append(records[0])
    with pytest.raises(RuntimeError, match=code):
        demo.inspect_protected(records, envelope)


def test_receipts_do_not_export_plaintext(demo, tmp_path):
    (tmp_path / "worker").mkdir()
    envelope = demo.command("SECRET-CANARY", tenant=demo.TENANT)
    demo.receipt(tmp_path, "worker", envelope)
    raw = (tmp_path / "worker/receipts.jsonl").read_text()
    assert "SECRET-CANARY" not in raw
    row = demo.read_receipts(tmp_path, "worker")[0]
    assert row["pid"] == os.getpid() and len(row["payloadSha256"]) == 64


async def test_negative_cases_use_real_publisher_boundary(demo, tmp_path, monkeypatch):
    monkeypatch.setattr(demo, "ports_available", lambda _: None)
    root = tmp_path / "fresh"
    demo.prepare(root, 24222)
    captured = []
    class Bus:
        async def _post(self, route, event):
            captured.append((route, event))
        async def close(self):
            pass
    monkeypatch.setattr(demo, "bus_for", lambda _: Bus())
    ids = await demo.negative_publications(root)
    assert set(ids) == {"plaintext", "tamper", "wrong-receiver", "wrong-key"}
    assert len(set(ids.values())) == 4
    assert all(route == demo.COMMAND for route, event in captured)
    assert captured[0][1]["datacontenttype"] == "application/json"
    protector = demo.protectors(demo.role_config(root, "worker"))[demo.COMMAND]
    from a2a_mq.domain import PermanentError
    for _, event in captured[1:]:
        with pytest.raises(PermanentError):
            await protector.open(demo.canonical(event["data"]).encode(), expected_route=demo.COMMAND)
    assert base64.b64decode(captured[1][1]["data"]["ciphertext"])


def test_output_profile_must_be_fresh(demo, tmp_path, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["demo", "--work-dir", str(tmp_path)])
    with pytest.raises(SystemExit) as error:
        demo.main()
    assert error.value.code == 2


def test_cleanup_attempts_every_owned_process_and_fails_closed(demo):
    from types import SimpleNamespace
    calls = []
    def broken():
        calls.append("broken")
        raise RuntimeError("stop failed")
    good = SimpleNamespace(process=object())
    def stop_good():
        calls.append("good")
        good.process = None
    good.stop = stop_good
    bad = SimpleNamespace(process=object(), stop=broken)
    report = {"status": "PASS"}
    with pytest.raises(RuntimeError, match="OWNED_PROCESS_CLEANUP_FAILED"):
        demo.stop_owned({"good": good, "bad": bad}, report)
    assert calls == ["broken", "good"]
    assert report == {"status": "FAIL", "ownedProcessesStopped": False}


def test_main_exception_after_pass_never_exports_pass(demo, tmp_path, monkeypatch):
    output = tmp_path / "report.json"
    monkeypatch.setattr(sys, "argv", ["demo", "--work-dir", str(tmp_path / "new"),
        "--tools-bin", str(tmp_path), "--output", str(output)])
    monkeypatch.setattr(demo, "_verified_bootstrap", lambda *args: {"binaries": {}})
    def failed_run(root, tools, base, report):
        report["status"] = "PASS"
        raise RuntimeError("CLEANUP_FAILED")
    monkeypatch.setattr(demo, "run", failed_run)
    assert demo.main() == 1
    assert json.loads(output.read_text())["status"] == "FAIL"


async def test_handler_entry_precedes_worker_effects(demo, tmp_path, monkeypatch):
    monkeypatch.setattr(demo, "ports_available", lambda _: None)
    root = tmp_path / "fresh"
    demo.prepare(root, 24222)
    captured = {}
    def fake_subscriber(handler, bus, **kwargs):
        captured.update(handler=handler, lifespan=kwargs["lifespan"])
        return object()
    monkeypatch.setattr(demo, "subscriber", fake_subscriber)
    async def fail_publish(self, envelope):
        raise RuntimeError("fixture effect failure")
    monkeypatch.setattr(demo.ProtectedPublisher, "publish", fail_publish)
    app = demo.make_app(root, "worker")
    envelope = demo.command("PRIVATE-CANARY", tenant=demo.TENANT)
    async with captured["lifespan"](app):
        with pytest.raises(RuntimeError, match="fixture effect failure"):
            await captured["handler"](envelope)
    text = (root / "worker/handler-entries.jsonl").read_text()
    assert json.loads(text)["messageId"] == envelope.as_dict()["messageId"]
    assert "PRIVATE-CANARY" not in text
    assert demo.read_receipts(root, "worker") == []


@pytest.mark.parametrize("accepted", [True, False])
async def test_publication_digest_requires_real_boundary_acceptance(demo, tmp_path, monkeypatch, accepted):
    (tmp_path / "producer").mkdir()
    async def boundary(self, topic, event):
        if not accepted:
            raise RuntimeError("sidecar rejected")
    monkeypatch.setattr(demo.DaprPublisher, "_post", boundary)
    publisher = demo.RecordedPublisher({"httpPort": 24233, "daprToken": "test-token"}, tmp_path)
    event = {"id": "message", "data": {"ciphertext": "opaque"}}
    try:
        if accepted:
            await publisher._post(demo.COMMAND, event)
            row = json.loads((tmp_path / "producer/publications.jsonl").read_text())
            assert row["sealedSha256"] == demo.digest(demo.canonical(event["data"]).encode())
        else:
            with pytest.raises(RuntimeError, match="sidecar rejected"):
                await publisher._post(demo.COMMAND, event)
            assert not (tmp_path / "producer/publications.jsonl").exists()
    finally:
        await publisher.close()
