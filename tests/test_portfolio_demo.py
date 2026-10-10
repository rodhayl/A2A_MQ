"""Harness contracts; the actual native demo is a separate, recorded live check."""
import importlib.util
from pathlib import Path
import json
import base64
import signal
from types import SimpleNamespace

import pytest


@pytest.fixture
def demo():
    path = Path(__file__).resolve().parents[1] / "scripts/portfolio_demo.py"
    spec = importlib.util.spec_from_file_location("portfolio_demo_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_measurement_nearest_rank(demo):
    assert demo.percentile(list(range(1, 21)), .95) == 19
    assert demo.percentile([5], .95) == 5


def test_synthetic_request_uses_stable_message_identity(demo):
    assert demo.request("same") == demo.request("same")
    assert demo.request("same") != demo.request("same", "changed")
    assert demo.request("same")["metadata"]["a2amq"]["transform"] == "upper"


def test_cleanup_targets_only_owned_group_even_after_app_crash(demo, tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(demo.os, "killpg", lambda pid, sig: calls.append((pid, sig)))
    monkeypatch.setattr(demo.time, "sleep", lambda _: None)
    stack = demo.OwnedStack(tmp_path, tmp_path)
    stack.process = SimpleNamespace(pid=7654321, wait=lambda timeout: -signal.SIGKILL)
    stack.stop()
    assert calls == [(7654321, signal.SIGTERM), (7654321, signal.SIGKILL)]
    assert stack.process is None
    stack.stop()
    assert len(calls) == 2


def test_existing_profile_is_never_overwritten(demo, tmp_path, monkeypatch):
    profile = tmp_path / "profile"
    profile.mkdir()
    marker = profile / "existing"
    marker.write_text("preserve")
    monkeypatch.setattr(demo.sys, "argv", ["demo", "--work-dir", str(profile), "--tools-bin", str(tmp_path),
                                         "--output", str(tmp_path / "report.json")])
    with pytest.raises(SystemExit) as error:
        demo.main()
    assert error.value.code == 2
    assert marker.read_text() == "preserve"
    assert not (tmp_path / "report.json").exists()


def test_source_digest_excludes_private_logs_and_changes_with_code(demo, tmp_path):
    (tmp_path / "src").mkdir()
    (tmp_path / "scripts").mkdir()
    source = tmp_path / "src/code.py"
    source.write_text("one")
    (tmp_path / "pyproject.toml").write_text("metadata")
    (tmp_path / "scripts/portfolio_demo.py").write_text("demo")
    before = demo.source_digest(tmp_path)
    (tmp_path / "private.log").write_text("should not enter digest")
    assert before == demo.source_digest(tmp_path)
    source.write_text("two")
    assert before != demo.source_digest(tmp_path)


def test_sigterm_enters_normal_cleanup_path(demo):
    with pytest.raises(KeyboardInterrupt):
        demo.stop_signal(signal.SIGTERM, None)


def test_receipt_checkpoint_rejects_prior_or_different_identity(demo, tmp_path):
    rows = [
        {"event": "broker_delivery", "taskId": "task", "messageId": "same", "timestamp": 10},
        {"event": "broker_delivery", "taskId": "task", "messageId": "other", "timestamp": 12},
        {"event": "broker_delivery", "taskId": "other", "messageId": "same", "timestamp": 12},
        {"event": "publish_attempt", "taskId": "task", "messageId": "same", "timestamp": 12},
        {"event": "broker_delivery", "taskId": "task", "messageId": "same", "timestamp": 13},
    ]
    (tmp_path / "fault-events.jsonl").write_text("\n".join(json.dumps(row) for row in rows))
    assert demo.physical_deliveries(tmp_path, "task", "same", after=10) == [rows[-1]]


@pytest.mark.parametrize("pending,floor,accepted", [(0, 7, True), (1, 7, False), (0, 6, False)])
def test_broker_ack_requires_floor_and_no_pending(demo, pending, floor, accepted):
    consumer = {"config": {"filter_subject": "command"}, "num_ack_pending": pending,
                "ack_floor": {"stream_seq": floor}}
    assert (demo.acknowledged_consumer([consumer], "command", 7) is consumer) is accepted


@pytest.mark.parametrize("count", [0, 2])
def test_broker_ack_rejects_ambiguous_consumer(demo, count):
    consumer = {"config": {"filter_subject": "command"}, "num_ack_pending": 0,
                "ack_floor": {"stream_seq": 7}}
    with pytest.raises(RuntimeError, match="COMMAND_CONSUMER_NOT_UNIQUE"):
        demo.acknowledged_consumer([consumer] * count, "command", 7)


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_id", ["same", "missing"])
async def test_original_checkpoint_reads_real_metadata_and_closes_connection(demo, tmp_path, monkeypatch,
                                                                            expected_id):
    import nats
    calls, closed = [], []
    (tmp_path / "nats-token").write_text("disposable-fixture")
    event = {"id": "same", "data": {"messageId": "same", "taskId": "task"}}
    values = {
        "$JS.API.STREAM.INFO.A2A_MQ": {"config": {"duplicate_window": 120_000_000_000},
                                       "state": {"first_seq": 7, "last_seq": 7}},
        "$JS.API.STREAM.MSG.GET.A2A_MQ": {"message": {"subject": "command", "seq": 7,
            "data": base64.b64encode(json.dumps(event).encode()).decode()}},
        "$JS.API.CONSUMER.LIST.A2A_MQ": {"total": 1, "consumers": [
            {"name": "worker", "config": {"filter_subject": "command"}, "num_ack_pending": 0,
             "ack_floor": {"stream_seq": 7}}]},
    }
    async def query(subject, body, timeout):
        calls.append(subject)
        return SimpleNamespace(data=json.dumps(values[subject]).encode())
    async def close():
        closed.append(True)
    async def connect(**kwargs):
        return SimpleNamespace(request=query, close=close)
    monkeypatch.setattr(nats, "connect", connect)
    if expected_id == "same":
        result = await demo.original_ack_checkpoint(tmp_path, "task", expected_id)
        assert result["dedupWindowSeconds"] == 120
        assert result["streamSequence"] == result["ackFloorStreamSequence"] == 7
        assert result["messageId"] == "same"
    else:
        with pytest.raises(RuntimeError, match="ORIGINAL_BROKER_MESSAGE_NOT_UNIQUE"):
            await demo.original_ack_checkpoint(tmp_path, "task", expected_id)
    assert closed == [True]
    assert all(".INFO." in subject or ".GET." in subject or ".LIST." in subject for subject in calls)
