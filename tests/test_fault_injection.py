import os
import tempfile
from pathlib import Path
from types import SimpleNamespace
import pytest

from a2a_mq import fault_injection


def test_fault_injection_inert_by_default():
    assert fault_injection.is_active("unacked_delivery", "msg-1") is False
    assert fault_injection.is_active("publish_before_bookkeeping", "act-1") is False
    assert fault_injection.is_active("execution_crash", "task-1") is False
    # Hooks do not raise or crash
    fault_injection.hook_broker_delivery({"messageId": "msg-1", "taskId": "t-1"})
    fault_injection.hook_before_ack({"messageId": "msg-1", "taskId": "t-1"})
    fault_injection.hook_publish_before_bookkeeping(SimpleNamespace(action_id="a1", task_id="t1", kind="publish"))
    fault_injection.hook_execution_crash(SimpleNamespace(task_id="t1"))
    fault_injection.hook_terminal_commit(SimpleNamespace(task_id="t1"), "before_completed")


def test_fault_injection_records_events_when_configured(tmp_path, monkeypatch):
    rec_file = tmp_path / "records.jsonl"
    monkeypatch.setattr(fault_injection, "_RECORD_FILE", str(rec_file))
    monkeypatch.setattr(fault_injection, "_MODE", "test_mode")
    monkeypatch.setattr(fault_injection, "_TARGET", "target-1")

    assert fault_injection.is_active("test_mode", "target-1") is True
    assert fault_injection.is_active("test_mode", "other") is False
    assert fault_injection.is_active("other_mode", "target-1") is False

    fault_injection.record_event("custom_event", {"foo": "bar"})
    assert rec_file.exists()
    content = rec_file.read_text()
    assert "custom_event" in content
    assert "bar" in content
