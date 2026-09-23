"""Deterministic fault-injection hooks for local runtime resilience certification.

Hooks are completely inert by default and activate only when the validation harness sets
the dedicated enable sentinel together with its fault configuration.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import time
from typing import Any

ENABLE_ENV = "A2AMQ_FAULT_INJECTION_ENABLE"
MODE_ENV = "A2AMQ_FAULT_INJECTION_MODE"
TARGET_ENV = "A2AMQ_FAULT_INJECTION_TARGET"
RECORD_FILE_ENV = "A2AMQ_FAULT_RECORD_FILE"
SYNC_DIR_ENV = "A2AMQ_FAULT_SYNC_DIR"

_ENABLE = os.environ.get(ENABLE_ENV)
_MODE = os.environ.get(MODE_ENV)
_TARGET = os.environ.get(TARGET_ENV)
_RECORD_FILE = os.environ.get(RECORD_FILE_ENV)
_SYNC_DIR = os.environ.get(SYNC_DIR_ENV)
_CRASHED: set[str] = set()


def enabled() -> bool:
    return _ENABLE == "local-validation-v1"


def is_active(mode: str, target: str | None = None) -> bool:
    if not enabled() or not _MODE or _MODE != mode:
        return False
    if _TARGET and _TARGET != "*" and target is not None:
        return _TARGET == target
    return True


def record_event(event_type: str, data: dict[str, Any]) -> None:
    if not enabled() or not _RECORD_FILE:
        return
    payload = {
        "event": event_type,
        "pid": os.getpid(),
        "timestamp": time.time(),
        **data,
    }
    line = json.dumps(payload, sort_keys=True) + "\n"
    try:
        path = Path(_RECORD_FILE)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())
    except Exception:
        pass


def _crash(reason: str, key: str, data: dict[str, Any]) -> None:
    _CRASHED.add(key)
    record_event(reason, {"key": key, **data})
    # Forceful abrupt process exit via SIGKILL to eliminate any graceful teardown or ACK
    os.kill(os.getpid(), signal.SIGKILL)


def hook_broker_delivery(envelope_dict: dict[str, Any]) -> None:
    """Record physical broker delivery to /events."""
    mid = envelope_dict.get("messageId", "")
    tid = envelope_dict.get("taskId", "")
    payload = envelope_dict.get("payload")
    user_mid = payload.get("message", {}).get("messageId", "") if isinstance(payload, dict) else ""
    record_event("broker_delivery", {"messageId": mid, "taskId": tid, "userMessageId": user_mid})


def hook_before_ack(envelope_dict: dict[str, Any]) -> None:
    """Invoked right after handler finishes but BEFORE returning SUCCESS ack to Dapr."""
    mid = envelope_dict.get("messageId", "")
    tid = envelope_dict.get("taskId", "")
    payload = envelope_dict.get("payload")
    user_mid = payload.get("message", {}).get("messageId", "") if isinstance(payload, dict) else ""
    if (is_active("unacked_delivery", mid) or is_active("unacked_delivery", tid)
            or is_active("unacked_delivery", user_mid)):
        key = f"unacked:{mid or tid or user_mid}"
        if key not in _CRASHED:
            _crash("unacked_delivery_crash", key, {"messageId": mid, "taskId": tid, "userMessageId": user_mid})


def hook_publish_before_bookkeeping(action: Any) -> None:
    """Invoked immediately after publisher.publish succeeds but before delivered() bookkeeping."""
    aid = getattr(action, "action_id", "")
    tid = getattr(action, "task_id", "")
    kind = getattr(action, "kind", "")
    record_event("publish_attempt", {"actionId": aid, "taskId": tid, "kind": kind})
    if is_active("publish_before_bookkeeping", aid) or is_active("publish_before_bookkeeping", tid):
        key = f"publish_before_bookkeeping:{aid or tid}"
        if key not in _CRASHED:
            _crash("publish_before_bookkeeping_crash", key, {"actionId": aid, "taskId": tid})


def hook_execution_crash(action: Any) -> None:
    """Invoked when executor has transitioned task to WORKING in SQLite and started executing."""
    tid = getattr(action, "task_id", "")
    record_event("executor_working", {"taskId": tid})
    if is_active("execution_crash", tid):
        key = f"execution_crash:{tid}"
        if key not in _CRASHED:
            _crash("execution_crash_kill", key, {"taskId": tid})


def hook_terminal_commit(action: Any, phase: str) -> None:
    """Invoked at boundaries around the terminal commit (before_completed, after_completed, after_finish)."""
    tid = getattr(action, "task_id", "")
    record_event("terminal_phase", {"taskId": tid, "phase": phase})

    # If deterministic synchronization is requested via sync files
    if enabled() and _SYNC_DIR:
        sync_dir = Path(_SYNC_DIR)
        pause_file = sync_dir / f"pause_{phase}_{tid}"
        resume_file = sync_dir / f"resume_{phase}_{tid}"
        if pause_file.exists():
            record_event("terminal_paused", {"taskId": tid, "phase": phase})
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline and not resume_file.exists():
                time.sleep(0.02)
            record_event("terminal_resumed", {"taskId": tid, "phase": phase})

    if is_active("terminal_commit_crash", f"{phase}:{tid}"):
        key = f"terminal_commit_crash:{phase}:{tid}"
        if key not in _CRASHED:
            _crash("terminal_commit_crash_kill", key, {"taskId": tid, "phase": phase})
