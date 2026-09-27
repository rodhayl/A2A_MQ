"""Deterministic fixture identities bind stable bytes across replay and restart."""
import os
from pathlib import Path
import subprocess
import sys

import pytest

import a2a_mq.domain as domain
from a2a_mq.domain import Envelope, command
from a2a_mq.reference import echo_events


def test_replay_preserves_exact_wires_across_clock_changes(monkeypatch):
    request = command("stable fixture")
    monkeypatch.setattr(domain, "now_text", lambda: "2026-09-16T10:00:00Z")
    first = echo_events(request)
    monkeypatch.setattr(domain, "now_text", lambda: "2026-09-17T10:00:00Z")
    second = echo_events(request)
    assert [e.wire for e in first] == [e.wire for e in second]
    assert [e.digest for e in first] == [e.digest for e in second]
    assert all(e.as_dict()["createdAt"] == request.as_dict()["createdAt"] for e in second)


def test_replay_preserves_bytes_in_fresh_processes():
    request = command("process restart")
    code = (
        "import json, sys; from a2a_mq.domain import Envelope; "
        "from a2a_mq.reference import echo_events; "
        "print(json.dumps([e.wire for e in echo_events(Envelope.parse(sys.stdin.read()))]))"
    )
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1] / "src"))
    results = [subprocess.run([sys.executable, "-c", code], input=request.wire,
                             text=True, capture_output=True, timeout=10, env=env) for _ in range(2)]
    assert all(r.returncode == 0 for r in results), [r.stderr for r in results]
    assert results[0].stdout == results[1].stdout


@pytest.mark.parametrize("change", ["tenant", "consumer"])
def test_identity_namespaces_remain_separate(change):
    request = command("same request id")
    first = echo_events(request)
    other = request.as_dict()
    if change == "tenant":
        other["tenantId"] = "other"
    second = echo_events(Envelope.from_dict(other), "other" if change == "consumer" else "worker-echo")
    assert {e.as_dict()["messageId"] for e in first}.isdisjoint(
        {e.as_dict()["messageId"] for e in second})
    assert first[-1].as_dict()["taskId"] != second[-1].as_dict()["taskId"]
