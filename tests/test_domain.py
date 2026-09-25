import copy
import json
import random
from datetime import datetime, timezone
from pathlib import Path

import pytest

from a2a_mq.domain import (
    Envelope, PermanentError, STATES, TERMINAL, can_transition, command,
    load_json, reachable, snapshot, token,
)

EXAMPLE = json.loads((Path(__file__).resolve().parents[1] / "spec/examples/send.json").read_text())


def test_round_trip_and_immutability():
    original = copy.deepcopy(EXAMPLE)
    original["payload"]["message"]["metadata"] = {"extension": [1, {"value": None}]}
    e = Envelope.from_dict(original)
    assert e.as_dict() == original
    original["payload"]["message"]["parts"][0]["text"] = "changed"
    e.as_dict()["payload"].clear()
    assert Envelope.parse(e.wire).digest == e.digest
    assert e.as_dict()["payload"]["message"]["parts"][0]["text"] == "hello"


@pytest.mark.parametrize("old", sorted(STATES))
@pytest.mark.parametrize("new", sorted(STATES))
def test_transition_matrix(old, new):
    allowed = {
        "SUBMITTED": {"WORKING", "FAILED", "CANCELED", "REJECTED"},
        "WORKING": STATES - {"SUBMITTED"},
        "INPUT_REQUIRED": {"WORKING", "CANCELED", "FAILED", "REJECTED"},
        "AUTH_REQUIRED": {"WORKING", "CANCELED", "FAILED", "REJECTED"},
    }
    assert can_transition(old, new) == (new in allowed.get(old, set()))
    if old in TERMINAL:
        assert reachable(old, new) == (old == new)


def test_reachability_is_not_adjacency():
    assert not can_transition("SUBMITTED", "COMPLETED")
    assert reachable("SUBMITTED", "COMPLETED")
    assert not reachable("COMPLETED", "WORKING")
    assert not reachable("unknown", "unknown")


@pytest.mark.parametrize("bad", ["", "a.b", "A", "a_b", "*", ">", "a/b", "a b", "-a", "a" * 64, "\u00e9"])
def test_route_rejects_without_lossy_normalization(bad):
    with pytest.raises(PermanentError):
        token(bad)


def test_routes_and_factories():
    e = command("hello")
    assert e.route == "a2a.v1.demo.command.skill.echo"
    d = e.as_dict()
    d["target"] = {"agentId": "echo-worker"}
    assert Envelope.from_dict(d).route == "a2a.v1.demo.command.agent.echo-worker"
    task = {"id": "opaque-task", "contextId": "opaque-context", "status": {"state": "TASK_STATE_COMPLETED"},
            "artifacts": [{"artifactId": "output", "parts": [{"data": None}]}]}
    event = snapshot(e, task, 3, "echo-worker")
    assert event.route == "a2a.v1.demo.event.task"
    assert event.as_dict()["causationId"] == e.as_dict()["messageId"]


@pytest.mark.parametrize("raw", [b'{}{}', b'\xff', b'{"x":1,"x":2}', b'{"nested":{"x":1,"x":2}}',
                                b'{"value":NaN}', b'{"value":1e1000}', b'[' * 40 + b'0' + b']' * 40])
def test_bad_json(raw):
    with pytest.raises(PermanentError):
        load_json(raw)


def test_byte_limit():
    with pytest.raises(PermanentError, match="MESSAGE_TOO_LARGE"):
        Envelope.from_dict(command("x" * 262144).as_dict())


@pytest.mark.parametrize("mutator", [
    lambda d: d.update(taskId="wrong"),
    lambda d: d["payload"]["message"].update(role="user"),
    lambda d: d["payload"]["message"].update(parts=[]),
    lambda d: d["payload"]["message"].update(parts=[{"text": "a", "data": {}}]),
    lambda d: d["payload"]["message"].update(parts=[{"kind": "text", "text": "a"}]),
    lambda d: d["payload"]["message"].update(parts=[{"raw": "!"}]),
    lambda d: d.update(traceparent="00-" + "0" * 32 + "-" + "1" * 16 + "-01"),
    lambda d: d.update(tracestate="vendor=value"),
    lambda d: d.update(expiresAt="2020-01-01T00:00:00Z"),
    lambda d: d.update(createdAt="2026-09-15T16:00:00"),
])
def test_semantic_errors(mutator):
    d = copy.deepcopy(EXAMPLE)
    mutator(d)
    with pytest.raises(PermanentError):
        Envelope.from_dict(d)


def test_expiry_boundary_and_trace():
    d = copy.deepcopy(EXAMPLE)
    d["expiresAt"] = "2026-09-16T00:00:00Z"
    d["traceparent"] = "00-" + "1" * 32 + "-" + "2" * 16 + "-01"
    d["tracestate"] = "vendor=value"
    e = Envelope.from_dict(d)
    e.check_expiry(datetime(2026, 9, 15, 23, 59, tzinfo=timezone.utc))
    with pytest.raises(PermanentError, match="EXPIRED"):
        e.check_expiry(datetime(2026, 9, 16, tzinfo=timezone.utc))


def test_malformed_fuzz_rejected():
    rng = random.Random(418)
    for _ in range(100):
        d = copy.deepcopy(EXAMPLE)
        d[rng.choice(["messageId", "kind", "source", "operation", "target"])] = rng.choice([None, [], 77, False, {}])
        with pytest.raises(PermanentError):
            Envelope.from_dict(d)


@pytest.mark.parametrize("operation", ["task.status", "task.artifact"])
def test_delta_codecs(operation):
    e = snapshot(command("hello"), {"id": "task", "contextId": "ctx", "status": {"state": "TASK_STATE_WORKING"}}, 1, "worker")
    d = e.as_dict()
    d["operation"] = operation
    d["payload"] = {"taskId": "task", "contextId": "ctx"}
    if operation == "task.status":
        d["payload"]["status"] = {"state": "TASK_STATE_WORKING"}
    else:
        d["payload"].update(artifact={"artifactId": "art", "parts": [{"text": "chunk"}]}, append=True, lastChunk=False)
    assert Envelope.from_dict(d).as_dict() == d
    d["payload"]["contextId"] = "wrong"
    with pytest.raises(PermanentError, match="IDENTIFIER_MISMATCH"):
        Envelope.from_dict(d)


@pytest.mark.parametrize("role", [[], {}, None, 1, True, "user"])
def test_bad_nested_role_is_permanent(role):
    d = copy.deepcopy(EXAMPLE)
    d["payload"]["message"]["role"] = role
    with pytest.raises(PermanentError):
        Envelope.from_dict(d)


@pytest.mark.upstream
@pytest.mark.parametrize("operation", ["message.send", "task.snapshot"])
def test_upstream_protobuf_payload_round_trip(operation):
    sdk = pytest.importorskip("a2a.types", reason="optional pinned upstream SDK not installed")
    json_format = pytest.importorskip("google.protobuf.json_format")
    if operation == "message.send":
        payload, model = EXAMPLE["payload"], sdk.SendMessageRequest()
    else:
        payload = {"id": "task", "contextId": "ctx", "status": {"state": "TASK_STATE_COMPLETED"},
                   "artifacts": [{"artifactId": "result", "parts": [{"text": "hello"}]}]}
        model = sdk.Task()
    json_format.ParseDict(payload, model)
    assert json_format.MessageToDict(model) == payload
