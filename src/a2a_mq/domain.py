"""Pure, model-free A2A-MQ contracts. No transport or storage dependency."""
from __future__ import annotations

import base64
import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib.resources import files
from typing import Any
from uuid import uuid4

from jsonschema import Draft202012Validator, FormatChecker

MAX_BYTES = 262144
TOKEN = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
STATES = frozenset("SUBMITTED WORKING COMPLETED FAILED CANCELED INPUT_REQUIRED REJECTED AUTH_REQUIRED".split())
TERMINAL = frozenset("COMPLETED FAILED CANCELED REJECTED".split())
TRANSITIONS = {
    "SUBMITTED": {"WORKING", "FAILED", "CANCELED", "REJECTED"},
    "WORKING": set(STATES) - {"SUBMITTED"},
    "INPUT_REQUIRED": {"WORKING", "CANCELED", "FAILED", "REJECTED"},
    "AUTH_REQUIRED": {"WORKING", "CANCELED", "FAILED", "REJECTED"},
    **{state: set() for state in TERMINAL},
}
SCHEMA = json.loads(files("a2a_mq").joinpath("schema.json").read_text())
VALIDATOR = Draft202012Validator(SCHEMA, format_checker=FormatChecker())


class PermanentError(ValueError):
    """Safe reason code; never include task content in the exception."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


class RetryableError(RuntimeError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def require(condition: bool, code: str = "INVALID_PAYLOAD") -> None:
    if not condition:
        raise PermanentError(code)


def token(value: str) -> str:
    require(isinstance(value, str) and TOKEN.fullmatch(value) is not None, "INVALID_ROUTE")
    return value


def canonical(value: Any) -> str:
    try:
        text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        text.encode("utf-8", errors="strict")
        return text
    except (TypeError, ValueError, RecursionError):
        raise PermanentError("INVALID_JSON") from None


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        require(key not in result, "DUPLICATE_JSON_KEY")
        result[key] = value
    return result


def _depth(value: Any, level: int = 0) -> None:
    require(level <= 32, "JSON_TOO_DEEP")
    if isinstance(value, dict):
        for key, child in value.items():
            key.encode("utf-8", errors="strict")
            _depth(child, level + 1)
    elif isinstance(value, list):
        for child in value:
            _depth(child, level + 1)
    elif isinstance(value, str):
        value.encode("utf-8", errors="strict")
    elif isinstance(value, float):
        require(math.isfinite(value), "INVALID_JSON")


def load_json(raw: bytes | str, limit: int = MAX_BYTES) -> Any:
    try:
        data = raw.encode("utf-8") if isinstance(raw, str) else raw
        require(len(data) <= limit, "MESSAGE_TOO_LARGE")
        value = json.loads(data.decode("utf-8"), object_pairs_hook=_pairs)
        _depth(value)
        return value
    except PermanentError:
        raise
    except (UnicodeError, ValueError, RecursionError):
        raise PermanentError("INVALID_JSON") from None


def timestamp(value: str) -> datetime:
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        require(result.tzinfo is not None, "INVALID_TIME")
        return result
    except (ValueError, TypeError, AttributeError):
        raise PermanentError("INVALID_TIME") from None


def now_text() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def state_of(task: dict[str, Any]) -> str:
    status = task.get("status")
    require(isinstance(status, dict))
    assert isinstance(status, dict)
    value = status.get("state")
    require(isinstance(value, str) and value.startswith("TASK_STATE_"))
    assert isinstance(value, str)
    state = value.removeprefix("TASK_STATE_")
    require(state in STATES)
    return state


def can_transition(old: str, new: str) -> bool:
    return new in TRANSITIONS.get(old, set())


def reachable(old: str, new: str) -> bool:
    """Snapshot versions may skip states; this is not an executor transition."""
    if old not in STATES or new not in STATES:
        return False
    seen, pending = set(), [old]
    while pending:
        current = pending.pop()
        if current == new:
            return True
        if current not in seen:
            seen.add(current)
            pending.extend(TRANSITIONS[current] - seen)
    return False


def _id(value: Any) -> None:
    require(isinstance(value, str) and 0 < len(value) <= 256)


def _parts(parts: Any) -> None:
    require(isinstance(parts, list) and 0 < len(parts) <= 128)
    for part in parts:
        require(isinstance(part, dict) and "kind" not in part)
        keys = set(part) & {"text", "raw", "url", "data"}
        require(len(keys) == 1)
        key = next(iter(keys))
        if key != "data":
            require(isinstance(part[key], str))
        if key == "raw":
            try:
                base64.b64decode(part[key], validate=True)
            except (ValueError, TypeError):
                raise PermanentError("INVALID_PAYLOAD") from None


def _message(message: Any) -> None:
    require(isinstance(message, dict))
    _id(message.get("messageId"))
    require(isinstance(message.get("role"), str) and message["role"] in {"ROLE_USER", "ROLE_AGENT"})
    _parts(message.get("parts"))
    for key in ("taskId", "contextId"):
        if key in message:
            _id(message[key])


def _artifact(artifact: Any) -> None:
    require(isinstance(artifact, dict))
    _id(artifact.get("artifactId"))
    _parts(artifact.get("parts"))


def validate_payload(e: dict[str, Any]) -> None:
    """Validate the supported structural subset, preserving extension data."""
    p, op = e["payload"], e["operation"]
    if op == "message.send":
        _message(p.get("message"))
        inner = p["message"]
        config = p.get("configuration", {})
        require(isinstance(config, dict))
        if "returnImmediately" in config:
            require(type(config["returnImmediately"]) is bool)
    elif op == "task.snapshot":
        _id(p.get("id"))
        _id(p.get("contextId"))
        state_of(p)
        artifacts, history = p.get("artifacts", []), p.get("history", [])
        require(isinstance(artifacts, list) and len(artifacts) <= 32)
        require(isinstance(history, list) and len(history) <= 32)
        for artifact in artifacts:
            _artifact(artifact)
        for message in history:
            _message(message)
        inner = {"taskId": p["id"], "contextId": p["contextId"]}
    else:
        _id(p.get("taskId"))
        _id(p.get("contextId"))
        if op == "task.status":
            state_of(p)
        else:
            _artifact(p.get("artifact"))
            for key in ("append", "lastChunk"):
                if key in p:
                    require(type(p[key]) is bool)
        inner = p
    for key in ("taskId", "contextId"):
        if key in e:
            require(inner.get(key) == e[key], "IDENTIFIER_MISMATCH")


@dataclass(frozen=True)
class Envelope:
    """Validated immutable wire value; returned dictionaries are defensive copies."""

    wire: str

    def __post_init__(self) -> None:
        value = load_json(self.wire)
        require(isinstance(value, dict), "INVALID_ENVELOPE")
        require(next(VALIDATOR.iter_errors(value), None) is None, "INVALID_ENVELOPE")
        created = timestamp(value["createdAt"])
        if "expiresAt" in value:
            require(timestamp(value["expiresAt"]) > created, "INVALID_EXPIRY")
        if "traceparent" in value:
            _, trace, span, _ = value["traceparent"].split("-")
            require(int(trace, 16) != 0 and int(span, 16) != 0, "INVALID_TRACE")
        if "tracestate" in value:
            require("traceparent" in value and not any(c in value["tracestate"] for c in "\r\n"), "INVALID_TRACE")
        validate_payload(value)
        object.__setattr__(self, "wire", canonical(value))

    @classmethod
    def parse(cls, raw: bytes | str) -> Envelope:
        return cls(canonical(load_json(raw)))

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> Envelope:
        return cls(canonical(value))

    def as_dict(self) -> dict[str, Any]:
        return json.loads(self.wire)

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.wire.encode()).hexdigest()

    @property
    def route(self) -> str:
        e = self.as_dict()
        prefix = f"a2a.v1.{e['tenantId']}"
        if e["kind"] == "event":
            return prefix + ".event.task"
        target = e["target"]
        key = "agentId" if "agentId" in target else "skill"
        return f"{prefix}.command.{'agent' if key == 'agentId' else 'skill'}.{target[key]}"

    def check_expiry(self, now: datetime | None = None) -> None:
        value = self.as_dict()
        if "expiresAt" in value:
            current = now or datetime.now(timezone.utc)
            require(current.tzinfo is not None, "INVALID_TIME")
            require(current < timestamp(value["expiresAt"]), "EXPIRED")


def command(text: str, tenant: str = "demo", skill: str = "echo", source: str = "planner") -> Envelope:
    mid = str(uuid4())
    return Envelope.from_dict({
        "envelopeVersion": "1.0", "a2aVersion": "1.0", "tenantId": tenant,
        "messageId": mid, "kind": "command", "operation": "message.send", "source": source,
        "target": {"skill": skill}, "correlationId": mid, "createdAt": now_text(),
        "payload": {"message": {"messageId": str(uuid4()), "role": "ROLE_USER", "parts": [{"text": text}]}},
    })


def snapshot(request: Envelope, task: dict[str, Any], sequence: int, source: str,
             event_id: str | None = None) -> Envelope:
    c = request.as_dict()
    return Envelope.from_dict({
        "envelopeVersion": "1.0", "a2aVersion": "1.0", "tenantId": c["tenantId"],
        "messageId": event_id or str(uuid4()), "kind": "event", "operation": "task.snapshot",
        "source": source, "correlationId": c["correlationId"], "causationId": c["messageId"],
        "taskId": task["id"], "contextId": task["contextId"], "sequence": sequence,
        "createdAt": now_text(), "payload": task,
        **{key: c[key] for key in ("traceparent", "tracestate") if key in c},
    })
