"""Deterministic model-free fixture. No external or irreversible tool invocation."""
from __future__ import annotations

from typing import Any
from uuid import NAMESPACE_URL, uuid5

from .domain import Envelope, require, snapshot, token


def echo_events(request: Envelope, owner: str = "worker-echo") -> list[Envelope]:
    c = request.as_dict()
    require(c["operation"] == "message.send", "UNSUPPORTED_OPERATION")
    message = c["payload"]["message"]
    require("taskId" not in message and "taskId" not in c, "CONTINUATION_NOT_IMPLEMENTED")
    require(all("text" in part for part in message["parts"]), "ECHO_REQUIRES_TEXT")
    text = "\n".join(part["text"] for part in message["parts"])
    require(len(text.encode()) <= 16384, "ECHO_INPUT_TOO_LARGE")
    identity = f"a2a-mq/{c['tenantId']}/{token(owner)}/{c['messageId']}"
    task_id = str(uuid5(NAMESPACE_URL, identity))
    context = message.get("contextId", str(uuid5(NAMESPACE_URL, identity + "/context")))
    result = []
    for sequence, state in enumerate(("SUBMITTED", "WORKING", "COMPLETED"), 1):
        task: dict[str, Any] = {"id": task_id, "contextId": context, "status": {"state": "TASK_STATE_" + state}}
        if state == "COMPLETED":
            task["artifacts"] = [{"artifactId": "echo", "parts": [{"text": text}]}]
        event = snapshot(request, task, sequence, owner,
                         str(uuid5(NAMESPACE_URL, identity + f"/event/{sequence}"))).as_dict()
        # This model-free fixture uses request time, not execution time. Replays
        # and fresh processes must emit identical bytes for each stable event ID.
        event["createdAt"] = c["createdAt"]
        result.append(Envelope.from_dict(event))
    return result
