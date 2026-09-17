"""Pure, bounded options for the model-free Dapr reference workflow."""
from __future__ import annotations

from .domain import require


def text_parts(message: dict) -> list[str]:
    require(isinstance(message, dict) and message.get("role") == "ROLE_USER", "INVALID_MESSAGE")
    parts = message.get("parts")
    require(isinstance(parts, list) and 1 <= len(parts) <= 32, "INVALID_PARTS")
    require(all(isinstance(p, dict) and isinstance(p.get("text"), str)
                and not set(p) & {"raw", "url", "data"} for p in parts), "TEXT_PARTS_REQUIRED")
    values = [p["text"] for p in parts]
    require(sum(len(v.encode("utf-8")) for v in values) <= 16384, "WORKFLOW_INPUT_TOO_LARGE")
    return values


def options(request: dict) -> dict:
    text_parts(request.get("message"))
    metadata = request.get("metadata", {})
    require(isinstance(metadata, dict), "INVALID_METADATA")
    config = metadata.get("a2amq", {})
    require(isinstance(config, dict) and set(config) <= {
        "inputRequired", "authRequired", "delaySeconds", "transform"}, "INVALID_WORKFLOW_OPTIONS")
    result = {"inputRequired": False, "authRequired": False, "delaySeconds": 0, "transform": "echo", **config}
    require(all(type(result[k]) is bool for k in ("inputRequired", "authRequired")), "INVALID_WORKFLOW_OPTIONS")
    require(type(result["delaySeconds"]) in {int, float} and 0 <= result["delaySeconds"] <= 86400,
            "INVALID_WORKFLOW_OPTIONS")
    require(isinstance(result["transform"], str) and result["transform"] in {"echo", "upper"}, "INVALID_WORKFLOW_OPTIONS")
    return result
