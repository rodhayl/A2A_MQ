"""Machine-readable decision for the supported A2A 1.0 binding surface."""
from __future__ import annotations

import json
from importlib.resources import files
from typing import Any

from .domain import PermanentError, canonical, require

IMPLEMENTED = frozenset({
    "message/send", "message/stream", "tasks/get", "tasks/list", "tasks/cancel",
    "tasks/subscribe", "agent/getAuthenticatedExtendedCard",
})
UNSUPPORTED = frozenset({
    "tasks/pushNotification/create", "tasks/pushNotification/get",
    "tasks/pushNotification/list", "tasks/pushNotification/delete",
})
INTERFACES = frozenset({("JSONRPC", "/rpc", "1.0"), ("HTTP+JSON", "/", "1.0")})


def load_binding() -> dict[str, Any]:
    try:
        document = json.loads(files("a2a_mq").joinpath("binding.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        raise PermanentError("INVALID_BINDING_MANIFEST") from None
    validate_binding(document)
    return document


def validate_binding(document: Any) -> None:
    require(isinstance(document, dict) and set(document) == {
        "schemaVersion", "binding", "protocolVersion", "sdk", "discovery", "interfaces",
        "authentication", "capabilities", "operations", "extensions",
    }, "INVALID_BINDING_MANIFEST")
    require(document["schemaVersion"] == 1 and document["binding"] == "A2A"
            and document["protocolVersion"] == "1.0", "INVALID_BINDING_MANIFEST")
    require(document["sdk"] == {"package": "a2a-sdk", "version": "1.1.2"},
            "INVALID_BINDING_MANIFEST")
    require(document["discovery"] == {"agentCardPath": "/.well-known/agent-card.json",
                                       "authenticatedExtendedCard": True},
            "INVALID_BINDING_MANIFEST")
    interfaces = document["interfaces"]
    require(isinstance(interfaces, list) and len(interfaces) == len(INTERFACES),
            "INVALID_BINDING_MANIFEST")
    parsed = frozenset((item.get("protocolBinding"), item.get("path"), item.get("protocolVersion"))
                       for item in interfaces if isinstance(item, dict))
    require(parsed == INTERFACES, "INVALID_BINDING_MANIFEST")
    require(document["authentication"] == {"scheme": "Bearer", "format": "JWT",
        "algorithms": ["RS256"], "tenantAuthority": "verified-principal"},
        "INVALID_BINDING_MANIFEST")
    require(document["capabilities"] == {"streaming": True, "pushNotifications": False,
                                          "extendedAgentCard": True},
            "INVALID_BINDING_MANIFEST")
    operations = document["operations"]
    require(isinstance(operations, dict) and set(operations) == IMPLEMENTED | UNSUPPORTED,
            "INVALID_BINDING_MANIFEST")
    require(all(operations[name] == "implemented" for name in IMPLEMENTED)
            and all(operations[name] == "unsupported" for name in UNSUPPORTED),
            "INVALID_BINDING_MANIFEST")
    require(document["extensions"] == {
        "approvalPath": "/operations/tasks/{task_id}/approve",
        "mcpIsSeparateProtocolSurface": True,
        "operationsApiIsSeparateAdministrativeSurface": True,
    }, "INVALID_BINDING_MANIFEST")
    canonical(document)


def assert_agent_card(card: Any, base_url: str) -> None:
    """Fail when the runtime Agent Card drifts from the reviewed binding manifest."""
    load_binding()
    interfaces = frozenset((item.protocol_binding,
                            item.url.removeprefix(base_url) or "/",
                            item.protocol_version)
                           for item in card.supported_interfaces)
    require(interfaces == INTERFACES, "AGENT_CARD_BINDING_DRIFT")
    capabilities = card.capabilities
    require(bool(capabilities.streaming) is True
            and bool(capabilities.push_notifications) is False
            and bool(capabilities.extended_agent_card) is True,
            "AGENT_CARD_BINDING_DRIFT")
