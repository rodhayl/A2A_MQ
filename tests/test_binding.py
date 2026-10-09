import copy
import json
from pathlib import Path

import pytest

from a2a_mq.binding import IMPLEMENTED, INTERFACES, UNSUPPORTED, load_binding, validate_binding
from a2a_mq.domain import PermanentError


def test_packaged_binding_is_complete_and_protocol_specific():
    binding = load_binding()
    assert binding["protocolVersion"] == "1.0"
    assert {(item["protocolBinding"], item["path"], item["protocolVersion"])
            for item in binding["interfaces"]} == INTERFACES
    assert {name for name, state in binding["operations"].items() if state == "implemented"} == IMPLEMENTED
    assert {name for name, state in binding["operations"].items() if state == "unsupported"} == UNSUPPORTED


def test_manifest_copies_are_byte_identical():
    root = Path(__file__).resolve().parents[1]
    assert (root / "spec/binding.json").read_bytes() == (root / "src/a2a_mq/binding.json").read_bytes()


def test_manifest_rejects_capability_drift():
    value = copy.deepcopy(load_binding())
    value["capabilities"]["pushNotifications"] = True
    with pytest.raises(PermanentError, match="INVALID_BINDING_MANIFEST"):
        validate_binding(value)


def test_manifest_is_plain_machine_readable_json():
    root = Path(__file__).resolve().parents[1]
    value = json.loads((root / "spec/binding.json").read_text())
    validate_binding(value)


@pytest.mark.upstream
def test_operation_inventory_matches_official_a2a_1_service_descriptor():
    """Do not self-validate stale v0.3 labels against equally stale constants."""
    from a2a.types import a2a_pb2

    service = a2a_pb2.DESCRIPTOR.services_by_name["A2AService"]
    assert set(load_binding()["operations"]) == {method.name for method in service.methods}
