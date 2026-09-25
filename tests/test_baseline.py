import copy
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = json.loads((ROOT / "spec/envelope.schema.json").read_text())
VALIDATOR = Draft202012Validator(SCHEMA, format_checker=FormatChecker())
EXAMPLE = json.loads((ROOT / "spec/examples/send.json").read_text())

def test_schema_is_valid_and_packaged():
    Draft202012Validator.check_schema(SCHEMA)
    assert (ROOT / "spec/envelope.schema.json").read_bytes() == (ROOT / "src/a2a_mq/schema.json").read_bytes()

def test_example():
    VALIDATOR.validate(EXAMPLE)

@pytest.mark.parametrize("target", [{}, {"agentId": None}, {"agentId": "a", "skill": "b"}, {"skill": "A"}, {"skill": "a.b"}, {"skill": "*"}])
def test_unsafe_target_rejected(target):
    candidate = copy.deepcopy(EXAMPLE)
    candidate["target"] = target
    assert list(VALIDATOR.iter_errors(candidate))

@pytest.mark.parametrize("field,value", [("envelopeVersion", "2.0"), ("tenantId", "a.b"), ("messageId", "not-uuid"), ("createdAt", "yesterday"), ("contentEncoding", "encrypted"), ("sequence", 1), ("unexpected", True)])
def test_invalid_envelope(field, value):
    candidate = copy.deepcopy(EXAMPLE)
    candidate[field] = value
    assert list(VALIDATOR.iter_errors(candidate))
