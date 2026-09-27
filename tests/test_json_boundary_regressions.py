"""JSON decoding errors have stable permanent outcomes before processing."""
import sys

import pytest

from a2a_mq.domain import PermanentError, canonical, command, load_json


@pytest.mark.parametrize("raw", [
    b'{"value":"\\ud800"}', b'{"value":"\\udfff"}',
    b'{"\\ud800":1}', b'{"nested":[{"\\udfff":"x"}]}',
], ids=["high-surrogate", "low-surrogate", "surrogate-key", "nested-key"])
def test_invalid_unicode_is_permanent(raw):
    with pytest.raises(PermanentError, match="INVALID_JSON"):
        load_json(raw)


def test_integer_decoder_limit_is_permanent():
    previous = sys.get_int_max_str_digits()
    try:
        sys.set_int_max_str_digits(4300)
        with pytest.raises(PermanentError, match="INVALID_JSON"):
            load_json(b'{"value":' + b'9' * 5000 + b'}')
    finally:
        sys.set_int_max_str_digits(previous)


@pytest.mark.parametrize("value", ["\ud800", "\udfff", {"\ud800": 1}, {"value": ["\udfff"]}])
def test_canonical_rejects_unencodable_values(value):
    with pytest.raises(PermanentError, match="INVALID_JSON"):
        canonical(value)


def test_factory_rejects_unencodable_message():
    with pytest.raises(PermanentError, match="INVALID_JSON"):
        command("\ud800")


def test_legal_non_bmp_unicode_round_trip():
    value = load_json(b'{"value":"\\ud83d\\ude00"}')
    assert load_json(canonical(value).encode()) == value
    assert command(value["value"]).digest


@pytest.mark.parametrize("raw,code", [(b'{"x":1,"x":2}', "DUPLICATE_JSON_KEY"),
                                      (b'0' * 262145, "MESSAGE_TOO_LARGE"),
                                      (b'[' * 40 + b'0' + b']' * 40, "JSON_TOO_DEEP")])
def test_preserve_specific_reason_codes(raw, code):
    with pytest.raises(PermanentError, match=code):
        load_json(raw)
