"""Кодеки значений и событий.

Проверка идёт против замороженного эталона 0.2.15: расхождение здесь ничего не
роняет, но делает уже записанную историю нечитаемой.
"""

from __future__ import annotations

import base64
import json
from pathlib import Path

from asyncua import ua

from uapg.codec import (
    decode_event_data,
    decode_variant,
    encode_event_fields,
    encode_variant,
    row_to_datavalue,
    status_code_value,
    value_text,
)
from tests.contract.wire_introspect import (
    LEGACY_EVENT_ROW,
    event_field_corpus,
    variant_corpus,
)

BASELINE_WIRE = Path(__file__).parents[1] / "contract" / "baseline" / "wire.json"


def _baseline() -> dict:
    return json.loads(BASELINE_WIRE.read_text())


class TestVariantCodec:
    def test_encoding_matches_baseline(self) -> None:
        expected = _baseline()["variants"]
        for name, variant in variant_corpus().items():
            encoded = base64.b64encode(encode_variant(variant)).decode()
            assert encoded == expected[name]["variant_binary_b64"], name

    def test_value_text_matches_baseline(self) -> None:
        expected = _baseline()["variants"]
        for name, variant in variant_corpus().items():
            assert value_text(variant) == expected[name]["value_text"], name

    def test_roundtrip_preserves_value_and_type(self) -> None:
        for name, variant in variant_corpus().items():
            restored = decode_variant(encode_variant(variant))
            assert restored.VariantType == variant.VariantType, name
            assert restored.Value == variant.Value, name

    def test_decodes_bytes_written_by_previous_version(self) -> None:
        """Главное свойство: старые строки в базе обязаны читаться новым кодом."""
        for name, entry in _baseline()["variants"].items():
            restored = decode_variant(base64.b64decode(entry["variant_binary_b64"]))
            assert int(restored.VariantType) == entry["variant_type"], name


class TestStatusCode:
    def test_values_match_baseline(self) -> None:
        expected = _baseline()["status_code_values"]
        assert status_code_value(ua.StatusCode(0)) == expected["good"]
        assert status_code_value(ua.StatusCode(0x40000000)) == expected["uncertain"]
        assert status_code_value(ua.StatusCode(0x80000000)) == expected["bad"]

    def test_missing_status_is_good(self) -> None:
        assert status_code_value(None) == 0


class TestEventCodec:
    def test_encoding_matches_baseline(self) -> None:
        assert encode_event_fields(event_field_corpus()) == _baseline()["event_data"]

    def test_bare_value_is_wrapped_into_variant(self) -> None:
        """Поле, пришедшее не Variant'ом, заворачивается, а не теряется."""
        encoded = encode_event_fields({"Bare": None, "Good": ua.Variant(1, ua.VariantType.Int32)})
        assert decode_event_data(encoded)["Bare"].Value is None
        assert decode_event_data(encoded)["Good"].Value == 1

    def test_legacy_rows_pass_through(self) -> None:
        decoded = decode_event_data(dict(LEGACY_EVENT_ROW))
        assert decoded["PlainField"] == "plain value"
        assert decoded["NullField"] is None

    def test_roundtrip(self) -> None:
        fields = {
            "Severity": ua.Variant(500, ua.VariantType.UInt16),
            "SourceName": ua.Variant("Sensor-1", ua.VariantType.String),
        }
        decoded = decode_event_data(encode_event_fields(fields))
        assert decoded["Severity"].Value == 500
        assert decoded["SourceName"].Value == "Sensor-1"

    def test_broken_payload_decodes_to_none(self) -> None:
        assert decode_event_data({"X": "base64:!!!not-base64!!!"})["X"] is None


def test_row_to_datavalue() -> None:
    variant = ua.Variant(42.125, ua.VariantType.Double)
    row = {
        "variantbinary": encode_variant(variant),
        "statuscode": 0,
        "sourcetimestamp": None,
        "servertimestamp": None,
    }
    datavalue = row_to_datavalue(row)
    assert datavalue.Value.Value == 42.125
    assert datavalue.StatusCode.value == 0
