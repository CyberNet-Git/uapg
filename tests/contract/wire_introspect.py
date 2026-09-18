"""Снимок формата данных, которым uapg пишет в PostgreSQL.

Это самая тихая часть контракта: ошибка в кодировании не ломает тесты и не
роняет сервер, но делает уже записанную историю нечитаемой. Поэтому корпус
значений фиксируется отдельно и проверяется побайтово.
"""

from __future__ import annotations

import base64
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

from asyncua import ua


def variant_corpus() -> Dict[str, ua.Variant]:
    """Набор значений, покрывающий типы Variant и граничные случаи."""
    return {
        "null": ua.Variant(None),
        "bool_true": ua.Variant(True, ua.VariantType.Boolean),
        "bool_false": ua.Variant(False, ua.VariantType.Boolean),
        "sbyte_min": ua.Variant(-128, ua.VariantType.SByte),
        "byte_max": ua.Variant(255, ua.VariantType.Byte),
        "int16_min": ua.Variant(-32768, ua.VariantType.Int16),
        "uint16_max": ua.Variant(65535, ua.VariantType.UInt16),
        "int32_min": ua.Variant(-2147483648, ua.VariantType.Int32),
        "uint32_max": ua.Variant(4294967295, ua.VariantType.UInt32),
        "int64_min": ua.Variant(-9223372036854775808, ua.VariantType.Int64),
        "uint64_max": ua.Variant(18446744073709551615, ua.VariantType.UInt64),
        "float_simple": ua.Variant(1.5, ua.VariantType.Float),
        "double_simple": ua.Variant(42.125, ua.VariantType.Double),
        "double_negative": ua.Variant(-0.000123, ua.VariantType.Double),
        "string_ascii": ua.Variant("hello", ua.VariantType.String),
        "string_unicode": ua.Variant("привет мир", ua.VariantType.String),
        "string_empty": ua.Variant("", ua.VariantType.String),
        "datetime": ua.Variant(
            datetime(2026, 9, 17, 12, 30, 45, 123456, tzinfo=timezone.utc),
            ua.VariantType.DateTime,
        ),
        "guid": ua.Variant(
            uuid.UUID("12345678-1234-5678-1234-567812345678"), ua.VariantType.Guid
        ),
        "bytestring": ua.Variant(b"\x00\x01\xfe\xff", ua.VariantType.ByteString),
        "nodeid_numeric": ua.Variant(ua.NodeId(1234, 2), ua.VariantType.NodeId),
        "nodeid_string": ua.Variant(ua.NodeId("MyVariable", 3), ua.VariantType.NodeId),
        "statuscode_good": ua.Variant(ua.StatusCode(0), ua.VariantType.StatusCode),
        "statuscode_bad": ua.Variant(ua.StatusCode(0x80000000), ua.VariantType.StatusCode),
        "localized_text": ua.Variant(
            ua.LocalizedText("Сообщение", "ru"), ua.VariantType.LocalizedText
        ),
        "qualified_name": ua.Variant(ua.QualifiedName("Name", 2), ua.VariantType.QualifiedName),
        "array_int32": ua.Variant([1, 2, 3], ua.VariantType.Int32),
        "array_string": ua.Variant(["a", "b"], ua.VariantType.String),
        "array_double_empty": ua.Variant([], ua.VariantType.Double),
    }


def event_field_corpus() -> Dict[str, Any]:
    """Поля события, включая нестандартное имя и None."""
    return {
        "Message": ua.Variant(ua.LocalizedText("Событие", "ru"), ua.VariantType.LocalizedText),
        "Severity": ua.Variant(500, ua.VariantType.UInt16),
        "SourceName": ua.Variant("Sensor-1", ua.VariantType.String),
        "Time": ua.Variant(
            datetime(2026, 9, 17, 12, 0, 0, tzinfo=timezone.utc), ua.VariantType.DateTime
        ),
        "/2:CustomField": ua.Variant("custom", ua.VariantType.String),
        "NoneField": None,
    }


# Строки, записанные до перехода на префикс base64: декодер обязан отдавать их как есть.
LEGACY_EVENT_ROW = {"PlainField": "plain value", "NullField": None}

STATUS_CODES = {
    "good": 0,
    "uncertain": 0x40000000,
    "bad": 0x80000000,
    "bad_out_of_service": 0x808D0000,
}


def wire_snapshot(encode_event_fields: Any, decode_event_data: Any) -> Dict[str, Any]:
    """Собрать снимок формата.

    ``encode_event_fields`` и ``decode_event_data`` передаются извне, чтобы один и тот
    же корпус можно было прогнать и через старую реализацию, и через новую.
    """
    from asyncua.ua.ua_binary import variant_to_binary

    variants = {
        name: {
            "variant_type": int(variant.VariantType),
            "variant_binary_b64": base64.b64encode(variant_to_binary(variant)).decode(),
            # Колонка value TEXT: её никто не читает, но формат должен сохраниться.
            "value_text": str(variant.Value),
        }
        for name, variant in variant_corpus().items()
    }

    decoded_legacy = decode_event_data(dict(LEGACY_EVENT_ROW))

    return {
        "variants": variants,
        "event_data": encode_event_fields(event_field_corpus()),
        "event_data_legacy_passthrough": {
            key: (value if isinstance(value, (str, int, float, bool, type(None))) else repr(value))
            for key, value in decoded_legacy.items()
        },
        "status_code_values": {
            name: ua.StatusCode(code).value for name, code in STATUS_CODES.items()
        },
    }
