"""Значение переменной: ua.DataValue ↔ колонки variables_history.

Формат менять нельзя ни в одну сторону: ``variantbinary`` хранит двоичное
представление ``ua.Variant``, и уже накопленная история читается только им.
"""

from __future__ import annotations

import inspect
from typing import Any, Mapping, Optional, cast

from asyncua import ua
from asyncua.common.utils import Buffer
from asyncua.ua.ua_binary import variant_from_binary, variant_to_binary

# asyncua 1.x принимает код качества как StatusCode_, asyncua 2.x — как StatusCode.
# Прод пинит 1.1.8, а свежая установка получает 2.x, поэтому имя выбирается один
# раз по сигнатуре, а не зашивается.
_STATUS_KWARG = (
    "StatusCode_"
    if "StatusCode_" in inspect.signature(ua.DataValue).parameters
    else "StatusCode"
)


def encode_variant(variant: ua.Variant) -> bytes:
    """Двоичное представление Variant для колонки variantbinary."""
    return variant_to_binary(variant)


def decode_variant(payload: bytes) -> ua.Variant:
    """Разобрать содержимое variantbinary обратно в Variant."""
    return variant_from_binary(Buffer(payload))


def value_text(variant: ua.Variant) -> str:
    """Содержимое колонки value.

    Колонку никто не читает — она нужна человеку, который смотрит в таблицу
    глазами. Формат сохраняется ради совместимости уже записанных строк.
    """
    return str(variant.Value)


def status_code_value(status: Optional[ua.StatusCode]) -> int:
    """Числовой код качества OPC UA (32 бита без знака)."""
    if status is None:
        return 0
    return int(status.value)


def status_code_to_column(status: Optional[ua.StatusCode]) -> int:
    """Код качества в том виде, в каком он ложится в колонку statuscode.

    Колонка объявлена INTEGER, а коды OPC UA занимают 32 бита без знака: всё,
    что начинается с 0x8000_0000, то есть любой Bad, в неё не помещается. В
    0.2.15 такое значение не записывалось вовсе — PostgreSQL отвергал параметр,
    и вместе с ним терялась вся пачка, где оно оказалось.

    Те же 32 бита сохраняются как знаковое целое. Коды Good и Uncertain меньше
    0x8000_0000 и записываются как прежде, поэтому уже накопленные данные
    читаются без изменений и менять тип колонки не требуется.
    """
    value = status_code_value(status) & 0xFFFFFFFF
    return value - 0x100000000 if value >= 0x80000000 else value


def status_code_from_column(raw: Optional[int]) -> ua.StatusCode:
    """Восстановить код качества из колонки statuscode."""
    return ua.StatusCode(cast(Any, (int(raw or 0)) & 0xFFFFFFFF))


def make_datavalue(
    *,
    value: ua.Variant,
    status: ua.StatusCode,
    source_timestamp: Any = None,
    server_timestamp: Any = None,
) -> ua.DataValue:
    """Создать DataValue независимо от версии asyncua."""
    kwargs: dict[str, Any] = {
        "Value": value,
        "SourceTimestamp": source_timestamp,
        "ServerTimestamp": server_timestamp,
        _STATUS_KWARG: status,
    }
    return ua.DataValue(**kwargs)


def row_to_datavalue(row: Mapping[str, Any]) -> ua.DataValue:
    """Собрать DataValue из строки variables_history или variables_last_value."""
    return make_datavalue(
        value=decode_variant(row["variantbinary"]),
        status=status_code_from_column(row["statuscode"]),
        source_timestamp=row["sourcetimestamp"],
        server_timestamp=row["servertimestamp"],
    )
