"""Значение переменной: ua.DataValue ↔ колонки variables_history.

Формат менять нельзя ни в одну сторону: ``variantbinary`` хранит двоичное
представление ``ua.Variant``, и уже накопленная история читается только им.
"""

from __future__ import annotations

import inspect
from typing import Any, Mapping, Optional

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
    """Числовой код качества для колонки statuscode.

    Колонка объявлена INTEGER, а коды OPC UA занимают 32 бита без знака, поэтому
    всё, что начинается с 0x8000_0000 (то есть любой Bad), уезжает в минус.
    Так писала и 0.2.15; чтение восстанавливает код через ua.StatusCode.
    """
    if status is None:
        return 0
    return int(status.value)


def make_datavalue(
    *,
    value: ua.Variant,
    status: ua.StatusCode,
    source_timestamp: Any = None,
    server_timestamp: Any = None,
) -> ua.DataValue:
    """Создать DataValue независимо от версии asyncua."""
    return ua.DataValue(
        Value=value,
        SourceTimestamp=source_timestamp,
        ServerTimestamp=server_timestamp,
        **{_STATUS_KWARG: status},
    )


def row_to_datavalue(row: Mapping[str, Any]) -> ua.DataValue:
    """Собрать DataValue из строки variables_history или variables_last_value."""
    return make_datavalue(
        value=decode_variant(row["variantbinary"]),
        status=ua.StatusCode(row["statuscode"]),
        source_timestamp=row["sourcetimestamp"],
        server_timestamp=row["servertimestamp"],
    )
