"""Элементы очереди записи.

Поля и их имена сохранены от 0.2.15: буфер и тесты обращаются к ним напрямую.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from asyncua import ua


@dataclass
class VariableWriteItem:
    """Одно значение переменной, ожидающее записи."""

    variable_id: int
    node_id_str: str
    source_timestamp: datetime
    server_timestamp: datetime
    status_code: int
    value_str: str
    variant_type: int
    variant_binary: bytes
    group_key: str
    datavalue: ua.DataValue
    # Заполняется только в синхронном режиме: по нему вызывающий узнаёт, что
    # значение действительно записано.
    future: Optional[asyncio.Future] = field(default=None, repr=False, compare=False)


@dataclass
class EventWriteItem:
    """Одно событие, ожидающее записи."""

    source_db_id: int
    event_type_id: int
    event_timestamp: datetime
    event_data_json: str
    group_key: str
    future: Optional[asyncio.Future] = field(default=None, repr=False, compare=False)
