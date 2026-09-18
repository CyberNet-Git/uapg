"""Поля события ↔ events_history.event_data.

``event_data`` — плоский JSONB, где каждое значение записано строкой
``"base64:"`` + двоичное представление ``ua.Variant``. Вложенности и тегов типа
нет: тип восстанавливается из самого Variant.

Декодер обязан пропускать значения без префикса как есть — так в базе лежат
строки, записанные до перехода на этот формат.
"""

from __future__ import annotations

import base64
import logging
from typing import Any, Dict, Mapping, Optional

from asyncua import ua

from .variant import decode_variant, encode_variant

_PREFIX = "base64:"

logger = logging.getLogger("uapg.codec.events")


def _encode_value(key: str, value: Any) -> Optional[str]:
    try:
        return _PREFIX + base64.b64encode(encode_variant(value)).decode("utf-8")
    except Exception as exc:
        # Значение могло прийти не завёрнутым в Variant — пробуем завернуть сами.
        try:
            return _PREFIX + base64.b64encode(encode_variant(ua.Variant(value))).decode("utf-8")
        except Exception as fallback_exc:
            logger.error(
                "Поле события %r не закодировано (%s; как Variant: %s), записан null",
                key,
                exc,
                fallback_exc,
            )
            return None


def encode_event_fields(fields: Mapping[str, Any]) -> Dict[str, Optional[str]]:
    """Подготовить поля события к записи в event_data."""
    return {key: _encode_value(key, value) for key, value in fields.items()}


def decode_event_data(data: Mapping[str, Any]) -> Dict[str, Any]:
    """Восстановить поля события из event_data."""
    result: Dict[str, Any] = {}
    for key, raw in data.items():
        if raw is None:
            result[key] = None
            continue
        if not isinstance(raw, str) or not raw.startswith(_PREFIX):
            # Строка старого формата: значение уже пригодно к использованию.
            result[key] = raw
            continue
        try:
            result[key] = decode_variant(base64.b64decode(raw[len(_PREFIX) :]))
        except Exception as exc:
            logger.error("Поле события %r не декодировано (%s), отдаём null", key, exc)
            result[key] = None
    return result
