"""Совместимый путь импорта: реализация — в ``uapg.storage.events_config``.

Путь ``uapg.v2.events_config`` использует opc-vibro-iot-server.
"""

from ..storage.events_config import (  # noqa: F401
    EventsV2Config,
    expand_sql_filter_fields,
    parse_csv_set,
    parse_field_aliases,
    typed_fields_supported,
)
