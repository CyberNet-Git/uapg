"""Совместимый путь импорта: реализация — в ``uapg.storage.storage_mode``.

Путь ``uapg.v2.storage_mode`` использует opc-vibro-iot-server.
"""

from ..storage.storage_mode import (  # noqa: F401
    StorageMode,
    get_events_storage_mode,
    get_variables_storage_mode,
    parse_storage_mode,
    should_read_v2,
    should_write_legacy,
    should_write_v2,
)
