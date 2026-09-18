"""uapg — бэкенд историзации OPC UA на PostgreSQL/TimescaleDB.

Реализует ``HistoryStorageInterface`` из asyncua: хранит значения переменных и
события и отдаёт их по HistoryRead.

    from uapg import HistoryTimescale

    storage = HistoryTimescale(user=..., password=..., database=..., host=...)
    server.iserver.history_manager.set_storage(storage)
    await storage.init()

``HistoryTimescaleV2`` — тот же бэкенд с типизированным хранением и поиском
событий на стороне БД.
"""

from importlib.metadata import PackageNotFoundError, version

from .history_timescale import HistoryTimescale
from .history_timescale_v2 import HistoryTimescaleV2

try:
    __version__ = version("uapg")
except PackageNotFoundError:  # pragma: no cover - запуск из исходников без установки
    __version__ = "3.0.0"

__all__ = ["HistoryTimescale", "HistoryTimescaleV2"]
