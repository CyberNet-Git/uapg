"""
UAPG - OPC UA PostgreSQL History Storage Backend

Модуль для хранения исторических данных OPC UA в PostgreSQL
с поддержкой TimescaleDB для эффективной работы с временными рядами.
"""

__version__ = "0.2.0"
__author__ = "RTS IoT"
__email__ = "panfilov@rts-iot.ru"

from .history_pgsql import HistoryPgSQL
from .history_timescale import HistoryTimescale
from .history_timescale_v2 import HistoryTimescaleV2
from .db_manager import DatabaseManager, create_database_standalone, backup_database_standalone

__all__ = [
    "HistoryPgSQL",
    "HistoryTimescale",
    "HistoryTimescaleV2",
    "DatabaseManager", 
    "create_database_standalone", 
    "backup_database_standalone"
]
