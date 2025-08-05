"""
UAPG - OPC UA PostgreSQL History Storage Backend

Модуль для хранения исторических данных OPC UA в PostgreSQL
с поддержкой TimescaleDB для эффективной работы с временными рядами.
"""

__version__ = "0.1.0"
__author__ = "RTS IoT"
__email__ = "panfilov@rts-iot.ru"

from .history_pgsql import HistoryPgSQL

__all__ = ["HistoryPgSQL"]
