"""
Модуль историзации OPC UA с использованием TimescaleDB

Новая архитектура: единые таблицы для всех переменных и событий вместо
создания отдельных таблиц для каждой переменной. Данные размещаются в
настраиваемой схеме с поддержкой TimescaleDB для временных рядов.
"""

import json
import asyncio
import random
import logging
import time
import importlib.metadata as importlib_metadata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, List, Optional, Tuple, Union, Dict, Callable, Coroutine

import asyncpg
from asyncua import ua
from asyncua.server.history import HistoryStorageInterface
from asyncua.ua.ua_binary import variant_from_binary, variant_to_binary

# Импорт для работы с зашифрованной конфигурацией
from .db_manager import DatabaseManager

# Правильный буфер для побайтного чтения в variant_from_binary
class Buffer:
    def __init__(self, data: bytes) -> None:
        self._data = data
        self._pos = 0
    
    def read(self, n: int) -> bytes:
        chunk = self._data[self._pos:self._pos + n]
        self._pos += n
        return chunk
    
    def copy(self, *args, **kwargs) -> 'Buffer':
        return Buffer(self._data[self._pos:])
    
    def skip(self, n: int) -> None:
        self._pos += n

# Импорт для работы с событиями
from asyncua.common.events import Event

# Импорт фильтрации событий
from .event_filter import apply_event_filter


@dataclass
class VariableWriteItem:
    """
    Элемент очереди на запись значения переменной.
    Используется HistoryWriteBuffer для батчевой записи.
    """
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
    future: Optional[asyncio.Future] = None


@dataclass
class EventWriteItem:
    """
    Элемент очереди на запись события.
    Используется HistoryWriteBuffer для батчевой записи.
    """
    source_db_id: int
    event_type_id: int
    event_timestamp: datetime
    event_data_json: str
    group_key: str
    future: Optional[asyncio.Future] = None


def _empty_timing_stats() -> Dict[str, float]:
    return {
        "count": 0,
        "total_ms": 0.0,
        "last_ms": 0.0,
        "max_ms": 0.0,
        "avg_ms": 0.0,
    }


def _observe_timing(stats: Dict[str, float], duration_ms: float) -> None:
    count = int(stats.get("count", 0)) + 1
    total = float(stats.get("total_ms", 0.0)) + duration_ms
    stats["count"] = count
    stats["total_ms"] = total
    stats["last_ms"] = duration_ms
    stats["max_ms"] = max(float(stats.get("max_ms", 0.0)), duration_ms)
    stats["avg_ms"] = total / count if count else 0.0


class HistoryWriteBuffer:
    """
    Универсальный буфер для батчевой записи значений в БД.

    Не знает о структуре таблиц — только управляет очередью, пакетированием
    и вызовом переданной функции flush_func.
    """

    def __init__(
        self,
        name: str,
        logger: logging.Logger,
        max_batch_size: int,
        max_batch_interval_sec: float,
        queue_max_size: int,
        durability_mode: str,
        flush_func: Callable[[List[Any]], Coroutine[Any, Any, None]],
    ) -> None:
        self._name = name
        self._logger = logger.getChild(f"buffer.{name}") if logger else logging.getLogger(f"HistoryWriteBuffer.{name}")
        self._max_batch_size = max(1, int(max_batch_size))
        self._max_batch_interval_sec = max(0.01, float(max_batch_interval_sec))
        self._durability_mode = durability_mode or "async"
        self._flush_func = flush_func
        # maxsize=0 означает неограниченную очередь
        self._queue: "asyncio.Queue[Any]" = asyncio.Queue(maxsize=max(0, int(queue_max_size)))
        self._task: Optional[asyncio.Task] = None
        self._stopped = False
        self._stats: Dict[str, Any] = self._new_stats()

    @staticmethod
    def _new_stats() -> Dict[str, Any]:
        return {
            "enqueue_attempts_total": 0,
            "enqueued_total": 0,
            "dropped_total": 0,
            "flushed_items_total": 0,
            "flush_batches_total": 0,
            "last_batch_size": 0,
            "max_batch_size_seen": 0,
            "flush_errors_total": 0,
            "last_flush_error": "",
            "flush_duration": _empty_timing_stats(),
        }

    def get_stats(self) -> Dict[str, Any]:
        queue_size = self._queue.qsize()
        queue_max_size = self._queue.maxsize
        fill_ratio = (queue_size / queue_max_size) if queue_max_size > 0 else 0.0
        flush_duration = dict(self._stats["flush_duration"])
        return {
            "queue_size": queue_size,
            "queue_max_size": queue_max_size,
            "queue_fill_ratio": fill_ratio,
            "enqueue_attempts_total": int(self._stats["enqueue_attempts_total"]),
            "enqueued_total": int(self._stats["enqueued_total"]),
            "dropped_total": int(self._stats["dropped_total"]),
            "flushed_items_total": int(self._stats["flushed_items_total"]),
            "flush_batches_total": int(self._stats["flush_batches_total"]),
            "last_batch_size": int(self._stats["last_batch_size"]),
            "max_batch_size_seen": int(self._stats["max_batch_size_seen"]),
            "flush_errors_total": int(self._stats["flush_errors_total"]),
            "last_flush_error": str(self._stats["last_flush_error"]),
            "last_flush_duration_ms": float(flush_duration["last_ms"]),
            "max_flush_duration_ms": float(flush_duration["max_ms"]),
            "avg_flush_duration_ms": float(flush_duration["avg_ms"]),
            "total_flush_duration_ms": float(flush_duration["total_ms"]),
        }

    def reset_stats(self) -> None:
        self._stats = self._new_stats()

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stopped = False
            self._task = asyncio.create_task(self._worker(), name=f"HistoryWriteBuffer-{self._name}")
            self._logger.info(
                "HistoryWriteBuffer '%s' started (max_batch_size=%s, max_batch_interval_sec=%.3f, queue_max_size=%s, durability_mode=%s)",
                self._name,
                self._max_batch_size,
                self._max_batch_interval_sec,
                self._queue.maxsize,
                self._durability_mode,
            )

    async def stop(self) -> None:
        self._stopped = True
        if self._task:
            # Даем воркеру возможность дописать оставшиеся элементы
            try:
                await asyncio.wait_for(self._task, timeout=self._max_batch_interval_sec * 2)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass

    async def enqueue(self, item: Any, sync: bool = False) -> None:
        """
        Добавление элемента в очередь.

        Если sync=True или durability_mode == 'sync', вызывающий ожидает завершения флаша.
        """
        future: Optional[asyncio.Future] = None
        sync_mode = sync or self._durability_mode == "sync"
        self._stats["enqueue_attempts_total"] += 1

        if sync_mode:
            loop = asyncio.get_running_loop()
            future = loop.create_future()
            # Ожидается, что элемент поддерживает атрибут future
            setattr(item, "future", future)

        try:
            if sync_mode:
                await self._queue.put(item)
            else:
                # В async-режиме не блокируемся, при переполнении просто логируем и отбрасываем
                self._queue.put_nowait(item)
            self._stats["enqueued_total"] += 1
        except asyncio.QueueFull:
            self._stats["dropped_total"] += 1
            self._logger.error("HistoryWriteBuffer '%s' queue is full, dropping item in async mode", self._name)
            if future and not future.done():
                future.set_exception(RuntimeError("HistoryWriteBuffer queue is full"))
            return

        if sync_mode and future is not None:
            await future

    async def _worker(self) -> None:
        """
        Основной цикл фонового воркера.
        Собирает пачки из очереди и передает их в flush_func.
        """
        pending: List[Any] = []

        while not self._stopped or not self._queue.empty():
            try:
                if not pending:
                    try:
                        item = await asyncio.wait_for(self._queue.get(), timeout=self._max_batch_interval_sec)
                    except asyncio.TimeoutError:
                        continue
                    pending.append(item)

                # Добираем пачку до max_batch_size без ожидания
                while len(pending) < self._max_batch_size:
                    try:
                        pending.append(self._queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break

                await self._flush_pending(pending)
                pending.clear()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._logger.error("HistoryWriteBuffer '%s' worker error: %s", self._name, e, exc_info=True)

        # Финальный флаш оставшихся данных
        if pending:
            try:
                await self._flush_pending(pending)
            except Exception as e:
                self._logger.error("HistoryWriteBuffer '%s' final flush error: %s", self._name, e, exc_info=True)

    async def _flush_pending(self, batch: List[Any]) -> None:
        if not batch:
            return

        started_at = time.perf_counter()
        try:
            await self._flush_func(batch)
            duration_ms = (time.perf_counter() - started_at) * 1000.0
            batch_size = len(batch)
            self._stats["flush_batches_total"] += 1
            self._stats["flushed_items_total"] += batch_size
            self._stats["last_batch_size"] = batch_size
            self._stats["max_batch_size_seen"] = max(
                int(self._stats["max_batch_size_seen"]),
                batch_size,
            )
            _observe_timing(self._stats["flush_duration"], duration_ms)
            # Уведомляем ожидающих о завершении
            now = time.time()
            for item in batch:
                fut: Optional[asyncio.Future] = getattr(item, "future", None)
                if fut is not None and not fut.done():
                    fut.set_result(now)
        except Exception as e:
            duration_ms = (time.perf_counter() - started_at) * 1000.0
            batch_size = len(batch)
            self._stats["flush_errors_total"] += 1
            self._stats["last_flush_error"] = str(e)
            self._stats["last_batch_size"] = batch_size
            self._stats["max_batch_size_seen"] = max(
                int(self._stats["max_batch_size_seen"]),
                batch_size,
            )
            _observe_timing(self._stats["flush_duration"], duration_ms)
            self._logger.error(
                "HistoryWriteBuffer '%s' flush failed for %d items: %s",
                self._name,
                len(batch),
                e,
                exc_info=True,
            )
            for item in batch:
                fut: Optional[asyncio.Future] = getattr(item, "future", None)
                if fut is not None and not fut.done():
                    fut.set_exception(e)

try:
    from asyncua.common.events import get_event_properties_from_type_node
except ImportError:
    # Fallback для старых версий asyncua
    async def get_event_properties_from_type_node(event_type):
        """Получение свойств события из типа узла"""
        return []

def validate_table_name(name: str) -> None:
    """
    Валидация имени таблицы для предотвращения SQL инъекций.
    
    Args:
        name: Имя таблицы для проверки
        
    Raises:
        ValueError: Если имя таблицы содержит недопустимые символы
    """
    import re
    if not re.match(r'^[\w\-]+$', name):
        raise ValueError(f"Invalid table name: {name}")


from .opc_node_id import coerce_node_id as _coerce_node_id


class HistoryTimescale(HistoryStorageInterface):
    """
    Backend для хранения исторических данных OPC UA в PostgreSQL с TimescaleDB.
    
    Новая архитектура использует единые таблицы:
    - variables_history: для всех переменных
    - events_history: для всех событий
    - variable_metadata: метаданные переменных
    - event_sources: источники событий (с периодом хранения)
    - event_types: типы событий (с расширенными полями)
    
    Особенности:
    - Единая таблица для всех переменных с полем variable_id
    - Единая таблица для всех событий с полями source_id и event_type_id
    - Настраиваемая схема (по умолчанию 'public')
    - TimescaleDB hypertables для оптимизации временных рядов
    - Дополнительное партиционирование по source_id (TimescaleDB 2+)
    - Период хранения и max_records устанавливается для источника событий
    
    Attributes:
        max_history_data_response_size (int): Максимальный размер ответа с историческими данными
        logger (logging.Logger): Логгер для записи событий
        _datachanges_period (dict): Словарь периодов хранения данных по узлам
        _conn_params (dict): Параметры подключения к базе данных
        _event_fields (dict): Словарь полей событий по источникам
        _pool (asyncpg.Pool): Пул соединений с базой данных
        _min_size (int): Минимальное количество соединений в пуле
        _max_size (int): Максимальное количество соединений в пуле
        _initialized (bool): Флаг инициализации таблиц
        _schema (str): Имя схемы для размещения таблиц истории
    """

    def _build_group_key_from_node_id(self, node_id_str: str) -> str:
        """
        Простая эвристика для группировки переменных по «вышестоящему узлу».

        По сути, используем префикс NodeId до последней точки, если она есть.
        Это позволяет группировать переменные, имена которых имеют иерархический формат.
        """
        try:
            if "." in node_id_str:
                return node_id_str.rsplit(".", 1)[0]
            return node_id_str
        except Exception:
            return node_id_str or "default"

    def _format_node_id(self, node_id: ua.NodeId) -> str:
        """
        Формирует стандартное имя узла OPC UA в формате ns=X;t=Y.
        
        Args:
            node_id: OPC UA NodeId или совместимый объект (Node, Variant)
            
        Returns:
            str: Строка в формате "ns=X;t=Y"
        """
        # Приведение к ua.NodeId при необходимости
        try:
            # Случай: asyncua Node
            if hasattr(node_id, 'nodeid'):
                node_id = node_id.nodeid
            # Случай: Variant с Value=NodeId
            if hasattr(node_id, 'Value') and isinstance(node_id.Value, ua.NodeId):
                node_id = node_id.Value
        except Exception:
            pass
        
        # Если после приведения это строка вида ns=..;t=.. — вернуть как есть
        if isinstance(node_id, str) and node_id.startswith('ns=') and ';' in node_id:
            return node_id
        
        # Если это уже ua.NodeId — собрать каноническую строку
        try:
            node_id_type_map = {
                ua.NodeIdType.TwoByte: 'i',
                ua.NodeIdType.FourByte: 'i', 
                ua.NodeIdType.Numeric: 'i',
                ua.NodeIdType.String: 's',
                ua.NodeIdType.Guid: 'g',
                ua.NodeIdType.ByteString: 'b'
            }
            type_key = getattr(node_id, 'NodeIdType', None)
            ns = getattr(node_id, 'NamespaceIndex', None)
            ident = getattr(node_id, 'Identifier', None)
            if type_key is not None and ns is not None and ident is not None:
                tchar = node_id_type_map.get(type_key, 'x')
                return f"ns={ns};{tchar}={ident}"
        except Exception:
            pass
        
        # Фоллбэк: строковое представление
        return str(node_id)

    def _normalize_event_type_name(self, name: str) -> str:
        """
        Нормализует имя типа события: убирает префикс вида 'ns=..;s=' если он присутствует.
        """
        if name.startswith('ns=') and ';s=' in name:
            try:
                return name.split(';s=', 1)[1]
            except Exception:
                return name
        return name

    def _get_node_data_type(self, node_id: ua.NodeId, datavalue: Optional[ua.DataValue] = None) -> str:
        """
        Определяет тип данных переменной на основе DataValue или контекста.
        
        Args:
            node_id: OPC UA NodeId переменной
            datavalue: DataValue переменной (опционально)
            
        Returns:
            str: Строковое представление типа данных переменной
        """
        # Если передан DataValue, определяем тип по нему
        if datavalue and hasattr(datavalue, 'Value') and datavalue.Value is not None:
            variant_type = datavalue.Value.VariantType
            if variant_type:
                # Маппинг типов OPC UA VariantType на читаемые названия
                variant_type_map = {
                    ua.VariantType.Boolean: 'Boolean',
                    ua.VariantType.SByte: 'SByte',
                    ua.VariantType.Byte: 'Byte',
                    ua.VariantType.Int16: 'Int16',
                    ua.VariantType.UInt16: 'UInt16',
                    ua.VariantType.Int32: 'Int32',
                    ua.VariantType.UInt32: 'UInt32',
                    ua.VariantType.Int64: 'Int64',
                    ua.VariantType.UInt64: 'UInt64',
                    ua.VariantType.Float: 'Float',
                    ua.VariantType.Double: 'Double',
                    ua.VariantType.String: 'String',
                    ua.VariantType.DateTime: 'DateTime',
                    ua.VariantType.Guid: 'Guid',
                    ua.VariantType.ByteString: 'ByteString',
                    ua.VariantType.XmlElement: 'XmlElement',
                    ua.VariantType.NodeId: 'NodeId',
                    ua.VariantType.ExpandedNodeId: 'ExpandedNodeId',
                    ua.VariantType.StatusCode: 'StatusCode',
                    ua.VariantType.QualifiedName: 'QualifiedName',
                    ua.VariantType.LocalizedText: 'LocalizedText',
                    ua.VariantType.ExtensionObject: 'ExtensionObject',
                    ua.VariantType.DataValue: 'DataValue',
                    ua.VariantType.Variant: 'Variant',
                    ua.VariantType.DiagnosticInfo: 'DiagnosticInfo',
                }
                return variant_type_map.get(variant_type, str(variant_type))
        
        # Если DataValue не передан, пытаемся определить по контексту NodeId
        # Это может быть полезно для предварительной регистрации переменных
        # Например, если знаем, что переменная с определенным NodeId всегда Double
        
        # Маппинг известных переменных по их NodeId
        known_variables = {
            # Пример: если знаем, что переменная с NodeId i=2 всегда Double
            # Можно расширить этот маппинг на основе специфики приложения
        }
        
        # Формируем ключ для поиска
        node_key = f"ns={node_id.NamespaceIndex};i={node_id.Identifier}"
        
        # Возвращаем известный тип или "Unknown" если не определен
        return known_variables.get(node_key, "Unknown")

    def __init__(
        self, 
        user: str = 'postgres', 
        password: str = 'postmaster', 
        database: str = 'opcua', 
        host: str = 'localhost', 
        port: int = 5432,
        min_size: int = 1,
        max_size: int = 10,
        schema: str = 'public',
        sslmode: Optional[str] = None,
        config_file: Optional[str] = None,
        encrypted_config: Optional[str] = None,
        master_password: Optional[str] = None,
        global_retention_period: Optional[timedelta] = None,
        # Параметры оптимизации записи истории и кэшей
        history_write_batch_enabled: bool = True,
        history_write_max_batch_size: int = 500,
        history_write_max_batch_interval_sec: float = 1.0,
        history_write_queue_max_size: int = 10000,
        history_write_durability_mode: str = "async",
        history_write_read_consistency_mode: str = "local",
        history_cache_enabled: bool = True,
        history_last_values_cache_enabled: bool = True,
        history_last_values_cache_max_size_mb: int = 100,
        history_last_values_init_batch_size: int = 1000,
        history_metadata_cache_enabled: bool = True,
        history_metadata_cache_init_max_rows: int = 500000,
        db_query_timeout_sec: Optional[float] = 30.0,
        db_pool_close_timeout_sec: float = 5.0,
        **kwargs
    ) -> None:
        """
        Инициализация HistoryTimescale.
        
        Args:
            user: Имя пользователя базы данных
            password: Пароль пользователя
            database: Имя базы данных
            host: Хост базы данных
            port: Порт базы данных
            min_size: Минимальное количество соединений в пуле
            max_size: Максимальное количество соединений в пуле
            schema: Имя схемы для размещения таблиц истории (по умолчанию 'public')
            sslmode: Режим SSL подключения ('disable', 'require', 'verify-ca', 'verify-full')
            config_file: Путь к файлу зашифрованной конфигурации
            encrypted_config: Зашифрованная конфигурация в виде строки
            master_password: Главный пароль для расшифровки конфигурации
            global_retention_period: Глобальная максимальная глубина хранения (TimescaleDB retention policy) для таблиц *history.
                Если None — глобальная политика не настраивается (поведение как раньше).
        """
        self.max_history_data_response_size = 1000
        self.logger = logging.getLogger('uapg.history_timescale')
        self._datachanges_period = {}
        self._event_fields = {}
        self._pool = None
        self._min_size = min_size
        self._max_size = max_size
        self._initialized = False
        self._schema = schema
        self._pool_lock = asyncio.Lock()
        self._reconnect_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()
        self._reconnect_task = None
        self._stopping = False
        self._reconnect_min_delay = 1.0
        self._reconnect_max_delay = 30.0
        self._was_healthy = True
        self._db_unavailable_since = None
        self._last_throttled_log_at = None
        self._db_query_timeout_sec = (
            None
            if db_query_timeout_sec is None or float(db_query_timeout_sec) <= 0
            else float(db_query_timeout_sec)
        )
        self._db_pool_close_timeout_sec = max(0.1, float(db_pool_close_timeout_sec))
        # Последнее время, когда мы писали агрегированное сообщение о длительной недоступности БД
        self._last_reconnect_outage_log_at = None
        self._failed_value_saves_counter = 0
        self._failed_event_saves_counter = 0

        # Глобальная политика хранения на уровне hypertable (TimescaleDB).
        # Используется как верхняя граница для per-node retention_period.
        self._global_retention_period: Optional[timedelta] = global_retention_period

        # OPC UA nodes для экспонирования текущих настроек хранения истории (только Timescale).
        # Заполняется через expose_history_settings_nodes().
        self._opcua_history_settings_nodes: Dict[str, Any] = {}
        self._opcua_history_settings_namespace_index: Optional[int] = None
        self._opcua_history_settings_parent: Any = None

        # Параметры оптимизации записи истории
        self._history_write_batch_enabled = history_write_batch_enabled
        self._history_write_max_batch_size = int(history_write_max_batch_size)
        self._history_write_max_batch_interval_sec = float(history_write_max_batch_interval_sec)
        self._history_write_queue_max_size = int(history_write_queue_max_size)
        self._history_write_durability_mode = history_write_durability_mode
        self._history_write_read_consistency_mode = history_write_read_consistency_mode
        self._history_cache_enabled = history_cache_enabled

        # Параметры и структуры кэша последних значений
        self._history_last_values_cache_enabled = history_last_values_cache_enabled
        self._history_last_values_cache_max_size_mb = int(history_last_values_cache_max_size_mb)
        self._history_last_values_init_batch_size = int(history_last_values_init_batch_size)
        # variable_id -> DataValue
        self._last_values_cache: Dict[int, ua.DataValue] = {}

        # Кэши метаданных (node_id / event_type -> внутренние идентификаторы)
        # Используются для снижения количества обращений к variable_metadata / event_sources / event_types.
        self._variable_metadata_cache: Dict[str, int] = {}
        self._event_source_cache: Dict[str, int] = {}
        self._event_type_cache: Dict[str, int] = {}

        # Параметры кэша метаданных
        self._history_metadata_cache_enabled = history_metadata_cache_enabled
        self._history_metadata_cache_init_max_rows = int(history_metadata_cache_init_max_rows)

        # Статистика эффективности кэшей (минимальный накладной расход — простые счётчики)
        # Значения увеличиваются только монотонно; сброс возможен через публичный метод.
        self._cache_stats: Dict[str, int] = {
            # In-memory кэш последних значений (variable_id -> DataValue)
            "last_values_memory_hits": 0,
            "last_values_memory_misses": 0,
            # Таблица variables_last_value как кэш в БД
            "last_values_table_hits": 0,
            "last_values_table_misses": 0,
            # Fallback к основной таблице истории (variables_history)
            "last_values_history_fallbacks": 0,
            # Кэш метаданных переменных (node_id -> variable_id)
            "variable_metadata_hits": 0,
            "variable_metadata_misses": 0,
            # Кэш источников событий (source_node_id -> source_id)
            "event_source_hits": 0,
            "event_source_misses": 0,
            # Кэш типов событий (event_type_name -> event_type_id)
            "event_type_hits": 0,
            "event_type_misses": 0,
        }

        # Метрики производительности записи/БД. Чтение не обращается к БД.
        self._performance_counters: Dict[str, int] = self._new_performance_counters()
        self._performance_timings: Dict[str, Dict[str, float]] = self._new_performance_timings()

        # Подавление первого уведомления datachange после подписки
        self.suppress_initial_datachange = True
        self._pending_initial_datachange_skip: Dict[str, bool] = {}

        # Буферы для батчевой записи истории
        self._value_write_buffer: Optional[HistoryWriteBuffer] = None
        self._event_write_buffer: Optional[HistoryWriteBuffer] = None

        # OPC UA nodes для экспонирования текущих метрик производительности.
        self._opcua_history_metrics_nodes: Dict[str, Any] = {}
        self._opcua_history_metrics_namespace_index: Optional[int] = None
        self._opcua_history_metrics_parent: Any = None

        # Инициализация параметров подключения
        self._conn_params = self._init_connection_params(
            user, password, database, host, port,
            sslmode, config_file, encrypted_config, master_password, **kwargs
        )
    
    def _init_connection_params(
        self,
        user: str,
        password: str,
        database: str,
        host: str,
        port: int,
        sslmode: Optional[str] = None,
        config_file: Optional[str] = None,
        encrypted_config: Optional[str] = None,
        master_password: Optional[str] = None,
        **kwargs
    ) -> dict:
        """
        Инициализация параметров подключения с поддержкой зашифрованной конфигурации.
        
        Args:
            user: Имя пользователя базы данных
            password: Пароль пользователя
            database: Имя базы данных
            host: Хост базы данных
            port: Порт базы данных
            sslmode: Режим SSL подключения ('disable', 'require', 'verify-ca', 'verify-full')
            config_file: Путь к файлу зашифрованной конфигурации
            encrypted_config: Зашифрованная конфигурация в виде строки
            master_password: Главный пароль для расшифровки конфигурации
            
        Returns:
            Словарь с параметрами подключения
        """
        # Приоритет: зашифрованная конфигурация > файл конфигурации > прямые параметры
        if encrypted_config and master_password:
            try:
                # Создаем временный DatabaseManager для расшифровки
                temp_manager = DatabaseManager(master_password)
                # Расшифровываем конфигурацию из строки
                decrypted_config = temp_manager._decrypt_config(encrypted_config.encode())
                self.logger.info("Using encrypted configuration from string")
                config_params = {
                    'user': decrypted_config.get('user', user),
                    'password': decrypted_config.get('password', password),
                    'database': decrypted_config.get('database', database),
                    'host': decrypted_config.get('host', host),
                    'port': decrypted_config.get('port', port),
                    'schema': decrypted_config.get('schema', self._schema),
                    'sslmode': decrypted_config.get('sslmode', sslmode)
                }
                # Обновляем схему если она указана в конфигурации
                if 'schema' in decrypted_config:
                    self._schema = decrypted_config['schema']
                # Добавляем дополнительные параметры из kwargs
                config_params.update(kwargs)
                return config_params
            except Exception as e:
                self.logger.warning(f"Failed to decrypt configuration string: {e}, using direct parameters")
        
        elif config_file and master_password:
            try:
                # Создаем DatabaseManager для загрузки конфигурации из файла
                temp_manager = DatabaseManager(master_password, config_file)
                if temp_manager.config:
                    self.logger.info(f"Using configuration from file: {config_file}")
                    config_params = {
                        'user': temp_manager.config.get('user', user),
                        'password': temp_manager.config.get('password', password),
                        'database': temp_manager.config.get('database', database),
                        'host': temp_manager.config.get('host', host),
                        'port': temp_manager.config.get('port', port),
                        'schema': temp_manager.config.get('schema', self._schema),
                        'sslmode': temp_manager.config.get('sslmode', sslmode)
                    }
                    # Обновляем схему если она указана в конфигурации
                    if 'schema' in temp_manager.config:
                        self._schema = temp_manager.config['schema']
                    # Добавляем дополнительные параметры из kwargs
                    config_params.update(kwargs)
                    return config_params
                else:
                    self.logger.warning(f"Configuration file {config_file} is empty or invalid, using direct parameters")
            except Exception as e:
                self.logger.warning(f"Failed to load configuration from file {config_file}: {e}, using direct parameters")
        
        # Используем прямые параметры как fallback
        self.logger.info("Using direct connection parameters")
        base_params = {
            'user': user,
            'password': password,
            'database': database,
            'host': host,
            'port': port,
            'schema': self._schema
        }
        # Добавляем sslmode если он указан
        if sslmode is not None:
            base_params['sslmode'] = sslmode
        # Добавляем дополнительные параметры из kwargs
        base_params.update(kwargs)
        return base_params

    def get_connection_info(self) -> dict:
        """
        Получение информации о текущих параметрах подключения.
        
        Returns:
            Словарь с информацией о подключении
        """
        return {
            'user': self._conn_params['user'],
            'host': self._conn_params['host'],
            'port': self._conn_params['port'],
            'database': self._conn_params['database'],
            'schema': self._schema,
            'min_size': self._min_size,
            'max_size': self._max_size,
            'initialized': self._initialized
        }

    def get_cache_stats(self) -> dict:
        """
        Получение текущей статистики эффективности кэшей модуля истории.

        Возвращаются только числовые счётчики, инкрементируемые при обращениях к кэшу.
        Метод не выполняет обращений к БД и имеет минимальный накладной расход.
        """
        # Возвращаем копию, чтобы внешний код не мог повлиять на внутренние счётчики.
        return dict(self._cache_stats)

    def reset_cache_stats(self) -> None:
        """
        Сброс статистики эффективности кэшей.

        Полезно при длительной работе сервера или перед началом измерений.
        """
        for key in self._cache_stats:
            self._cache_stats[key] = 0

    @staticmethod
    def _new_performance_counters() -> Dict[str, int]:
        return {
            "save_node_value_calls_total": 0,
            "save_node_value_errors_total": 0,
            "save_event_calls_total": 0,
            "save_event_errors_total": 0,
            "db_operation_timeouts_total": 0,
            "db_reconnects_total": 0,
        }

    @staticmethod
    def _new_performance_timings() -> Dict[str, Dict[str, float]]:
        return {
            "variable_flush_total": _empty_timing_stats(),
            "variable_insert_history": _empty_timing_stats(),
            "variable_upsert_last_value": _empty_timing_stats(),
            "event_flush_total": _empty_timing_stats(),
            "event_insert_history": _empty_timing_stats(),
        }

    def _perf_inc(self, key: str, amount: int = 1) -> None:
        self._performance_counters[key] = self._performance_counters.get(key, 0) + amount

    def _perf_observe_ms(self, key: str, duration_ms: float) -> None:
        stats = self._performance_timings.setdefault(key, _empty_timing_stats())
        _observe_timing(stats, duration_ms)

    @staticmethod
    def _format_timing_metrics(prefix: str, stats: Dict[str, float]) -> Dict[str, float]:
        return {
            f"{prefix}_count": int(stats.get("count", 0)),
            f"{prefix}_total_ms": float(stats.get("total_ms", 0.0)),
            f"{prefix}_last_ms": float(stats.get("last_ms", 0.0)),
            f"{prefix}_max_ms": float(stats.get("max_ms", 0.0)),
            f"{prefix}_avg_ms": float(stats.get("avg_ms", 0.0)),
        }

    def get_performance_metrics(self) -> dict:
        """
        Возвращает снимок метрик производительности без обращений к БД.
        """
        variable_buffer = (
            self._value_write_buffer.get_stats()
            if self._value_write_buffer is not None
            else self._empty_buffer_metrics()
        )
        event_buffer = (
            self._event_write_buffer.get_stats()
            if self._event_write_buffer is not None
            else self._empty_buffer_metrics()
        )

        variable_metrics: Dict[str, Any] = {
            "save_node_value_calls_total": self._performance_counters["save_node_value_calls_total"],
            "save_node_value_errors_total": self._performance_counters["save_node_value_errors_total"],
            **variable_buffer,
            **self._format_timing_metrics("flush", self._performance_timings["variable_flush_total"]),
            **self._format_timing_metrics("insert_history", self._performance_timings["variable_insert_history"]),
            **self._format_timing_metrics("upsert_last_value", self._performance_timings["variable_upsert_last_value"]),
        }
        event_metrics: Dict[str, Any] = {
            "save_event_calls_total": self._performance_counters["save_event_calls_total"],
            "save_event_errors_total": self._performance_counters["save_event_errors_total"],
            **event_buffer,
            **self._format_timing_metrics("flush", self._performance_timings["event_flush_total"]),
            **self._format_timing_metrics("insert_history", self._performance_timings["event_insert_history"]),
        }

        return {
            "write": {
                "variables": variable_metrics,
                "events": event_metrics,
            },
            "db": {
                "timeouts_total": self._performance_counters["db_operation_timeouts_total"],
                "reconnects_total": self._performance_counters["db_reconnects_total"],
            },
            "retention": {
                "per_variable_cleanup_enabled": False,
                "per_event_cleanup_enabled": False,
            },
            "config": {
                "history_write_batch_enabled": bool(self._history_write_batch_enabled),
                "history_write_max_batch_size": int(self._history_write_max_batch_size),
                "history_write_max_batch_interval_sec": float(self._history_write_max_batch_interval_sec),
                "history_write_queue_max_size": int(self._history_write_queue_max_size),
                "history_write_durability_mode": str(self._history_write_durability_mode),
                "history_write_read_consistency_mode": str(self._history_write_read_consistency_mode),
                "db_query_timeout_sec": self._db_query_timeout_sec,
            },
        }

    @staticmethod
    def _empty_buffer_metrics() -> Dict[str, Any]:
        return {
            "queue_size": 0,
            "queue_max_size": 0,
            "queue_fill_ratio": 0.0,
            "enqueue_attempts_total": 0,
            "enqueued_total": 0,
            "dropped_total": 0,
            "flushed_items_total": 0,
            "flush_batches_total": 0,
            "last_batch_size": 0,
            "max_batch_size_seen": 0,
            "flush_errors_total": 0,
            "last_flush_error": "",
            "last_flush_duration_ms": 0.0,
            "max_flush_duration_ms": 0.0,
            "avg_flush_duration_ms": 0.0,
            "total_flush_duration_ms": 0.0,
        }

    def reset_performance_metrics(self) -> None:
        """
        Сбрасывает метрики производительности и статистику буферов записи.
        """
        self._performance_counters = self._new_performance_counters()
        self._performance_timings = self._new_performance_timings()
        if self._value_write_buffer is not None:
            self._value_write_buffer.reset_stats()
        if self._event_write_buffer is not None:
            self._event_write_buffer.reset_stats()

    @classmethod
    def from_config_file(
        cls,
        config_file: str,
        master_password: str,
        min_size: int = 1,
        max_size: int = 10
    ) -> 'HistoryTimescale':
        """
        Создание экземпляра из файла зашифрованной конфигурации.
        
        Args:
            config_file: Путь к файлу зашифрованной конфигурации
            master_password: Главный пароль для расшифровки
            min_size: Минимальное количество соединений в пуле
            max_size: Максимальное количество соединений в пуле
            
        Returns:
            Экземпляр HistoryTimescale с загруженной конфигурацией
        """
        return cls(
            config_file=config_file,
            master_password=master_password,
            min_size=min_size,
            max_size=max_size
        )
    
    @classmethod
    def from_encrypted_config(
        cls,
        encrypted_config: str,
        master_password: str,
        min_size: int = 1,
        max_size: int = 10
    ) -> 'HistoryTimescale':
        """
        Создание экземпляра из зашифрованной конфигурации в виде строки.
        
        Args:
            encrypted_config: Зашифрованная конфигурация в виде строки
            master_password: Главный пароль для расшифровки
            min_size: Минимальное количество соединений в пуле
            max_size: Максимальное количество соединений в пуле
            
        Returns:
            Экземпляр HistoryTimescale с расшифрованной конфигурацией
        """
        return cls(
            encrypted_config=encrypted_config,
            master_password=master_password,
            min_size=min_size,
            max_size=max_size
        )

    def update_config(
        self,
        config_file: Optional[str] = None,
        encrypted_config: Optional[str] = None,
        master_password: Optional[str] = None
    ) -> bool:
        """
        Обновление конфигурации подключения.
        
        Args:
            config_file: Путь к файлу зашифрованной конфигурации
            encrypted_config: Зашифрованная конфигурация в виде строки
            master_password: Главный пароль для расшифровки конфигурации
            
        Returns:
            True если конфигурация обновлена успешно
        """
        if self._pool:
            self.logger.warning("Cannot update config while pool is active. Call stop() first.")
            return False
        
        try:
            # Сбрасываем флаг инициализации
            self._initialized = False
            
            # Обновляем параметры подключения
            self._conn_params = self._init_connection_params(
                self._conn_params.get('user', 'postgres'),
                self._conn_params.get('password', 'postmaster'),
                self._conn_params.get('database', 'opcua'),
                self._conn_params.get('host', 'localhost'),
                self._conn_params.get('port', 5432),
                config_file, encrypted_config, master_password
            )
            
            self.logger.info("Configuration updated successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to update configuration: {e}")
            return False

    async def init(self) -> None:
        """Инициализация подключения к базе данных и создание таблиц метаданных."""
        try:
            self._stopping = False
            await self._ensure_pool()

            if not self._initialized:
                await self._create_metadata_tables()
                self._initialized = True

            # Инициализируем кэш метаданных переменных после готовности схемы/таблиц
            if self._history_metadata_cache_enabled:
                await self._init_metadata_cache()

            # Инициализируем буферы записи истории при первом запуске
            if self._history_write_batch_enabled:
                if self._value_write_buffer is None:
                    self._value_write_buffer = HistoryWriteBuffer(
                        name="variables",
                        logger=self.logger,
                        max_batch_size=self._history_write_max_batch_size,
                        max_batch_interval_sec=self._history_write_max_batch_interval_sec,
                        queue_max_size=self._history_write_queue_max_size,
                        durability_mode=self._history_write_durability_mode,
                        flush_func=self._flush_variable_batch,
                    )
                    self._value_write_buffer.start()

                if self._event_write_buffer is None:
                    self._event_write_buffer = HistoryWriteBuffer(
                        name="events",
                        logger=self.logger,
                        max_batch_size=self._history_write_max_batch_size,
                        max_batch_interval_sec=self._history_write_max_batch_interval_sec,
                        queue_max_size=self._history_write_queue_max_size,
                        durability_mode=self._history_write_durability_mode,
                        flush_func=self._flush_event_batch,
                    )
                    self._event_write_buffer.start()

            # Инициализируем in-memory кэш последних значений из таблицы variables_last_value
            if self._history_last_values_cache_enabled:
                await self._init_last_values_cache()

            if self._reconnect_task is None or self._reconnect_task.done():
                self._stop_event.clear()
                self._reconnect_task = asyncio.create_task(self._reconnect_monitor())
                self.logger.info("Reconnect monitor started")

            # Если OPC UA узлы настроек уже экспонированы — обновим их значениями
            await self.refresh_history_settings_nodes()

            self.logger.info("HistoryTimescale initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize HistoryTimescale: {e}")
            raise

    async def expose_history_settings_nodes(
        self,
        server: Any,
        namespace_index: int,
        *,
        parent: Any = None,
    ) -> None:
        """
        Экспортирует read-only переменные с настройками хранения истории в адресное пространство OPC UA.

        Размещение по умолчанию: под `0:Server` (внутри `0:Objects`).

        Args:
            server: экземпляр asyncua.Server
            namespace_index: индекс namespace, в котором создавать пользовательские узлы
            parent: опционально, явный родительский узел. Если не задан — используется `0:Server`.
        """
        if server is None:
            raise ValueError("server is required")

        idx = int(namespace_index)

        async def _get_server_parent() -> Any:
            if parent is not None:
                return parent
            # Пытаемся использовать server.nodes.server, если доступно
            try:
                srv_node = getattr(getattr(server, "nodes", None), "server", None)
                if srv_node is not None:
                    return srv_node
            except Exception:
                pass
            # Fallback: ищем 0:Server под 0:Objects
            return await server.nodes.objects.get_child(["0:Server"])

        async def _get_or_add_object(parent_node: Any, name: str) -> Any:
            qn = f"{idx}:{name}"
            try:
                return await parent_node.get_child([qn])
            except Exception:
                return await parent_node.add_object(idx, name)

        async def _get_or_add_variable(parent_node: Any, name: str, initial_value: ua.Variant) -> Any:
            qn = f"{idx}:{name}"
            try:
                return await parent_node.get_child([qn])
            except Exception:
                # В asyncua переменные по умолчанию read-only, поэтому set_writable() не вызываем
                return await parent_node.add_variable(idx, name, initial_value)

        server_parent = await _get_server_parent()
        history_obj = await _get_or_add_object(server_parent, "History")
        settings_obj = await _get_or_add_object(history_obj, "HistorySettings")

        nodes: Dict[str, Any] = {}
        nodes["UapgVersion"] = await _get_or_add_variable(
            settings_obj, "UapgVersion", ua.Variant("unknown", ua.VariantType.String)
        )
        nodes["StorageType"] = await _get_or_add_variable(
            settings_obj, "StorageType", ua.Variant("timescale", ua.VariantType.String)
        )
        nodes["Schema"] = await _get_or_add_variable(
            settings_obj, "Schema", ua.Variant(str(self._schema), ua.VariantType.String)
        )
        nodes["GlobalRetentionSeconds"] = await _get_or_add_variable(
            settings_obj, "GlobalRetentionSeconds", ua.Variant(-1, ua.VariantType.Int64)
        )
        nodes["WriteBatchEnabled"] = await _get_or_add_variable(
            settings_obj, "WriteBatchEnabled", ua.Variant(bool(self._history_write_batch_enabled), ua.VariantType.Boolean)
        )
        nodes["WriteMaxBatchSize"] = await _get_or_add_variable(
            settings_obj, "WriteMaxBatchSize", ua.Variant(int(self._history_write_max_batch_size), ua.VariantType.Int32)
        )
        nodes["WriteMaxBatchIntervalSec"] = await _get_or_add_variable(
            settings_obj, "WriteMaxBatchIntervalSec", ua.Variant(float(self._history_write_max_batch_interval_sec), ua.VariantType.Double)
        )
        nodes["WriteQueueMaxSize"] = await _get_or_add_variable(
            settings_obj, "WriteQueueMaxSize", ua.Variant(int(self._history_write_queue_max_size), ua.VariantType.Int32)
        )
        nodes["WriteDurabilityMode"] = await _get_or_add_variable(
            settings_obj, "WriteDurabilityMode", ua.Variant(str(self._history_write_durability_mode), ua.VariantType.String)
        )
        nodes["WriteReadConsistencyMode"] = await _get_or_add_variable(
            settings_obj, "WriteReadConsistencyMode", ua.Variant(str(self._history_write_read_consistency_mode), ua.VariantType.String)
        )
        nodes["TimescaleExtensionAvailable"] = await _get_or_add_variable(
            settings_obj, "TimescaleExtensionAvailable", ua.Variant(False, ua.VariantType.Boolean)
        )

        self._opcua_history_settings_nodes = nodes
        self._opcua_history_settings_namespace_index = idx
        self._opcua_history_settings_parent = server_parent

        await self.refresh_history_settings_nodes()

    async def refresh_history_settings_nodes(self) -> None:
        """
        Обновляет значения OPC UA переменных (если они были экспонированы).

        No-op если expose_history_settings_nodes() ещё не вызывался.
        """
        if not self._opcua_history_settings_nodes:
            return

        try:
            try:
                uapg_version = importlib_metadata.version("uapg")
            except Exception:
                uapg_version = "unknown"

            retention = self._global_retention_period
            retention_sec = int(retention.total_seconds()) if retention is not None else -1

            ext_available = await self._timescaledb_available()

            values: Dict[str, ua.Variant] = {
                "UapgVersion": ua.Variant(str(uapg_version), ua.VariantType.String),
                "StorageType": ua.Variant("timescale", ua.VariantType.String),
                "Schema": ua.Variant(str(self._schema), ua.VariantType.String),
                "GlobalRetentionSeconds": ua.Variant(retention_sec, ua.VariantType.Int64),
                "WriteBatchEnabled": ua.Variant(bool(self._history_write_batch_enabled), ua.VariantType.Boolean),
                "WriteMaxBatchSize": ua.Variant(int(self._history_write_max_batch_size), ua.VariantType.Int32),
                "WriteMaxBatchIntervalSec": ua.Variant(float(self._history_write_max_batch_interval_sec), ua.VariantType.Double),
                "WriteQueueMaxSize": ua.Variant(int(self._history_write_queue_max_size), ua.VariantType.Int32),
                "WriteDurabilityMode": ua.Variant(str(self._history_write_durability_mode), ua.VariantType.String),
                "WriteReadConsistencyMode": ua.Variant(str(self._history_write_read_consistency_mode), ua.VariantType.String),
                "TimescaleExtensionAvailable": ua.Variant(bool(ext_available), ua.VariantType.Boolean),
            }

            for k, v in values.items():
                node = self._opcua_history_settings_nodes.get(k)
                if node is None:
                    continue
                try:
                    await node.write_value(v)
                except Exception:
                    # Если узел удалён/недоступен — просто пропускаем обновление
                    continue
        except Exception:
            # Никогда не ломаем основную функциональность истории из-за OPC UA витрины настроек
            return

    @staticmethod
    def _metric_node_name(metric_path: str) -> str:
        parts: List[str] = []
        for path_part in metric_path.split("."):
            for word in path_part.split("_"):
                if word:
                    parts.append(word[:1].upper() + word[1:])
        return "".join(parts)

    @staticmethod
    def _flatten_metrics(metrics: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
        flattened: Dict[str, Any] = {}
        for key, value in metrics.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            if isinstance(value, dict):
                flattened.update(HistoryTimescale._flatten_metrics(value, path))
            else:
                flattened[path] = value
        return flattened

    @staticmethod
    def _metric_variant(value: Any) -> ua.Variant:
        if isinstance(value, bool):
            return ua.Variant(value, ua.VariantType.Boolean)
        if isinstance(value, int):
            return ua.Variant(value, ua.VariantType.Int64)
        if isinstance(value, float):
            return ua.Variant(value, ua.VariantType.Double)
        if value is None:
            return ua.Variant("", ua.VariantType.String)
        return ua.Variant(str(value), ua.VariantType.String)

    async def expose_history_metrics_nodes(
        self,
        server: Any,
        namespace_index: int,
        *,
        parent: Any = None,
    ) -> None:
        """
        Экспортирует read-only переменные с метриками производительности в OPC UA.

        Метрики размещаются в `History/HistoryMetrics` и обновляются только
        явным вызовом `refresh_history_metrics_nodes()`.
        """
        if server is None:
            raise ValueError("server is required")

        idx = int(namespace_index)

        async def _get_server_parent() -> Any:
            if parent is not None:
                return parent
            try:
                srv_node = getattr(getattr(server, "nodes", None), "server", None)
                if srv_node is not None:
                    return srv_node
            except Exception:
                pass
            return await server.nodes.objects.get_child(["0:Server"])

        async def _get_or_add_object(parent_node: Any, name: str) -> Any:
            qn = f"{idx}:{name}"
            try:
                return await parent_node.get_child([qn])
            except Exception:
                return await parent_node.add_object(idx, name)

        async def _get_or_add_variable(parent_node: Any, name: str, initial_value: ua.Variant) -> Any:
            qn = f"{idx}:{name}"
            try:
                return await parent_node.get_child([qn])
            except Exception:
                return await parent_node.add_variable(idx, name, initial_value)

        server_parent = await _get_server_parent()
        history_obj = await _get_or_add_object(server_parent, "History")
        metrics_obj = await _get_or_add_object(history_obj, "HistoryMetrics")

        nodes: Dict[str, Any] = {}
        for metric_path, value in self._flatten_metrics(self.get_performance_metrics()).items():
            node_name = self._metric_node_name(metric_path)
            nodes[metric_path] = await _get_or_add_variable(
                metrics_obj,
                node_name,
                self._metric_variant(value),
            )

        self._opcua_history_metrics_nodes = nodes
        self._opcua_history_metrics_namespace_index = idx
        self._opcua_history_metrics_parent = server_parent

        await self.refresh_history_metrics_nodes()

    async def refresh_history_metrics_nodes(self) -> None:
        """
        Обновляет OPC UA переменные метрик, если они были экспонированы.
        """
        if not self._opcua_history_metrics_nodes:
            return

        try:
            flattened = self._flatten_metrics(self.get_performance_metrics())
            for metric_path, value in flattened.items():
                node = self._opcua_history_metrics_nodes.get(metric_path)
                if node is None:
                    continue
                try:
                    await node.write_value(self._metric_variant(value))
                except Exception:
                    continue
        except Exception:
            return

    def _build_pool_params(self) -> dict:
        pool_params = {
            'user': self._conn_params['user'],
            'password': self._conn_params['password'],
            'database': self._conn_params['database'],
            'host': self._conn_params['host'],
            'port': self._conn_params['port'],
            'min_size': self._min_size,
            'max_size': self._max_size
        }

        exclude_params = {'user', 'password', 'database', 'host', 'port', 'min_size', 'max_size', 'sslmode', 'schema'}
        for key, value in self._conn_params.items():
            if key not in exclude_params:
                pool_params[key] = value

        if self._conn_params.get('sslmode') == 'disable':
            pool_params['ssl'] = False
        elif self._conn_params.get('sslmode') in ('require', 'verify-ca', 'verify-full'):
            pool_params['ssl'] = True

        return pool_params

    @staticmethod
    def _is_pool_open(pool: Optional[asyncpg.Pool]) -> bool:
        return pool is not None and not pool._closed and not getattr(pool, "_closing", False)

    async def _run_db_operation(self, awaitable: Any, operation: str) -> Any:
        timeout = self._db_query_timeout_sec
        try:
            if timeout is None or timeout <= 0:
                return await awaitable
            return await asyncio.wait_for(awaitable, timeout=timeout)
        except asyncio.TimeoutError:
            self._perf_inc("db_operation_timeouts_total")
            self.logger.error("PostgreSQL %s timed out after %.1f seconds", operation, timeout)
            raise

    async def _close_pool_with_timeout(self, pool: Optional[asyncpg.Pool], reason: str) -> None:
        if pool is None:
            return
        try:
            await asyncio.wait_for(pool.close(), timeout=self._db_pool_close_timeout_sec)
        except asyncio.TimeoutError:
            self.logger.warning(
                "Timed out closing PostgreSQL pool during %s after %.1f seconds; terminating connections",
                reason,
                self._db_pool_close_timeout_sec,
            )
            pool.terminate()
        except Exception as e:
            self.logger.warning("Error closing PostgreSQL pool during %s: %r", reason, e, exc_info=True)

    async def _ensure_pool(self) -> None:
        """
        Гарантирует наличие рабочего пула соединений.

        Пул считается непригодным, если он закрыт или находится в процессе закрытия.
        """
        if self._stopping:
            raise RuntimeError("HistoryTimescale is stopping")
        if self._is_pool_open(self._pool):
            return
        if self._reconnect_lock.locked():
            async with self._reconnect_lock:
                pass
            if self._is_pool_open(self._pool):
                return
        async with self._pool_lock:
            if self._is_pool_open(self._pool):
                return
            pool_params = self._build_pool_params()
            self._pool = await asyncpg.create_pool(**pool_params)
            self.logger.info("Connection pool created")

    async def _is_pool_healthy(self) -> bool:
        try:
            await self._ensure_pool()
            async def _op() -> Any:
                async with self._pool.acquire(timeout=self._db_query_timeout_sec) as conn:
                    return await conn.fetchval('SELECT 1', timeout=self._db_query_timeout_sec)
            val = await self._run_db_operation(_op(), "healthcheck")
            return val == 1
        except Exception as e:
            # Логируем технические детали неудачной проверки соединения с PostgreSQL
            conn_params = getattr(self, "_conn_params", {}) or {}
            self.logger.debug(
                "PostgreSQL healthcheck failed (db=%s, user=%s, host=%s, port=%s): %r",
                conn_params.get("database"),
                conn_params.get("user"),
                conn_params.get("host"),
                conn_params.get("port"),
                e,
            )
            return False

    async def _reconnect_monitor(self) -> None:
        """
        Фоновый монитор состояния подключения к PostgreSQL.

        Периодически выполняет healthcheck и при необходимости вызывает _force_reconnect.
        Все неожиданные ошибки внутри цикла логируются, чтобы корутина не «исчезала» тихо.
        """
        delay = self._reconnect_min_delay
        while not self._stop_event.is_set():
            try:
                healthy = await self._is_pool_healthy()
                if healthy:
                    if not self._was_healthy:
                        # Соединение с PostgreSQL восстановлено
                        now = datetime.now(timezone.utc)
                        conn_params = getattr(self, "_conn_params", {}) or {}
                        if self._db_unavailable_since is not None:
                            outage = now - self._db_unavailable_since
                            self.logger.info(
                                "PostgreSQL connection restored (db=%s, user=%s, host=%s, port=%s) "
                                "after %.1f seconds of unavailability",
                                conn_params.get("database"),
                                conn_params.get("user"),
                                conn_params.get("host"),
                                conn_params.get("port"),
                                outage.total_seconds(),
                            )
                        else:
                            self.logger.info(
                                "PostgreSQL connection restored (db=%s, user=%s, host=%s, port=%s)",
                                conn_params.get("database"),
                                conn_params.get("user"),
                                conn_params.get("host"),
                                conn_params.get("port"),
                            )
                        self._was_healthy = True
                        self._reset_outage_stats()
                        # Сбрасываем таймер аггрегированного логирования реконнекта
                        self._last_reconnect_outage_log_at = None
                    delay = self._reconnect_min_delay
                    try:
                        await asyncio.wait_for(self._stop_event.wait(), timeout=5.0)
                    except asyncio.TimeoutError:
                        pass
                    continue

                # Соединение нездорово — начинаем или продолжаем попытки реконнекта
                now = datetime.now(timezone.utc)
                conn_params = getattr(self, "_conn_params", {}) or {}
                # Фиксируем момент начала недоступности, если ещё не зафиксирован
                if self._db_unavailable_since is None:
                    self._db_unavailable_since = now

                if self._was_healthy:
                    # Первый переход в состояние недоступности
                    self.logger.error(
                        "PostgreSQL became unreachable (db=%s, user=%s, host=%s, port=%s). "
                        "Starting reconnect attempts.",
                        conn_params.get("database"),
                        conn_params.get("user"),
                        conn_params.get("host"),
                        conn_params.get("port"),
                    )
                else:
                    self.logger.warning(
                        "PostgreSQL is still unhealthy, will continue reconnect attempts "
                        "(db=%s, host=%s, port=%s).",
                        conn_params.get("database"),
                        conn_params.get("host"),
                        conn_params.get("port"),
                    )
                self._was_healthy = False
                # Периодическое агрегированное сообщение о длительной недоступности PostgreSQL
                if self._db_unavailable_since is not None:
                    outage = now - self._db_unavailable_since
                    if outage >= timedelta(minutes=1):
                        if (
                            self._last_reconnect_outage_log_at is None
                            or (now - self._last_reconnect_outage_log_at) >= timedelta(minutes=1)
                        ):
                            self._last_reconnect_outage_log_at = now
                            self.logger.error(
                                "PostgreSQL has been unreachable for %.1f seconds "
                                "(db=%s, user=%s, host=%s, port=%s). Reconnect attempts continue.",
                                outage.total_seconds(),
                                conn_params.get("database"),
                                conn_params.get("user"),
                                conn_params.get("host"),
                                conn_params.get("port"),
                            )

                try:
                    await self._force_reconnect(self._pool)
                    self.logger.info("Reconnected to database successfully")
                    delay = self._reconnect_min_delay
                except Exception as e:
                    # _force_reconnect уже залогировал critical, здесь фиксируем, что монитор продолжает попытки
                    self.logger.error(f"Reconnect attempt failed in monitor: {e}")
                    jitter = random.uniform(0, 0.3 * delay)
                    await asyncio.sleep(delay + jitter)
                    delay = min(delay * 2, self._reconnect_max_delay)
            except asyncio.CancelledError:
                # Нормальное завершение по stop()
                break
            except Exception as e:
                # Любая неожиданная ошибка внутри монитора — логируем и продолжаем,
                # чтобы корутина не завершилась тихо.
                self.logger.error("Reconnect monitor unexpected error: %r", e, exc_info=True)
                jitter = random.uniform(0, 0.3 * delay)
                await asyncio.sleep(delay + jitter)

    def _log_connection_restored_if_needed(self) -> None:
        """
        Фиксирует в логе восстановление подключения к PostgreSQL,
        если ранее оно считалось недоступным.

        Этот метод вызывается на пути успешного выполнения произвольного SQL‑запроса,
        чтобы гарантировать появление сообщения «соединение восстановлено» даже
        если это произошло не через фоновый монитор реконнекта.
        """
        if self._was_healthy:
            return

        now = datetime.now(timezone.utc)
        conn_params = getattr(self, "_conn_params", {}) or {}

        if self._db_unavailable_since is not None:
            outage = now - self._db_unavailable_since
            self.logger.info(
                "PostgreSQL connection restored via successful query "
                "(db=%s, user=%s, host=%s, port=%s) after %.1f seconds of unavailability",
                conn_params.get("database"),
                conn_params.get("user"),
                conn_params.get("host"),
                conn_params.get("port"),
                outage.total_seconds(),
            )
        else:
            self.logger.info(
                "PostgreSQL connection restored via successful query "
                "(db=%s, user=%s, host=%s, port=%s)",
                conn_params.get("database"),
                conn_params.get("user"),
                conn_params.get("host"),
                conn_params.get("port"),
            )

        self._was_healthy = True
        self._reset_outage_stats()

    def _reset_outage_stats(self) -> None:
        if self._failed_value_saves_counter or self._failed_event_saves_counter:
            self.logger.info(
                f"During outage suppressed failures: values={self._failed_value_saves_counter}, events={self._failed_event_saves_counter}"
            )
        self._db_unavailable_since = None
        self._last_throttled_log_at = None
        self._last_reconnect_outage_log_at = None
        self._failed_value_saves_counter = 0
        self._failed_event_saves_counter = 0

    def _log_save_failure_throttled(self, kind: str, node_repr: str, error: Exception, datavalue_repr: str = None) -> None:
        now = datetime.now(timezone.utc)
        if self._db_unavailable_since is None:
            self._db_unavailable_since = now
        if kind == 'value':
            self._failed_value_saves_counter += 1
            count = self._failed_value_saves_counter
        else:
            self._failed_event_saves_counter += 1
            count = self._failed_event_saves_counter

        elapsed = now - self._db_unavailable_since
        if elapsed < timedelta(minutes=10):
            # Полная детализация в первые 10 минут
            if datavalue_repr is not None:
                self.logger.error(f"Failed to save {kind} for {node_repr}: {error} \n {datavalue_repr}")
            else:
                self.logger.error(f"Failed to save {kind} for {node_repr}: {error}")
            return

        # После 10 минут — не чаще 1 раза в 10 секунд, с агрегацией
        if self._last_throttled_log_at is None or (now - self._last_throttled_log_at) >= timedelta(seconds=10):
            self._last_throttled_log_at = now
            self.logger.error(
                f"Database still unavailable. Aggregated {kind} save failures: {count}. Latest error: {error}"
            )
            # Сбрасываем только соответствующий счётчик, чтобы считать новый интервал
            if kind == 'value':
                self._failed_value_saves_counter = 0
            else:
                self._failed_event_saves_counter = 0

    async def stop(self) -> None:
        """Остановка и закрытие пула соединений."""
        self._stop_event.set()
        for buffer in (self._value_write_buffer, self._event_write_buffer):
            if buffer is not None:
                try:
                    await buffer.stop()
                except Exception as e:
                    self.logger.warning("Error stopping history write buffer: %r", e, exc_info=True)
        self._value_write_buffer = None
        self._event_write_buffer = None
        self._stopping = True

        if self._reconnect_task and not self._reconnect_task.done():
            try:
                await asyncio.wait_for(self._reconnect_task, timeout=self._db_pool_close_timeout_sec)
            except asyncio.TimeoutError:
                self.logger.warning(
                    "Timed out stopping PostgreSQL reconnect monitor after %.1f seconds; cancelling it",
                    self._db_pool_close_timeout_sec,
                )
                self._reconnect_task.cancel()
                try:
                    await self._reconnect_task
                except asyncio.CancelledError:
                    pass
            except Exception:
                pass
        self._reconnect_task = None
        if self._pool:
            pool = self._pool
            self._pool = None
            await self._close_pool_with_timeout(pool, "stop")
        self.logger.info("HistoryTimescale stopped")

    async def _execute(self, query: str, *args) -> Any:
        """
        Выполнение SQL запроса.
        
        Args:
            query: SQL запрос
            *args: Аргументы для запроса
            
        Returns:
            Результат выполнения запроса
        """
        await self._ensure_pool()
        failed_pool = self._pool
        try:
            async def _op() -> Any:
                async with failed_pool.acquire(timeout=self._db_query_timeout_sec) as conn:
                    return await conn.execute(query, *args, timeout=self._db_query_timeout_sec)
            result = await self._run_db_operation(_op(), "execute")
            self._log_connection_restored_if_needed()
            return result
        except Exception as e:
            if isinstance(e, asyncio.TimeoutError):
                self.logger.error(f"Execute timed out, will reconnect without retrying: {e}")
                await self._force_reconnect(failed_pool)
                raise
            # Ошибка выполнения запроса или проблемы с соединением — логируем как error
            self.logger.error(f"Execute failed, will try to reconnect and retry: {e}")
            # Попытка принудительного переподключения; при неудаче _force_reconnect сам залогирует critical и выбросит исключение
            await self._force_reconnect(failed_pool)
            # Вторая попытка выполнения запроса
            try:
                retry_pool = self._pool
                async def _op_retry() -> Any:
                    async with retry_pool.acquire(timeout=self._db_query_timeout_sec) as conn:
                        return await conn.execute(query, *args, timeout=self._db_query_timeout_sec)
                result = await self._run_db_operation(_op_retry(), "execute retry")
                self._log_connection_restored_if_needed()
                return result
            except Exception as e2:
                # Ошибка выполнения SQL после переподключения — тоже error
                self.logger.error(f"Execute failed after reconnect: {e2}")
                raise

    async def _fetch(self, query: str, *args) -> List[asyncpg.Record]:
        """
        Выполнение SQL запроса с возвратом результатов.
        
        Args:
            query: SQL запрос
            *args: Аргументы для запроса
            
        Returns:
            Список записей
        """
        await self._ensure_pool()
        failed_pool = self._pool
        try:
            async def _op() -> Any:
                async with failed_pool.acquire(timeout=self._db_query_timeout_sec) as conn:
                    return await conn.fetch(query, *args, timeout=self._db_query_timeout_sec)
            rows = await self._run_db_operation(_op(), "fetch")
            self._log_connection_restored_if_needed()
            return rows
        except Exception as e:
            if isinstance(e, asyncio.TimeoutError):
                self.logger.error(f"Fetch timed out, will reconnect without retrying: {e}")
                await self._force_reconnect(failed_pool)
                raise
            self.logger.error(f"Fetch failed, will try to reconnect and retry: {e}")
            await self._force_reconnect(failed_pool)
            try:
                retry_pool = self._pool
                async def _op_retry() -> Any:
                    async with retry_pool.acquire(timeout=self._db_query_timeout_sec) as conn:
                        return await conn.fetch(query, *args, timeout=self._db_query_timeout_sec)
                rows = await self._run_db_operation(_op_retry(), "fetch retry")
                self._log_connection_restored_if_needed()
                return rows
            except Exception as e2:
                self.logger.error(f"Fetch failed after reconnect: {e2}")
                raise

    async def _fetchval(self, query: str, *args) -> Any:
        """
        Выполнение SQL запроса с возвратом одного значения.
        
        Args:
            query: SQL запрос
            *args: Аргументы для запроса
            
        Returns:
            Значение
        """
        await self._ensure_pool()
        failed_pool = self._pool
        try:
            async def _op() -> Any:
                async with failed_pool.acquire(timeout=self._db_query_timeout_sec) as conn:
                    return await conn.fetchval(query, *args, timeout=self._db_query_timeout_sec)
            value = await self._run_db_operation(_op(), "fetchval")
            self._log_connection_restored_if_needed()
            return value
        except Exception as e:
            if isinstance(e, asyncio.TimeoutError):
                self.logger.error(f"Fetchval timed out, will reconnect without retrying: {e}")
                await self._force_reconnect(failed_pool)
                raise
            self.logger.error(f"Fetchval failed, will try to reconnect and retry: {e}")
            await self._force_reconnect(failed_pool)
            try:
                retry_pool = self._pool
                async def _op_retry() -> Any:
                    async with retry_pool.acquire(timeout=self._db_query_timeout_sec) as conn:
                        return await conn.fetchval(query, *args, timeout=self._db_query_timeout_sec)
                value = await self._run_db_operation(_op_retry(), "fetchval retry")
                self._log_connection_restored_if_needed()
                return value
            except Exception as e2:
                self.logger.error(f"Fetchval failed after reconnect: {e2}")
                raise

    async def _fetchrow(self, query: str, *args) -> Optional[asyncpg.Record]:
        """
        Выполнение SQL запроса с возвратом одной строки.
        
        Args:
            query: SQL запрос
            *args: Аргументы для запроса
            
        Returns:
            Одна строка из результата запроса или None
        """
        await self._ensure_pool()
        failed_pool = self._pool
        try:
            async def _op() -> Any:
                async with failed_pool.acquire(timeout=self._db_query_timeout_sec) as conn:
                    return await conn.fetchrow(query, *args, timeout=self._db_query_timeout_sec)
            row = await self._run_db_operation(_op(), "fetchrow")
            self._log_connection_restored_if_needed()
            return row
        except Exception as e:
            if isinstance(e, asyncio.TimeoutError):
                self.logger.error(f"Fetchrow timed out, will reconnect without retrying: {e}")
                await self._force_reconnect(failed_pool)
                raise
            self.logger.error(f"Fetchrow failed, will try to reconnect and retry: {e}")
            await self._force_reconnect(failed_pool)
            try:
                retry_pool = self._pool
                async def _op_retry() -> Any:
                    async with retry_pool.acquire(timeout=self._db_query_timeout_sec) as conn:
                        return await conn.fetchrow(query, *args, timeout=self._db_query_timeout_sec)
                row = await self._run_db_operation(_op_retry(), "fetchrow retry")
                self._log_connection_restored_if_needed()
                return row
            except Exception as e2:
                self.logger.error(f"Fetchrow failed after reconnect: {e2}")
                raise

    async def _force_reconnect(self, failed_pool: Optional[asyncpg.Pool] = None) -> None:
        """
        Полное пересоздание пула соединений.

        Старый пул закрывается синхронно, чтобы избежать гонок состояний
        внутри asyncpg (ошибки вида «another operation is in progress»).
        """
        async with self._reconnect_lock:
            if self._stopping:
                raise RuntimeError("HistoryTimescale is stopping")
            old_pool: Optional[asyncpg.Pool] = None
            async with self._pool_lock:
                try:
                    if (
                        failed_pool is not None
                        and self._pool is not failed_pool
                        and self._is_pool_open(self._pool)
                    ):
                        self.logger.debug("Skipping PostgreSQL reconnect: pool was already replaced")
                        return
                    if self._pool:
                        old_pool = self._pool
                    # Обнуляем ссылку на пул: новые операции дождутся завершения реконнекта
                    # в _ensure_pool и не создадут конкурирующий пул.
                    self._pool = None
                except Exception as e:
                    self.logger.error("Error while preparing to recreate pool: %r", e, exc_info=True)
                    raise

            # Закрываем старый пул уже вне блокировки, но синхронно
            if old_pool is not None:
                await self._close_pool_with_timeout(old_pool, "reconnect")

            # Создаём новый пул под блокировкой, без использования _ensure_pool,
            # чтобы избежать рекурсивного захвата замка.
            async with self._pool_lock:
                try:
                    pool_params = self._build_pool_params()
                    self._pool = await asyncpg.create_pool(**pool_params)
                    self._perf_inc("db_reconnects_total")
                    self.logger.info("Connection pool recreated successfully after failure")
                except Exception as e:
                    # Невозможно восстановить подключение к БД — критический уровень и проброс исключения наверх
                    self.logger.critical(f"Force reconnect failed, database remains unavailable: {e}")
                    raise

    async def _create_metadata_tables(self) -> None:
        """Создание единых таблиц для историзации в указанной схеме."""
        try:
            # Создаем схему если она не существует
            await self._execute(f'CREATE SCHEMA IF NOT EXISTS "{self._schema}"')
            
                        # Единая таблица для всех переменных
            await self._execute(f'''
                CREATE TABLE IF NOT EXISTS "{self._schema}".variables_history (
                    id BIGSERIAL,
                    variable_id BIGINT NOT NULL,
                    servertimestamp TIMESTAMPTZ NOT NULL,
                    sourcetimestamp TIMESTAMPTZ NOT NULL,
                    statuscode INTEGER,
                    value TEXT,
                    varianttype INTEGER,
                    variantbinary BYTEA,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            ''')

            # Единая таблица для всех событий
            await self._execute(f'''
                CREATE TABLE IF NOT EXISTS "{self._schema}".events_history (
                    id BIGSERIAL,
                    source_id BIGINT NOT NULL,
                    event_type_id BIGINT NOT NULL,
                    event_timestamp TIMESTAMPTZ NOT NULL,
                    event_data JSONB,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            ''')

            # Таблица метаданных переменных
            await self._execute(f'''
                CREATE TABLE IF NOT EXISTS "{self._schema}".variable_metadata (
                    id BIGSERIAL PRIMARY KEY,
                    variable_id BIGINT GENERATED ALWAYS AS (id) STORED,
                    node_id TEXT NOT NULL,
                    data_type TEXT,
                    retention_period INTERVAL,
                    max_records INTEGER,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(variable_id)
                )
            ''')

            # Таблица источников событий
            await self._execute(f'''
                CREATE TABLE IF NOT EXISTS "{self._schema}".event_sources (
                    id BIGSERIAL PRIMARY KEY,
                    source_id BIGINT GENERATED ALWAYS AS (id) STORED,
                    source_node_id TEXT NOT NULL,
                    retention_period INTERVAL,
                    max_records INTEGER,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(source_id)
                )
            ''')

            # Таблица типов событий
            await self._execute(f'''
                CREATE TABLE IF NOT EXISTS "{self._schema}".event_types (
                    id BIGSERIAL PRIMARY KEY,
                    event_type_id BIGINT GENERATED ALWAYS AS (id) STORED,
                    event_type_name TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(event_type_id)
                )
            ''')
            
            # Таблица кэша последних значений переменных.
            # Инвариант: для каждой зарегистрированной переменной здесь есть строка
            # (см. seed_last_values) — чтение последних значений не обращается к истории.
            # is_seed=TRUE — строка-дефолт, ещё не сверенная с историей
            # (снимается реальной записью значения или backfill_last_values).
            await self._execute(f'''
                CREATE TABLE IF NOT EXISTS "{self._schema}".variables_last_value (
                    variable_id BIGINT PRIMARY KEY,
                    sourcetimestamp TIMESTAMPTZ NOT NULL,
                    servertimestamp TIMESTAMPTZ NOT NULL,
                    statuscode INTEGER NOT NULL,
                    varianttype INTEGER NOT NULL,
                    variantbinary BYTEA NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            ''')
            await self._execute(f'''
                ALTER TABLE "{self._schema}".variables_last_value
                ADD COLUMN IF NOT EXISTS is_seed BOOLEAN NOT NULL DEFAULT FALSE
            ''')
            
            # Создаем индексы для производительности и связей
            # Индексы для таблиц истории (bigint поля для оптимизации)
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_variables_variable_id ON "{self._schema}".variables_history(variable_id)')
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_variables_timestamp ON "{self._schema}".variables_history(sourcetimestamp)')
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_variables_server_timestamp ON "{self._schema}".variables_history(servertimestamp)')
            # Уникальный индекс должен включать столбцы партиционирования TimescaleDB
            await self._execute(f'CREATE UNIQUE INDEX IF NOT EXISTS idx_variables_varid_sourcets ON "{self._schema}".variables_history(variable_id, sourcetimestamp)')

            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_events_source_id ON "{self._schema}".events_history(source_id)')
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_events_event_type_id ON "{self._schema}".events_history(event_type_id)')
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_events_timestamp ON "{self._schema}".events_history(event_timestamp)')
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_events_data_gin ON "{self._schema}".events_history USING GIN (event_data)')
            # Уникальный индекс должен включать столбцы партиционирования TimescaleDB
            await self._execute(f'CREATE UNIQUE INDEX IF NOT EXISTS idx_events_sourceid_eventts ON "{self._schema}".events_history(source_id, event_timestamp)')

            # Индексы для таблиц метаданных (bigint поля)
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_variable_metadata_variable_id ON "{self._schema}".variable_metadata(variable_id)')
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_event_sources_source_id ON "{self._schema}".event_sources(source_id)')
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_event_types_event_type_id ON "{self._schema}".event_types(event_type_id)')
            
            # Уникальные индексы для event_sources
            await self._execute(f'CREATE UNIQUE INDEX IF NOT EXISTS idx_event_sources_node_id ON "{self._schema}".event_sources(source_node_id)')
            
            # Уникальный индекс для event_types по имени типа события
            await self._execute(f'CREATE UNIQUE INDEX IF NOT EXISTS idx_event_types_name ON "{self._schema}".event_types(event_type_name)')
            
            # Уникальный индекс для variable_metadata по node_id
            await self._execute(f'CREATE UNIQUE INDEX IF NOT EXISTS idx_variable_metadata_node_id ON "{self._schema}".variable_metadata(node_id)')

            # Дополнительные индексы для оптимизации связей (bigint поля)
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_variables_history_variable_id_timestamp ON "{self._schema}".variables_history(variable_id, sourcetimestamp)')
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_events_history_source_timestamp ON "{self._schema}".events_history(source_id, event_timestamp)')
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_events_history_type_source ON "{self._schema}".events_history(event_type_id, source_id)')

            # Составной индекс для оптимизации поиска по типу события и источнику
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_events_history_event_type_source ON "{self._schema}".events_history(event_type_id, source_id)')

            # Индексы для каскадных операций удаления
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_variable_metadata_created ON "{self._schema}".variable_metadata(created_at)')
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_event_sources_created ON "{self._schema}".event_sources(created_at)')
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_event_types_created ON "{self._schema}".event_types(created_at)')
            
            # Индекс для кэш-таблицы последних значений
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_variables_last_value_updated ON "{self._schema}".variables_last_value(updated_at)')
            
            # Покрывающий индекс для fallback-запросов последнего значения (без variantbinary из-за размера)
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_variables_history_vid_ts_desc_covering ON "{self._schema}".variables_history (variable_id, sourcetimestamp DESC) INCLUDE (statuscode, varianttype, servertimestamp)')
            
            self.logger.info(f"Unified history tables created successfully in schema '{self._schema}'")
            
            # Настраиваем TimescaleDB hypertables после создания всех индексов
            await self._setup_timescale_hypertable(f'{self._schema}.variables_history', 'sourcetimestamp', 'variable_id', 128)
            await self._setup_timescale_hypertable(f'{self._schema}.events_history', 'event_timestamp', 'source_id', 64)

            # Настраиваем глобальную retention policy (если задана)
            await self._ensure_global_retention_policies()
        except Exception as e:
            self.logger.error(f"Failed to create unified history tables: {e}")
            raise

    async def _setup_timescale_hypertable(self, table: str, partition_column: str, space_partition_column: Optional[str] = None, space_partitions: Optional[int] = None) -> None:
        """
        Настройка TimescaleDB hypertable с возможностью дополнительного партиционирования.
        
        Args:
            table: Имя таблицы
            partition_column: Колонка для временного партиционирования
            space_partition_column: Дополнительная колонка для пространственного партиционирования (TimescaleDB 2+)
            space_partitions: Количество партиций для space-измерения (1..32767)
        """
        try:
            # Проверяем, доступно ли расширение TimescaleDB
            extension_check = await self._fetchval("SELECT COUNT(*) FROM pg_extension WHERE extname = 'timescaledb'")
            if extension_check == 0:
                self.logger.warning("TimescaleDB extension not found. Creating regular table without hypertable.")
                return
            
            # Проверяем версию TimescaleDB
            timescale_version = await self._fetchval("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")
            if timescale_version:
                major_version = int(timescale_version.split('.')[0])
                if major_version >= 2 and space_partition_column:
                    # Устанавливаем дефолт для количества партиций, если не задано
                    partitions = space_partitions if (space_partitions and 1 <= space_partitions <= 32767) else 32
                    await self._execute(
                        f"SELECT create_hypertable('{table}', '{partition_column}', partitioning_column => '{space_partition_column}', number_partitions => {partitions}, if_not_exists => TRUE)"
                    )
                    self.logger.info(f"TimescaleDB hypertable created for table {table} with space partitioning on {space_partition_column} (number_partitions={partitions})")
                else:
                    # Стандартное партиционирование только по времени
                    await self._execute(
                        f"SELECT create_hypertable('{table}', '{partition_column}', if_not_exists => TRUE)"
                    )
                    self.logger.info(f"TimescaleDB hypertable created for table {table}")
            else:
                # Fallback для старых версий
                await self._execute(
                    f"SELECT create_hypertable('{table}', '{partition_column}', if_not_exists => TRUE)"
                )
                self.logger.info(f"TimescaleDB hypertable created for table {table}")
        except Exception as e:
            self.logger.warning(f"Failed to create TimescaleDB hypertable for table {table}: {e}")
            self.logger.info("Continuing with regular table (without TimescaleDB optimization)")

    def _get_effective_retention_period(self, requested: Optional[timedelta]) -> Optional[timedelta]:
        """
        Вычисляет эффективный retention_period для узла/источника событий.

        Правило:
        - если глобальный период не задан — используем requested как есть;
        - если глобальный задан:
          - requested is None или requested >= global → используем global;
          - иначе → используем requested.
        """
        global_period = self._global_retention_period
        if global_period is None:
            return requested
        if requested is None:
            return global_period
        try:
            return requested if requested < global_period else global_period
        except Exception:
            # На всякий случай (если прилетел неожиданный тип)
            return global_period

    async def _timescaledb_available(self) -> bool:
        """True если расширение TimescaleDB установлено в текущей БД."""
        try:
            extension_check = await self._fetchval("SELECT COUNT(*) FROM pg_extension WHERE extname = 'timescaledb'")
            return bool(extension_check and int(extension_check) > 0)
        except Exception:
            return False

    async def _setup_retention_policy(self, table_name: str, drop_after: timedelta) -> None:
        """
        Идемпотентно обеспечивает наличие TimescaleDB retention policy для hypertable.

        Args:
            table_name: имя таблицы без схемы (например, 'variables_history')
            drop_after: interval хранения (chunks старше now()-drop_after будут удаляться)
        """
        if drop_after is None:
            return
        if drop_after.total_seconds() <= 0:
            self.logger.warning(f"Global retention period must be >0, got {drop_after}. Skipping policy setup.")
            return

        if not await self._timescaledb_available():
            # Нет Timescale — ничего не делаем
            return

        drop_after_seconds = int(drop_after.total_seconds())
        try:
            await self._execute(
                "SELECT add_retention_policy("
                "  format('%I.%I', $1::text, $2::text)::regclass, "
                "  drop_after => make_interval(secs => $3::integer), "
                "  if_not_exists => TRUE"
                ")",
                self._schema,
                table_name,
                drop_after_seconds,
            )
            self.logger.info(
                f"Retention policy ensured for {self._schema}.{table_name}: drop_after={drop_after_seconds}s"
            )
        except Exception as e:
            # Не критично для работы — предупреждаем и продолжаем
            self.logger.warning(f"Failed to ensure retention policy for {self._schema}.{table_name}: {e}")

    async def _ensure_global_retention_policies(self) -> None:
        """Настраивает глобальную retention policy для таблиц *history (если задано)."""
        if self._global_retention_period is None:
            return
        await self._setup_retention_policy("variables_history", self._global_retention_period)
        await self._setup_retention_policy("events_history", self._global_retention_period)

    async def reapply_global_retention_policy(
        self,
        period: Optional[timedelta] = None,
        *,
        drop_immediately: bool = False,
    ) -> None:
        """
        Принудительная переустановка глобальной retention policy для hypertables.

        Использование:
        - вызвать после `await init()`
        - при смене `global_retention_period` в рантайме, чтобы обновить Timescale policy без перезапуска

        Args:
            period: новый глобальный период. Если None — используется текущий self._global_retention_period.
                Если итоговый период None — политика будет удалена (глобальный лимит отключён).
            drop_immediately: если True и период задан — выполнить разовый drop_chunks, чтобы
                уменьшение retention применилось сразу (а не по расписанию фоновой job).
        """
        await self._ensure_pool()

        if not await self._timescaledb_available():
            self.logger.warning("TimescaleDB extension not found. Cannot (re)apply retention policies.")
            return

        if period is not None:
            self._global_retention_period = period

        effective = self._global_retention_period

        async def _remove(table: str) -> None:
            await self._execute(
                "SELECT remove_retention_policy(format('%I.%I', $1::text, $2::text)::regclass, if_exists => TRUE)",
                self._schema,
                table,
            )

        async def _add(table: str, td: timedelta) -> None:
            secs = int(td.total_seconds())
            await self._execute(
                "SELECT add_retention_policy("
                "  format('%I.%I', $1::text, $2::text)::regclass, "
                "  drop_after => make_interval(secs => $3::integer), "
                "  if_not_exists => FALSE"
                ")",
                self._schema,
                table,
                secs,
            )
            if drop_immediately:
                await self._execute(
                    "SELECT drop_chunks("
                    "  format('%I.%I', $1::text, $2::text)::regclass, "
                    "  older_than => make_interval(secs => $3::integer)"
                    ")",
                    self._schema,
                    table,
                    secs,
                )

        for t in ("variables_history", "events_history"):
            try:
                await _remove(t)
                if effective is not None:
                    if effective.total_seconds() <= 0:
                        raise ValueError(f"Global retention period must be >0, got {effective}")
                    await _add(t, effective)
            except Exception as e:
                self.logger.warning(f"Failed to reapply retention policy for {self._schema}.{t}: {e}")

        # Обновим OPC UA витрину настроек, если она включена
        await self.refresh_history_settings_nodes()

    async def _flush_variable_batch(self, items: List[VariableWriteItem]) -> None:
        """
        Флаш батча значений переменных в таблицы variables_history и variables_last_value.
        """
        if not items:
            return

        history_params = [
            (
                it.variable_id,
                it.server_timestamp,
                it.source_timestamp,
                it.status_code,
                it.value_str,
                it.variant_type,
                it.variant_binary,
            )
            for it in items
        ]

        last_value_params = [
            (
                it.variable_id,
                it.source_timestamp,
                it.server_timestamp,
                it.status_code,
                it.variant_type,
                it.variant_binary,
            )
            for it in items
        ]

        # Делаем до двух попыток записи батча: первая — с текущим пулом,
        # вторая — после принудительного реконнекта при ошибке.
        for attempt in (1, 2):
            await self._ensure_pool()
            failed_pool = self._pool
            try:
                async def _op() -> None:
                    async with failed_pool.acquire(timeout=self._db_query_timeout_sec) as conn:
                        async with conn.transaction():
                            insert_started_at = time.perf_counter()
                            await conn.executemany(
                                f'INSERT INTO "{self._schema}".variables_history '
                                f'(variable_id, servertimestamp, sourcetimestamp, statuscode, value, varianttype, variantbinary) '
                                f'VALUES ($1, $2, $3, $4, $5, $6, $7) '
                                f'ON CONFLICT (variable_id, sourcetimestamp) DO NOTHING',
                                history_params,
                                timeout=self._db_query_timeout_sec,
                            )
                            self._perf_observe_ms(
                                "variable_insert_history",
                                (time.perf_counter() - insert_started_at) * 1000.0,
                            )

                            upsert_started_at = time.perf_counter()
                            await conn.executemany(
                                f'''
                                INSERT INTO "{self._schema}".variables_last_value
                                    (variable_id, sourcetimestamp, servertimestamp, statuscode, varianttype, variantbinary)
                                VALUES ($1, $2, $3, $4, $5, $6)
                                ON CONFLICT (variable_id) DO UPDATE
                                    SET sourcetimestamp = EXCLUDED.sourcetimestamp,
                                        servertimestamp = EXCLUDED.servertimestamp,
                                        statuscode = EXCLUDED.statuscode,
                                        varianttype = EXCLUDED.varianttype,
                                        variantbinary = EXCLUDED.variantbinary,
                                        is_seed = FALSE,
                                        updated_at = NOW()
                                    WHERE "{self._schema}".variables_last_value.is_seed
                                       OR "{self._schema}".variables_last_value.sourcetimestamp <= EXCLUDED.sourcetimestamp
                                ''',
                                last_value_params,
                                timeout=self._db_query_timeout_sec,
                            )
                            self._perf_observe_ms(
                                "variable_upsert_last_value",
                                (time.perf_counter() - upsert_started_at) * 1000.0,
                            )
                flush_started_at = time.perf_counter()
                await self._run_db_operation(_op(), "flush variable batch")
                self._perf_observe_ms(
                    "variable_flush_total",
                    (time.perf_counter() - flush_started_at) * 1000.0,
                )

                # Успешная запись батча — считаем, что соединение восстановлено
                self._log_connection_restored_if_needed()

                # Обновляем in-memory кэш последних значений
                for it in items:
                    self._update_last_values_cache(it.variable_id, it.datavalue)
                return
            except Exception as e:
                if attempt == 1:
                    if isinstance(e, asyncio.TimeoutError):
                        self.logger.error(f"Flush variable batch timed out, will reconnect without retrying: {e}")
                        await self._force_reconnect(failed_pool)
                        raise
                    self.logger.error(f"Flush variable batch failed, will try to reconnect and retry: {e}")
                    await self._force_reconnect(failed_pool)
                else:
                    self.logger.error(f"Flush variable batch failed after reconnect: {e}")
                    raise

    async def _flush_event_batch(self, items: List[EventWriteItem]) -> None:
        """
        Флаш батча событий в таблицу events_history.
        """
        if not items:
            return

        params = [
            (
                it.source_db_id,
                it.event_type_id,
                it.event_timestamp,
                it.event_data_json,
            )
            for it in items
        ]

        # Делаем до двух попыток записи батча: первая — с текущим пулом,
        # вторая — после принудительного реконнекта при ошибке.
        for attempt in (1, 2):
            await self._ensure_pool()
            failed_pool = self._pool
            try:
                async def _op() -> None:
                    async with failed_pool.acquire(timeout=self._db_query_timeout_sec) as conn:
                        async with conn.transaction():
                            insert_started_at = time.perf_counter()
                            await conn.executemany(
                                f'INSERT INTO "{self._schema}".events_history '
                                f'(source_id, event_type_id, event_timestamp, event_data) '
                                f'VALUES ($1, $2, $3, $4) '
                                f'ON CONFLICT (source_id, event_timestamp) DO NOTHING',
                                params,
                                timeout=self._db_query_timeout_sec,
                            )
                            self._perf_observe_ms(
                                "event_insert_history",
                                (time.perf_counter() - insert_started_at) * 1000.0,
                            )
                flush_started_at = time.perf_counter()
                await self._run_db_operation(_op(), "flush event batch")
                self._perf_observe_ms(
                    "event_flush_total",
                    (time.perf_counter() - flush_started_at) * 1000.0,
                )
                return
            except Exception as e:
                if attempt == 1:
                    if isinstance(e, asyncio.TimeoutError):
                        self.logger.error(f"Flush event batch timed out, will reconnect without retrying: {e}")
                        await self._force_reconnect(failed_pool)
                        raise
                    self.logger.error(f"Flush event batch failed, will try to reconnect and retry: {e}")
                    await self._force_reconnect(failed_pool)
                else:
                    self.logger.error(f"Flush event batch failed after reconnect: {e}")
                    raise

    async def _save_variable_metadata(self, node_id: ua.NodeId, period: Optional[timedelta], count: int) -> int:
        """
        Сохранение метаданных переменной.

        Args:
            node_id: Идентификатор узла
            period: Период хранения
            count: Максимальное количество записей

        Returns:
            int: variable_id для использования в таблице истории
        """
        # Сохраняем метаданные переменной (используем INSERT ... RETURNING для получения ID)
        # Создаем полное имя узла для уникальной идентификации
        node_id_str = self._format_node_id(node_id)
        
        # При регистрации переменной тип данных пока неизвестен
        # Будет обновлен при первом сохранении значения
        data_type = "Unknown"

        # Приводим период хранения к глобальному максимуму (если задан)
        period = self._get_effective_retention_period(period)
        
        result = await self._fetchval(f'''
            INSERT INTO "{self._schema}".variable_metadata (node_id, data_type, retention_period, max_records)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (node_id) DO UPDATE SET
                data_type = EXCLUDED.data_type,
                retention_period = EXCLUDED.retention_period,
                max_records = EXCLUDED.max_records,
                updated_at = NOW()
            RETURNING variable_id
        ''', node_id_str, data_type, period, count)

        if result is None:
            # Если не удалось вставить, получаем существующий ID
            result = await self._fetchval(f'''
                SELECT variable_id FROM "{self._schema}".variable_metadata
                WHERE node_id = $1
                LIMIT 1
            ''', node_id_str)
        
        # Обновляем кэш метаданных
        if result is not None:
            self._variable_metadata_cache[node_id_str] = result
        
        return result

    async def _save_event_source(self, source_id: ua.NodeId, period: Optional[timedelta], count: int) -> int:
        """
        Сохранение источника событий.

        Args:
            source_id: Идентификатор источника событий
            period: Период хранения
            count: Максимальное количество записей

        Returns:
            int: source_id для использования в таблице истории
        """
        source_node_id_str = self._format_node_id(source_id)

        # Приводим период хранения к глобальному максимуму (если задан)
        period = self._get_effective_retention_period(period)
        
        result = await self._fetchval(f'''
            INSERT INTO "{self._schema}".event_sources (source_node_id, retention_period, max_records)
            VALUES ($1, $2, $3)
            ON CONFLICT (source_node_id) DO UPDATE SET
                retention_period = EXCLUDED.retention_period,
                max_records = EXCLUDED.max_records,
                updated_at = NOW()
            RETURNING source_id
        ''', source_node_id_str, period, count)

        if result is None:
            # Если не удалось вставить, получаем существующий ID
            result = await self._fetchval(f'''
                SELECT source_id FROM "{self._schema}".event_sources
                WHERE source_node_id = $1
                LIMIT 1
            ''', source_node_id_str)
        
        # Обновляем кэш источников событий
        if result is not None:
            self._event_source_cache[source_node_id_str] = result
        
        return result

    async def _save_event_metadata(self, event_type: ua.NodeId, source_id: ua.NodeId, fields: List[str], period: Optional[timedelta], count: int) -> Tuple[int, int]:
        """
        Сохранение метаданных события.

        Args:
            event_type: Тип события
            source_id: Идентификатор источника
            fields: Список расширенных полей события
            period: Период хранения
            count: Максимальное количество записей

        Returns:
            Tuple[int, int]: (source_id, event_type_id) для использования в таблице истории
        """
        # Сначала создаем или получаем источник событий
        source_db_id = await self._save_event_source(source_id, period, count)
        
        # Теперь создаем запись для типа события
        event_type_name = self._format_node_id(event_type)
        
        event_db_id = await self._fetchval(f'''
            INSERT INTO "{self._schema}".event_types (event_type_name)
            VALUES ($1)
            ON CONFLICT (event_type_name) DO UPDATE SET
                updated_at = NOW()
            RETURNING event_type_id
        ''', event_type_name)

        if event_db_id is None:
            # Если не удалось вставить, получаем существующий ID
            event_db_id = await self._fetchval(f'''
                SELECT event_type_id FROM "{self._schema}".event_types
                WHERE event_type_name = $1
                LIMIT 1
            ''', event_type_name)
        
        # Обновляем кэш типов событий
        if event_db_id is not None:
            self._event_type_cache[event_type_name] = event_db_id
        
        return source_db_id, event_db_id

    async def _init_last_values_cache(self) -> None:
        """
        Инициализация in-memory кэша последних значений из таблицы variables_last_value.

        Загрузка выполняется пакетами, с грубой оценкой потребления памяти и
        ограничением по конфигурируемому порогу (history_last_values_cache_max_size_mb).
        """
        if not self._history_last_values_cache_enabled:
            return

        try:
            await self._ensure_pool()
            max_bytes = self._history_last_values_cache_max_size_mb * 1024 * 1024
            approx_bytes = 0
            batch_size = max(1, self._history_last_values_init_batch_size)
            offset = 0
            total_loaded = 0

            async with self._pool.acquire(timeout=self._db_query_timeout_sec) as conn:
                # Пытаемся оценить размер таблицы на стороне БД
                try:
                    rel_name = f'{self._schema}.variables_last_value'
                    rel_size = await conn.fetchval(
                        "SELECT pg_total_relation_size($1::regclass)",
                        rel_name,
                        timeout=self._db_query_timeout_sec,
                    )
                    if rel_size is not None:
                        approx_mb = rel_size / (1024 * 1024)
                        limit_mb = max_bytes / (1024 * 1024)
                        self.logger.info(
                            "Estimated relation size for %s: %.1f MB (cache limit %.1f MB)",
                            rel_name,
                            approx_mb,
                            limit_mb,
                        )
                except Exception as e:
                    self.logger.debug(f"Failed to estimate variables_last_value size: {e}")

                while True:
                    rows = await conn.fetch(
                        f'''
                        SELECT variable_id, sourcetimestamp, servertimestamp,
                               statuscode, varianttype, variantbinary
                        FROM "{self._schema}".variables_last_value
                        ORDER BY variable_id
                        LIMIT $1 OFFSET $2
                        ''',
                        batch_size,
                        offset,
                        timeout=self._db_query_timeout_sec,
                    )
                    if not rows:
                        break

                    for row in rows:
                        vid = row["variable_id"]
                        dv = ua.DataValue(
                            Value=variant_from_binary(Buffer(row["variantbinary"])),
                            StatusCode_=ua.StatusCode(row["statuscode"]),
                            SourceTimestamp=row["sourcetimestamp"],
                            ServerTimestamp=row["servertimestamp"],
                        )
                        self._last_values_cache[vid] = dv
                        total_loaded += 1

                        # Грубая оценка потребления памяти: размер бинарника + константа
                        vb = row["variantbinary"] or b""
                        approx_bytes += len(vb) + 128
                        if approx_bytes >= max_bytes:
                            self.logger.warning(
                                "Last values cache memory limit reached (%.1f MB), "
                                "stopping further loading (loaded %d entries)",
                                approx_bytes / (1024 * 1024),
                                total_loaded,
                            )
                            return

                    offset += len(rows)

            self.logger.info(
                "Last values cache initialized: %d entries (approx %.1f MB)",
                total_loaded,
                approx_bytes / (1024 * 1024),
            )
        except Exception as e:
            self.logger.error(f"Failed to initialize last values cache: {e}")

    async def _init_metadata_cache(self) -> None:
        """
        Инициализация кэша метаданных переменных (node_id -> variable_id).

        Загружаем пары (node_id, variable_id) из таблицы variable_metadata
        пакетами, с ограничением по максимальному числу строк.
        """
        if not self._history_metadata_cache_enabled:
            return

        try:
            await self._ensure_pool()
            max_rows = max(1, self._history_metadata_cache_init_max_rows)
            batch_size = min(10000, max_rows)
            total_loaded = 0
            last_variable_id = 0

            async with self._pool.acquire(timeout=self._db_query_timeout_sec) as conn:
                while total_loaded < max_rows:
                    rows = await conn.fetch(
                        f'''
                        SELECT node_id, variable_id
                        FROM "{self._schema}".variable_metadata
                        WHERE variable_id > $1
                        ORDER BY variable_id
                        LIMIT $2
                        ''',
                        last_variable_id,
                        min(batch_size, max_rows - total_loaded),
                        timeout=self._db_query_timeout_sec,
                    )
                    if not rows:
                        break

                    for row in rows:
                        node_id_str = row["node_id"]
                        vid = row["variable_id"]
                        self._variable_metadata_cache[node_id_str] = vid
                        total_loaded += 1
                        last_variable_id = vid
                        if total_loaded >= max_rows:
                            break

            self.logger.info(
                "Variable metadata cache initialized: %d entries (limit %d)",
                total_loaded,
                max_rows,
            )
        except Exception as e:
            self.logger.error(f"Failed to initialize variable metadata cache: {e}")

    def _extract_variant_values(self, event_data: dict) -> dict:
        """
        Извлекает значения из Variant объектов для JSON сериализации.
        Преобразует все несериализуемые типы в сериализуемые.
        
        Args:
            event_data: Словарь с данными события, содержащий Variant объекты
            
        Returns:
            Словарь с извлеченными значениями, готовыми для JSON сериализации
        """
        extracted = {}
        for key, value in event_data.items():
            if hasattr(value, 'Value'):
                # Если это Variant, извлекаем значение и рекурсивно обрабатываем
                extracted[key] = self._make_json_serializable(value.Value)
            else:
                # Если не Variant, обрабатываем значение
                extracted[key] = self._make_json_serializable(value)
        return extracted

    def _make_json_serializable(self, value: Any) -> Any:
        """
        Преобразует значение в JSON-сериализуемый тип.
        
        Args:
            value: Значение для преобразования
            
        Returns:
            JSON-сериализуемое значение
        """
        if value is None:
            return None

    def _update_last_values_cache(self, variable_id: int, datavalue: ua.DataValue) -> None:
        """
        Обновление in-memory кэша последних значений.

        Используется как при инициализации из БД, так и при новых записях.
        """
        if not self._history_last_values_cache_enabled:
            return
        if variable_id is None or datavalue is None:
            return
        self._last_values_cache[variable_id] = datavalue

    def _event_to_binary_map(self, ev_dict: dict) -> dict:
        import base64
        result = {}
        for key, variant in ev_dict.items():
            try:
                # Диагностика для ExtensionObject
                if hasattr(variant, 'VariantType') and variant.VariantType == ua.VariantType.ExtensionObject:
                    #self.logger.debug(f"_event_to_binary_map: Processing ExtensionObject for key '{key}': {variant.Value}")
                    try:
                        binary_data = variant_to_binary(variant)
                        #self.logger.debug(f"_event_to_binary_map: variant_to_binary success for '{key}', binary length: {len(binary_data)}")
                        result[key] = f"base64:{base64.b64encode(binary_data).decode('utf-8')}"
                    except Exception as e:
                        self.logger.error(f"_event_to_binary_map: variant_to_binary failed for '{key}': {e}")
                        result[key] = None
                else:
                    # Обычная обработка для не-ExtensionObject
                    binary_data = variant_to_binary(variant)
                    result[key] = f"base64:{base64.b64encode(binary_data).decode('utf-8')}"
            except Exception as e:
                self.logger.error(f"_event_to_binary_map: Failed to process key '{key}' with value {variant}: {e}")
                # На всякий случай, если вдруг попадётся не Variant
                try:
                    binary_data = variant_to_binary(ua.Variant(variant))
                    result[key] = f"base64:{base64.b64encode(binary_data).decode('utf-8')}"
                except Exception as e2:
                    self.logger.error(f"_event_to_binary_map: Fallback also failed for '{key}': {e2}")
                    result[key] = None
        return result

    def _binary_map_to_event_values(self, data: dict) -> dict:
        import base64
        result = {}
        for key, b64s in data.items():
            try:
                if b64s is None:
                    self.logger.debug(f"_binary_map_to_event_values: Skipping None value for key '{key}'")
                    result[key] = None
                    continue
                    
                if not isinstance(b64s, str) or not b64s.startswith('base64:'):
                    self.logger.debug(f"_binary_map_to_event_values: Non-base64 value for key '{key}': {type(b64s)} - {b64s}")
                    result[key] = b64s
                    continue
                    
                raw = base64.b64decode(b64s[7:])
                self.logger.debug(f"_binary_map_to_event_values: Decoded binary for key '{key}', length: {len(raw)}")
                
                v = variant_from_binary(Buffer(raw))
                self.logger.debug(f"_binary_map_to_event_values: variant_from_binary success for '{key}': {v}")
                
                # Диагностика для ExtensionObject
                if hasattr(v, 'VariantType') and v.VariantType == ua.VariantType.ExtensionObject:
                    self.logger.debug(f"_binary_map_to_event_values: Recovered ExtensionObject for key '{key}': {v.Value}")
                
                result[key] = v
            except Exception as e:
                self.logger.error(f"_binary_map_to_event_values: Failed to process key '{key}' with value {b64s}: {e}")
                # Фоллбэк: вернуть None
                result[key] = None
        return result

    async def _get_event_fields(self, evtypes: List[ua.NodeId]) -> List[str]:
        """
        Получение полей событий из типов узлов.
        
        Args:
            evtypes: Список типов событий
            
        Returns:
            Список имен полей событий
        """
        ev_aggregate_fields = []
        for event_type in evtypes:
            if isinstance(event_type, ua.NodeId):
                self.logger.warning(
                    "Cannot introspect event fields from NodeId %s without server Node; "
                    "pass asyncua Node from historize_event",
                    event_type,
                )
                continue
            ev_aggregate_fields.extend(await get_event_properties_from_type_node(event_type))
        ev_fields = []
        for field in set(ev_aggregate_fields):
            ev_fields.append((await field.read_display_name()).Text)
        return ev_fields
    
    async def new_historized_node(
        self,
        node_id: ua.NodeId,
        period: Optional[timedelta],
        count: int = 0
    ) -> None:
        """
        Регистрация нового узла для историзации в единой таблице.
        Таблица уже создана при инициализации.

        Args:
            node_id: Идентификатор узла OPC UA
            period: Период хранения данных (None для бесконечного хранения)
            count: Максимальное количество записей (0 для неограниченного)
        """
        #self.logger.debug("new_historized_node: node_id=%s period=%s count=%s",node_id, period, count,)

        try:
            effective_period = self._get_effective_retention_period(period)
            node_id_str = self._format_node_id(node_id)
            # Если variable_id уже есть в кэше метаданных (прогревается из БД при init),
            # upsert не нужен: retention применяется глобально через Timescale policy,
            # а data_type обновляется при сохранении значения.
            variable_id = self._variable_metadata_cache.get(node_id_str)
            if variable_id is None:
                variable_id = await self._save_variable_metadata(node_id, effective_period, count)

            # Сохраняем mapping node_id -> variable_id для быстрого доступа
            self._datachanges_period[node_id] = (effective_period, count, variable_id)

            if self.suppress_initial_datachange:
                self._pending_initial_datachange_skip[node_id_str] = True

            #self.logger.info(f"Variable node {node_id} registered for historization in unified table (variable_id: {variable_id})")
        except Exception as e:
            self.logger.error(f"Failed to register variable node {node_id}: {e}")
            raise

    async def new_historized_nodes(
        self,
        node_ids: List[ua.NodeId],
        period: Optional[timedelta],
        count: int = 0
    ) -> None:
        """
        Батчевая регистрация узлов для историзации: один SQL-запрос на все узлы,
        отсутствующие в кэше метаданных, вместо upsert-роундтрипа на каждый узел.

        Семантика идентична последовательным вызовам new_historized_node()
        с одинаковыми period/count.

        Args:
            node_ids: Список идентификаторов узлов OPC UA
            period: Период хранения данных (None для бесконечного хранения)
            count: Максимальное количество записей (0 для неограниченного)
        """
        effective_period = self._get_effective_retention_period(period)

        # Дедупликация по строковому node_id с сохранением порядка
        pairs: List[Tuple[ua.NodeId, str]] = []
        seen: set = set()
        for node_id in node_ids:
            node_id_str = self._format_node_id(node_id)
            if node_id_str in seen:
                continue
            seen.add(node_id_str)
            pairs.append((node_id, node_id_str))

        to_upsert = [node_id_str for _, node_id_str in pairs
                     if node_id_str not in self._variable_metadata_cache]
        if to_upsert:
            # data_type при конфликте не сбрасываем в 'Unknown': реальный тип
            # уже определён при сохранении значений и не должен теряться
            rows = await self._fetch(f'''
                INSERT INTO "{self._schema}".variable_metadata (node_id, data_type, retention_period, max_records)
                SELECT t.node_id, 'Unknown', $2, $3
                FROM unnest($1::text[]) AS t(node_id)
                ON CONFLICT (node_id) DO UPDATE SET
                    retention_period = EXCLUDED.retention_period,
                    max_records = EXCLUDED.max_records,
                    updated_at = NOW()
                RETURNING node_id, variable_id
            ''', to_upsert, effective_period, count)
            for row in rows:
                self._variable_metadata_cache[row["node_id"]] = row["variable_id"]

        missing: List[ua.NodeId] = []
        registered = 0
        for node_id, node_id_str in pairs:
            variable_id = self._variable_metadata_cache.get(node_id_str)
            if variable_id is None:
                missing.append(node_id)
                continue
            self._datachanges_period[node_id] = (effective_period, count, variable_id)
            if self.suppress_initial_datachange:
                self._pending_initial_datachange_skip[node_id_str] = True
            registered += 1

        # Фоллбэк на поштучную регистрацию (не должен срабатывать в норме)
        for node_id in missing:
            await self.new_historized_node(node_id, period, count)

        self.logger.info(
            "Bulk historized-node registration: %d nodes (%d upserted, %d fallback)",
            registered + len(missing), len(to_upsert), len(missing),
        )

    async def new_historized_event(
        self,
        source_id: ua.NodeId,
        evtypes: List[ua.NodeId],
        period: Optional[timedelta],
        count: int = 0
    ) -> None:
        """
        Регистрация нового источника событий для историзации в единой таблице.
        Таблица уже создана при инициализации.

        Args:
            source_id: Идентификатор источника событий
            evtypes: Список типов событий
            period: Период хранения данных (None для бесконечного хранения)
            count: Максимальное количество записей (0 для неограниченного)
        """
        self.logger.debug(
            "new_historized_event: source_id=%s evtypes=%s period=%s count=%s",
            source_id, evtypes, period, count,
        )

        try:
            evtypes_raw = evtypes
            source_id = _coerce_node_id(source_id)
            evtypes = [_coerce_node_id(event_type) for event_type in evtypes_raw]
            effective_period = self._get_effective_retention_period(period)
            # Поля читаем из asyncua Node до приведения к NodeId
            ev_fields = await self._get_event_fields(evtypes_raw)
            self._event_fields[source_id] = ev_fields

            # Сохраняем метаданные для каждого типа события и получаем IDs
            event_ids = {}
            for event_type in evtypes:
                source_db_id, event_db_id = await self._save_event_metadata(event_type, source_id, ev_fields, effective_period, count)
                event_ids[event_type] = (source_db_id, event_db_id)

            # Сохраняем mapping source_id -> (period, count, source_db_id, event_ids)
            self._datachanges_period[source_id] = (effective_period, count, source_db_id, event_ids)

            self.logger.info(f"Event source {source_id} registered for historization in unified table (source_id: {source_db_id})")
        except Exception as e:
            self.logger.error(f"Failed to register event source {source_id}: {e}")
            raise
    
    async def save_node_value(self, node_id: ua.NodeId, datavalue: ua.DataValue) -> None:
        """
        Сохранение значения узла в единую таблицу истории переменных.

        Args:
            node_id: Идентификатор узла OPC UA
            datavalue: Значение данных для сохранения
        """
        #self.logger.debug(
        #    "save_node_value: node_id=%s source_ts=%s server_ts=%s status=%s",
        #    node_id, getattr(datavalue, 'SourceTimestamp', None), getattr(datavalue, 'ServerTimestamp', None), getattr(datavalue, 'StatusCode', None),
        #)
        self._perf_inc("save_node_value_calls_total")

        node_id_str = self._format_node_id(node_id)

        if self.suppress_initial_datachange:
            # Подавляем первое уведомление после подписки, чтобы не перезаписывать данные из БД
            if self._pending_initial_datachange_skip.pop(node_id_str, False):
                #self.logger.debug("save_node_value: suppressed initial datachange for %s", node_id_str)
                return
        else:
            # Если подавление выключено, очищаем возможный накопленный флаг
            self._pending_initial_datachange_skip.pop(node_id_str, None)

        try:
            # Получаем variable_id из mapping
            node_data = self._datachanges_period.get(node_id)
            if node_data is None:
                variable_id = None
            else:
                # Проверяем формат данных
                if len(node_data) == 3:
                    period, count, variable_id = node_data
                elif len(node_data) == 4:
                    # Формат для событий: (period, count, source_db_id, event_ids)
                    self.logger.warning(f"Node {node_id} is registered as event source, not variable")
                    return
                else:
                    self.logger.warning(f"Unexpected data format for node {node_id}: {node_data}")
                    return
                    
            if variable_id is None:
                # Если mapping не найден, пробуем получить метаданные из кэша/БД
                cached_vid = self._variable_metadata_cache.get(node_id_str)
                if cached_vid is not None:
                    self._cache_stats["variable_metadata_hits"] += 1
                else:
                    self._cache_stats["variable_metadata_misses"] += 1

                meta_row = await self._fetchrow(f'''
                    SELECT variable_id, retention_period, max_records
                    FROM "{self._schema}".variable_metadata
                    WHERE node_id = $1
                    LIMIT 1
                ''', node_id_str)

                if meta_row is not None:
                    variable_id = int(meta_row["variable_id"])
                    period = meta_row["retention_period"]
                    count = int(meta_row["max_records"] or 0)
                else:
                    # Если метаданные не найдены, создаем их (с учетом глобального retention)
                    effective_period = self._get_effective_retention_period(None)
                    variable_id = await self._save_variable_metadata(node_id, effective_period, 0)
                    period = effective_period
                    count = 0

                # Обновляем in-memory mapping и кэш метаданных
                self._datachanges_period[node_id] = (period, count, variable_id)
                self._variable_metadata_cache[node_id_str] = variable_id

            # Подготовка данных для записи
            value_str = str(datavalue.Value.Value)
            variant_type = int(datavalue.Value.VariantType)
            variant_binary = variant_to_binary(datavalue.Value)

            # Обновляем in-memory кэш последних значений (read-after-write внутри процесса)
            self._update_last_values_cache(variable_id, datavalue)

            if self._history_write_batch_enabled and self._value_write_buffer is not None:
                # Батчированная запись через HistoryWriteBuffer
                node_id_str = self._format_node_id(node_id)
                group_key = self._build_group_key_from_node_id(node_id_str)
                item = VariableWriteItem(
                    variable_id=variable_id,
                    node_id_str=node_id_str,
                    source_timestamp=datavalue.SourceTimestamp,
                    server_timestamp=datavalue.ServerTimestamp,
                    status_code=datavalue.StatusCode.value,
                    value_str=value_str,
                    variant_type=variant_type,
                    variant_binary=variant_binary,
                    group_key=group_key,
                    datavalue=datavalue,
                )
                # В режиме global ожидаем завершения флаша
                sync = self._history_write_read_consistency_mode == "global"
                await self._value_write_buffer.enqueue(item, sync=sync)
            else:
                # Синхронная запись как раньше (без батчирования)
                await self._execute(
                    f'INSERT INTO "{self._schema}".variables_history (variable_id, servertimestamp, sourcetimestamp, statuscode, value, varianttype, variantbinary) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (variable_id, sourcetimestamp) DO NOTHING',
                    variable_id,
                    datavalue.ServerTimestamp,
                    datavalue.SourceTimestamp,
                    datavalue.StatusCode.value,
                    value_str,
                    variant_type,
                    variant_binary,
                )

                await self._execute(f'''
                    INSERT INTO "{self._schema}".variables_last_value
                    (variable_id, sourcetimestamp, servertimestamp, statuscode, varianttype, variantbinary)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (variable_id) DO UPDATE
                        SET sourcetimestamp = EXCLUDED.sourcetimestamp,
                            servertimestamp = EXCLUDED.servertimestamp,
                            statuscode = EXCLUDED.statuscode,
                            varianttype = EXCLUDED.varianttype,
                            variantbinary = EXCLUDED.variantbinary,
                            is_seed = FALSE,
                            updated_at = NOW()
                        WHERE "{self._schema}".variables_last_value.is_seed
                           OR "{self._schema}".variables_last_value.sourcetimestamp <= EXCLUDED.sourcetimestamp
                ''', variable_id, datavalue.SourceTimestamp, datavalue.ServerTimestamp,
                    datavalue.StatusCode.value, variant_type, variant_binary)

            # Обновляем тип данных в метаданных на основе реального DataValue только при изменении
            if datavalue and hasattr(datavalue, 'Value') and datavalue.Value is not None:
                actual_data_type = self._get_node_data_type(node_id, datavalue)
                if actual_data_type != "Unknown":
                    # Проверяем, изменился ли тип данных
                    current_data_type = await self._fetchval(f'''
                        SELECT data_type FROM "{self._schema}".variable_metadata 
                        WHERE variable_id = $1
                    ''', variable_id)
                    
                    # Обновляем только если тип изменился
                    if current_data_type != actual_data_type:
                        await self._execute(f'''
                            UPDATE "{self._schema}".variable_metadata 
                            SET data_type = $1, updated_at = NOW() 
                            WHERE variable_id = $2
                        ''', actual_data_type, variable_id)

        except Exception as e:
            self._perf_inc("save_node_value_errors_total")
            # Антиспам логирование при длительной недоступности БД
            self._log_save_failure_throttled('value', str(node_id), e, str(datavalue))
    
    async def save_event(self, event: Any) -> None:
        """
        Сохранение события в единую таблицу истории событий.

        Args:
            event: Событие OPC UA для сохранения
        """
        #self.logger.debug(f"save_event: {type(event)}")
        #self.logger.debug(f"save_event: {dir(event)}")
        #self.logger.debug(f"save_event: {event.get_event_props_as_fields_dict()}")
        self._perf_inc("save_event_calls_total")

        if event is None or not hasattr(event, 'SourceNode') or event.SourceNode is None:
            self.logger.error("save_event: invalid event")
            return

        event_type = getattr(event, 'EventType', None)

        if event_type is None:
            self.logger.error("save_event: event.EventType is None")
            return

        try:
            # Получаем source_id и event_type_id из mapping
            source_data = self._datachanges_period.get(event.SourceNode)
            if source_data is None:
                source_db_id = None
                event_db_id = None
            else:
                # Проверяем формат данных
                if len(source_data) == 4:
                    period, count, source_db_id, event_ids = source_data
                    event_db_id = event_ids.get(event_type, (None, None))[1]
                elif len(source_data) == 3:
                    # Старый формат для переменных: (period, count, variable_id)
                    self.logger.warning(f"Source {event.SourceNode} is registered as variable, not event source")
                    return
                else:
                    self.logger.warning(f"Unexpected data format for source {event.SourceNode}: {source_data}")
                    return

            if source_db_id is None or event_db_id is None:
                # Если mapping не найден, получаем IDs из кэша или базы данных
                # Сначала получаем source_id из event_sources
                source_node_id_str = self._format_node_id(event.SourceNode)
                cached_sid = self._event_source_cache.get(source_node_id_str)
                if cached_sid is not None:
                    self._cache_stats["event_source_hits"] += 1
                    source_db_id = cached_sid
                else:
                    self._cache_stats["event_source_misses"] += 1
                    source_db_id = await self._fetchval(f'''
                        SELECT source_id FROM "{self._schema}".event_sources 
                        WHERE source_node_id = $1
                        LIMIT 1
                    ''', source_node_id_str)
                
                if source_db_id is None:
                    # Если источник не найден, создаем его
                    source_db_id = await self._save_event_source(event.SourceNode, None, 0)

                # Теперь получаем event_type_id из event_types (через кэш)
                event_type_name = self._format_node_id(event_type)
                cached_eid = self._event_type_cache.get(event_type_name)
                if cached_eid is not None:
                    self._cache_stats["event_type_hits"] += 1
                    event_db_id = cached_eid
                else:
                    self._cache_stats["event_type_misses"] += 1
                    event_db_id = await self._fetchval(f'''
                        SELECT event_type_id FROM "{self._schema}".event_types 
                        WHERE event_type_name = $1
                        LIMIT 1
                    ''', event_type_name)
                
                if event_db_id is None:
                    # Если тип события не найден, создаем его
                    ev_fields = self._event_fields.get(event.SourceNode, [])
                    source_db_id, event_db_id = await self._save_event_metadata(event_type, event.SourceNode, ev_fields, None, 0)
                    if event.SourceNode not in self._datachanges_period:
                        self._datachanges_period[event.SourceNode] = (None, 0, source_db_id, {event_type: (source_db_id, event_db_id)})

            # Получаем время события
            event_time = getattr(event, 'Time', None) or getattr(event, 'time', None) or datetime.now(timezone.utc)

            # Получаем все поля события (Variant) и сериализуем в бинарь (base64)
            raw_event_data = event.get_event_props_as_fields_dict() if hasattr(event, 'get_event_props_as_fields_dict') else {}
            bin_event_data = self._event_to_binary_map(raw_event_data)

            event_data_json = json.dumps(bin_event_data)  # asyncpg требует сериализованную строку для JSONB

            if self._history_write_batch_enabled and self._event_write_buffer is not None:
                # Батчированная запись событий
                try:
                    source_node_id_str = self._format_node_id(event.SourceNode)
                except Exception:
                    source_node_id_str = str(getattr(event, "SourceNode", "unknown"))
                group_key = self._build_group_key_from_node_id(source_node_id_str)
                item = EventWriteItem(
                    source_db_id=source_db_id,
                    event_type_id=event_db_id,
                    event_timestamp=event_time,
                    event_data_json=event_data_json,
                    group_key=group_key,
                )
                sync = self._history_write_read_consistency_mode == "global"
                await self._event_write_buffer.enqueue(item, sync=sync)
            else:
                # Синхронная запись как раньше (без батчирования)
                await self._execute(
                    f'INSERT INTO "{self._schema}".events_history (source_id, event_type_id, event_timestamp, event_data) VALUES ($1, $2, $3, $4) ON CONFLICT (source_id, event_timestamp) DO NOTHING',
                    source_db_id,
                    event_db_id,
                    event_time,
                    event_data_json,
                )

        except Exception as e:
            self._perf_inc("save_event_errors_total")
            # Антиспам логирование при длительной недоступности БД
            src = getattr(event, 'SourceNode', 'unknown')
            self._log_save_failure_throttled('event', str(src), e)

    async def read_node_history(
        self,
        node_id: ua.NodeId,
        start: Optional[datetime],
        end: Optional[datetime],
        nb_values: Optional[int],
        return_bounds: bool = False
    ) -> Tuple[List[ua.DataValue], Optional[datetime]]:
        """
        Чтение истории узла из единой таблицы переменных.

        Args:
            node_id: Идентификатор узла
            start: Начальное время
            end: Конечное время
            nb_values: Количество значений
            return_bounds: Возвращать ли границы

        Returns:
            Кортеж (список значений, время продолжения)
        """
        #self.logger.debug(f"read_node_history: {node_id} {start} {end} {nb_values} {return_bounds}")
        start_time, end_time, order, limit = self._get_bounds(start, end, nb_values)

        try:
            # Получаем variable_id
            node_data = self._datachanges_period.get(node_id)
            if node_data is None:
                variable_id = None
            else:
                # Проверяем формат данных
                if len(node_data) == 3:
                    period, count, variable_id = node_data
                elif len(node_data) == 4:
                    # Формат для событий: (period, count, source_db_id, event_ids)
                    self.logger.warning(f"Node {node_id} is registered as event source, not variable")
                    return [], None
                else:
                    self.logger.warning(f"Unexpected data format for node {node_id}: {node_data}")
                    return [], None
                    
            if variable_id is None:
                # Если mapping не найден, пробуем получить variable_id из кэша по node_id_str
                node_id_str = self._format_node_id(node_id)
                cached_vid = self._variable_metadata_cache.get(node_id_str)
                if cached_vid is not None:
                    self._cache_stats["variable_metadata_hits"] += 1
                    variable_id = cached_vid
                else:
                    self._cache_stats["variable_metadata_misses"] += 1
                    # Если в кэше нет, получаем variable_id из базы данных
                    variable_id = await self._fetchval(f'''
                        SELECT variable_id FROM "{self._schema}".variable_metadata
                        WHERE node_id = $1
                        LIMIT 1
                    ''', node_id_str)

                if variable_id is not None:
                    # Обновляем кэш
                    self._variable_metadata_cache[node_id_str] = variable_id

            if variable_id is None:
                self.logger.warning(f"No metadata found for node {node_id}")
                return [], None

            # Запрос к единой таблице переменных
            select_sql = f'''
                SELECT servertimestamp, sourcetimestamp, statuscode, value, varianttype, variantbinary
                FROM "{self._schema}".variables_history
                WHERE variable_id = $1 AND sourcetimestamp BETWEEN $2 AND $3
                ORDER BY sourcetimestamp {order}
                LIMIT $4
            '''
            #self.logger.debug(f"read_node_history: {select_sql}")
            rows = await self._fetch(select_sql, variable_id, start_time, end_time, limit)
            #self.logger.debug(f"read_node_history: {len(rows)} rows")
            # Преобразуем в DataValue
            results = []
            for row in rows:
                #self.logger.debug(f"read_node_history: {row}")
                datavalue = ua.DataValue(
                    Value=variant_from_binary(Buffer(row['variantbinary'])),
                    StatusCode_=ua.StatusCode(row['statuscode']),
                    SourceTimestamp=row['sourcetimestamp'],
                    ServerTimestamp=row['servertimestamp']
                )
                results.append(datavalue)
                #self.logger.debug(f"read_node_history: {datavalue}")

            # Определяем время продолжения
            cont = None
            if len(results) == limit and len(rows) > 0:
                cont = rows[-1]['sourcetimestamp']

            #self.logger.debug(f"read_node_history: {len(results)} results")
            return results, cont

        except Exception as e:
            self.logger.error(f"Failed to read node history for {node_id}: {e}")
            return [], None

    async def read_event_history(
        self,
        source_id: ua.NodeId,
        start: Optional[datetime],
        end: Optional[datetime],
        nb_values: Optional[int],
        evfilter: Any
    ) -> Tuple[List[Any], Optional[datetime]]:
        """
        Чтение истории событий из единой таблицы событий.

        Args:
            source_id: Идентификатор источника событий
            start: Начальное время
            end: Конечное время
            nb_values: Количество значений
            evfilter: Фильтр событий

        Returns:
            Кортеж (список событий, время продолжения)
        """
        start_time, end_time, order, limit = self._get_bounds(start, end, nb_values)
        #self.logger.debug(f"read_event_history: {source_id} {start} {end} nb_values evfilter")
        try:
            # Получаем source_db_id
            source_data = self._datachanges_period.get(source_id)
            if source_data is None:
                # Если mapping не найден, пробуем получить source_db_id из кэша или базы данных
                source_node_id_str = self._format_node_id(source_id)
                cached_sid = self._event_source_cache.get(source_node_id_str)

                if cached_sid is not None:
                    self._cache_stats["event_source_hits"] += 1
                    source_db_id = cached_sid
                else:
                    self._cache_stats["event_source_misses"] += 1
                    source_db_id = await self._fetchval(f'''
                        SELECT source_id FROM "{self._schema}".event_sources 
                        WHERE source_node_id = $1
                        LIMIT 1
                    ''', source_node_id_str)

                if source_db_id is None:
                    self.logger.warning(f"No metadata found for source {source_id}")
                    return [], None
                else:
                    # Обновляем кэш
                    self._event_source_cache[source_node_id_str] = source_db_id
            else:
                # Проверяем формат данных
                if len(source_data) == 4:
                    #self.logger.debug(f"read_event_history: source_data: {source_data}")
                    period, count, source_db_id, event_ids = source_data
                    #self.logger.debug(f"read_event_history: using cached source_db_id: {source_db_id}")
                    
                elif len(source_data) == 3:
                    # Старый формат для переменных: (period, count, variable_id)
                    self.logger.warning(f"Source {source_id} is registered as variable, not event source")
                    return [], None
                else:
                    self.logger.warning(f"Unexpected data format for source {source_id}: {source_data}")
                    return [], None

            # Запрос к единой таблице событий
            select_sql = f'''
                SELECT event_timestamp, event_type_id, event_data
                FROM "{self._schema}".events_history
                WHERE source_id = $1 AND event_timestamp BETWEEN $2 AND $3
                ORDER BY event_timestamp {order}
                LIMIT $4
            '''

            rows = await self._fetch(select_sql, source_db_id, start_time, end_time, limit)
            #self.logger.debug(f"read_event_history: query: {select_sql}")
            #self.logger.debug(f"read_event_history: params: source_db_id={source_db_id}, start_time={start_time}, end_time={end_time}, limit={limit}")
            #self.logger.debug(f"read_event_history: {len(rows)} rows")
            # Преобразуем в события
            results = []
            for row in rows:
                data = row['event_data']
                if isinstance(data, str):
                    data = json.loads(data)
                values = self._binary_map_to_event_values(data)
                #payload = {"Time": row["event_timestamp"], "EventType": row["event_type_id"], **values}
                try:
                    #self.logger.debug(f"read_event_history: event: {values}")
                    event = Event.from_field_dict(values)
                    results.append(event)
                except Exception as e:
                    # Фоллбэк, если from_field_dict недоступен у конкретной реализации Event
                    self.logger.debug(f"read_event_history fallback: {e}")
                    self.logger.debug(f"read_event_history fallback: event: {values}")
                    #results.append(Event(**values))

            # Применяем EventFilter для фильтрации событий
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug(
                    "read_event_history: applying event filter %s to %d events",
                    "present" if evfilter else "absent",
                    len(results),
                )
            results = apply_event_filter(results, evfilter)
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug(
                    "read_event_history: after filter %d events (from %d)",
                    len(results),
                    len(rows),
                )

            # Определяем время продолжения
            cont = None
            if len(results) == limit and len(rows) > 0:
                cont = rows[-1]['event_timestamp']

            return results, cont
        except Exception as e:
            self.logger.error(f"Failed to read event history for {source_id}: {e}")
            return [], None

    @staticmethod
    def _get_bounds(
        start: Optional[datetime], 
        end: Optional[datetime], 
        nb_values: Optional[int]
    ) -> Tuple[datetime, datetime, str, int]:
        """
        Определение границ и параметров для запроса истории.
        
        Args:
            start: Начальное время
            end: Конечное время
            nb_values: Количество значений
            
        Returns:
            Кортеж (начальное время, конечное время, порядок сортировки, лимит)
        """
        order = "ASC"
        if start is None or start == ua.get_win_epoch():
            order = "DESC"
            start = ua.get_win_epoch()
        if end is None or end == ua.get_win_epoch():
            end = datetime.now(timezone.utc) + timedelta(days=1)
        if start < end:
            start_time = start
            end_time = end
        else:
            order = "DESC"
            start_time = end
            end_time = start
        limit = nb_values if nb_values else 10000
        
        return start_time, end_time, order, limit

    async def execute_sql_delete(
        self, 
        condition: str, 
        args: Iterable, 
        table: str, 
        node_id: ua.NodeId
    ) -> None:
        """
        Выполнение SQL запроса удаления данных.
        
        Args:
            condition: SQL условие для удаления
            args: Аргументы для SQL запроса
            table: Имя таблицы (variables_history или events_history)
            node_id: Идентификатор узла для логирования
        """
        try:
            # Определяем полное имя таблицы со схемой
            if table == "variables_history":
                full_table = f'"{self._schema}".variables_history'
            elif table == "events_history":
                full_table = f'"{self._schema}".events_history'
            else:
                # Для обратной совместимости
                full_table = f'"{self._schema}".{table}'
            
            await self._execute(f'DELETE FROM {full_table} WHERE {condition}', *args)
        except Exception as e:
            self.logger.error(f"Failed to delete data for {node_id}: {e}")

    async def read_last_value(self, node_id: ua.NodeId) -> Optional[ua.DataValue]:
        """
        Быстрое получение последнего сохраненного значения переменной.
        
        Args:
            node_id: Идентификатор узла OPC UA
            
        Returns:
            Последнее значение или None если не найдено
        """
        try:
            # Получаем variable_id
            node_data = self._datachanges_period.get(node_id)
            if node_data is None:
                # Если mapping не найден, пробуем получить variable_id из кэша по node_id_str
                node_id_str = self._format_node_id(node_id)
                cached_vid = self._variable_metadata_cache.get(node_id_str)
                if cached_vid is not None:
                    self._cache_stats["variable_metadata_hits"] += 1
                    variable_id = cached_vid
                else:
                    self._cache_stats["variable_metadata_misses"] += 1
                    # Если в кэше нет, получаем variable_id из базы данных
                    variable_id = await self._fetchval(f'''
                        SELECT variable_id FROM "{self._schema}".variable_metadata
                        WHERE node_id = $1
                        LIMIT 1
                    ''', node_id_str)

                if variable_id is not None:
                    # Обновляем кэш
                    self._variable_metadata_cache[node_id_str] = variable_id
            else:
                # Проверяем формат данных
                if len(node_data) == 3:
                    period, count, variable_id = node_data
                elif len(node_data) == 4:
                    # Формат для событий: (period, count, source_db_id, event_ids)
                    self.logger.warning(f"Node {node_id} is registered as event source, not variable")
                    return None
                else:
                    self.logger.warning(f"Unexpected data format for node {node_id}: {node_data}")
                    return None
            
            if variable_id is None:
                return None

            # Пытаемся получить из in-memory кэша последних значений
            if self._history_last_values_cache_enabled:
                cached = self._last_values_cache.get(variable_id)
                if cached is not None:
                    self._cache_stats["last_values_memory_hits"] += 1
                    return cached
                else:
                    self._cache_stats["last_values_memory_misses"] += 1

            # Если в памяти нет, читаем из таблицы кэша в БД
            row = await self._fetchrow(f'''
                SELECT sourcetimestamp, servertimestamp, statuscode, varianttype, variantbinary
                FROM "{self._schema}".variables_last_value
                WHERE variable_id = $1
            ''', variable_id)
            
            if row is not None:
                # Попали в таблицу-кэш последних значений
                self._cache_stats["last_values_table_hits"] += 1
                # Преобразуем в DataValue
                return ua.DataValue(
                    Value=variant_from_binary(Buffer(row['variantbinary'])),
                    StatusCode_=ua.StatusCode(row['statuscode']),
                    SourceTimestamp=row['sourcetimestamp'],
                    ServerTimestamp=row['servertimestamp']
                )
            
            # Fallback: получаем из основной таблицы через покрывающий индекс
            self._cache_stats["last_values_table_misses"] += 1
            row = await self._fetchrow(f'''
                SELECT sourcetimestamp, servertimestamp, statuscode, varianttype
                FROM "{self._schema}".variables_history
                WHERE variable_id = $1
                ORDER BY sourcetimestamp DESC
                LIMIT 1
            ''', variable_id)
            
            if row is not None:
                self._cache_stats["last_values_history_fallbacks"] += 1
                # Получаем variantbinary отдельным запросом
                variantbinary_row = await self._fetchrow(f'''
                    SELECT variantbinary
                    FROM "{self._schema}".variables_history
                    WHERE variable_id = $1 AND sourcetimestamp = $2
                    LIMIT 1
                ''', variable_id, row['sourcetimestamp'])
                
                if variantbinary_row is not None:
                    variantbinary = variantbinary_row['variantbinary']
                else:
                    return None
            
            if row is not None:
                # Преобразуем в DataValue
                return ua.DataValue(
                    Value=variant_from_binary(Buffer(variantbinary)),
                    StatusCode_=ua.StatusCode(row['statuscode']),
                    SourceTimestamp=row['sourcetimestamp'],
                    ServerTimestamp=row['servertimestamp']
                )
            
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to read last value for {node_id}: {e}")
            return None

    async def read_last_values(
        self,
        node_ids: List[ua.NodeId],
        history_lookback: Optional[timedelta] = None,
    ) -> dict:
        """
        Быстрое получение последних сохраненных значений для списка переменных.

        Args:
            node_ids: Список идентификаторов узлов OPC UA
            history_lookback: Ограничение фоллбэк-чтения из variables_history
                последними history_lookback времени. Позволяет TimescaleDB
                исключить старые чанки: без ограничения переменные, у которых
                нет данных вообще, заставляют пробегать индексы всех чанков.
                None — без ограничения (полная история).

        Returns:
            Словарь {node_id: DataValue} или {node_id: None} для отсутствующих
        """
        result = {}
        
        try:
            # Получаем variable_id для всех узлов
            variable_ids = []
            node_to_variable = {}
            
            for node_id in node_ids:
                node_data = self._datachanges_period.get(node_id)
                if node_data is None:
                    # Если mapping не найден, пробуем получить variable_id из кэша по node_id_str
                    node_id_str = self._format_node_id(node_id)
                    cached_vid = self._variable_metadata_cache.get(node_id_str)
                    if cached_vid is not None:
                        self._cache_stats["variable_metadata_hits"] += 1
                        variable_id = cached_vid
                    else:
                        self._cache_stats["variable_metadata_misses"] += 1
                        # Если в кэше нет, получаем variable_id из базы данных
                        variable_id = await self._fetchval(f'''
                            SELECT variable_id FROM "{self._schema}".variable_metadata
                            WHERE node_id = $1
                            LIMIT 1
                        ''', node_id_str)

                    if variable_id is not None:
                        # Обновляем кэш
                        self._variable_metadata_cache[node_id_str] = variable_id
                else:
                    # Проверяем формат данных
                    if len(node_data) == 3:
                        period, count, variable_id = node_data
                    elif len(node_data) == 4:
                        # Формат для событий: (period, count, source_db_id, event_ids)
                        result[node_id] = None
                        continue
                    else:
                        self.logger.warning(f"Unexpected data format for node {node_id}: {node_data}")
                        result[node_id] = None
                        continue
                
                if variable_id is not None:
                    variable_ids.append(variable_id)
                    node_to_variable[variable_id] = node_id
                else:
                    result[node_id] = None

            if not variable_ids:
                return result

            # Сначала пробуем получить значения из in-memory кэша
            remaining_variable_ids: List[int] = []
            if self._history_last_values_cache_enabled:
                for vid in variable_ids:
                    cached = self._last_values_cache.get(vid)
                    if cached is not None:
                        self._cache_stats["last_values_memory_hits"] += 1
                        node_id = node_to_variable[vid]
                        result[node_id] = cached
                    else:
                        self._cache_stats["last_values_memory_misses"] += 1
                        remaining_variable_ids.append(vid)
            else:
                remaining_variable_ids = list(variable_ids)

            if not remaining_variable_ids:
                return result

            # Получаем отсутствующие значения из таблицы кэша в БД батчем
            rows = await self._fetch(f'''
                SELECT variable_id, sourcetimestamp, servertimestamp, statuscode, varianttype, variantbinary
                FROM "{self._schema}".variables_last_value
                WHERE variable_id = ANY($1)
            ''', remaining_variable_ids)

            # Обрабатываем результаты из таблицы кэша
            cached_variable_ids = set()
            for row in rows:
                variable_id = row['variable_id']
                node_id = node_to_variable[variable_id]
                cached_variable_ids.add(variable_id)
                
                dv = ua.DataValue(
                    Value=variant_from_binary(Buffer(row['variantbinary'])),
                    StatusCode_=ua.StatusCode(row['statuscode']),
                    SourceTimestamp=row['sourcetimestamp'],
                    ServerTimestamp=row['servertimestamp']
                )
                result[node_id] = dv
                self._update_last_values_cache(variable_id, dv)
                self._cache_stats["last_values_table_hits"] += 1
            
            # Fallback для узлов, которых нет ни в памяти, ни в таблице кэша.
            # LATERAL top-1 на каждый variable_id: планировщик идёт по индексу
            # (variable_id, sourcetimestamp DESC) от новых чанков к старым и
            # останавливается на первом значении. DISTINCT ON ... ORDER BY на
            # hypertable с большим числом чанков деградирует до слияния индексов
            # всех чанков (секунды на вызов).
            missing_variable_ids = [vid for vid in remaining_variable_ids if vid not in cached_variable_ids]
            if missing_variable_ids:
                if history_lookback is not None:
                    since = datetime.now(timezone.utc) - history_lookback
                    fallback_rows = await self._fetch(f'''
                        SELECT v.variable_id, h.sourcetimestamp, h.servertimestamp,
                               h.statuscode, h.varianttype, h.variantbinary
                        FROM unnest($1::bigint[]) AS v(variable_id)
                        CROSS JOIN LATERAL (
                            SELECT sourcetimestamp, servertimestamp, statuscode, varianttype, variantbinary
                            FROM "{self._schema}".variables_history
                            WHERE variable_id = v.variable_id
                              AND sourcetimestamp >= $2
                            ORDER BY sourcetimestamp DESC
                            LIMIT 1
                        ) h
                    ''', missing_variable_ids, since)
                else:
                    fallback_rows = await self._fetch(f'''
                        SELECT v.variable_id, h.sourcetimestamp, h.servertimestamp,
                               h.statuscode, h.varianttype, h.variantbinary
                        FROM unnest($1::bigint[]) AS v(variable_id)
                        CROSS JOIN LATERAL (
                            SELECT sourcetimestamp, servertimestamp, statuscode, varianttype, variantbinary
                            FROM "{self._schema}".variables_history
                            WHERE variable_id = v.variable_id
                            ORDER BY sourcetimestamp DESC
                            LIMIT 1
                        ) h
                    ''', missing_variable_ids)

                if fallback_rows:
                    self._cache_stats["last_values_history_fallbacks"] += len(fallback_rows)
                    # Самозалечивание: найденное фоллбэком фиксируем в таблице-кэше
                    # variables_last_value, чтобы при следующих чтениях (и рестартах)
                    # фоллбэк по истории для этих переменных больше не требовался
                    try:
                        await self._execute(f'''
                            INSERT INTO "{self._schema}".variables_last_value
                            (variable_id, sourcetimestamp, servertimestamp, statuscode, varianttype, variantbinary)
                            SELECT * FROM unnest(
                                $1::bigint[], $2::timestamptz[], $3::timestamptz[],
                                $4::integer[], $5::integer[], $6::bytea[]
                            )
                            ON CONFLICT (variable_id) DO UPDATE
                                SET sourcetimestamp = EXCLUDED.sourcetimestamp,
                                    servertimestamp = EXCLUDED.servertimestamp,
                                    statuscode = EXCLUDED.statuscode,
                                    varianttype = EXCLUDED.varianttype,
                                    variantbinary = EXCLUDED.variantbinary,
                                    is_seed = FALSE,
                                    updated_at = NOW()
                                WHERE "{self._schema}".variables_last_value.is_seed
                                   OR "{self._schema}".variables_last_value.sourcetimestamp <= EXCLUDED.sourcetimestamp
                        ''',
                            [r['variable_id'] for r in fallback_rows],
                            [r['sourcetimestamp'] for r in fallback_rows],
                            [r['servertimestamp'] for r in fallback_rows],
                            [r['statuscode'] for r in fallback_rows],
                            [r['varianttype'] for r in fallback_rows],
                            [r['variantbinary'] for r in fallback_rows],
                        )
                    except Exception as e:
                        self.logger.debug(f"Failed to backfill variables_last_value from fallback: {e}")

                for row in fallback_rows:
                    variable_id = row['variable_id']
                    node_id = node_to_variable[variable_id]
                    dv = ua.DataValue(
                        Value=variant_from_binary(Buffer(row['variantbinary'])),
                        StatusCode_=ua.StatusCode(row['statuscode']),
                        SourceTimestamp=row['sourcetimestamp'],
                        ServerTimestamp=row['servertimestamp']
                    )
                    result[node_id] = dv
                    self._update_last_values_cache(variable_id, dv)
            
            # Заполняем None для узлов, которых вообще нет в истории
            for node_id in node_ids:
                if node_id not in result:
                    result[node_id] = None
            
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to read last values: {e}")
            # Возвращаем None для всех узлов при ошибке
            return {node_id: None for node_id in node_ids}

    def _resolve_variable_id_cached(self, node_id: ua.NodeId) -> Optional[int]:
        """variable_id из mapping историзации или кэша метаданных, без обращения к БД."""
        node_data = self._datachanges_period.get(node_id)
        if node_data is not None and len(node_data) == 3:
            return node_data[2]
        return self._variable_metadata_cache.get(self._format_node_id(node_id))

    async def seed_last_values(self, items: List[Tuple[ua.NodeId, ua.DataValue]]) -> int:
        """
        Гарантирует строку в variables_last_value для каждой переданной переменной:
        вставляет значение по умолчанию с is_seed=TRUE, НЕ трогая существующие
        строки (ON CONFLICT DO NOTHING). Вместе с backfill_last_values поддерживает
        инвариант «у каждой зарегистрированной переменной есть строка последнего
        значения», благодаря которому чтение последних значений никогда не
        обращается к таблице истории.

        Args:
            items: Пары (node_id, DataValue с текущим/дефолтным значением узла)

        Returns:
            Число вставленных строк-сидов.
        """
        now = datetime.now(timezone.utc)
        vids: List[int] = []
        source_ts: List[datetime] = []
        server_ts: List[datetime] = []
        statuscodes: List[int] = []
        varianttypes: List[int] = []
        binaries: List[bytes] = []
        seen: set = set()
        dv_by_vid: Dict[int, ua.DataValue] = {}
        for node_id, dv in items:
            variable_id = self._resolve_variable_id_cached(node_id)
            if variable_id is None or variable_id in seen:
                continue
            try:
                variant = getattr(dv, 'Value', None) if dv is not None else None
                if variant is None:
                    variant = ua.Variant(None)
                binary = variant_to_binary(variant)
            except Exception as e:
                self.logger.debug(f"seed_last_values: cannot serialize value for {node_id}: {e}")
                continue
            seen.add(variable_id)
            vids.append(variable_id)
            source_ts.append(getattr(dv, 'SourceTimestamp', None) or now)
            server_ts.append(getattr(dv, 'ServerTimestamp', None) or now)
            sc = getattr(dv, 'StatusCode', None)
            statuscodes.append(sc.value if sc is not None else 0)
            varianttypes.append(variant.VariantType.value)
            binaries.append(binary)
            dv_by_vid[variable_id] = dv

        if not vids:
            return 0

        rows = await self._fetch(f'''
            INSERT INTO "{self._schema}".variables_last_value
                (variable_id, sourcetimestamp, servertimestamp, statuscode, varianttype, variantbinary, is_seed)
            SELECT u.variable_id, u.sourcetimestamp, u.servertimestamp,
                   u.statuscode, u.varianttype, u.variantbinary, TRUE
            FROM unnest(
                $1::bigint[], $2::timestamptz[], $3::timestamptz[],
                $4::integer[], $5::integer[], $6::bytea[]
            ) AS u(variable_id, sourcetimestamp, servertimestamp, statuscode, varianttype, variantbinary)
            ON CONFLICT (variable_id) DO NOTHING
            RETURNING variable_id
        ''', vids, source_ts, server_ts, statuscodes, varianttypes, binaries)

        inserted = len(rows)
        for row in rows:
            dv = dv_by_vid.get(row['variable_id'])
            if dv is not None:
                self._update_last_values_cache(row['variable_id'], dv)
        self.logger.info(
            "seed_last_values: %d of %d rows seeded (existing preserved)",
            inserted, len(vids),
        )
        return inserted

    async def backfill_last_values(
        self,
        *,
        chunk_size: int = 1000,
        pause_sec: float = 0.5,
        query_timeout_sec: float = 120.0,
    ) -> dict:
        """
        Фоновая идемпотентная сверка variables_last_value с историей:
          - зарегистрированным переменным без строки добавляется последнее
            значение из истории (если оно есть);
          - строки-сиды (is_seed=TRUE) замещаются реальным последним значением
            из истории; сиды без истории помечаются сверенными (is_seed=FALSE).

        После первого полного прохода кандидатов не остаётся и вызов
        завершается мгновенно. Предназначен для запуска фоновой задачей
        после старта сервера.

        Returns:
            Статистика {'candidates', 'restored_from_history', 'confirmed_defaults', 'errors'}
        """
        await self._ensure_pool()
        rows = await self._fetch(f'''
            SELECT m.variable_id
            FROM "{self._schema}".variable_metadata m
            LEFT JOIN "{self._schema}".variables_last_value lv ON lv.variable_id = m.variable_id
            WHERE lv.variable_id IS NULL OR lv.is_seed
            ORDER BY m.variable_id
        ''')
        candidate_ids = [r['variable_id'] for r in rows]
        stats = {
            "candidates": len(candidate_ids),
            "restored_from_history": 0,
            "confirmed_defaults": 0,
            "errors": 0,
        }
        if not candidate_ids:
            return stats
        self.logger.info("Backfill variables_last_value: %d candidates", len(candidate_ids))

        chunk_size = max(1, int(chunk_size))
        for i in range(0, len(candidate_ids), chunk_size):
            chunk = candidate_ids[i:i + chunk_size]
            try:
                pool = self._pool
                async def _op():
                    async with pool.acquire(timeout=query_timeout_sec) as conn:
                        found = await conn.fetch(f'''
                            SELECT v.variable_id, h.sourcetimestamp, h.servertimestamp,
                                   h.statuscode, h.varianttype, h.variantbinary
                            FROM unnest($1::bigint[]) AS v(variable_id)
                            CROSS JOIN LATERAL (
                                SELECT sourcetimestamp, servertimestamp, statuscode, varianttype, variantbinary
                                FROM "{self._schema}".variables_history
                                WHERE variable_id = v.variable_id
                                ORDER BY sourcetimestamp DESC
                                LIMIT 1
                            ) h
                        ''', chunk, timeout=query_timeout_sec)
                        if found:
                            await conn.execute(f'''
                                INSERT INTO "{self._schema}".variables_last_value
                                    (variable_id, sourcetimestamp, servertimestamp, statuscode, varianttype, variantbinary)
                                SELECT * FROM unnest(
                                    $1::bigint[], $2::timestamptz[], $3::timestamptz[],
                                    $4::integer[], $5::integer[], $6::bytea[]
                                )
                                ON CONFLICT (variable_id) DO UPDATE
                                    SET sourcetimestamp = EXCLUDED.sourcetimestamp,
                                        servertimestamp = EXCLUDED.servertimestamp,
                                        statuscode = EXCLUDED.statuscode,
                                        varianttype = EXCLUDED.varianttype,
                                        variantbinary = EXCLUDED.variantbinary,
                                        is_seed = FALSE,
                                        updated_at = NOW()
                                    WHERE "{self._schema}".variables_last_value.is_seed
                                       OR "{self._schema}".variables_last_value.sourcetimestamp <= EXCLUDED.sourcetimestamp
                            ''',
                                [r['variable_id'] for r in found],
                                [r['sourcetimestamp'] for r in found],
                                [r['servertimestamp'] for r in found],
                                [r['statuscode'] for r in found],
                                [r['varianttype'] for r in found],
                                [r['variantbinary'] for r in found],
                                timeout=query_timeout_sec,
                            )
                        # Сиды, для которых истории не нашлось, считаем сверенными:
                        # их дефолт и есть последнее известное состояние
                        await conn.execute(f'''
                            UPDATE "{self._schema}".variables_last_value
                            SET is_seed = FALSE
                            WHERE variable_id = ANY($1) AND is_seed
                        ''', chunk, timeout=query_timeout_sec)
                        return found
                found = await asyncio.wait_for(_op(), timeout=query_timeout_sec * 2)
                stats["restored_from_history"] += len(found)
                stats["confirmed_defaults"] += len(chunk) - len(found)
            except Exception as e:
                stats["errors"] += 1
                self.logger.warning("Backfill chunk failed (%d ids): %r", len(chunk), e)
            if pause_sec > 0:
                await asyncio.sleep(pause_sec)

        self.logger.info("Backfill variables_last_value done: %s", stats)
        return stats

    async def close(self) -> None:
        """Закрытие модуля историзации"""
        # Останавливаем фоновые буферы записи
        if self._value_write_buffer:
            await self._value_write_buffer.stop()
        if self._event_write_buffer:
            await self._event_write_buffer.stop()

        if self._pool:
            await self._pool.close()
            self.logger.info("HistoryTimescale closed")
