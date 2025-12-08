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
        except asyncio.QueueFull:
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

        try:
            await self._flush_func(batch)
            # Уведомляем ожидающих о завершении
            now = time.time()
            for item in batch:
                fut: Optional[asyncio.Future] = getattr(item, "future", None)
                if fut is not None and not fut.done():
                    fut.set_result(now)
        except Exception as e:
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
    from asyncua.server.history import get_event_properties_from_type_node
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
        self._stop_event = asyncio.Event()
        self._reconnect_task = None
        self._reconnect_min_delay = 1.0
        self._reconnect_max_delay = 30.0
        self._was_healthy = True
        self._db_unavailable_since = None
        self._last_throttled_log_at = None
        # Последнее время, когда мы писали агрегированное сообщение о длительной недоступности БД
        self._last_reconnect_outage_log_at = None
        self._failed_value_saves_counter = 0
        self._failed_event_saves_counter = 0

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

        # Подавление первого уведомления datachange после подписки
        self.suppress_initial_datachange = True
        self._pending_initial_datachange_skip: Dict[str, bool] = {}

        # Буферы для батчевой записи истории
        self._value_write_buffer: Optional[HistoryWriteBuffer] = None
        self._event_write_buffer: Optional[HistoryWriteBuffer] = None

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

            self.logger.info("HistoryTimescale initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize HistoryTimescale: {e}")
            raise

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

    async def _ensure_pool(self) -> None:
        """
        Гарантирует наличие рабочего пула соединений.

        Пул считается непригодным, если он закрыт или находится в процессе закрытия.
        """
        if self._pool and not self._pool._closed and not getattr(self._pool, "_closing", False):
            return
        async with self._pool_lock:
            if self._pool and not self._pool._closed and not getattr(self._pool, "_closing", False):
                return
            pool_params = self._build_pool_params()
            self._pool = await asyncpg.create_pool(**pool_params)
            self.logger.info("Connection pool created")

    async def _is_pool_healthy(self) -> bool:
        try:
            await self._ensure_pool()
            async with self._pool.acquire() as conn:
                val = await conn.fetchval('SELECT 1')
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
                    await self._force_reconnect()
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
        if self._reconnect_task and not self._reconnect_task.done():
            try:
                await self._reconnect_task
            except Exception:
                pass
        self._reconnect_task = None
        if self._pool:
            try:
                await self._pool.close()
            finally:
                self._pool = None
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
        try:
            async with self._pool.acquire() as conn:
                result = await conn.execute(query, *args)
                self._log_connection_restored_if_needed()
                return result
        except Exception as e:
            # Ошибка выполнения запроса или проблемы с соединением — логируем как error
            self.logger.error(f"Execute failed, will try to reconnect and retry: {e}")
            # Попытка принудительного переподключения; при неудаче _force_reconnect сам залогирует critical и выбросит исключение
            await self._force_reconnect()
            # Вторая попытка выполнения запроса
            try:
                async with self._pool.acquire() as conn:
                    result = await conn.execute(query, *args)
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
        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(query, *args)
                self._log_connection_restored_if_needed()
                return rows
        except Exception as e:
            self.logger.error(f"Fetch failed, will try to reconnect and retry: {e}")
            await self._force_reconnect()
            try:
                async with self._pool.acquire() as conn:
                    rows = await conn.fetch(query, *args)
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
        try:
            async with self._pool.acquire() as conn:
                value = await conn.fetchval(query, *args)
                self._log_connection_restored_if_needed()
                return value
        except Exception as e:
            self.logger.error(f"Fetchval failed, will try to reconnect and retry: {e}")
            await self._force_reconnect()
            try:
                async with self._pool.acquire() as conn:
                    value = await conn.fetchval(query, *args)
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
        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(query, *args)
                self._log_connection_restored_if_needed()
                return row
        except Exception as e:
            self.logger.error(f"Fetchrow failed, will try to reconnect and retry: {e}")
            await self._force_reconnect()
            try:
                async with self._pool.acquire() as conn:
                    row = await conn.fetchrow(query, *args)
                    self._log_connection_restored_if_needed()
                    return row
            except Exception as e2:
                self.logger.error(f"Fetchrow failed after reconnect: {e2}")
                raise

    async def _force_reconnect(self) -> None:
        """
        Полное пересоздание пула соединений.

        Старый пул закрывается синхронно, чтобы избежать гонок состояний
        внутри asyncpg (ошибки вида «another operation is in progress»).
        """
        old_pool: Optional[asyncpg.Pool] = None
        async with self._pool_lock:
            try:
                if self._pool:
                    old_pool = self._pool
                # Обнуляем ссылку на пул: новые операции будут создавать
                # соединения только после успешного создания нового пула.
                self._pool = None
            except Exception as e:
                self.logger.error("Error while preparing to recreate pool: %r", e, exc_info=True)
                raise

        # Закрываем старый пул уже вне блокировки, но синхронно
        if old_pool is not None:
            try:
                await old_pool.close()
            except Exception as e:
                self.logger.warning("Error closing old PostgreSQL pool during reconnect: %r", e, exc_info=True)

        # Создаём новый пул под блокировкой, без использования _ensure_pool,
        # чтобы избежать рекурсивного захвата замка.
        async with self._pool_lock:
            try:
                pool_params = self._build_pool_params()
                self._pool = await asyncpg.create_pool(**pool_params)
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
            
            # Таблица кэша последних значений переменных
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
            try:
                async with self._pool.acquire() as conn:
                    async with conn.transaction():
                        await conn.executemany(
                            f'INSERT INTO "{self._schema}".variables_history '
                            f'(variable_id, servertimestamp, sourcetimestamp, statuscode, value, varianttype, variantbinary) '
                            f'VALUES ($1, $2, $3, $4, $5, $6, $7) '
                            f'ON CONFLICT (variable_id, sourcetimestamp) DO NOTHING',
                            history_params,
                        )

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
                                    updated_at = NOW()
                                WHERE "{self._schema}".variables_last_value.sourcetimestamp <= EXCLUDED.sourcetimestamp
                            ''',
                            last_value_params,
                        )

                # Успешная запись батча — считаем, что соединение восстановлено
                self._log_connection_restored_if_needed()

                # Обновляем in-memory кэш последних значений
                for it in items:
                    self._update_last_values_cache(it.variable_id, it.datavalue)
                return
            except Exception as e:
                if attempt == 1:
                    self.logger.error(f"Flush variable batch failed, will try to reconnect and retry: {e}")
                    await self._force_reconnect()
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
            try:
                async with self._pool.acquire() as conn:
                    async with conn.transaction():
                        await conn.executemany(
                            f'INSERT INTO "{self._schema}".events_history '
                            f'(source_id, event_type_id, event_timestamp, event_data) '
                            f'VALUES ($1, $2, $3, $4) '
                            f'ON CONFLICT (source_id, event_timestamp) DO NOTHING',
                            params,
                        )
                return
            except Exception as e:
                if attempt == 1:
                    self.logger.error(f"Flush event batch failed, will try to reconnect and retry: {e}")
                    await self._force_reconnect()
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

            async with self._pool.acquire() as conn:
                # Пытаемся оценить размер таблицы на стороне БД
                try:
                    rel_name = f'{self._schema}.variables_last_value'
                    rel_size = await conn.fetchval(
                        "SELECT pg_total_relation_size($1::regclass)",
                        rel_name,
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

            async with self._pool.acquire() as conn:
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
            # Сохраняем метаданные переменной и получаем variable_id
            variable_id = await self._save_variable_metadata(node_id, period, count)

            # Сохраняем mapping node_id -> variable_id для быстрого доступа
            self._datachanges_period[node_id] = (period, count, variable_id)

            if self.suppress_initial_datachange:
                node_id_str = self._format_node_id(node_id)
                self._pending_initial_datachange_skip[node_id_str] = True

            #self.logger.info(f"Variable node {node_id} registered for historization in unified table (variable_id: {variable_id})")
        except Exception as e:
            self.logger.error(f"Failed to register variable node {node_id}: {e}")
            raise
    
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
            # Получаем поля событий
            ev_fields = await self._get_event_fields(evtypes)
            self._event_fields[source_id] = ev_fields

            # Сохраняем метаданные для каждого типа события и получаем IDs
            event_ids = {}
            for event_type in evtypes:
                source_db_id, event_db_id = await self._save_event_metadata(event_type, source_id, ev_fields, period, count)
                event_ids[event_type] = (source_db_id, event_db_id)

            # Сохраняем mapping source_id -> (period, count, source_db_id, event_ids)
            self._datachanges_period[source_id] = (period, count, source_db_id, event_ids)

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

        node_id_str = self._format_node_id(node_id)

        if self.suppress_initial_datachange:
            # Подавляем первое уведомление после подписки, чтобы не перезаписывать данные из БД
            if self._pending_initial_datachange_skip.pop(node_id_str, False):
                self.logger.debug("save_node_value: suppressed initial datachange for %s", node_id_str)
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
                # Если mapping не найден, пробуем получить variable_id из кэша по node_id_str
                cached_vid = self._variable_metadata_cache.get(node_id_str)
                if cached_vid is not None:
                    self._cache_stats["variable_metadata_hits"] += 1
                    variable_id = cached_vid
                else:
                    self._cache_stats["variable_metadata_misses"] += 1
                    # Если в кэше нет, пробуем получить из базы данных
                    variable_id = await self._fetchval(f'''
                        SELECT variable_id FROM "{self._schema}".variable_metadata
                        WHERE node_id = $1
                        LIMIT 1
                    ''', node_id_str)

                if variable_id is None:
                    # Если метаданные не найдены, создаем их
                    variable_id = await self._save_variable_metadata(node_id, None, 0)

                # Обновляем in-memory mapping и кэш метаданных
                self._datachanges_period[node_id] = (None, 0, variable_id)
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
                            updated_at = NOW()
                        WHERE "{self._schema}".variables_last_value.sourcetimestamp <= EXCLUDED.sourcetimestamp
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

            # Очистка старых данных по периоду хранения
            if period:
                date_limit = datetime.now(timezone.utc) - period
                await self._execute(f'DELETE FROM "{self._schema}".variables_history WHERE variable_id = $1 AND sourcetimestamp < $2', variable_id, date_limit)
            elif count > 0:
                # Удаляем лишние записи по количеству для конкретного узла
                await self._execute(f'''
                    DELETE FROM "{self._schema}".variables_history
                    WHERE variable_id = $1 AND id NOT IN (
                        SELECT id FROM "{self._schema}".variables_history
                        WHERE variable_id = $1
                        ORDER BY sourcetimestamp DESC LIMIT $2
                    )
                ''', variable_id, count)

        except Exception as e:
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

            # Очистка старых данных по периоду хранения
            # Получаем параметры хранения из event_sources
            retention_rows = await self._fetch(f'''
                SELECT retention_period, max_records FROM "{self._schema}".event_sources 
                WHERE source_id = $1
                LIMIT 1
            ''', source_db_id)
            
            if retention_rows:
                retention_period = retention_rows[0]['retention_period']
                max_records = retention_rows[0]['max_records']
                if retention_period:
                    date_limit = datetime.now(timezone.utc) - retention_period
                    await self._execute(f'DELETE FROM "{self._schema}".events_history WHERE source_id = $1 AND event_timestamp < $2', source_db_id, date_limit)
                elif max_records and max_records > 0:
                    # Удаляем лишние записи по количеству для конкретного источника
                    await self._execute(f'''
                        DELETE FROM "{self._schema}".events_history
                        WHERE source_id = $1 AND id NOT IN (
                            SELECT id FROM "{self._schema}".events_history
                            WHERE source_id = $1
                            ORDER BY event_timestamp DESC LIMIT $2
                        )
                    ''', source_db_id, max_records)

        except Exception as e:
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
            results = apply_event_filter(results, evfilter)

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

    async def read_last_values(self, node_ids: List[ua.NodeId]) -> dict:
        """
        Быстрое получение последних сохраненных значений для списка переменных.
        
        Args:
            node_ids: Список идентификаторов узлов OPC UA
            
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
            
            # Fallback для узлов, которых нет ни в памяти, ни в таблице кэша
            missing_variable_ids = [vid for vid in remaining_variable_ids if vid not in cached_variable_ids]
            if missing_variable_ids:
                fallback_rows = await self._fetch(f'''
                    SELECT DISTINCT ON (variable_id) variable_id, sourcetimestamp, servertimestamp, statuscode, varianttype
                    FROM "{self._schema}".variables_history
                    WHERE variable_id = ANY($1)
                    ORDER BY variable_id, sourcetimestamp DESC
                ''', missing_variable_ids)
                
                if fallback_rows:
                    self._cache_stats["last_values_history_fallbacks"] += len(fallback_rows)

                for row in fallback_rows:
                    variable_id = row['variable_id']
                    node_id = node_to_variable[variable_id]
                    
                    # Получаем variantbinary отдельным запросом
                    variantbinary_row = await self._fetchrow(f'''
                        SELECT variantbinary
                        FROM "{self._schema}".variables_history
                        WHERE variable_id = $1 AND sourcetimestamp = $2
                        LIMIT 1
                    ''', variable_id, row['sourcetimestamp'])
                    
                    if variantbinary_row is not None:
                        dv = ua.DataValue(
                            Value=variant_from_binary(Buffer(variantbinary_row['variantbinary'])),
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
