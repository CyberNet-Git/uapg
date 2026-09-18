"""Бэкенд историзации OPC UA на PostgreSQL/TimescaleDB.

``HistoryTimescale`` — реализация ``HistoryStorageInterface`` из asyncua и
единственная точка входа пакета. Сам класс ничего не хранит и не знает SQL: он
разбирает вызовы OPC UA и раскладывает их по слоям — соединение и его надзор
(core), хранение (storage), кодирование значений (codec), семантика чтения и
публикация узлов (opcua).

Публичная поверхность — конструктор, методы и ключи метрик — совпадает с
0.2.15, и это проверяется контрактными тестами против замороженного эталона:
на неё опирается opc-vibro-iot-server.
"""

import asyncio
import importlib.metadata as importlib_metadata
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Coroutine, Dict, Iterable, List, Optional, Tuple, Union

from asyncua import ua
from asyncua.common.events import Event, get_event_properties_from_type_node
from asyncua.server.history import HistoryStorageInterface

from .codec import encode_event_fields, encode_variant, status_code_value, value_text
from .codec.node_id import coerce_node_id, data_type_name, format_node_id, group_key
from .codec.variant import row_to_datavalue
from .core.buffer import HistoryWriteBuffer
from .core.config import (
    DEFAULT_DB_APPLICATION_NAME,
    DEFAULT_FLUSH_TIMEOUT_SEC,
    DEFAULT_WORKER_STALL_TIMEOUT_SEC,
    CacheSettings,
    ConnectionSettings,
    Keepalive,
    StorageSettings,
    Timeouts,
    WriteSettings,
)
from .core.database import Database
from .core.metrics import MetricsRegistry
from .core.secrets import load_connection_config
from .core.sql import validate_identifier
from .core.supervisor import ConnectionSupervisor
from .event_filter import apply_event_filter
from .opcua import nodes as opc_nodes
from .opcua.reads import continuation_point, resolve_window
from .storage.bootstrap import SchemaBootstrap
from .storage.cache import Caches
from .storage.event_search import EventSearchStore
from .storage.events import EventRepository, decode_payload
from .storage.events_config import EventsV2Config
from .storage.items import EventWriteItem, VariableWriteItem
from .storage.migrations import SqlMigrator
from .storage.typed_events import EventSchemaRegistry, TypedEventTables
from .storage.variables import VariableRepository
from .v2.storage_mode import StorageMode, should_read_v2, should_write_v2

__all__ = [
    "HistoryTimescale",
    "HistoryWriteBuffer",
    "VariableWriteItem",
    "EventWriteItem",
    "validate_table_name",
    "DEFAULT_FLUSH_TIMEOUT_SEC",
    "DEFAULT_WORKER_STALL_TIMEOUT_SEC",
    "DEFAULT_DB_APPLICATION_NAME",
]

# Сколько после начала недоступности БД писать каждую неудачную запись
# подробно; дальше — сводкой, чтобы не утопить лог.
DETAILED_FAILURE_LOG_WINDOW = timedelta(minutes=10)
AGGREGATED_FAILURE_LOG_INTERVAL = timedelta(seconds=10)


def validate_table_name(name: str) -> None:
    """Проверить имя таблицы перед подстановкой в SQL."""
    if not re.match(r"^[\w\-]+$", name):
        raise ValueError(f"Invalid table name: {name}")


class HistoryTimescale(HistoryStorageInterface):  # type: ignore[misc]
    """Хранение истории OPC UA в PostgreSQL с TimescaleDB.

    Переменные пишутся в ``variables_history``, события — в ``events_history``;
    обе таблицы — гипертаблицы TimescaleDB. Запись идёт пачками через
    ограниченный буфер, последние значения переменных держатся в памяти и в
    ``variables_last_value``.

    Типизированный поиск событий (слой v2) включается подклассом
    ``HistoryTimescaleV2``; у этого класса он выключен, как и в 0.2.15.
    """

    # Публикуются ли узлы возможностей слоя поиска событий (EventsStorage*).
    _publish_event_capabilities = False

    def __init__(
        self,
        user: str = "postgres",
        password: str = "postmaster",
        database: str = "opcua",
        host: str = "localhost",
        port: int = 5432,
        min_size: int = 1,
        max_size: int = 10,
        schema: str = "public",
        sslmode: Optional[str] = None,
        config_file: Optional[str] = None,
        encrypted_config: Optional[str] = None,
        master_password: Optional[str] = None,
        global_retention_period: Optional[timedelta] = None,
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
        db_pool_create_timeout_sec: float = 30.0,
        db_lock_wait_timeout_sec: float = 60.0,
        db_command_timeout_sec: Optional[float] = 60.0,
        db_tcp_keepalive_idle_sec: int = 30,
        db_tcp_keepalive_interval_sec: int = 10,
        db_tcp_keepalive_count: int = 3,
        db_tcp_user_timeout_sec: float = 60.0,
        history_flush_timeout_sec: float = DEFAULT_FLUSH_TIMEOUT_SEC,
        history_worker_stall_timeout_sec: float = DEFAULT_WORKER_STALL_TIMEOUT_SEC,
        db_application_name: str = DEFAULT_DB_APPLICATION_NAME,
        **kwargs: Any,
    ) -> None:
        self.max_history_data_response_size = 1000
        self.logger = logging.getLogger("uapg.history_timescale")
        # Первое уведомление после подписки несёт значение, которое уже лежит в
        # базе: записывать его повторно — значит перетирать историю.
        self.suppress_initial_datachange = True

        connection = ConnectionSettings.build(
            user=user,
            password=password,
            database=database,
            host=host,
            port=port,
            min_size=min_size,
            max_size=max_size,
            schema=schema,
            sslmode=sslmode,
            application_name=db_application_name,
            extra=kwargs,
        )
        decrypted = load_connection_config(
            config_file=config_file,
            encrypted_config=encrypted_config,
            master_password=master_password,
        )
        if decrypted:
            connection = connection.with_overrides(decrypted)

        self._settings = StorageSettings(
            connection=connection,
            timeouts=Timeouts.build(
                query_sec=db_query_timeout_sec,
                command_sec=db_command_timeout_sec,
                pool_close_sec=db_pool_close_timeout_sec,
                pool_create_sec=db_pool_create_timeout_sec,
                lock_wait_sec=db_lock_wait_timeout_sec,
                flush_sec=history_flush_timeout_sec,
                worker_stall_sec=history_worker_stall_timeout_sec,
            ),
            keepalive=Keepalive.build(
                idle_sec=db_tcp_keepalive_idle_sec,
                interval_sec=db_tcp_keepalive_interval_sec,
                count=db_tcp_keepalive_count,
                user_timeout_sec=db_tcp_user_timeout_sec,
            ),
            write=WriteSettings.build(
                batch_enabled=history_write_batch_enabled,
                max_batch_size=history_write_max_batch_size,
                max_batch_interval_sec=history_write_max_batch_interval_sec,
                queue_max_size=history_write_queue_max_size,
                durability_mode=history_write_durability_mode,
                read_consistency_mode=history_write_read_consistency_mode,
            ),
            cache=CacheSettings.build(
                enabled=history_cache_enabled,
                last_values_enabled=history_last_values_cache_enabled,
                last_values_max_size_mb=history_last_values_cache_max_size_mb,
                last_values_init_batch_size=history_last_values_init_batch_size,
                metadata_enabled=history_metadata_cache_enabled,
                metadata_init_max_rows=history_metadata_cache_init_max_rows,
            ),
            global_retention_period=global_retention_period,
        )
        self._build_components()

        self._events_mode = StorageMode.LEGACY
        self._events_config = EventsV2Config()
        self._v2_ready = False
        self._event_tables: Optional[TypedEventTables] = None
        self._event_registry: Optional[EventSchemaRegistry] = None
        self._event_search: Optional[EventSearchStore] = None

    def _build_components(self) -> None:
        """Собрать слои по текущим настройкам (при создании и смене конфигурации)."""
        schema = validate_identifier(self._settings.schema)
        self._metrics = MetricsRegistry()
        self._caches = Caches(
            self._metrics.cache, last_values_enabled=self._settings.cache.last_values_enabled
        )
        self._db = Database(
            self._settings.connection,
            self._settings.timeouts,
            self._settings.keepalive,
            self._metrics.database,
            logger=self.logger,
        )
        self._supervisor = ConnectionSupervisor(self._db, logger=self.logger)
        self._bootstrap = SchemaBootstrap(self._db, schema, logger=self.logger)
        self._variables = VariableRepository(
            self._db, schema, self._metrics.variables, logger=self.logger
        )
        self._events = EventRepository(self._db, schema, self._metrics.events, logger=self.logger)

        self._initialized = False
        # Узел -> variable_id; источник -> (source_id, {тип события: event_type_id}).
        self._variable_ids: Dict[Any, int] = {}
        self._event_source_ids: Dict[Any, Tuple[int, Dict[Any, int]]] = {}
        self._event_fields: Dict[Any, List[str]] = {}
        self._known_data_types: Dict[int, str] = {}
        self._pending_initial_skip: Dict[str, bool] = {}

        self._value_buffer: Optional[HistoryWriteBuffer[VariableWriteItem]] = None
        self._event_buffer: Optional[HistoryWriteBuffer[EventWriteItem]] = None

        self._settings_nodes: Dict[str, Any] = {}
        self._metrics_nodes: Dict[str, Any] = {}

        self._failure_since: Optional[datetime] = None
        self._failure_log_at: Optional[datetime] = None
        self._failed_saves: Dict[str, int] = {"value": 0, "event": 0}

    def _configure_events(
        self, mode: StorageMode, config: Optional[EventsV2Config] = None
    ) -> None:
        self._events_mode = mode
        self._events_config = config or EventsV2Config()

    @property
    def _schema(self) -> str:
        return self._settings.schema

    # ================================================================== конфигурация

    @classmethod
    def from_config_file(
        cls,
        config_file: str,
        master_password: str,
        min_size: int = 1,
        max_size: int = 10,
    ) -> "HistoryTimescale":
        """Создать бэкенд из зашифрованного файла конфигурации."""
        return cls(
            config_file=config_file,
            master_password=master_password,
            min_size=min_size,
            max_size=max_size,
        )

    @classmethod
    def from_encrypted_config(
        cls,
        encrypted_config: str,
        master_password: str,
        min_size: int = 1,
        max_size: int = 10,
    ) -> "HistoryTimescale":
        """Создать бэкенд из зашифрованной строки конфигурации."""
        return cls(
            encrypted_config=encrypted_config,
            master_password=master_password,
            min_size=min_size,
            max_size=max_size,
        )

    def update_config(
        self,
        config_file: Optional[str] = None,
        encrypted_config: Optional[str] = None,
        master_password: Optional[str] = None,
    ) -> bool:
        """Применить зашифрованную конфигурацию подключения.

        Работает только на остановленном бэкенде: менять параметры под живым пулом
        нельзя. В 0.2.15 метод передавал аргументы не в те позиции и не работал.
        """
        if self._db.is_connected:
            self.logger.warning("Конфигурацию нельзя менять при активном пуле: сначала stop()")
            return False
        decrypted = load_connection_config(
            config_file=config_file,
            encrypted_config=encrypted_config,
            master_password=master_password,
        )
        if not decrypted:
            self.logger.error("Конфигурация не обновлена: расшифровать её не удалось")
            return False
        try:
            self._settings = StorageSettings(
                connection=self._settings.connection.with_overrides(decrypted),
                timeouts=self._settings.timeouts,
                keepalive=self._settings.keepalive,
                write=self._settings.write,
                cache=self._settings.cache,
                global_retention_period=self._settings.global_retention_period,
            )
            self._build_components()
        except Exception as exc:
            self.logger.error("Конфигурация не обновлена: %s", exc)
            return False
        self.logger.info("Конфигурация подключения обновлена")
        return True

    def get_connection_info(self) -> dict:
        connection = self._settings.connection
        return {
            "user": connection.user,
            "host": connection.host,
            "port": connection.port,
            "database": connection.database,
            "schema": connection.schema,
            "min_size": connection.min_size,
            "max_size": connection.max_size,
            "initialized": self._initialized,
        }

    # ================================================================== жизненный цикл

    async def init(self) -> None:
        """Подключиться к БД, привести схему к нужному виду, запустить запись."""
        try:
            await self._db.start()
            if not self._initialized:
                await self._terminate_stale_backends()
                await self._bootstrap.ensure_core_schema(self._settings.global_retention_period)
                self._initialized = True

            if self._events_mode != StorageMode.LEGACY:
                await self._init_event_search()

            if self._settings.cache.metadata_enabled:
                await self._load_metadata_cache()

            if self._settings.write.batch_enabled:
                self._start_buffers()

            if self._settings.cache.last_values_enabled:
                await self._load_last_values_cache()

            self._supervisor.start()
            await self.refresh_history_settings_nodes()
            self.logger.info("Историзация запущена (схема %s)", self._schema)
        except Exception as exc:
            self.logger.error("Историзация не запущена: %s", exc)
            raise

    async def _init_event_search(self) -> None:
        migrator = SqlMigrator(self._db, self._schema, logger=self.logger)
        await migrator.apply_all()
        self._v2_ready = await migrator.detect_v2_ready()
        if not self._v2_ready:
            self.logger.warning("Слой поиска событий не готов: события пишутся по-старому")
            return
        self._event_tables = TypedEventTables(
            self._db, self._schema, self._events_config, logger=self.logger
        )
        self._event_registry = EventSchemaRegistry(
            self._db, self._schema, self._event_tables, logger=self.logger
        )
        self._event_search = EventSearchStore(
            self._db,
            self._schema,
            self._events,
            self._event_tables,
            self._event_registry,
            logger=self.logger,
        )
        self.logger.info("Поиск событий включён (режим %s)", self._events_mode.value)

    def _start_buffers(self) -> None:
        write = self._settings.write
        timeouts = self._settings.timeouts
        if self._value_buffer is None:
            self._value_buffer = HistoryWriteBuffer(
                "variables",
                self._flush_values,
                self._metrics.variables.buffer,
                max_batch_size=write.max_batch_size,
                max_batch_interval_sec=write.max_batch_interval_sec,
                queue_max_size=write.queue_max_size,
                durability_mode=write.durability_mode,
                flush_timeout_sec=timeouts.buffer_flush_sec,
                stall_timeout_sec=timeouts.worker_stall_sec,
                logger=self.logger,
            )
            self._value_buffer.start()
        if self._event_buffer is None:
            self._event_buffer = HistoryWriteBuffer(
                "events",
                self._flush_events,
                self._metrics.events.buffer,
                max_batch_size=write.max_batch_size,
                max_batch_interval_sec=write.max_batch_interval_sec,
                queue_max_size=write.queue_max_size,
                durability_mode=write.durability_mode,
                flush_timeout_sec=timeouts.buffer_flush_sec,
                stall_timeout_sec=timeouts.worker_stall_sec,
                logger=self.logger,
            )
            self._event_buffer.start()

    async def _stop_buffers(self) -> None:
        for buffer in (self._value_buffer, self._event_buffer):
            if buffer is None:
                continue
            try:
                await buffer.stop()
            except Exception as exc:
                self.logger.warning("Буфер записи остановлен с ошибкой: %r", exc)
        self._value_buffer = None
        self._event_buffer = None

    async def stop(self) -> None:
        """Дописать очереди, остановить надзор и закрыть пул."""
        await self._stop_buffers()
        await self._supervisor.stop()
        await self._db.stop()
        self.logger.info("Историзация остановлена")

    async def close(self) -> None:
        """Остановить запись и закрыть пул (без остановки надзора, как в 0.2.15)."""
        await self._stop_buffers()
        await self._supervisor.stop()
        await self._db.stop()

    async def _terminate_stale_backends(self) -> None:
        """Снять зависшие сессии своего же приложения.

        После SIGKILL контейнера старый backend может часами держать INSERT и
        блокировать CREATE INDEX и новые записи. Отличаем свои сессии по
        application_name; своё соединение и чужие приложения не трогаем.
        """
        app_name = self._settings.connection.application_name
        try:
            rows = await self._db.fetch(
                """
                SELECT pid FROM pg_stat_activity
                WHERE application_name = $1
                  AND pid <> pg_backend_pid()
                  AND datname = current_database()
                """,
                app_name,
            )
        except Exception as exc:
            self.logger.warning("Список зависших сессий не получен: %s", exc)
            return

        # Собственные соединения пула тоже носят это имя — их не трогаем.
        own = await self._own_backend_pids()
        for row in rows:
            pid = row["pid"]
            if pid in own:
                continue
            try:
                await self._db.execute("SELECT pg_terminate_backend($1)", pid)
                self.logger.warning("Снята зависшая сессия pid=%s (%s)", pid, app_name)
            except Exception as exc:
                self.logger.warning("Сессию pid=%s снять не удалось: %s", pid, exc)

    async def _own_backend_pids(self) -> set:
        try:
            pool = self._db._pool
            if pool is None:
                return set()
            pids = set()
            for holder in getattr(pool, "_holders", []):
                connection = getattr(holder, "_con", None)
                if connection is not None and not connection.is_closed():
                    pids.add(connection.get_server_pid())
            return pids
        except Exception:
            return set()

    async def _load_metadata_cache(self) -> None:
        try:
            limit = max(1, self._settings.cache.metadata_init_max_rows)
            mapping = await self._variables.load_metadata_cache(limit)
            self._caches.variables.update(mapping)
            self.logger.info("Кэш метаданных: %d переменных (лимит %d)", len(mapping), limit)
        except Exception as exc:
            self.logger.error("Кэш метаданных не загружен: %s", exc)

    async def _load_last_values_cache(self) -> None:
        try:
            cache = self._settings.cache
            values = await self._variables.iter_last_values(
                cache.last_values_init_batch_size,
                max_bytes=cache.last_values_max_size_mb * 1024 * 1024,
            )
            self._caches.last_values.update(values)
            self.logger.info("Кэш последних значений: %d значений", len(values))
        except Exception as exc:
            self.logger.error("Кэш последних значений не загружен: %s", exc)

    # ================================================================== регистрация

    def _effective_retention(self, requested: Optional[timedelta]) -> Optional[timedelta]:
        """Период хранения узла не может превышать глобальный."""
        limit = self._settings.global_retention_period
        if limit is None:
            return requested
        if requested is None:
            return limit
        return requested if requested < limit else limit

    async def new_historized_node(
        self,
        node_id: ua.NodeId,
        period: Optional[timedelta],
        count: int = 0,
    ) -> None:
        """Зарегистрировать переменную для историзации."""
        try:
            node_id_str = format_node_id(node_id)
            variable_id = self._caches.variables.peek(node_id_str)
            if variable_id is None:
                variable_id = await self._variables.ensure_metadata(
                    node_id_str,
                    retention_period=self._effective_retention(period),
                    max_records=count,
                )
                self._caches.variables.put(node_id_str, variable_id)
            self._variable_ids[node_id] = variable_id
            if self.suppress_initial_datachange:
                self._pending_initial_skip[node_id_str] = True
        except Exception as exc:
            self.logger.error("Переменная %s не зарегистрирована: %s", node_id, exc)
            raise

    async def new_historized_nodes(
        self,
        node_ids: List[ua.NodeId],
        period: Optional[timedelta],
        count: int = 0,
    ) -> None:
        """Зарегистрировать переменные одним запросом вместо запроса на узел."""
        pairs: List[Tuple[Any, str]] = []
        seen = set()
        for node_id in node_ids:
            node_id_str = format_node_id(node_id)
            if node_id_str not in seen:
                seen.add(node_id_str)
                pairs.append((node_id, node_id_str))

        missing = [key for _, key in pairs if key not in self._caches.variables]
        if missing:
            mapping = await self._variables.ensure_metadata_many(
                missing, retention_period=self._effective_retention(period), max_records=count
            )
            self._caches.variables.update(mapping)

        fallback = []
        for node_id, node_id_str in pairs:
            variable_id = self._caches.variables.peek(node_id_str)
            if variable_id is None:
                fallback.append(node_id)
                continue
            self._variable_ids[node_id] = variable_id
            if self.suppress_initial_datachange:
                self._pending_initial_skip[node_id_str] = True

        for node_id in fallback:
            await self.new_historized_node(node_id, period, count)

        self.logger.info(
            "Зарегистрировано %d переменных (новых в БД %d, поштучно %d)",
            len(pairs),
            len(missing),
            len(fallback),
        )

    async def new_historized_event(
        self,
        source_id: ua.NodeId,
        evtypes: List[Any],
        period: Optional[timedelta],
        count: int = 0,
    ) -> None:
        """Зарегистрировать источник событий и типы его событий."""
        try:
            raw_types = list(evtypes)
            source = coerce_node_id(source_id)
            # Поля читаются из asyncua Node до приведения к NodeId.
            fields = await self._get_event_fields(raw_types)
            self._event_fields[source] = fields

            source_db_id = await self._events.ensure_source(
                format_node_id(source),
                retention_period=self._effective_retention(period),
                max_records=count,
            )
            self._caches.event_sources.put(format_node_id(source), source_db_id)

            type_ids: Dict[Any, int] = {}
            for raw_type in raw_types:
                type_node = coerce_node_id(raw_type)
                type_name = format_node_id(type_node)
                type_db_id = await self._events.ensure_type(type_name)
                self._caches.event_types.put(type_name, type_db_id)
                type_ids[type_node] = type_db_id
                await self._sync_event_type(type_db_id, type_node, fields)

            self._event_source_ids[source] = (source_db_id, type_ids)
            self.logger.info("Источник событий %s зарегистрирован (id %d)", source, source_db_id)
        except Exception as exc:
            self.logger.error("Источник событий %s не зарегистрирован: %s", source_id, exc)
            raise

    async def _sync_event_type(
        self, type_db_id: int, type_node: ua.NodeId, fields: List[str]
    ) -> None:
        if (
            not should_write_v2(self._events_mode)
            or self._event_registry is None
            or self._event_search is None
        ):
            return
        table, version = await self._event_registry.sync_event_type(
            type_db_id, type_node, self._event_registry.describe_fields(fields)
        )
        self._event_search.remember_type(type_db_id, table, version)

    async def _get_event_fields(self, evtypes: List[Any]) -> List[str]:
        """Имена полей событий по узлам типов.

        Поля берутся из узла asyncua; по голому NodeId их не узнать — нужен
        сервер, поэтому такие типы пропускаются с предупреждением.
        """
        properties: List[Any] = []
        for event_type in evtypes:
            if isinstance(event_type, ua.NodeId):
                self.logger.warning(
                    "Поля события %s не определить по NodeId: нужен узел сервера", event_type
                )
                continue
            properties.extend(await get_event_properties_from_type_node(event_type) or [])
        names = []
        for prop in set(properties):
            names.append((await prop.read_display_name()).Text)
        return names

    # ================================================================== запись

    async def save_node_value(self, node_id: ua.NodeId, datavalue: ua.DataValue) -> None:
        """Записать значение переменной."""
        self._metrics.variables.record_call()
        node_id_str = format_node_id(node_id)

        if self.suppress_initial_datachange:
            if self._pending_initial_skip.pop(node_id_str, False):
                return
        else:
            self._pending_initial_skip.pop(node_id_str, None)

        try:
            if node_id in self._event_source_ids:
                self.logger.warning("Узел %s зарегистрирован как источник событий", node_id)
                return
            variable_id = await self._resolve_variable_id(node_id, create=True)
            assert variable_id is not None

            variant = datavalue.Value
            if variant is None:
                raise ValueError("DataValue без значения")
            # Колонки времени объявлены NOT NULL, и значение без метки уронило бы
            # всю пачку, в которую попало. Без времени источника значение
            # датируется временем сервера; без обоих — отвергается поштучно.
            source_ts = datavalue.SourceTimestamp or datavalue.ServerTimestamp
            server_ts = datavalue.ServerTimestamp or datavalue.SourceTimestamp
            if source_ts is None or server_ts is None:
                raise ValueError("DataValue без меток времени")
            item = VariableWriteItem(
                variable_id=variable_id,
                node_id_str=node_id_str,
                source_timestamp=source_ts,
                server_timestamp=server_ts,
                status_code=status_code_value(datavalue.StatusCode),
                value_str=value_text(variant),
                variant_type=int(variant.VariantType),
                variant_binary=encode_variant(variant),
                group_key=group_key(node_id_str),
                datavalue=datavalue,
            )
            # Чтение последнего значения сразу после записи должно видеть его,
            # даже если в базу оно доедет позже.
            self._caches.last_values.put(variable_id, datavalue)

            if self._value_buffer is not None:
                await self._value_buffer.enqueue(item, sync=self._settings.write.wait_for_flush)
            else:
                await self._variables.flush([item])
            await self._remember_data_type(variable_id, datavalue)
            self._note_save_succeeded()
        except Exception as exc:
            self._metrics.variables.record_error()
            self._log_save_failure("value", str(node_id), exc, str(datavalue))

    async def _remember_data_type(self, variable_id: int, datavalue: ua.DataValue) -> None:
        """Записать тип данных переменной, когда он стал известен.

        В 0.2.15 это делалось только без батчинга и двумя запросами на каждое
        значение. Здесь — один UPDATE на переменную за жизнь процесса.
        """
        name = data_type_name(datavalue)
        if name == "Unknown" or self._known_data_types.get(variable_id) == name:
            return
        await self._variables.update_data_type(variable_id, name)
        self._known_data_types[variable_id] = name

    async def _flush_values(self, items: List[VariableWriteItem]) -> None:
        await self._variables.flush(items)

    async def _resolve_variable_id(self, node_id: Any, *, create: bool) -> Optional[int]:
        variable_id = self._variable_ids.get(node_id)
        if variable_id is not None:
            return variable_id

        node_id_str = format_node_id(node_id)
        variable_id = self._caches.variables.get(node_id_str)
        if variable_id is None:
            row = await self._variables.find_metadata(node_id_str)
            if row is not None:
                variable_id = int(row["variable_id"])
            elif create:
                variable_id = await self._variables.ensure_metadata(
                    node_id_str, retention_period=self._effective_retention(None)
                )
            if variable_id is not None:
                self._caches.variables.put(node_id_str, variable_id)
        if variable_id is not None and create:
            self._variable_ids[node_id] = variable_id
        return variable_id

    async def save_event(self, event: Any) -> None:
        """Записать событие."""
        self._metrics.events.record_call()

        source = getattr(event, "SourceNode", None) if event is not None else None
        if source is None:
            self.logger.error("save_event: событие без источника")
            return
        event_type = getattr(event, "EventType", None)
        if event_type is None:
            self.logger.error("save_event: у события нет EventType")
            return

        try:
            source_db_id, type_db_id = await self._resolve_event_ids(source, event_type)
            moment = getattr(event, "Time", None) or getattr(event, "time", None) or datetime.now(timezone.utc)
            fields = (
                event.get_event_props_as_fields_dict()
                if hasattr(event, "get_event_props_as_fields_dict")
                else {}
            )
            item = EventWriteItem(
                source_db_id=source_db_id,
                event_type_id=type_db_id,
                event_timestamp=moment,
                event_data_json=json.dumps(encode_event_fields(fields)),
                group_key=group_key(format_node_id(source)),
            )
            if self._event_buffer is not None:
                await self._event_buffer.enqueue(item, sync=self._settings.write.wait_for_flush)
            else:
                await self._flush_events([item])
            self._note_save_succeeded()
        except Exception as exc:
            self._metrics.events.record_error()
            self._log_save_failure("event", str(source), exc)

    async def _resolve_event_ids(self, source: Any, event_type: Any) -> Tuple[int, int]:
        registered = self._event_source_ids.get(source)
        if registered is not None:
            source_db_id, type_ids = registered
            type_db_id = type_ids.get(event_type)
            if type_db_id is not None:
                return source_db_id, type_db_id

        source_key = format_node_id(source)
        source_db_id_opt = self._caches.event_sources.get(source_key)
        if source_db_id_opt is None:
            source_db_id_opt = await self._events.find_source(source_key)
            if source_db_id_opt is None:
                source_db_id_opt = await self._events.ensure_source(
                    source_key, retention_period=self._effective_retention(None)
                )
            self._caches.event_sources.put(source_key, source_db_id_opt)

        type_key = format_node_id(event_type)
        type_db_id_opt = self._caches.event_types.get(type_key)
        if type_db_id_opt is None:
            type_db_id_opt = await self._events.find_type(type_key)
            if type_db_id_opt is None:
                type_db_id_opt = await self._events.ensure_type(type_key)
                try:
                    await self._sync_event_type(
                        type_db_id_opt, coerce_node_id(event_type), self._event_fields.get(source, [])
                    )
                except Exception as exc:
                    self.logger.warning("Тип события %s не синхронизирован: %s", type_key, exc)
            self._caches.event_types.put(type_key, type_db_id_opt)

        if source not in self._event_source_ids:
            self._event_source_ids[source] = (source_db_id_opt, {event_type: type_db_id_opt})
        else:
            self._event_source_ids[source][1][event_type] = type_db_id_opt
        return source_db_id_opt, type_db_id_opt

    async def _flush_events(self, items: List[EventWriteItem]) -> None:
        if should_write_v2(self._events_mode) and self._event_search is not None:
            await self._event_search.flush(items)
        else:
            await self._events.flush(items)

    def _note_save_succeeded(self) -> None:
        if self._failure_since is None:
            return
        self.logger.info(
            "Запись истории восстановилась после %.1f с; неудачных записей: значения %d, события %d",
            (datetime.now(timezone.utc) - self._failure_since).total_seconds(),
            self._failed_saves["value"],
            self._failed_saves["event"],
        )
        self._failure_since = None
        self._failure_log_at = None
        self._failed_saves = {"value": 0, "event": 0}

    def _log_save_failure(
        self, kind: str, node: str, error: Exception, value: Optional[str] = None
    ) -> None:
        """Сообщить о неудачной записи, не утопив лог при долгой недоступности БД.

        Первые десять минут пишется каждая ошибка с подробностями; дальше —
        сводка не чаще раза в десять секунд.
        """
        now = datetime.now(timezone.utc)
        if self._failure_since is None:
            self._failure_since = now
        self._failed_saves[kind] += 1

        if now - self._failure_since < DETAILED_FAILURE_LOG_WINDOW:
            if value is not None:
                self.logger.error("Не записано (%s) для %s: %s\n %s", kind, node, error, value)
            else:
                self.logger.error("Не записано (%s) для %s: %s", kind, node, error)
            return

        if self._failure_log_at is None or now - self._failure_log_at >= AGGREGATED_FAILURE_LOG_INTERVAL:
            self._failure_log_at = now
            self.logger.error(
                "БД по-прежнему недоступна: не записано (%s) %d, последняя ошибка: %s",
                kind,
                self._failed_saves[kind],
                error,
            )
            self._failed_saves[kind] = 0

    # ================================================================== чтение

    async def read_node_history(
        self,
        node_id: ua.NodeId,
        start: Optional[datetime],
        end: Optional[datetime],
        nb_values: Optional[int],
        return_bounds: bool = False,
    ) -> Tuple[List[ua.DataValue], Optional[datetime]]:
        """Прочитать историю переменной (HistoryRead, ReadRawModifiedDetails)."""
        window = resolve_window(start, end, nb_values)
        try:
            if node_id in self._event_source_ids:
                self.logger.warning("Узел %s зарегистрирован как источник событий", node_id)
                return [], None
            variable_id = await self._resolve_variable_id(node_id, create=False)
            if variable_id is None:
                self.logger.warning("Узел %s не историзуется", node_id)
                return [], None

            rows = await self._variables.read_history_rows(
                variable_id, window.start, window.end, window.limit, window.order
            )
            values = [row_to_datavalue(row) for row in rows]
            return values, continuation_point(rows, len(values), window.limit, "sourcetimestamp")
        except Exception as exc:
            self.logger.error("История %s не прочитана: %s", node_id, exc)
            return [], None

    async def read_event_history(
        self,
        source_id: ua.NodeId,
        start: Optional[datetime],
        end: Optional[datetime],
        nb_values: Optional[int],
        evfilter: Any,
    ) -> Tuple[List[Any], Optional[datetime]]:
        """Прочитать историю событий (HistoryRead, ReadEventDetails)."""
        window = resolve_window(start, end, nb_values)
        try:
            if source_id in self._variable_ids:
                self.logger.warning("Узел %s зарегистрирован как переменная", source_id)
                return [], None
            source_db_id = await self._resolve_source_id(source_id)
            if source_db_id is None:
                self.logger.warning("Источник событий %s не историзуется", source_id)
                return [], None

            if should_read_v2(self._events_mode) and self._event_search is not None:
                result = await self._event_search.read(
                    source_db_id, window.start, window.end, window.limit, window.order, evfilter
                )
                return result.events, result.cursor[0] if result.cursor else None

            rows = await self._events.read_history_rows(
                source_db_id, window.start, window.end, window.limit, window.order
            )
            events = []
            for row in rows:
                try:
                    events.append(Event.from_field_dict(decode_payload(row["event_data"])))
                except Exception as exc:
                    self.logger.debug("Событие не собрано: %s", exc)
            events = apply_event_filter(events, evfilter)
            return events, continuation_point(rows, len(events), window.limit, "event_timestamp")
        except Exception as exc:
            self.logger.error("История событий %s не прочитана: %s", source_id, exc)
            return [], None

    async def _resolve_source_id(self, source_id: Any) -> Optional[int]:
        registered = self._event_source_ids.get(source_id)
        if registered is not None:
            return registered[0]
        key = format_node_id(source_id)
        value = self._caches.event_sources.get(key)
        if value is None:
            value = await self._events.find_source(key)
            if value is not None:
                self._caches.event_sources.put(key, value)
        return value

    async def execute_sql_delete(
        self,
        condition: str,
        args: Iterable,
        table: str,
        node_id: ua.NodeId,
    ) -> None:
        """Удалить строки истории по условию.

        Условие подставляется в SQL как есть — метод достался от интерфейса
        asyncua и предназначен для вызова кодом сервера, не клиентом.
        """
        try:
            validate_table_name(table)
            await self._db.execute(
                f'DELETE FROM "{self._schema}"."{table}" WHERE {condition}', *args
            )
        except Exception as exc:
            self.logger.error("Данные %s не удалены: %s", node_id, exc)

    # ================================================================== последние значения

    async def read_last_value(self, node_id: ua.NodeId) -> Optional[ua.DataValue]:
        """Последнее значение переменной: из памяти, из кэша в БД или из истории."""
        try:
            if node_id in self._event_source_ids:
                return None
            variable_id = await self._resolve_variable_id(node_id, create=False)
            if variable_id is None:
                return None

            cached = self._caches.last_values.get(variable_id)
            if cached is not None:
                return cached

            value = await self._variables.read_last_value(variable_id)
            if value is not None:
                self._caches.stats.hit("last_values_table_hits")
                return value
            self._caches.stats.hit("last_values_table_misses")

            value = await self._variables.read_latest_from_history(variable_id)
            if value is not None:
                self._caches.stats.hit("last_values_history_fallbacks")
            return value
        except Exception as exc:
            self.logger.error("Последнее значение %s не прочитано: %s", node_id, exc)
            return None

    async def read_last_values(
        self,
        node_ids: List[ua.NodeId],
        history_lookback: Optional[timedelta] = None,
        *,
        allow_history_fallback: bool = True,
    ) -> dict:
        """Последние значения списка переменных.

        ``history_lookback`` ограничивает поиск в истории, позволяя TimescaleDB
        отбросить старые чанки: без него переменная без данных заставляет
        пробегать индексы всех чанков. ``allow_history_fallback=False`` не ходит
        в историю вовсе — нужно при массовом восстановлении на старте.
        """
        result: Dict[Any, Optional[ua.DataValue]] = {}
        try:
            by_variable: Dict[int, Any] = {}
            for node_id in node_ids:
                if node_id in self._event_source_ids:
                    result[node_id] = None
                    continue
                variable_id = await self._resolve_variable_id(node_id, create=False)
                if variable_id is None:
                    result[node_id] = None
                else:
                    by_variable[variable_id] = node_id

            remaining = []
            for variable_id, node_id in by_variable.items():
                cached = self._caches.last_values.get(variable_id)
                if cached is not None:
                    result[node_id] = cached
                else:
                    remaining.append(variable_id)

            if remaining:
                stored = await self._variables.read_last_values(remaining)
                for variable_id, value in stored.items():
                    result[by_variable[variable_id]] = value
                    self._caches.last_values.put(variable_id, value)
                    self._caches.stats.hit("last_values_table_hits")
                remaining = [v for v in remaining if v not in stored]

            if remaining and not allow_history_fallback:
                self._caches.stats.hit("last_values_history_fallbacks_skipped", len(remaining))
                remaining = []

            if remaining:
                since = (
                    datetime.now(timezone.utc) - history_lookback
                    if history_lookback is not None
                    else None
                )
                found = await self._variables.latest_from_history_many(remaining, since)
                if found:
                    self._caches.stats.hit("last_values_history_fallbacks", len(found))
                    try:
                        await self._variables.upsert_last_values_rows(found)
                    except Exception as exc:
                        self.logger.debug("Кэш последних значений не дополнен: %s", exc)
                for row in found:
                    variable_id = int(row["variable_id"])
                    value = row_to_datavalue(row)
                    result[by_variable[variable_id]] = value
                    self._caches.last_values.put(variable_id, value)

            for node_id in node_ids:
                result.setdefault(node_id, None)
            return result
        except Exception as exc:
            self.logger.error("Последние значения не прочитаны: %s", exc)
            return {node_id: None for node_id in node_ids}

    async def seed_last_values(self, items: List[Tuple[ua.NodeId, ua.DataValue]]) -> int:
        """Создать строки последних значений для переменных, у которых их нет.

        Существующие строки не трогаются. Вместе с backfill_last_values это
        держит инвариант «у каждой зарегистрированной переменной есть строка»,
        благодаря которому чтение последнего значения не ходит в историю.
        """
        prepared: List[Tuple[int, ua.DataValue]] = []
        seen = set()
        for node_id, datavalue in items:
            variable_id = self._variable_ids.get(node_id)
            if variable_id is None:
                variable_id = self._caches.variables.peek(format_node_id(node_id))
            if variable_id is None or variable_id in seen:
                continue
            seen.add(variable_id)
            prepared.append(
                (variable_id, datavalue if datavalue is not None else ua.DataValue(Value=ua.Variant(None)))
            )

        created = await self._variables.seed_last_values(prepared)
        values = dict(prepared)
        for variable_id in created:
            self._caches.last_values.put(variable_id, values[variable_id])
        self.logger.info("Созданы строки последних значений: %d из %d", len(created), len(prepared))
        return len(created)

    async def backfill_last_values(
        self,
        *,
        chunk_size: int = 100,
        pause_sec: float = 0.5,
        query_timeout_sec: float = 120.0,
        history_lookback: Optional[timedelta] = None,
        on_chunk_restored: Optional[
            Callable[[List[Tuple[str, ua.DataValue]]], Union[Coroutine[Any, Any, Any], Any]]
        ] = None,
    ) -> dict:
        """Сверить строки-заглушки последних значений с историей.

        Заглушка заменяется реальным последним значением из истории; заглушка,
        для которой истории нет, помечается сверенной. Идёт мелкими порциями,
        чтобы не упираться в таймауты запросов.
        """
        candidates = await self._variables.seed_candidates()
        node_by_id = {int(row["variable_id"]): row["node_id"] for row in candidates}
        ids = list(node_by_id)
        stats: Dict[str, Any] = {
            "candidates": len(ids),
            "restored_from_history": 0,
            "confirmed_defaults": 0,
            "errors": 0,
            "restored_items": [],
        }
        if not ids:
            return stats

        since = (
            datetime.now(timezone.utc) - history_lookback if history_lookback is not None else None
        )
        step = max(1, int(chunk_size))
        for offset in range(0, len(ids), step):
            chunk = ids[offset : offset + step]
            try:

                async def _chunk() -> List[Any]:
                    found = await self._variables.latest_from_history_many(chunk, since)
                    await self._variables.upsert_last_values_rows(found)
                    await self._variables.confirm_seeds(chunk)
                    return found

                found = await asyncio.wait_for(_chunk(), timeout=query_timeout_sec * 2)
                stats["restored_from_history"] += len(found)
                stats["confirmed_defaults"] += len(chunk) - len(found)

                restored: List[Tuple[str, ua.DataValue]] = []
                for row in found:
                    variable_id = int(row["variable_id"])
                    value = row_to_datavalue(row)
                    self._caches.last_values.put(variable_id, value)
                    restored.append((node_by_id[variable_id], value))
                    # Полный список — только без обратного вызова, иначе десятки
                    # тысяч значений держались бы в памяти до конца сверки.
                    if on_chunk_restored is None:
                        stats["restored_items"].append((node_by_id[variable_id], value))

                if restored and on_chunk_restored is not None:
                    try:
                        outcome = on_chunk_restored(restored)
                        if asyncio.iscoroutine(outcome):
                            await outcome
                    except Exception as exc:
                        self.logger.warning("Обработчик восстановленных значений упал: %r", exc)
            except Exception as exc:
                stats["errors"] += 1
                self.logger.warning("Порция сверки (%d переменных) не прошла: %r", len(chunk), exc)
            if pause_sec > 0:
                await asyncio.sleep(pause_sec)

        summary = {k: v for k, v in stats.items() if k != "restored_items"}
        summary["restored_items_count"] = stats["restored_from_history"]
        self.logger.info("Сверка последних значений завершена: %s", summary)
        return stats

    # ================================================================== ретенция

    async def reapply_global_retention_policy(
        self,
        period: Optional[timedelta] = None,
        *,
        drop_immediately: bool = False,
    ) -> None:
        """Переустановить глобальную политику хранения без перезапуска.

        ``period=None`` оставляет текущий период; если и текущий не задан,
        политика снимается — данные хранятся бессрочно.
        """
        if not await self._bootstrap.timescaledb_available():
            self.logger.warning("TimescaleDB не найдена: политику хранения не применить")
            return
        if period is not None:
            self._settings = StorageSettings(
                connection=self._settings.connection,
                timeouts=self._settings.timeouts,
                keepalive=self._settings.keepalive,
                write=self._settings.write,
                cache=self._settings.cache,
                global_retention_period=period,
            )
        await self._bootstrap.reapply_retention(
            self._settings.global_retention_period, drop_immediately=drop_immediately
        )
        await self.refresh_history_settings_nodes()

    # ================================================================== метрики

    def get_performance_metrics(self) -> dict:
        """Снимок метрик без обращения к БД."""
        return self._metrics.snapshot(self._settings.metrics_snapshot())

    def reset_performance_metrics(self) -> None:
        self._metrics.reset()

    def get_cache_stats(self) -> dict:
        return self._caches.stats.as_dict()

    def reset_cache_stats(self) -> None:
        self._caches.stats.reset()

    # ================================================================== узлы OPC UA

    async def expose_history_settings_nodes(
        self,
        server: Any,
        namespace_index: int,
        *,
        parent: Any = None,
    ) -> None:
        """Опубликовать настройки хранения в ``History/HistorySettings``."""
        if server is None:
            raise ValueError("server is required")
        idx = int(namespace_index)
        root = await opc_nodes.server_parent(server, parent)
        history = await opc_nodes.get_or_add_object(root, idx, "History")
        settings = await opc_nodes.get_or_add_object(history, idx, "HistorySettings")

        nodes: Dict[str, Any] = {}
        for name, initial in self._settings_values(timescale_available=False).items():
            nodes[name] = await opc_nodes.get_or_add_variable(settings, idx, name, initial)
        if self._publish_event_capabilities:
            for name, initial in self._event_capability_values(backfill_complete=True).items():
                nodes[name] = await opc_nodes.get_or_add_variable(settings, idx, name, initial)
        self._settings_nodes = nodes
        await self.refresh_history_settings_nodes()

    async def refresh_history_settings_nodes(self) -> None:
        if not self._settings_nodes:
            return
        try:
            values = self._settings_values(
                timescale_available=await self._bootstrap.timescaledb_available()
            )
            if self._publish_event_capabilities:
                values.update(
                    self._event_capability_values(backfill_complete=await self._backfill_complete())
                )
            await opc_nodes.write_values(self._settings_nodes, values)
        except Exception:
            return

    def _settings_values(self, *, timescale_available: bool) -> Dict[str, ua.Variant]:
        try:
            version = importlib_metadata.version("uapg")
        except Exception:
            version = "unknown"
        retention = self._settings.global_retention_period
        write = self._settings.write
        return {
            "UapgVersion": ua.Variant(str(version), ua.VariantType.String),
            "StorageType": ua.Variant("timescale", ua.VariantType.String),
            "Schema": ua.Variant(str(self._schema), ua.VariantType.String),
            "GlobalRetentionSeconds": ua.Variant(
                int(retention.total_seconds()) if retention is not None else -1,
                ua.VariantType.Int64,
            ),
            "WriteBatchEnabled": ua.Variant(bool(write.batch_enabled), ua.VariantType.Boolean),
            "WriteMaxBatchSize": ua.Variant(int(write.max_batch_size), ua.VariantType.Int32),
            "WriteMaxBatchIntervalSec": ua.Variant(
                float(write.max_batch_interval_sec), ua.VariantType.Double
            ),
            "WriteQueueMaxSize": ua.Variant(int(write.queue_max_size), ua.VariantType.Int32),
            "WriteDurabilityMode": ua.Variant(str(write.durability_mode), ua.VariantType.String),
            "WriteReadConsistencyMode": ua.Variant(
                str(write.read_consistency_mode), ua.VariantType.String
            ),
            "TimescaleExtensionAvailable": ua.Variant(
                bool(timescale_available), ua.VariantType.Boolean
            ),
        }

    def _event_capability_values(self, *, backfill_complete: bool) -> Dict[str, ua.Variant]:
        sql_supported = (
            self._v2_ready
            and self._events_mode != StorageMode.LEGACY
            and bool(self._events_config.sql_filter_fields)
        )
        return {
            "EventsStorageVersion": ua.Variant("v2" if self._v2_ready else "v1", ua.VariantType.String),
            "EventsStorageMode": ua.Variant(self._events_mode.value, ua.VariantType.String),
            "EventsSqlFilterSupported": ua.Variant(sql_supported, ua.VariantType.Boolean),
            "EventsSqlFilterFields": ua.Variant(
                self._events_config.sql_filter_fields_csv() if sql_supported else "",
                ua.VariantType.String,
            ),
            "EventsBackfillComplete": ua.Variant(backfill_complete, ua.VariantType.Boolean),
        }

    async def _backfill_complete(self) -> bool:
        if not self._v2_ready:
            return True
        try:
            return await self._events.backfill_lag() == 0
        except Exception:
            return False

    async def expose_history_metrics_nodes(
        self,
        server: Any,
        namespace_index: int,
        *,
        parent: Any = None,
    ) -> None:
        """Опубликовать метрики в ``History/HistoryMetrics``.

        Значения обновляются только явным вызовом refresh_history_metrics_nodes().
        """
        if server is None:
            raise ValueError("server is required")
        idx = int(namespace_index)
        root = await opc_nodes.server_parent(server, parent)
        history = await opc_nodes.get_or_add_object(root, idx, "History")
        metrics = await opc_nodes.get_or_add_object(history, idx, "HistoryMetrics")

        nodes: Dict[str, Any] = {}
        for path, value in opc_nodes.flatten_metrics(self.get_performance_metrics()).items():
            nodes[path] = await opc_nodes.get_or_add_variable(
                metrics, idx, opc_nodes.metric_node_name(path), opc_nodes.metric_variant(value)
            )
        self._metrics_nodes = nodes
        await self.refresh_history_metrics_nodes()

    async def refresh_history_metrics_nodes(self) -> None:
        if not self._metrics_nodes:
            return
        try:
            flattened = opc_nodes.flatten_metrics(self.get_performance_metrics())
            await opc_nodes.write_values(
                self._metrics_nodes,
                {path: opc_nodes.metric_variant(value) for path, value in flattened.items()},
            )
        except Exception:
            return

    # ================================================================== поиск событий (v2)

    async def _run_events_backfill(self, batch_size: int) -> Dict[str, Any]:
        if self._event_search is None:
            return {"backfill_lag_rows": -1, "v2_coverage_pct": 0.0}
        return await self._event_search.backfill(batch_size)

    async def _explain_event_filter(
        self,
        source_id: ua.NodeId,
        start: Any,
        end: Any,
        nb_values: Optional[int],
        evfilter: Any,
    ) -> str:
        if self._event_search is None:
            return ""
        window = resolve_window(start, end, nb_values)
        source_db_id = await self._resolve_source_id(source_id)
        if source_db_id is None:
            return ""
        from .storage.filter_plan import EventFilterPlanner

        planner = EventFilterPlanner(field_aliases=self._events_config.field_aliases)
        type_ids = await self._event_search._resolve_types(planner, planner.build(evfilter))
        return await self._event_search.explain(
            source_db_id, window.start, window.end, window.limit, window.order, type_ids
        )

