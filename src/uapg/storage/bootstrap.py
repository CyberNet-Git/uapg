"""Создание схемы историзации.

Схема приводится к нужному виду при каждом старте и обязана быть идемпотентной:
сервер перезапускают часто, и повторный запуск не должен ни падать, ни менять
уже существующие объекты.

Отдельное требование — не мешать живой записи. Создание индекса на горячей
таблице ждёт блокировку за работающим INSERT, поэтому индексы создаются по
одному, с проверкой существования, и неудача любого из них не валит запуск:
без индекса история пишется медленнее, без запуска — не пишется вовсе.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import Optional, Sequence, Tuple

from ..core.database import Database
from ..core.sql import index_name, load_sql, split_statements

# (таблица, колонка времени, колонка пространственного измерения, число партиций)
HYPERTABLES: Sequence[Tuple[str, str, str, int]] = (
    ("variables_history", "sourcetimestamp", "variable_id", 128),
    ("events_history", "event_timestamp", "source_id", 64),
)

RETENTION_TABLES: Sequence[str] = ("variables_history", "events_history")

# Слой поиска событий живёт ровно столько же, сколько сами события: строка
# поиска без своей полезной нагрузки в events_history — это молча потерянное
# событие, поэтому отдельного периода хранения у него нет.
EVENT_SEARCH_RETENTION_TABLES: Sequence[str] = ("events_ts",)

DEFAULT_SPACE_PARTITIONS = 32


class SchemaBootstrap:
    """Приводит схему к виду, который ожидает бэкенд."""

    def __init__(
        self,
        database: Database,
        schema: str,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._db = database
        self._schema = schema
        self.logger = logger or logging.getLogger("uapg.schema")

    async def ensure_core_schema(self, global_retention: Optional[timedelta] = None) -> None:
        await self._create_tables()
        await self._create_indexes()
        await self._create_hypertables()
        if global_retention is not None:
            await self.apply_retention(global_retention)

    async def _create_tables(self) -> None:
        for statement in split_statements(load_sql("010_tables.sql", self._schema)):
            await self._db.execute(statement)

    async def _create_indexes(self) -> None:
        for statement in split_statements(load_sql("020_indexes.sql", self._schema)):
            name = index_name(statement)
            if name is None:
                await self._db.execute(statement)
                continue
            await self._ensure_index(name, statement)

    async def _ensure_index(self, name: str, statement: str) -> None:
        try:
            if await self._index_exists(name):
                return
            await self._db.execute(statement)
        except (asyncio.TimeoutError, TimeoutError):
            # Ждать блокировку на горячей таблице можно бесконечно, а запись
            # важнее скорости чтения: индекс доедет при следующем старте.
            self.logger.warning("Индекс %s не создан: таймаут при старте", name)
        except Exception as exc:
            self.logger.warning("Индекс %s не создан: %s", name, exc)

    async def _index_exists(self, name: str) -> bool:
        found = await self._db.fetchval(
            "SELECT 1 FROM pg_indexes WHERE schemaname = $1 AND indexname = $2",
            self._schema,
            name,
        )
        return found is not None

    # ------------------------------------------------------------------ TimescaleDB

    async def timescaledb_available(self) -> bool:
        try:
            version = await self.timescaledb_version()
            return version is not None
        except Exception:
            return False

    async def timescaledb_version(self) -> Optional[str]:
        value = await self._db.fetchval(
            "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"
        )
        return str(value) if value else None

    async def _create_hypertables(self) -> None:
        version = await self.timescaledb_version()
        if version is None:
            self.logger.warning(
                "Расширение TimescaleDB не найдено: таблицы останутся обычными"
            )
            return

        supports_space_partition = int(version.split(".")[0]) >= 2
        for table, time_column, space_column, partitions in HYPERTABLES:
            await self._create_hypertable(
                table,
                time_column,
                space_column if supports_space_partition else None,
                partitions,
            )

    async def _create_hypertable(
        self,
        table: str,
        time_column: str,
        space_column: Optional[str],
        partitions: int,
    ) -> None:
        if not 1 <= partitions <= 32767:
            partitions = DEFAULT_SPACE_PARTITIONS
        try:
            if space_column:
                await self._db.execute(
                    "SELECT create_hypertable("
                    "  format('%I.%I', $1::text, $2::text)::regclass,"
                    "  $3::name,"
                    "  partitioning_column => $4::name,"
                    "  number_partitions => $5::integer,"
                    "  if_not_exists => TRUE"
                    ")",
                    self._schema,
                    table,
                    time_column,
                    space_column,
                    partitions,
                )
            else:
                await self._db.execute(
                    "SELECT create_hypertable("
                    "  format('%I.%I', $1::text, $2::text)::regclass,"
                    "  $3::name,"
                    "  if_not_exists => TRUE"
                    ")",
                    self._schema,
                    table,
                    time_column,
                )
        except Exception as exc:
            # Обычная таблица работает, просто без преимуществ гипертаблицы.
            self.logger.warning("Гипертаблица %s не создана: %s", table, exc)

    # ------------------------------------------------------------------ ретенция

    async def apply_retention(self, period: Optional[timedelta]) -> None:
        """Глобальная политика хранения для таблиц истории."""
        if period is None:
            return
        if period.total_seconds() <= 0:
            self.logger.warning("Период хранения должен быть положительным, получено %s", period)
            return
        if not await self.timescaledb_available():
            return

        for table in RETENTION_TABLES:
            await self._set_retention_policy(table, period)

    async def apply_event_search_retention(self, period: Optional[timedelta]) -> None:
        """Согласовать политику слоя поиска событий с глобальной.

        Миграция 004 ставила слою поиска свои 365 дней, никак не связанные с
        ``global_retention_period``. При меньшем глобальном периоде появлялось
        окно, в котором строка поиска есть, а самого события в
        ``events_history`` уже нет: ``HistoryRead`` молча отдавал меньше
        событий, чем нашёл фильтр. Отдельно период слоя поиска не настраивается,
        источник истины один, поэтому расхождение исправляется в обе стороны —
        включая снятие политики, когда глобальный период не задан.
        """
        if period is not None and period.total_seconds() <= 0:
            return
        if not await self.timescaledb_available():
            return

        for table in EVENT_SEARCH_RETENTION_TABLES:
            if not await self._table_exists(table):
                continue
            await self._set_retention_policy(table, period)

    async def _set_retention_policy(self, table: str, period: Optional[timedelta]) -> None:
        """Привести политику таблицы к ``period``; ``None`` — снять политику.

        Политика переустанавливается только при расхождении: лишняя пара
        remove/add сбрасывала бы расписание фоновой задачи на каждом старте.
        """
        try:
            current = await self._retention_period(table)
        except Exception as exc:
            self.logger.warning("Политика хранения %s не прочитана: %s", table, exc)
            return
        if current == period:
            return
        if current is not None:
            await self._remove_retention_policy(table)
        if period is not None:
            await self._add_retention_policy(table, int(period.total_seconds()))
            self.logger.info("Политика хранения %s: %s", table, period)

    async def _retention_period(self, table: str) -> Optional[timedelta]:
        value = await self._db.fetchval(
            "SELECT (config->>'drop_after')::interval"
            "  FROM timescaledb_information.jobs"
            " WHERE proc_name = 'policy_retention'"
            "   AND hypertable_schema = $1"
            "   AND hypertable_name = $2",
            self._schema,
            table,
        )
        return value if isinstance(value, timedelta) else None

    async def _table_exists(self, table: str) -> bool:
        found = await self._db.fetchval(
            "SELECT 1 FROM pg_class c"
            "  JOIN pg_namespace n ON n.oid = c.relnamespace"
            " WHERE n.nspname = $1 AND c.relname = $2",
            self._schema,
            table,
        )
        return found is not None

    async def _add_retention_policy(self, table: str, seconds: int) -> None:
        try:
            await self._db.execute(
                "SELECT add_retention_policy("
                "  format('%I.%I', $1::text, $2::text)::regclass,"
                "  drop_after => make_interval(secs => $3::integer),"
                "  if_not_exists => TRUE"
                ")",
                self._schema,
                table,
                seconds,
            )
        except Exception as exc:
            self.logger.warning("Политика хранения для %s не применена: %s", table, exc)

    async def _remove_retention_policy(self, table: str) -> None:
        try:
            await self._db.execute(
                "SELECT remove_retention_policy("
                "  format('%I.%I', $1::text, $2::text)::regclass,"
                "  if_exists => TRUE"
                ")",
                self._schema,
                table,
            )
        except Exception as exc:
            self.logger.warning("Политика хранения для %s не снята: %s", table, exc)

    async def reapply_retention(
        self,
        period: Optional[timedelta],
        *,
        drop_immediately: bool = False,
    ) -> None:
        """Заменить действующую политику хранения.

        Снятие политики без установки новой означает «хранить вечно», поэтому
        период None — это осознанное отключение, а не пропуск операции.
        """
        if not await self.timescaledb_available():
            return

        tables = list(RETENTION_TABLES)
        for table in EVENT_SEARCH_RETENTION_TABLES:
            if await self._table_exists(table):
                tables.append(table)

        for table in tables:
            await self._remove_retention_policy(table)

        if period is None or period.total_seconds() <= 0:
            return

        seconds = int(period.total_seconds())
        for table in tables:
            await self._add_retention_policy(table, seconds)
            if drop_immediately:
                await self._drop_old_chunks(table, seconds)

    async def _drop_old_chunks(self, table: str, seconds: int) -> None:
        try:
            await self._db.execute(
                "SELECT drop_chunks("
                "  format('%I.%I', $1::text, $2::text)::regclass,"
                "  older_than => make_interval(secs => $3::integer)"
                ")",
                self._schema,
                table,
                seconds,
            )
        except Exception as exc:
            self.logger.warning("Старые чанки %s не удалены: %s", table, exc)
