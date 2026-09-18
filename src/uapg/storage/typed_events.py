"""Типизированные таблицы событий (``evt_<slug>``).

Слой поиска: у каждого типа события своя таблица, где интересные поля лежат
отдельными колонками. Это позволяет фильтровать на стороне БД, а не выбирать
строки и отсеивать их в памяти после LIMIT.

Состав полей события здесь не хранится — только то, по чему ищут. Полное
событие восстанавливается из ``events_history`` (см. storage/events.py).

Два исправления против 0.2.15, оба видны только под нагрузкой:

Ключ advisory lock брался от ``hash(str)``. Хэш строк в Python рандомизирован
от запуска к запуску, поэтому два процесса брали разные замки и не
синхронизировались вовсе — замок существовал, но ничего не защищал.

Проверка и досоздание колонок делались на каждом записываемом событии: замок,
запрос к information_schema и DDL — всё это на одно событие. Теперь известные
колонки помнятся в памяти процесса, и в горячем пути не остаётся ни замка, ни
обращения к каталогу.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any, Dict, List, Optional, Sequence, Set

from asyncua import ua

from ..core.database import Database
from ..core.sql import validate_identifier
from .events_config import EventsV2Config

# Поля, которые уже есть в events_ts и в самом событии: дублировать их колонками
# в каждой типизированной таблице незачем.
BASE_EVENT_FIELDS = frozenset({"Time", "EventType", "SourceNode", "ReceiveTime", "LocalTime"})

_COLUMN_NAME = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_SLUG_CLEANUP = re.compile(r"[^a-zA-Z0-9_]+")

MAX_SLUG_LENGTH = 48
MAX_INDEX_NAME_LENGTH = 58


def slug_from_node_id(node_id: ua.NodeId) -> str:
    """Имя типа события, пригодное для имени таблицы."""
    from ..opc_node_id import coerce_node_id

    nid = coerce_node_id(node_id)
    identifier = nid.Identifier
    # У непрозрачного NodeId идентификатор — байты. Их представление участвует в
    # имени таблицы, поэтому менять его нельзя: у существующих установок таблицы
    # уже названы именно так.
    rendered = repr(identifier) if isinstance(identifier, (bytes, bytearray)) else str(identifier)
    slug = _SLUG_CLEANUP.sub("_", f"{nid.NamespaceIndex}_{rendered}").strip("_").lower()
    if not slug:
        slug = "unknown"
    if slug[0].isdigit():
        slug = f"t_{slug}"
    return slug[:MAX_SLUG_LENGTH]


def physical_table_name(slug: str) -> str:
    return f"evt_{slug}"


def advisory_lock_key(table: str) -> int:
    """Устойчивый ключ advisory lock.

    Именно устойчивый: встроенный hash() рандомизирован PYTHONHASHSEED, поэтому
    разные процессы брали разные ключи и не мешали друг другу выполнять DDL
    одновременно.
    """
    digest = hashlib.blake2b(table.encode("utf-8"), digest_size=4).digest()
    return int.from_bytes(digest, "big") % (2**31 - 1)


def value_for_column(value: Any) -> Any:
    """Привести значение поля события к тому, что принимает колонка TEXT."""
    if value is None:
        return None
    if isinstance(value, ua.Variant):
        return value_for_column(value.Value)
    if isinstance(value, ua.NodeId):
        return str(value)
    if isinstance(value, ua.LocalizedText):
        return value.Text
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        return value
    return str(value)


class TypedEventTables:
    """Создание и заполнение типизированных таблиц событий."""

    def __init__(
        self,
        database: Database,
        schema: str,
        config: Optional[EventsV2Config] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._db = database
        self._schema = validate_identifier(schema)
        self._config = config or EventsV2Config()
        self.logger = logger or logging.getLogger("uapg.storage.typed_events")
        # table -> известные колонки. Избавляет горячий путь от обращений к каталогу.
        self._columns: Dict[str, Set[str]] = {}

    @property
    def config(self) -> EventsV2Config:
        return self._config

    def column_name(self, field_name: str) -> Optional[str]:
        """Имя колонки для поля события или None, если поле для колонки не годится."""
        if field_name in BASE_EVENT_FIELDS:
            return None
        column = str(self._config.field_aliases.get(field_name, field_name))
        # Поля вроде "/2:CustomField" колонкой стать не могут; событие от этого
        # не теряется — полный состав остаётся в events_history.
        return column if _COLUMN_NAME.match(column) else None

    def columns_for(self, values: Dict[str, Any]) -> Dict[str, Any]:
        """Отобрать из полей события те, что ложатся в колонки."""
        columns: Dict[str, Any] = {}
        for field_name, value in values.items():
            column = self.column_name(field_name)
            if column is None:
                continue
            columns[column] = value_for_column(value)
        return columns

    # ------------------------------------------------------------------ DDL

    async def ensure_table(self, table: str, columns: Sequence[str]) -> None:
        """Создать таблицу и недостающие колонки.

        Вызывается при регистрации типа события и при появлении незнакомого
        поля, но не на каждом событии.
        """
        validate_identifier(table)
        known = self._columns.get(table)
        if known is not None and all(column in known for column in columns):
            return

        lock_key = advisory_lock_key(table)
        await self._db.execute("SELECT pg_advisory_lock($1)", lock_key)
        try:
            await self._create_table(table)
            existing = await self._load_columns(table)
            for column in columns:
                if column in existing:
                    continue
                validate_identifier(column)
                await self._db.execute(
                    f'ALTER TABLE "{self._schema}"."{table}" '
                    f'ADD COLUMN IF NOT EXISTS "{column}" TEXT'
                )
                existing.add(column)
                if self._is_indexed(column):
                    await self._create_column_index(table, column)
            self._columns[table] = existing
        finally:
            await self._db.execute("SELECT pg_advisory_unlock($1)", lock_key)

    def _is_indexed(self, column: str) -> bool:
        indexed = self._config.indexed_fields
        if column in indexed:
            return True
        return any(self._config.field_aliases.get(name, name) == column for name in indexed)

    async def _create_table(self, table: str) -> None:
        await self._db.execute(
            f'''
            CREATE TABLE IF NOT EXISTS "{self._schema}"."{table}" (
                event_id BIGINT NOT NULL,
                event_timestamp TIMESTAMPTZ NOT NULL,
                source_id BIGINT NOT NULL,
                PRIMARY KEY (event_id, event_timestamp)
            )
            '''
        )
        await self._db.execute(
            f'''
            CREATE INDEX IF NOT EXISTS "idx_{table}_source_ts"
            ON "{self._schema}"."{table}" (source_id, event_timestamp DESC, event_id DESC)
            '''
        )

    async def _create_column_index(self, table: str, column: str) -> None:
        index_name = f"idx_{table}_{column}"[:MAX_INDEX_NAME_LENGTH]
        await self._db.execute(
            f'CREATE INDEX IF NOT EXISTS "{index_name}" '
            f'ON "{self._schema}"."{table}" ("{column}")'
        )

    async def _load_columns(self, table: str) -> Set[str]:
        rows = await self._db.fetch(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            """,
            self._schema,
            table,
        )
        return {row["column_name"] for row in rows}

    async def known_columns(self, table: str) -> Set[str]:
        """Колонки таблицы; при первом обращении читаются из каталога."""
        columns = self._columns.get(table)
        if columns is None:
            columns = await self._load_columns(table)
            if columns:
                self._columns[table] = columns
        return columns

    def forget(self, table: Optional[str] = None) -> None:
        """Забыть запомненные колонки (нужно после внешних изменений схемы)."""
        if table is None:
            self._columns.clear()
        else:
            self._columns.pop(table, None)

    # ------------------------------------------------------------------ запись

    def insert_statement(self, table: str, columns: Sequence[str]) -> str:
        names = ", ".join(
            ['"event_id"', '"event_timestamp"', '"source_id"']
            + [f'"{column}"' for column in columns]
        )
        placeholders = ", ".join(f"${i}" for i in range(1, len(columns) + 4))
        return (
            f'INSERT INTO "{self._schema}"."{table}" ({names}) VALUES ({placeholders}) '
            f"ON CONFLICT (event_id, event_timestamp) DO NOTHING"
        )

    async def insert_row(
        self,
        table: str,
        *,
        event_id: int,
        event_timestamp: Any,
        source_id: int,
        values: Dict[str, Any],
    ) -> None:
        """Записать строку поиска для одного события."""
        columns = self.columns_for(values)
        await self.ensure_table(table, list(columns))
        await self._db.execute(
            self.insert_statement(table, list(columns)),
            event_id,
            event_timestamp,
            source_id,
            *columns.values(),
        )


class EventSchemaRegistry:
    """Реестр схем типов событий (``event_type_schema``, ``event_type_storage``)."""

    def __init__(
        self,
        database: Database,
        schema: str,
        tables: TypedEventTables,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._db = database
        self._schema = validate_identifier(schema)
        self._tables = tables
        self.logger = logger or logging.getLogger("uapg.storage.typed_events")

    def describe_fields(self, field_names: Sequence[str]) -> List[Dict[str, Any]]:
        """Описание полей для реестра схем.

        Все колонки объявляются TEXT: тип поля события известен только в момент
        его прихода, а менять тип уже существующей колонки нельзя.
        """
        indexed = self._tables.config.indexed_fields
        fields: List[Dict[str, Any]] = []
        for name in field_names:
            column = self._tables.column_name(name)
            if column is None:
                continue
            fields.append(
                {
                    "name": column,
                    "opc_name": name,
                    "opc_datatype": "String",
                    "sql_type": "TEXT",
                    "nullable": True,
                    "index": column in indexed or name in indexed,
                }
            )
        return fields

    async def sync_event_type(
        self,
        event_type_id: int,
        event_type_node: ua.NodeId,
        fields: List[Dict[str, Any]],
        *,
        parent_node_id: Optional[ua.NodeId] = None,
    ) -> tuple[str, int]:
        """Зарегистрировать тип события и привести его таблицу к нужному виду."""
        table = physical_table_name(slug_from_node_id(event_type_node))
        version = await self._next_schema_version(event_type_id, fields)

        await self._db.execute(
            f'CALL "{self._schema}".uapg_sync_event_type_schema($1, $2, $3, $4::jsonb, $5, $6)',
            int(event_type_id),
            str(event_type_node),
            str(parent_node_id) if parent_node_id else None,
            json.dumps(fields),
            version,
            table,
        )
        await self._tables.ensure_table(table, [field["name"] for field in fields])
        return table, version

    async def _next_schema_version(
        self, event_type_id: int, fields: List[Dict[str, Any]]
    ) -> int:
        """Версия растёт только при изменении состава полей."""
        row = await self._db.fetchrow(
            f'''
            SELECT schema_version, fields
            FROM "{self._schema}".event_type_schema
            WHERE event_type_id = $1
            ORDER BY schema_version DESC
            LIMIT 1
            ''',
            int(event_type_id),
        )
        if row is None:
            return 1
        current = row["fields"]
        if isinstance(current, str):
            current = json.loads(current)
        return int(row["schema_version"]) if current == fields else int(row["schema_version"]) + 1

    async def storage_table(self, event_type_id: int) -> Optional[str]:
        value = await self._db.fetchval(
            f'SELECT physical_table FROM "{self._schema}".event_type_storage WHERE event_type_id = $1',
            int(event_type_id),
        )
        return str(value) if value else None

    async def storage_tables(self, event_type_ids: Sequence[int]) -> Dict[int, str]:
        if not event_type_ids:
            return {}
        rows = await self._db.fetch(
            f'''
            SELECT event_type_id, physical_table
            FROM "{self._schema}".event_type_storage
            WHERE event_type_id = ANY($1::bigint[])
            ''',
            [int(i) for i in event_type_ids],
        )
        return {int(row["event_type_id"]): str(row["physical_table"]) for row in rows}

    async def fields_of(self, event_type_id: int) -> Set[str]:
        row = await self._db.fetchrow(
            f'''
            SELECT fields FROM "{self._schema}".event_type_schema
            WHERE event_type_id = $1
            ORDER BY schema_version DESC
            LIMIT 1
            ''',
            int(event_type_id),
        )
        if row is None:
            return set()
        fields = row["fields"]
        if isinstance(fields, str):
            fields = json.loads(fields)
        return {
            str(field["name"])
            for field in (fields or [])
            if isinstance(field, dict) and field.get("name")
        }

    async def fields_by_type(self, event_type_ids: Sequence[int]) -> Dict[int, Set[str]]:
        """Состав полей каждого типа: нужен, чтобы понять, куда можно опустить фильтр."""
        if not event_type_ids:
            return {}
        rows = await self._db.fetch(
            f'''
            SELECT DISTINCT ON (event_type_id) event_type_id, fields
            FROM "{self._schema}".event_type_schema
            WHERE event_type_id = ANY($1::bigint[])
            ORDER BY event_type_id, schema_version DESC
            ''',
            [int(i) for i in event_type_ids],
        )
        result: Dict[int, Set[str]] = {}
        for row in rows:
            fields = row["fields"]
            if isinstance(fields, str):
                fields = json.loads(fields)
            result[int(row["event_type_id"])] = {
                str(field["name"])
                for field in (fields or [])
                if isinstance(field, dict) and field.get("name")
            }
        return result
