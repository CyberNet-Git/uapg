"""OPC UA event type introspection and typed table DDL."""

from __future__ import annotations

import hashlib
import logging
import re
from typing import Any, Dict, FrozenSet, Iterable, List, Mapping, Optional, Set, Tuple

from asyncua import ua

from .events_config import EventsV2Config, expand_sql_filter_fields

_logger = logging.getLogger(__name__)

_OPC_TO_SQL = {
    ua.VariantType.Boolean: "BOOLEAN",
    ua.VariantType.SByte: "SMALLINT",
    ua.VariantType.Byte: "SMALLINT",
    ua.VariantType.Int16: "SMALLINT",
    ua.VariantType.UInt16: "INTEGER",
    ua.VariantType.Int32: "INTEGER",
    ua.VariantType.UInt32: "BIGINT",
    ua.VariantType.Int64: "BIGINT",
    ua.VariantType.UInt64: "NUMERIC(20,0)",
    ua.VariantType.Float: "REAL",
    ua.VariantType.Double: "DOUBLE PRECISION",
    ua.VariantType.String: "TEXT",
    ua.VariantType.DateTime: "TIMESTAMPTZ",
    ua.VariantType.Guid: "UUID",
}


def slug_from_node_id(node_id: ua.NodeId) -> str:
    from ..opc_node_id import coerce_node_id

    nid = coerce_node_id(node_id)
    raw = f"{nid.NamespaceIndex}_{nid.Identifier}"
    slug = re.sub(r"[^a-zA-Z0-9_]+", "_", str(raw)).strip("_").lower()
    if not slug:
        slug = "unknown"
    if slug[0].isdigit():
        slug = f"t_{slug}"
    return slug[:48]


def physical_table_name(slug: str) -> str:
    return f"evt_{slug}"


# Предел идентификатора PostgreSQL — 63 байта; имя должно быть детерминированным,
# иначе CREATE INDEX IF NOT EXISTS перестанет распознавать уже созданный индекс.
MAX_IDENTIFIER_BYTES = 63

# Типы колонок, для которых имеет смысл trgm: ILIKE '%...%' по btree не индексируется.
TRGM_COLUMN_TYPES = ("text", "character varying", "character")


# Текстовые колонки из indexed_fields во всех typed-таблицах; $1 — схема,
# $2 — имена колонок, $3 — допустимые типы.
TRGM_CANDIDATES_SQL = '''
    SELECT ets.physical_table AS table_name, c.column_name
    FROM "{schema}".event_type_storage ets
    JOIN information_schema.columns c
      ON c.table_schema = $1 AND c.table_name = ets.physical_table
    WHERE c.column_name = ANY($2::text[])
      AND c.data_type = ANY($3::text[])
    ORDER BY ets.physical_table, c.column_name
'''

# Имена и валидность индексов схемы; $1 — схема, $2 — имена.
INDEX_VALIDITY_SQL = '''
    SELECT ix.indexname, i.indisvalid
    FROM pg_indexes ix
    JOIN pg_namespace n ON n.nspname = ix.schemaname
    JOIN pg_class c ON c.relname = ix.indexname AND c.relnamespace = n.oid
    JOIN pg_index i ON i.indexrelid = c.oid
    WHERE ix.schemaname = $1 AND ix.indexname = ANY($2::text[])
'''


def trgm_index_name(table: str, column: str) -> str:
    name = f"idx_{table}_{column}_trgm"
    if len(name.encode("utf-8")) <= MAX_IDENTIFIER_BYTES:
        return name
    digest = hashlib.md5(f"{table}:{column}".encode("utf-8")).hexdigest()[:16]
    return f"idx_{digest}_trgm"


def trgm_index_ddl(schema: str, table: str, column: str) -> str:
    return (
        f'CREATE INDEX IF NOT EXISTS "{trgm_index_name(table, column)}"'
        f' ON "{schema}"."{table}" USING gin ("{column}" gin_trgm_ops)'
    )


def opc_variant_to_sql_type(variant_type: ua.VariantType) -> str:
    return _OPC_TO_SQL.get(variant_type, "TEXT")


def python_value_to_sql(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, ua.NodeId):
        return str(value)
    if isinstance(value, ua.Variant):
        return python_value_to_sql(value.Value)
    if isinstance(value, ua.LocalizedText):
        return value.Text
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, ua.DateTime):
        return value
    # Typed event tables use TEXT columns; asyncpg rejects int/bool for TEXT.
    if isinstance(value, (bool, int, float)):
        return str(value)
    if not isinstance(value, str):
        return str(value)
    return value


class EventSchemaRegistry:
    """Maintains event_type_schema registry and typed physical tables."""

    def __init__(
        self,
        schema: str,
        execute: Any,
        fetch: Any,
        fetchrow: Any,
        fetchval: Any,
        logger: Optional[logging.Logger] = None,
        *,
        events_config: Optional[EventsV2Config] = None,
        trgm_index_enabled: bool = True,
    ) -> None:
        self._schema = schema
        self._execute = execute
        self._fetch = fetch
        self._fetchrow = fetchrow
        self._fetchval = fetchval
        self._logger = logger or _logger
        self._events_config = events_config or EventsV2Config()
        self._trgm_index_enabled = bool(trgm_index_enabled)
        # Какие колонки typed-таблиц уже заведомо есть. Без этого
        # _ensure_physical_table (advisory lock + DDL + information_schema.columns)
        # выполнялся на каждый insert_typed_row. Пополняется только тем, что
        # реально прочитали из каталога или сами создали.
        self._known_columns: Dict[str, Set[str]] = {}

    @property
    def events_config(self) -> EventsV2Config:
        return self._events_config

    async def introspect_fields(self, evtypes: List[ua.NodeId], get_fields_cb: Any) -> List[Dict[str, Any]]:
        names = await get_fields_cb(evtypes)
        indexed = self._events_config.indexed_fields
        fields: List[Dict[str, Any]] = []
        for name in names:
            column = self._events_config.column_name(name)
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
        parent_node_id: Optional[ua.NodeId],
        fields: List[Dict[str, Any]],
        gateway: Any,
    ) -> Tuple[str, int]:
        slug = slug_from_node_id(event_type_node)
        table = physical_table_name(slug)
        schema_version = await self._next_schema_version(event_type_id, fields)
        node_id_str = str(event_type_node)
        parent_str = str(parent_node_id) if parent_node_id else None

        await gateway.sync_event_type_schema(
            event_type_id,
            node_id_str,
            parent_str,
            fields,
            schema_version,
            table,
        )
        await self._ensure_physical_table(table, fields)
        return table, schema_version

    async def _next_schema_version(self, event_type_id: int, fields: List[Dict[str, Any]]) -> int:
        row = await self._fetchrow(
            f'''
            SELECT schema_version, fields
            FROM "{self._schema}".event_type_schema
            WHERE event_type_id = $1
            ORDER BY schema_version DESC
            LIMIT 1
            ''',
            event_type_id,
        )
        if row is None:
            return 1
        current_fields = row["fields"]
        if isinstance(current_fields, str):
            import json

            current_fields = json.loads(current_fields)
        if current_fields == fields:
            return int(row["schema_version"])
        return int(row["schema_version"]) + 1

    async def get_storage_table(self, event_type_id: int) -> Optional[str]:
        tables = await self.get_storage_tables([int(event_type_id)])
        return tables.get(int(event_type_id))

    async def get_storage_tables(self, event_type_ids: Iterable[int]) -> Dict[int, str]:
        """physical_table сразу для набора типов: на чтении это был запрос на каждый тип."""
        ids = sorted({int(i) for i in event_type_ids})
        if not ids:
            return {}
        rows = await self._fetch(
            f'''
            SELECT event_type_id, physical_table
            FROM "{self._schema}".event_type_storage
            WHERE event_type_id = ANY($1::bigint[])
            ''',
            ids,
        )
        return {
            int(row["event_type_id"]): str(row["physical_table"])
            for row in rows
            if row["physical_table"]
        }

    async def get_allowed_fields(self, event_type_ids: List[int]) -> Set[str]:
        if not event_type_ids:
            return set()
        configured = set(self._events_config.sql_filter_fields)
        if configured:
            return configured
        rows = await self._fetch(
            f'''
            SELECT DISTINCT ON (event_type_id) fields
            FROM "{self._schema}".event_type_schema
            WHERE event_type_id = ANY($1::bigint[])
            ORDER BY event_type_id, schema_version DESC
            ''',
            event_type_ids,
        )
        allowed: Set[str] = set()
        for row in rows:
            fields = row["fields"]
            if isinstance(fields, str):
                import json

                fields = json.loads(fields)
            for fld in fields or []:
                name = fld.get("name")
                if name:
                    allowed.add(str(name))
        return allowed

    async def get_schema_fields_for_event_type(self, event_type_id: int) -> Set[str]:
        row = await self._fetchrow(
            f'''
            SELECT fields
            FROM "{self._schema}".event_type_schema
            WHERE event_type_id = $1
            ORDER BY schema_version DESC
            LIMIT 1
            ''',
            int(event_type_id),
        )
        if not row:
            return set()
        fields = row["fields"]
        if isinstance(fields, str):
            import json

            fields = json.loads(fields)
        names: Set[str] = set()
        for fld in fields or []:
            name = fld.get("name") if isinstance(fld, dict) else None
            if name:
                names.add(str(name))
        return names

    async def get_schema_fields_for_event_types(
        self, event_type_ids: Iterable[int]
    ) -> Dict[int, Set[str]]:
        """Имена полей последней схемы сразу для набора типов.

        На чтении это были два отдельных прохода по одному запросу на тип
        (_common_pushdown_fields и _event_type_ids_with_fields).
        """
        ids = sorted({int(i) for i in event_type_ids})
        if not ids:
            return {}
        rows = await self._fetch(
            f'''
            SELECT DISTINCT ON (event_type_id) event_type_id, fields
            FROM "{self._schema}".event_type_schema
            WHERE event_type_id = ANY($1::bigint[])
            ORDER BY event_type_id, schema_version DESC
            ''',
            ids,
        )
        result: Dict[int, Set[str]] = {int(i): set() for i in ids}
        for row in rows:
            result[int(row["event_type_id"])] = self._field_names(row["fields"])
        return result

    @staticmethod
    def _field_names(fields: Any) -> Set[str]:
        if isinstance(fields, str):
            import json

            fields = json.loads(fields)
        names: Set[str] = set()
        for fld in fields or []:
            name = fld.get("name") if isinstance(fld, dict) else None
            if name:
                names.add(str(name))
        return names

    def trgm_candidate_columns(self) -> Set[str]:
        """Колонки, для которых имеет смысл GIN trgm: indexed_fields плюс их алиасы."""
        return expand_sql_filter_fields(
            set(self._events_config.indexed_fields),
            self._events_config.field_aliases,
        )

    async def plan_trgm_indexes(self) -> List[Dict[str, str]]:
        """Какие trgm-индексы отсутствуют: [{table, column, index, ddl}].

        _ensure_physical_table создаёт индекс по колонке только в ветке «колонка
        только что добавлена», поэтому для уже существующих колонок нужен этот
        догоняющий проход.
        """
        columns = self.trgm_candidate_columns()
        if not columns:
            return []
        rows = await self._fetch(
            TRGM_CANDIDATES_SQL.format(schema=self._schema),
            self._schema,
            sorted(columns),
            list(TRGM_COLUMN_TYPES),
        )
        candidates = [
            (str(row["table_name"]), str(row["column_name"]))
            for row in rows
        ]
        if not candidates:
            return []
        names = sorted({trgm_index_name(t, c) for t, c in candidates})
        existing_rows = await self._fetch(INDEX_VALIDITY_SQL, self._schema, names)
        # Прерванный CREATE INDEX CONCURRENTLY оставляет индекс с indisvalid = false:
        # имя занято, но планировщик его не использует.
        validity = {
            str(row["indexname"]): bool(row.get("indisvalid", True))
            for row in existing_rows
        }
        planned: List[Dict[str, Any]] = []
        for table, column in candidates:
            index = trgm_index_name(table, column)
            if validity.get(index):
                continue
            planned.append(
                {
                    "table": table,
                    "column": column,
                    "index": index,
                    "ddl": trgm_index_ddl(self._schema, table, column),
                    "invalid": index in validity,
                }
            )
        return planned

    async def _ensure_physical_table(self, table: str, fields: List[Dict[str, Any]]) -> None:
        known = self._known_columns.get(table)
        if known is not None and all(fld["name"] in known for fld in fields):
            # Все колонки уже есть: ни advisory lock, ни DDL, ни information_schema
            # на каждую запись события.
            return
        lock_key = abs(hash(table)) % (2**31 - 1)
        await self._execute("SELECT pg_advisory_lock($1)", lock_key)
        try:
            await self._execute(
                f'''
                CREATE TABLE IF NOT EXISTS "{self._schema}"."{table}" (
                    event_id BIGINT NOT NULL,
                    event_timestamp TIMESTAMPTZ NOT NULL,
                    source_id BIGINT NOT NULL,
                    PRIMARY KEY (event_id, event_timestamp)
                )
                '''
            )
            existing = await self._fetch(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2
                """,
                self._schema,
                table,
            )
            existing_names = {r["column_name"] for r in existing}
            trgm_columns = self.trgm_candidate_columns() if self._trgm_index_enabled else set()
            for fld in fields:
                name = fld["name"]
                if name in existing_names:
                    continue
                sql_type = fld.get("sql_type", "TEXT")
                await self._execute(
                    f'ALTER TABLE "{self._schema}"."{table}" ADD COLUMN IF NOT EXISTS "{name}" {sql_type}'
                )
                existing_names.add(name)
                if fld.get("index"):
                    idx_name = f"idx_{table}_{name}"[:58]
                    await self._execute(
                        f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{self._schema}"."{table}" ("{name}")'
                    )
                # btree бесполезен для ILIKE '%...%', поэтому текстовым колонкам
                # из indexed_fields сразу даём GIN trgm.
                if name in trgm_columns and str(sql_type).upper().startswith("TEXT"):
                    await self._ensure_trgm_index(table, name)
            await self._execute(
                f'''
                CREATE INDEX IF NOT EXISTS "idx_{table}_source_ts"
                ON "{self._schema}"."{table}" (source_id, event_timestamp DESC, event_id DESC)
                '''
            )
            self._known_columns[table] = existing_names
        finally:
            await self._execute("SELECT pg_advisory_unlock($1)", lock_key)

    async def _ensure_trgm_index(self, table: str, column: str) -> None:
        """Создание trgm-индекса не должно валить запись события, если pg_trgm нет."""
        try:
            await self._execute(trgm_index_ddl(self._schema, table, column))
        except Exception as e:
            self._logger.warning(
                "Cannot create trgm index on %s.%s (pg_trgm installed?): %r",
                table,
                column,
                e,
            )

    async def ensure_columns_from_typed_values(
        self, table: str, typed_values: Dict[str, Any]
    ) -> None:
        """Добавляет в typed-таблицу колонки, отсутствующие в схеме (lazy migration)."""
        skip = frozenset({"Time", "EventType", "SourceNode", "ReceiveTime", "LocalTime"})
        indexed = self._events_config.indexed_fields
        aliases = self._events_config.field_aliases
        fields: List[Dict[str, Any]] = []
        for key in typed_values:
            if key in skip:
                continue
            column = str(aliases.get(key, key))
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", column):
                continue
            fields.append(
                {
                    "name": column,
                    "sql_type": "TEXT",
                    "index": column in indexed or key in indexed,
                }
            )
        if fields:
            await self._ensure_physical_table(table, fields)

    async def insert_typed_row(
        self,
        table: str,
        event_id: int,
        event_timestamp: Any,
        source_id: int,
        typed_values: Dict[str, Any],
        *,
        ensure_columns: bool = True,
    ) -> None:
        """Вставка строки в typed-таблицу.

        ensure_columns=False — для батчевых вызовов, которые уже вызвали
        ensure_columns_from_typed_values один раз на объединение ключей батча:
        иначе advisory lock, DDL и запрос к information_schema.columns выполняются
        на каждую строку.
        """
        if ensure_columns:
            await self.ensure_columns_from_typed_values(table, typed_values)
        aliases = self._events_config.field_aliases
        base_cols = ["event_id", "event_timestamp", "source_id"]
        base_vals = [event_id, event_timestamp, source_id]
        extra_cols: List[str] = []
        extra_vals: List[Any] = []
        for key, value in typed_values.items():
            if key in ("Time", "EventType", "SourceNode", "ReceiveTime", "LocalTime"):
                continue
            column = str(aliases.get(key, key))
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", column):
                continue
            extra_cols.append(f'"{column}"')
            extra_vals.append(python_value_to_sql(value))
        col_sql = ", ".join(
            ['"event_id"', '"event_timestamp"', '"source_id"'] + extra_cols
        )
        placeholders = ", ".join(f"${i}" for i in range(1, len(base_cols) + len(extra_cols) + 1))
        await self._execute(
            f'''
            INSERT INTO "{self._schema}"."{table}" ({col_sql})
            VALUES ({placeholders})
            ON CONFLICT (event_id, event_timestamp) DO NOTHING
            ''',
            *base_vals,
            *extra_vals,
        )
