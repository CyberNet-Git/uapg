"""OPC UA event type introspection and typed table DDL."""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, FrozenSet, List, Mapping, Optional, Set, Tuple

from asyncua import ua

from .events_config import EventsV2Config

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
    ) -> None:
        self._schema = schema
        self._execute = execute
        self._fetch = fetch
        self._fetchrow = fetchrow
        self._fetchval = fetchval
        self._logger = logger or _logger
        self._events_config = events_config or EventsV2Config()

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
        return await self._fetchval(
            f'''
            SELECT physical_table
            FROM "{self._schema}".event_type_storage
            WHERE event_type_id = $1
            ''',
            event_type_id,
        )

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

    async def _ensure_physical_table(self, table: str, fields: List[Dict[str, Any]]) -> None:
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
            for fld in fields:
                name = fld["name"]
                if name in existing_names:
                    continue
                sql_type = fld.get("sql_type", "TEXT")
                await self._execute(
                    f'ALTER TABLE "{self._schema}"."{table}" ADD COLUMN IF NOT EXISTS "{name}" {sql_type}'
                )
                if fld.get("index"):
                    idx_name = f"idx_{table}_{name}"[:58]
                    await self._execute(
                        f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{self._schema}"."{table}" ("{name}")'
                    )
            await self._execute(
                f'''
                CREATE INDEX IF NOT EXISTS "idx_{table}_source_ts"
                ON "{self._schema}"."{table}" (source_id, event_timestamp DESC, event_id DESC)
                '''
            )
        finally:
            await self._execute("SELECT pg_advisory_unlock($1)", lock_key)

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
    ) -> None:
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
