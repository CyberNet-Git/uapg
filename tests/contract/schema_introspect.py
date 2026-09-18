"""Нормализованный снимок схемы PostgreSQL/TimescaleDB.

Используется и при снятии эталона со старого кода, и контрактным тестом нового:
обе стороны обязаны видеть схему одинаково, поэтому интроспекция живёт в одном месте.

Снимок не зависит от имени схемы (оно заменяется на ``{schema}``) и от чанков
TimescaleDB, которые создаются по мере поступления данных.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List

import asyncpg


async def snapshot_schema(conn: asyncpg.Connection, schema: str) -> Dict[str, Any]:
    """Собрать нормализованный снимок объектов схемы."""
    return {
        "tables": await _tables(conn, schema),
        "indexes": await _indexes(conn, schema),
        "constraints": await _constraints(conn, schema),
        "routines": await _routines(conn, schema),
        "views": await _views(conn, schema),
        "timescale": await _timescale(conn, schema),
    }


def _char(value: Any) -> str | None:
    """Колонки типа "char" asyncpg отдаёт байтами.

    Отсутствие свойства PostgreSQL кодирует символом NUL (``attidentity`` и
    ``attgenerated`` у обычной колонки), а не пустой строкой.
    """
    if value is None:
        return None
    if isinstance(value, bytes):
        value = value.decode()
    text = str(value)
    if text in ("", "\x00"):
        return None
    return text


def _as_json(value: Any) -> Any:
    """JSONB приходит строкой, пока не зарегистрирован кодек asyncpg."""
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


def _normalize_schema(text: str | None, schema: str) -> str | None:
    if text is None:
        return None
    return re.sub(rf'\b"?{re.escape(schema)}"?\.', "{schema}.", text)


async def _tables(conn: asyncpg.Connection, schema: str) -> Dict[str, List[Dict[str, Any]]]:
    rows = await conn.fetch(
        """
        SELECT c.relname AS table_name,
               a.attname AS column_name,
               format_type(a.atttypid, a.atttypmod) AS data_type,
               a.attnotnull AS not_null,
               pg_get_expr(d.adbin, d.adrelid) AS default_expr,
               a.attidentity AS identity,
               a.attgenerated AS generated
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
        LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
        WHERE n.nspname = $1 AND c.relkind = 'r'
          AND NOT EXISTS (
              SELECT 1 FROM pg_depend d
              WHERE d.objid = c.oid AND d.deptype = 'e'
          )
        ORDER BY c.relname, a.attnum
        """,
        schema,
    )
    tables: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        tables.setdefault(row["table_name"], []).append(
            {
                "column": row["column_name"],
                "type": row["data_type"],
                "not_null": row["not_null"],
                "default": _normalize_schema(row["default_expr"], schema),
                "identity": _char(row["identity"]),
                "generated": _char(row["generated"]),
            }
        )
    return tables


async def _indexes(conn: asyncpg.Connection, schema: str) -> Dict[str, List[str]]:
    rows = await conn.fetch(
        """
        SELECT tablename, indexdef
        FROM pg_indexes
        WHERE schemaname = $1
        ORDER BY tablename, indexname
        """,
        schema,
    )
    indexes: Dict[str, List[str]] = {}
    for row in rows:
        definition = _normalize_schema(row["indexdef"], schema)
        assert definition is not None
        indexes.setdefault(row["tablename"], []).append(definition)
    return indexes


async def _constraints(conn: asyncpg.Connection, schema: str) -> Dict[str, List[str]]:
    rows = await conn.fetch(
        """
        SELECT c.relname AS table_name,
               con.conname AS name,
               pg_get_constraintdef(con.oid) AS definition
        FROM pg_constraint con
        JOIN pg_class c ON c.oid = con.conrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1
        ORDER BY c.relname, con.conname
        """,
        schema,
    )
    constraints: Dict[str, List[str]] = {}
    for row in rows:
        definition = _normalize_schema(f'{row["name"]}: {row["definition"]}', schema)
        assert definition is not None
        constraints.setdefault(row["table_name"], []).append(definition)
    return constraints


async def _routines(conn: asyncpg.Connection, schema: str) -> Dict[str, Dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT p.proname AS name,
               pg_get_function_identity_arguments(p.oid) AS args,
               pg_get_function_result(p.oid) AS result,
               p.prokind AS kind,
               p.provolatile AS volatility
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = $1
          AND NOT EXISTS (
              SELECT 1 FROM pg_depend d
              WHERE d.objid = p.oid AND d.deptype = 'e'
          )
        ORDER BY p.proname, pg_get_function_identity_arguments(p.oid)
        """,
        schema,
    )
    routines: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        signature = f'{row["name"]}({row["args"]})'
        routines[signature] = {
            "result": row["result"],
            "kind": _char(row["kind"]),
            "volatility": _char(row["volatility"]),
        }
    return routines


async def _views(conn: asyncpg.Connection, schema: str) -> Dict[str, str]:
    rows = await conn.fetch(
        """
        SELECT c.relname AS name, c.relkind AS kind
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relkind IN ('v', 'm')
          AND NOT EXISTS (
              SELECT 1 FROM pg_depend d
              WHERE d.objid = c.oid AND d.deptype = 'e'
          )
        ORDER BY c.relname
        """,
        schema,
    )
    return {row["name"]: _char(row["kind"]) for row in rows}


async def _timescale(conn: asyncpg.Connection, schema: str) -> Dict[str, Any]:
    available = await conn.fetchval(
        "SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'"
    )
    if not available:
        return {"available": False}

    hypertables = await conn.fetch(
        """
        SELECT hypertable_name
        FROM timescaledb_information.hypertables
        WHERE hypertable_schema = $1
        ORDER BY hypertable_name
        """,
        schema,
    )
    dimensions = await conn.fetch(
        """
        SELECT hypertable_name, dimension_number, column_name, column_type,
               dimension_type, time_interval, num_partitions
        FROM timescaledb_information.dimensions
        WHERE hypertable_schema = $1
        ORDER BY hypertable_name, dimension_number
        """,
        schema,
    )
    jobs = await conn.fetch(
        """
        SELECT proc_name, hypertable_name, config, schedule_interval
        FROM timescaledb_information.jobs
        WHERE hypertable_schema = $1
        ORDER BY proc_name, hypertable_name
        """,
        schema,
    )
    caggs = await conn.fetch(
        """
        SELECT view_name, materialization_hypertable_name, compression_enabled
        FROM timescaledb_information.continuous_aggregates
        WHERE view_schema = $1
        ORDER BY view_name
        """,
        schema,
    )
    return {
        "available": True,
        "hypertables": [row["hypertable_name"] for row in hypertables],
        "dimensions": [
            {
                "hypertable": row["hypertable_name"],
                "number": row["dimension_number"],
                "column": row["column_name"],
                "column_type": row["column_type"],
                "type": row["dimension_type"],
                "time_interval": str(row["time_interval"]) if row["time_interval"] else None,
                "num_partitions": row["num_partitions"],
            }
            for row in dimensions
        ],
        "jobs": [
            {
                "proc": row["proc_name"],
                "hypertable": row["hypertable_name"],
                "config": _as_json(row["config"]),
                "schedule_interval": str(row["schedule_interval"]),
            }
            for row in jobs
        ],
        "continuous_aggregates": [
            {
                "view": row["view_name"],
                "compression_enabled": row["compression_enabled"],
            }
            for row in caggs
        ],
    }
