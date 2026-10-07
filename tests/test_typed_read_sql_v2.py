"""Регрессия: индексируемый ILIKE и ранний выход из UNION ALL.

На стенде HistoryRead с `techplace ILIKE '%77-12-29%'` выбирался медленно по двум
независимым причинам: btree не обслуживает шаблон с ведущим `%`, а ORDER BY и LIMIT
стояли только снаружи UNION ALL (в ветки PostgreSQL их не заносит), из-за чего каждая
ветка материализовала всё окно и фильтр применялся уже после JOIN с events_ts.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from uapg.history_timescale_v2 import HistoryTimescaleV2
from uapg.v2.event_store import EventStoreV2
from uapg.v2.events_config import EventsV2Config
from uapg.v2.schema_registry import (
    EventSchemaRegistry,
    trgm_index_ddl,
    trgm_index_name,
)
from uapg.v2.sql_migrator import MIGRATION_ORDER, load_migration_sql
from uapg.v2.storage_mode import StorageMode

W0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
W1 = datetime(2026, 5, 1, tzinfo=timezone.utc)
ILIKE_PLAN = {"field": "techplace", "op": "ilike", "value": "%77-12-29%"}


# --------------------------------------------------------------------------
# Форма typed-чтения
# --------------------------------------------------------------------------


def _make_store(tables: Dict[int, str]):
    calls: List[tuple] = []

    class _Pool:
        async def fetch(self, sql, *args):
            calls.append((sql, args))
            return []

    registry = MagicMock(spec=EventSchemaRegistry)
    registry.events_config = EventsV2Config.from_csv(indexed="techplace")
    registry.get_storage_tables = AsyncMock(return_value=dict(tables))
    registry.get_storage_table = AsyncMock(
        side_effect=lambda tid: tables.get(int(tid))
    )
    store = EventStoreV2("history", _Pool(), registry, MagicMock())
    return store, calls


async def _multi(limit=50, order="DESC", cursor=None, tables=None):
    store, calls = _make_store(tables or {10: "evt_a", 11: "evt_b"})
    await store._read_typed_multi_rows(
        1,
        sorted((tables or {10: "evt_a", 11: "evt_b"}).keys()),
        W0,
        W1,
        limit,
        order,
        ILIKE_PLAN,
        {"techplace"},
        cursor[0] if cursor else None,
        cursor[1] if cursor else None,
    )
    return calls[-1]


@pytest.mark.asyncio
async def test_branch_carries_its_own_order_and_limit():
    sql, args = await _multi()
    # Три LIMIT $4: по одному на ветку плюс внешний.
    assert sql.count("LIMIT $4") == 3
    assert sql.count("ORDER BY t.event_timestamp DESC, t.event_id DESC") == 2
    assert "ORDER BY m.event_timestamp DESC, m.event_id DESC" in sql
    assert args[3] == 50


@pytest.mark.asyncio
async def test_branch_reads_only_typed_table_join_after_merge():
    sql, _args = await _multi()
    inner, outer = sql.split(") m", 1)
    # Внутри ветвей events_ts нет — фильтр идёт по ведущему отношению.
    assert "events_ts" not in inner
    assert 'INNER JOIN "history".events_ts e' in outer
    assert "e.source_id = $1" in outer
    assert "e.event_timestamp = m.event_timestamp" in outer
    assert "e.event_id = m.event_id" in outer


@pytest.mark.asyncio
async def test_filter_is_rendered_on_typed_alias():
    sql, args = await _multi()
    assert 't."techplace" ILIKE $6' in sql
    assert 't."techplace" ILIKE $8' in sql
    assert args[5] == "%77-12-29%"
    assert args[7] == "%77-12-29%"


@pytest.mark.asyncio
async def test_each_branch_pins_its_event_type():
    sql, args = await _multi()
    assert "$5::bigint AS event_type_id" in sql
    assert "$7::bigint AS event_type_id" in sql
    assert args[4] == 10
    assert args[6] == 11
    assert sql.count("UNION ALL") == 1


@pytest.mark.asyncio
async def test_cursor_goes_inside_each_branch():
    cursor = (datetime(2026, 3, 1, tzinfo=timezone.utc), 777)
    sql, args = await _multi(cursor=cursor)
    inner, outer = sql.split(") m", 1)
    assert inner.count("t.event_timestamp < $") == 2
    # Снаружи курсора больше нет: раньше он фильтровал уже слитый результат.
    assert "event_timestamp < $" not in outer
    assert args.count(cursor[0]) == 2
    assert args.count(777) == 2


@pytest.mark.asyncio
async def test_ascending_order_flips_branch_and_merge():
    sql, _args = await _multi(order="ASC")
    assert sql.count("ORDER BY t.event_timestamp ASC, t.event_id ASC") == 2
    assert "ORDER BY m.event_timestamp ASC, m.event_id ASC" in sql


@pytest.mark.asyncio
async def test_single_type_read_uses_same_shape():
    store, calls = _make_store({42: "evt_single"})
    await store._read_typed_rows(
        1, 42, W0, W1, 25, "DESC", ILIKE_PLAN, None, None
    )
    sql, args = calls[-1]
    assert "UNION ALL" not in sql
    assert "$5::bigint AS event_type_id" in sql
    assert 't."techplace" ILIKE $6' in sql
    assert sql.count("LIMIT $4") == 2
    inner, outer = sql.split(") m", 1)
    assert "events_ts" not in inner
    assert 'INNER JOIN "history".events_ts e' in outer
    assert args[:6] == (1, W0, W1, 25, 42, "%77-12-29%")


@pytest.mark.asyncio
async def test_type_without_storage_table_is_skipped():
    sql, args = await _multi(tables={10: "evt_a", 11: None})  # type: ignore[dict-item]
    assert sql.count("UNION ALL") == 0
    assert "evt_a" in sql


# --------------------------------------------------------------------------
# Метаданные одним запросом
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_read_resolves_metadata_without_per_type_queries():
    """Было 3×K обращений на чтение: схемы полей дважды и typed-таблица на тип."""
    store, _calls = _make_store({10: "evt_a", 11: "evt_b", 12: "evt_c"})
    store._registry.get_schema_fields_for_event_types = AsyncMock(
        return_value={i: {"techplace"} for i in (10, 11, 12)}
    )
    store._registry.get_schema_fields_for_event_type = AsyncMock(
        side_effect=AssertionError("per-type schema query must not be used")
    )
    store._registry.get_storage_table = AsyncMock(
        side_effect=AssertionError("per-type storage query must not be used")
    )

    rows, partial = await store._fetch_rows(
        1, W0, W1, 50, "DESC", ILIKE_PLAN, [10, 11, 12], {"techplace"}, None
    )

    assert rows == []
    assert partial is False
    assert store._registry.get_schema_fields_for_event_types.await_count == 1
    assert store._registry.get_storage_tables.await_count == 1


# --------------------------------------------------------------------------
# Планирование trgm-индексов
# --------------------------------------------------------------------------


def _make_registry(columns_rows, existing_indexes=(), *, indexed="techplace", aliases=None):
    queries: List[str] = []

    async def fetch(sql, *args):
        queries.append(sql)
        if "information_schema.columns" in sql:
            return list(columns_rows)
        if "pg_indexes" in sql:
            return [{"indexname": n} for n in existing_indexes]
        return []

    cfg = EventsV2Config.from_csv(indexed=indexed, aliases=aliases)
    registry = EventSchemaRegistry(
        "history", AsyncMock(), fetch, AsyncMock(), AsyncMock(), events_config=cfg
    )
    return registry, queries


@pytest.mark.asyncio
async def test_plan_trgm_indexes_builds_gin_ddl():
    registry, _q = _make_registry(
        [{"table_name": "evt_a", "column_name": "techplace"}]
    )
    planned = await registry.plan_trgm_indexes()
    assert len(planned) == 1
    assert planned[0]["table"] == "evt_a"
    assert planned[0]["column"] == "techplace"
    assert planned[0]["index"] == "idx_evt_a_techplace_trgm"
    assert 'USING gin ("techplace" gin_trgm_ops)' in planned[0]["ddl"]
    assert 'ON "history"."evt_a"' in planned[0]["ddl"]


@pytest.mark.asyncio
async def test_plan_trgm_indexes_filters_by_column_type_in_sql():
    """Нетекстовые колонки отсекает сам запрос — GIN trgm к ним неприменим."""
    registry, queries = _make_registry([])
    assert await registry.plan_trgm_indexes() == []
    columns_sql = next(q for q in queries if "information_schema.columns" in q)
    assert "c.data_type = ANY($3::text[])" in columns_sql
    assert "c.column_name = ANY($2::text[])" in columns_sql


@pytest.mark.asyncio
async def test_plan_trgm_indexes_skips_existing():
    registry, _q = _make_registry(
        [
            {"table_name": "evt_a", "column_name": "techplace"},
            {"table_name": "evt_b", "column_name": "techplace"},
        ],
        existing_indexes=["idx_evt_a_techplace_trgm"],
    )
    planned = await registry.plan_trgm_indexes()
    assert [p["table"] for p in planned] == ["evt_b"]


@pytest.mark.asyncio
async def test_plan_trgm_indexes_honours_field_aliases():
    registry, _q = _make_registry(
        [{"table_name": "evt_a", "column_name": "tech_place"}],
        indexed="techplace",
        aliases="techplace:tech_place",
    )
    assert registry.trgm_candidate_columns() == {"techplace", "tech_place"}
    planned = await registry.plan_trgm_indexes()
    assert [p["column"] for p in planned] == ["tech_place"]


@pytest.mark.asyncio
async def test_plan_trgm_indexes_empty_without_indexed_fields():
    registry, queries = _make_registry(
        [{"table_name": "evt_a", "column_name": "techplace"}], indexed=""
    )
    assert await registry.plan_trgm_indexes() == []
    assert queries == []


def test_trgm_index_name_fits_identifier_limit_and_is_stable():
    short = trgm_index_name("evt_alarm", "techplace")
    assert short == "idx_evt_alarm_techplace_trgm"

    table = "evt_" + "x" * 48
    column = "a_very_long_technological_place_column_name"
    name = trgm_index_name(table, column)
    assert len(name.encode("utf-8")) <= 63
    assert name == trgm_index_name(table, column)
    assert name != trgm_index_name(table, column + "2")
    assert trgm_index_ddl("history", table, column).startswith(
        f'CREATE INDEX IF NOT EXISTS "{name}"'
    )


# --------------------------------------------------------------------------
# Создание индексов на старте
# --------------------------------------------------------------------------


def _make_history(**kwargs) -> HistoryTimescaleV2:
    history = HistoryTimescaleV2(
        user="u",
        password="p",
        database="d",
        host="h",
        port=5432,
        events_storage_mode=StorageMode.DUAL,
        **kwargs,
    )
    history._pool = SimpleNamespace(_closed=False, _closing=False)
    history._v2_ready = True
    history._registry = MagicMock(spec=EventSchemaRegistry)
    history._registry.plan_trgm_indexes = AsyncMock(
        return_value=[
            {
                "table": "evt_a",
                "column": "techplace",
                "index": "idx_evt_a_techplace_trgm",
                "ddl": trgm_index_ddl("history", "evt_a", "techplace"),
            }
        ]
    )
    return history


@pytest.mark.asyncio
async def test_trgm_indexes_created_on_start():
    history = _make_history()
    executed: List[tuple] = []

    async def fake_execute(_self, sql, timeout):
        executed.append((sql, timeout))
        return True

    with patch.object(HistoryTimescaleV2, "_best_effort_execute", fake_execute), patch.object(
        HistoryTimescaleV2, "_probe_fetchval", AsyncMock(return_value=1)
    ):
        await history._ensure_trgm_indexes()

    assert any("gin_trgm_ops" in sql for sql, _ in executed)
    # DDL получает собственный бюджет, а не db_query_timeout_sec.
    ddl_timeout = next(t for sql, t in executed if "gin_trgm_ops" in sql)
    assert ddl_timeout == 300.0
    metrics = history.get_performance_metrics()["events_v2"]
    assert metrics["trgm_extension_available"] is True
    assert metrics["trgm_indexes_created_total"] == 1
    assert metrics["trgm_indexes_missing"] == 0


@pytest.mark.asyncio
async def test_trgm_indexes_skipped_when_disabled():
    history = _make_history(events_trgm_index_enabled=False)
    execute = AsyncMock(return_value=True)

    with patch.object(HistoryTimescaleV2, "_best_effort_execute", execute):
        await history._ensure_trgm_indexes()

    execute.assert_not_awaited()
    history._registry.plan_trgm_indexes.assert_not_awaited()
    assert history.get_performance_metrics()["events_v2"]["trgm_index_enabled"] is False


@pytest.mark.asyncio
async def test_missing_pg_trgm_warns_without_creating_or_reconnecting():
    history = _make_history()
    executed: List[str] = []

    async def fake_execute(_self, sql, timeout):
        executed.append(sql)
        return False  # CREATE EXTENSION не разрешён

    reconnect = AsyncMock()
    with patch.object(HistoryTimescaleV2, "_best_effort_execute", fake_execute), patch.object(
        HistoryTimescaleV2, "_probe_fetchval", AsyncMock(return_value=None)
    ), patch.object(HistoryTimescaleV2, "_force_reconnect", reconnect):
        await history._ensure_trgm_indexes()

    assert executed == ["CREATE EXTENSION IF NOT EXISTS pg_trgm"]
    assert not any("gin_trgm_ops" in sql for sql in executed)
    reconnect.assert_not_awaited()
    metrics = history.get_performance_metrics()["events_v2"]
    assert metrics["trgm_extension_available"] is False
    assert metrics["trgm_indexes_missing"] == 1


@pytest.mark.asyncio
async def test_failed_ddl_is_counted_not_raised():
    history = _make_history()

    async def fake_execute(_self, sql, timeout):
        return "pg_trgm" in sql  # расширение есть, индекс не создался

    with patch.object(HistoryTimescaleV2, "_best_effort_execute", fake_execute), patch.object(
        HistoryTimescaleV2, "_probe_fetchval", AsyncMock(return_value=1)
    ):
        await history._ensure_trgm_indexes()

    metrics = history.get_performance_metrics()["events_v2"]
    assert metrics["trgm_indexes_missing"] == 1
    assert metrics["trgm_index_failures_total"] == 1


# --------------------------------------------------------------------------
# Memo колонок: DDL-проход не на каждую запись
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_known_columns_memo_skips_repeated_ddl_pass():
    executed: List[str] = []

    async def execute(sql, *args):
        executed.append(sql)
        return None

    async def fetch(sql, *args):
        if "information_schema.columns" in sql:
            return [
                {"column_name": "event_id"},
                {"column_name": "event_timestamp"},
                {"column_name": "source_id"},
                {"column_name": "techplace"},
            ]
        return []

    registry = EventSchemaRegistry(
        "history",
        execute,
        fetch,
        AsyncMock(),
        AsyncMock(),
        events_config=EventsV2Config.from_csv(indexed="techplace"),
    )
    fields = [{"name": "techplace", "sql_type": "TEXT", "index": True}]

    await registry._ensure_physical_table("evt_a", fields)
    first = len([sql for sql in executed if "pg_advisory_lock" in sql])
    await registry._ensure_physical_table("evt_a", fields)
    second = len([sql for sql in executed if "pg_advisory_lock" in sql])

    assert first == 1
    assert second == 1  # второй проход не брал блокировку и не трогал DDL
    assert registry._known_columns["evt_a"] >= {"techplace", "source_id"}


@pytest.mark.asyncio
async def test_new_text_column_gets_trgm_index_immediately():
    executed: List[str] = []

    async def execute(sql, *args):
        executed.append(sql)
        return None

    async def fetch(sql, *args):
        return []  # колонок ещё нет

    registry = EventSchemaRegistry(
        "history",
        execute,
        fetch,
        AsyncMock(),
        AsyncMock(),
        events_config=EventsV2Config.from_csv(indexed="techplace"),
    )
    await registry._ensure_physical_table(
        "evt_a",
        [
            {"name": "techplace", "sql_type": "TEXT", "index": True},
            {"name": "severity", "sql_type": "INTEGER", "index": True},
        ],
    )

    trgm = [sql for sql in executed if "gin_trgm_ops" in sql]
    assert len(trgm) == 1
    assert "techplace" in trgm[0]
    # Для нетекстовой колонки trgm не нужен, обычный btree остаётся.
    assert any('"idx_evt_a_severity"' in sql for sql in executed)


@pytest.mark.asyncio
async def test_trgm_failure_does_not_break_column_creation():
    async def execute(sql, *args):
        if "gin_trgm_ops" in sql:
            raise RuntimeError("operator class gin_trgm_ops does not exist")
        return None

    async def fetch(sql, *args):
        return []

    registry = EventSchemaRegistry(
        "history",
        execute,
        fetch,
        AsyncMock(),
        AsyncMock(),
        events_config=EventsV2Config.from_csv(indexed="techplace"),
    )
    await registry._ensure_physical_table(
        "evt_a", [{"name": "techplace", "sql_type": "TEXT", "index": True}]
    )


# --------------------------------------------------------------------------
# Батчевые методы registry
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_batch_registry_lookups_use_one_query_each():
    queries: List[tuple] = []

    async def fetch(sql, *args):
        queries.append((sql, args))
        if "event_type_storage" in sql:
            return [
                {"event_type_id": 10, "physical_table": "evt_a"},
                {"event_type_id": 11, "physical_table": "evt_b"},
            ]
        return [
            {"event_type_id": 10, "fields": [{"name": "techplace"}]},
            {"event_type_id": 11, "fields": '[{"name": "message"}]'},
        ]

    registry = EventSchemaRegistry(
        "history", AsyncMock(), fetch, AsyncMock(), AsyncMock()
    )

    tables = await registry.get_storage_tables([11, 10, 10])
    assert tables == {10: "evt_a", 11: "evt_b"}

    fields = await registry.get_schema_fields_for_event_types([10, 11, 12])
    assert fields == {10: {"techplace"}, 11: {"message"}, 12: set()}
    assert len(queries) == 2
    assert all("ANY($1::bigint[])" in sql for sql, _ in queries)


@pytest.mark.asyncio
async def test_batch_registry_lookups_short_circuit_on_empty_input():
    fetch = AsyncMock()
    registry = EventSchemaRegistry("history", AsyncMock(), fetch, AsyncMock(), AsyncMock())
    assert await registry.get_storage_tables([]) == {}
    assert await registry.get_schema_fields_for_event_types([]) == {}
    fetch.assert_not_awaited()


# --------------------------------------------------------------------------
# Миграция 006
# --------------------------------------------------------------------------


def test_migration_006_is_sargable():
    assert "006_events_v2_read.sql" in MIGRATION_ORDER
    assert MIGRATION_ORDER.index("006_events_v2_read.sql") > MIGRATION_ORDER.index(
        "003_events_v2_functions.sql"
    )

    sql = load_migration_sql("006_events_v2_read.sql", "opcua_history")
    assert "{schema}" not in sql
    assert 'CREATE OR REPLACE FUNCTION "opcua_history".uapg_read_events_v2' in sql

    body = "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )
    # Ключ исправления: ORDER BY без CASE, иначе индекс не используется.
    assert "ORDER BY" in body
    assert "CASE" not in body
    assert body.count("RETURN QUERY") == 4
    assert body.count("ORDER BY e.event_timestamp DESC, e.event_id DESC") == 2
    assert body.count("ORDER BY e.event_timestamp ASC, e.event_id ASC") == 2
    # Тай-брейк записан строковым сравнением — его PostgreSQL берёт как индексный предикат.
    assert body.count("(e.event_timestamp, e.event_id)") == 2
