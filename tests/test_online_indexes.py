"""Онлайн-обслуживание индексов: каталог, план, сборка и CLI."""

from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from uapg.history_timescale_v2 import HistoryTimescaleV2
from uapg.maintenance import indexes_cli
from uapg.maintenance.online_indexes import (
    CORE_INDEX_SPECS,
    SCOPE_TRGM,
    V2_INDEX_SPECS,
    IndexPlan,
    IndexPlanItem,
    IndexSpec,
    apply_plan,
    build_plan,
    find_index_spec,
    trgm_index_spec,
)
from uapg.v2.events_config import EventsV2Config
from uapg.v2.schema_registry import EventSchemaRegistry, trgm_index_ddl
from uapg.v2.storage_mode import StorageMode


class FakeConn:
    """Минимальный asyncpg-двойник: ответы выбираются по фрагментам SQL."""

    def __init__(
        self,
        *,
        extensions=("timescaledb",),
        relations=("events_ts", "event_type_storage"),
        tables=("variables_history", "events_history", "variable_metadata", "event_sources",
                "event_types", "variables_last_value", "evt_alarm"),
        hypertables=("variables_history", "events_history"),
        indexes: Optional[Dict[str, bool]] = None,
        trgm_rows=(),
        fail_on: str = "",
        in_transaction: bool = False,
    ) -> None:
        self.extensions = set(extensions)
        self.relations = set(relations)
        self.tables = set(tables)
        self.hypertables = set(hypertables)
        self.indexes = dict(indexes or {})
        self.trgm_rows = list(trgm_rows)
        self.fail_on = fail_on
        self.in_transaction = in_transaction
        self.executed: List[str] = []

    def is_in_transaction(self) -> bool:
        return self.in_transaction

    async def fetchval(self, sql: str, *args: Any) -> Any:
        if "pg_extension" in sql:
            return 1 if args[0] in self.extensions else None
        if "to_regclass" in sql:
            return f"{args[0]}.{args[1]}" if args[1] in self.relations else None
        raise AssertionError(sql)

    async def fetch(self, sql: str, *args: Any) -> List[Dict[str, Any]]:
        if "information_schema.columns" in sql:
            return list(self.trgm_rows)
        if "relkind" in sql:
            return [{"relname": t} for t in args[1] if t in self.tables]
        if "timescaledb_information.hypertables" in sql:
            return [{"hypertable_name": t} for t in self.hypertables]
        if "pg_index" in sql:
            return [{"relname": n, "indisvalid": v} for n, v in self.indexes.items() if n in args[1]]
        raise AssertionError(sql)

    async def execute(self, sql: str, *args: Any) -> str:
        self.executed.append(sql)
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("canceling statement due to lock timeout")
        return "OK"


def _all_present(**overrides: bool) -> Dict[str, bool]:
    result = {s.name: True for s in (*CORE_INDEX_SPECS, *V2_INDEX_SPECS)}
    result.update(overrides)
    return result


# --------------------------------------------------------------------------
# Каталог и DDL
# --------------------------------------------------------------------------


def test_catalog_names_are_unique_and_lookup_works():
    names = [s.name for s in (*CORE_INDEX_SPECS, *V2_INDEX_SPECS)]
    assert len(names) == len(set(names))
    assert len(CORE_INDEX_SPECS) == 24
    assert find_index_spec("idx_events_history_id").table == "events_history"
    assert find_index_spec("idx_nope") is None


def test_create_sql_startup_plain_and_online_variants():
    spec = IndexSpec("idx_a", "t", "(x)", unique=True)
    assert spec.create_sql("h") == 'CREATE UNIQUE INDEX IF NOT EXISTS "idx_a" ON "h"."t" (x)'
    assert spec.create_sql("h", online=True) == (
        'CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "idx_a" ON "h"."t" (x)'
    )
    hyper = spec.create_sql("h", online=True, hypertable=True)
    assert "CONCURRENTLY" not in hyper
    assert hyper.endswith("(x) WITH (timescaledb.transaction_per_chunk)")
    assert spec.drop_sql("h", online=True) == 'DROP INDEX CONCURRENTLY IF EXISTS "h"."idx_a"'
    assert spec.drop_sql("h", online=True, hypertable=True) == 'DROP INDEX IF EXISTS "h"."idx_a"'


def test_covering_index_keeps_include_before_with_clause():
    spec = find_index_spec("idx_variables_history_vid_ts_desc_covering")
    sql = spec.create_sql("h", online=True, hypertable=True)
    assert "INCLUDE (statuscode, varianttype, servertimestamp) WITH (timescaledb.transaction_per_chunk)" in sql


def test_trgm_spec_matches_startup_ddl_name_and_definition():
    spec = trgm_index_spec("evt_alarm", "techplace")
    assert spec.scope == SCOPE_TRGM
    assert spec.create_sql("h") == trgm_index_ddl("h", "evt_alarm", "techplace")


# --------------------------------------------------------------------------
# План
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_build_plan_reports_statuses_and_hypertables():
    conn = FakeConn(
        indexes=_all_present(idx_events_timestamp=False, idx_events_history_id=None),
        trgm_rows=[{"table_name": "evt_alarm", "column_name": "techplace"}],
    )
    del conn.indexes["idx_events_history_id"]
    plan = await build_plan(conn, "h", events_config=EventsV2Config.from_csv(indexed="techplace"))

    by_name = {i.spec.name: i for i in plan.items}
    assert by_name["idx_events_timestamp"].status == "invalid"
    assert by_name["idx_events_timestamp"].hypertable is True
    assert by_name["idx_events_history_id"].status == "missing"
    trgm = by_name["idx_evt_alarm_techplace_trgm"]
    assert trgm.status == "missing" and trgm.hypertable is False
    assert "CONCURRENTLY" in trgm.online_sql("h")
    assert by_name["idx_event_types_name"].status == "present"
    assert plan.timescaledb_available is True
    assert plan.trgm_extension_available is False
    assert {i.spec.name for i in plan.pending} == {
        "idx_events_timestamp",
        "idx_events_history_id",
        "idx_evt_alarm_techplace_trgm",
    }


@pytest.mark.asyncio
async def test_build_plan_without_v2_schema_skips_v2_and_trgm():
    conn = FakeConn(relations=(), indexes=_all_present())
    plan = await build_plan(conn, "h", events_config=EventsV2Config.from_csv(indexed="techplace"))
    assert {i.spec.scope for i in plan.items} == {"core"}
    assert plan.pending == []


@pytest.mark.asyncio
async def test_build_plan_scope_and_missing_table():
    conn = FakeConn(tables=("events_history",), indexes={})
    plan = await build_plan(conn, "h", scopes=["v2"])
    assert [(i.spec.name, i.status) for i in plan.items] == [("idx_events_history_id", "missing")]

    plan = await build_plan(conn, "h", scopes=["core"])
    statuses = {i.spec.table: i.status for i in plan.items}
    assert statuses["variables_history"] == "no_table"
    assert statuses["events_history"] == "missing"


@pytest.mark.asyncio
async def test_build_plan_rejects_unknown_scope():
    with pytest.raises(ValueError):
        await build_plan(FakeConn(), "h", scopes=["bogus"])


def _plan(*items: IndexPlanItem, trgm: bool = True) -> IndexPlan:
    return IndexPlan(schema="h", items=list(items), timescaledb_available=True, trgm_extension_available=trgm)


def test_sql_script_for_psql():
    plan = _plan(
        IndexPlanItem(find_index_spec("idx_events_timestamp"), "missing", hypertable=True),
        IndexPlanItem(trgm_index_spec("evt_a", "techplace"), "invalid"),
        IndexPlanItem(find_index_spec("idx_event_types_name"), "present"),
        trgm=False,
    )
    script = plan.sql_script(lock_timeout_sec=5)
    assert "SET statement_timeout = 0;" in script
    assert "SET lock_timeout = '5000ms';" in script
    assert "CREATE EXTENSION IF NOT EXISTS pg_trgm;" in script
    assert "WITH (timescaledb.transaction_per_chunk);" in script
    # INVALID без --fix-invalid только закомментирован.
    assert '-- DROP INDEX CONCURRENTLY IF EXISTS "h"."idx_evt_a_techplace_trgm";' in script
    assert "idx_event_types_name" not in script

    fixed = plan.sql_script(fix_invalid=True)
    assert '\nDROP INDEX CONCURRENTLY IF EXISTS "h"."idx_evt_a_techplace_trgm";' in fixed
    assert 'CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_evt_a_techplace_trgm"' in fixed


# --------------------------------------------------------------------------
# Применение
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_plan_builds_online_one_by_one():
    conn = FakeConn()
    plan = _plan(
        IndexPlanItem(find_index_spec("idx_events_timestamp"), "missing", hypertable=True),
        IndexPlanItem(trgm_index_spec("evt_a", "techplace"), "missing"),
        IndexPlanItem(find_index_spec("idx_event_types_name"), "present"),
    )
    results = await apply_plan(conn, plan, lock_timeout_sec=3)

    assert conn.executed[:2] == ["SET statement_timeout = 0", "SET lock_timeout = '3000ms'"]
    assert conn.executed[2].endswith("WITH (timescaledb.transaction_per_chunk)")
    assert "CONCURRENTLY" in conn.executed[3]
    assert [r.action for r in results] == ["created", "created"]
    assert plan.pending == []


@pytest.mark.asyncio
async def test_apply_plan_invalid_requires_fix_flag():
    item = IndexPlanItem(trgm_index_spec("evt_a", "techplace"), "invalid")
    conn = FakeConn()
    results = await apply_plan(conn, _plan(item))
    assert [r.action for r in results] == ["skipped"]
    assert not any("CREATE INDEX" in sql for sql in conn.executed)

    results = await apply_plan(conn, _plan(item), fix_invalid=True)
    assert [r.action for r in results] == ["rebuilt"]
    assert conn.executed[-2].startswith("DROP INDEX CONCURRENTLY")
    assert conn.executed[-1].startswith("CREATE INDEX CONCURRENTLY")


@pytest.mark.asyncio
async def test_apply_plan_failure_is_reported_and_next_index_continues():
    conn = FakeConn(fail_on="idx_events_timestamp")
    plan = _plan(
        IndexPlanItem(find_index_spec("idx_events_timestamp"), "missing", hypertable=True),
        IndexPlanItem(find_index_spec("idx_events_source_id"), "missing", hypertable=True),
    )
    results = await apply_plan(conn, plan)
    assert [r.action for r in results] == ["failed", "created"]
    assert "lock timeout" in results[0].error
    assert [i.spec.name for i in plan.pending] == ["idx_events_timestamp"]


@pytest.mark.asyncio
async def test_apply_plan_trgm_without_extension():
    conn = FakeConn(fail_on="CREATE EXTENSION")
    plan = _plan(IndexPlanItem(trgm_index_spec("evt_a", "techplace"), "missing"), trgm=False)
    results = await apply_plan(conn, plan)
    assert [r.action for r in results] == ["failed"]
    assert "pg_trgm" in results[0].error
    assert not any("gin_trgm_ops" in sql for sql in conn.executed)

    conn = FakeConn()
    plan = _plan(IndexPlanItem(trgm_index_spec("evt_a", "techplace"), "missing"), trgm=False)
    results = await apply_plan(conn, plan)
    assert "CREATE EXTENSION IF NOT EXISTS pg_trgm" in conn.executed
    assert [r.action for r in results] == ["created"]


@pytest.mark.asyncio
async def test_apply_plan_dry_run_and_transaction_guard():
    plan = _plan(IndexPlanItem(find_index_spec("idx_events_timestamp"), "missing", hypertable=True))
    conn = FakeConn()
    results = await apply_plan(conn, plan, dry_run=True)
    assert [r.action for r in results] == ["dry_run"]
    assert conn.executed == []

    with pytest.raises(RuntimeError, match="outside of a transaction"):
        await apply_plan(FakeConn(in_transaction=True), plan)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def test_cli_parser_uses_deployment_defaults_and_overrides():
    defaults = {"host": "db", "port": 5432, "schema": "history", "indexed_fields": "techplace"}
    parser = indexes_cli.build_parser(defaults=defaults)
    args = parser.parse_args(["plan", "--sql"])
    assert (args.host, args.schema, args.indexed_fields, args.sql) == ("db", "history", "techplace", True)
    args = parser.parse_args(["apply", "--schema", "other", "--scope", "core,trgm", "--fix-invalid"])
    assert args.schema == "other"
    assert args.scope == ["core", "trgm"]
    assert args.fix_invalid is True
    with pytest.raises(SystemExit):
        parser.parse_args(["apply", "--scope", "bogus"])


@pytest.mark.asyncio
async def test_cli_plan_check_exit_code(capsys):
    conn = FakeConn(relations=(), indexes=_all_present(idx_events_timestamp=False))
    conn.close = AsyncMock()
    with patch.object(indexes_cli, "connect_maintenance", AsyncMock(return_value=conn)):
        code = await indexes_cli.run(["plan", "--check", "--schema", "h"])
    assert code == indexes_cli.EXIT_PENDING
    assert "invalid" in capsys.readouterr().out
    conn.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_cli_apply_returns_failure_code(capsys):
    conn = FakeConn(relations=(), indexes=_all_present(idx_events_timestamp=None), fail_on="idx_events_timestamp")
    del conn.indexes["idx_events_timestamp"]
    conn.close = AsyncMock()
    with patch.object(indexes_cli, "connect_maintenance", AsyncMock(return_value=conn)):
        code = await indexes_cli.run(["apply", "--schema", "h"])
    assert code == indexes_cli.EXIT_FAILED
    assert "failed" in capsys.readouterr().out


# --------------------------------------------------------------------------
# Старт: trgm и INVALID
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_registry_plan_marks_invalid_trgm_index():
    async def fetch(sql, *args):
        if "information_schema.columns" in sql:
            return [{"table_name": "evt_a", "column_name": "techplace"}]
        if "pg_indexes" in sql:
            return [{"indexname": "idx_evt_a_techplace_trgm", "indisvalid": False}]
        return []

    registry = EventSchemaRegistry(
        "h", AsyncMock(), fetch, AsyncMock(), AsyncMock(),
        events_config=EventsV2Config.from_csv(indexed="techplace"),
    )
    planned = await registry.plan_trgm_indexes()
    assert [(p["index"], p["invalid"]) for p in planned] == [("idx_evt_a_techplace_trgm", True)]


def _v2_history(planned, **kwargs) -> HistoryTimescaleV2:
    history = HistoryTimescaleV2(
        user="u", password="p", database="d", host="h", port=5432, schema="h",
        events_storage_mode=StorageMode.DUAL, **kwargs,
    )
    history._pool = MagicMock(_closed=False, _closing=False)
    history._v2_ready = True
    history._registry = MagicMock(spec=EventSchemaRegistry)
    history._registry.plan_trgm_indexes = AsyncMock(return_value=planned)
    return history


def _trgm_item(invalid: bool = False) -> Dict[str, Any]:
    return {
        "table": "evt_a",
        "column": "techplace",
        "index": "idx_evt_a_techplace_trgm",
        "ddl": trgm_index_ddl("h", "evt_a", "techplace"),
        "invalid": invalid,
    }


@pytest.mark.asyncio
async def test_trgm_startup_skipped_when_startup_indexes_disabled():
    history = _v2_history([_trgm_item()], ensure_indexes_on_startup=False)
    execute = AsyncMock(return_value=True)
    with patch.object(HistoryTimescaleV2, "_best_effort_execute", execute), patch.object(
        HistoryTimescaleV2, "_probe_fetchval", AsyncMock(return_value=1)
    ):
        await history._ensure_trgm_indexes()
    assert not any("gin_trgm_ops" in call.args[0] for call in execute.await_args_list)
    assert history.get_performance_metrics()["events_v2"]["trgm_indexes_missing"] == 1


@pytest.mark.asyncio
async def test_trgm_startup_drops_invalid_before_create():
    history = _v2_history([_trgm_item(invalid=True)])
    executed: List[str] = []

    async def fake_execute(_self, sql, timeout):
        executed.append(sql)
        return True

    with patch.object(HistoryTimescaleV2, "_best_effort_execute", fake_execute), patch.object(
        HistoryTimescaleV2, "_probe_fetchval", AsyncMock(return_value=1)
    ):
        await history._ensure_trgm_indexes()
    assert executed == [
        'DROP INDEX IF EXISTS "h"."idx_evt_a_techplace_trgm"',
        trgm_index_ddl("h", "evt_a", "techplace"),
    ]


@pytest.mark.asyncio
async def test_registry_write_path_respects_trgm_disabled():
    execute = AsyncMock()

    async def fetch(sql, *args):
        return []  # колонок ещё нет

    registry = EventSchemaRegistry(
        "h", execute, fetch, AsyncMock(), AsyncMock(),
        events_config=EventsV2Config.from_csv(indexed="techplace"),
        trgm_index_enabled=False,
    )
    await registry.ensure_columns_from_typed_values("evt_a", {"techplace": "x"})
    statements = [call.args[0] for call in execute.await_args_list]
    assert any('ADD COLUMN IF NOT EXISTS "techplace"' in s for s in statements)
    assert not any("gin_trgm_ops" in s for s in statements)
