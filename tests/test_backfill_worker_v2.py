"""Регрессия: батч бэкфила не построчный, а typed-проход реально продвигается.

До 0.2.17 `ProcedureGateway.backfill_events_batch` дублировал на Python логику
SQL-объекта и делал 1+2N round-trip на батч, а `_backfill_typed_rows` брал
`ORDER BY legacy_row_id DESC LIMIT n` без курсора — то есть каждый вызов
обрабатывал одни и те же свежие строки и до старых не доходил никогда.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pytest

from uapg.v2.backfill_worker import EventsBackfillWorker
from uapg.v2.procedure_gateway import ProcedureGateway
from uapg.v2.sql_migrator import MIGRATION_ORDER, load_migration_sql

SCHEMA = "uapg"
TS = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)


class _RecordingPool:
    """Фейковый пул с маршрутизацией ответов по тексту запроса."""

    def __init__(
        self,
        *,
        typed_rows: Optional[List[Dict[str, Any]]] = None,
        payloads: Optional[List[Dict[str, Any]]] = None,
        typed_cursor: int = 0,
        batch_result: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.typed_rows = typed_rows or []
        self.payloads = payloads or []
        self.typed_cursor = typed_cursor
        self.batch_result = batch_result or {
            "last_legacy_id": 900,
            "rows_processed": 850,
            "rows_inserted": 50,
        }
        self.calls: List[tuple] = []

    @property
    def queries(self) -> List[str]:
        return [q for q, _, _ in self.calls]

    def _record(self, query, args, timeout):
        self.calls.append((query, args, timeout))

    async def fetchrow(self, query, *args, timeout=None):
        self._record(query, args, timeout)
        if "uapg_backfill_events_batch" in query:
            return self.batch_result
        if "uapg_backfill_state" in query:
            domain = args[0] if args else "events"
            if domain == "events_typed":
                return {"last_legacy_id": self.typed_cursor, "rows_processed": 0}
            return {"last_legacy_id": 500, "rows_processed": 400}
        if "lag_rows" in query:
            return {"lag_rows": 0, "min_id": 1, "max_id": 1000}
        return None

    async def fetch(self, query, *args, timeout=None):
        self._record(query, args, timeout)
        if "event_type_storage" in query:
            return list(self.typed_rows)
        if "event_data" in query:
            return list(self.payloads)
        return []

    async def execute(self, query, *args, timeout=None):
        self._record(query, args, timeout)
        return "INSERT 0 1"


def _typed_row(legacy_id: int, event_id: int, table: str = "evt_alarm") -> Dict[str, Any]:
    return {
        "legacy_row_id": legacy_id,
        "event_id": event_id,
        "event_timestamp": TS,
        "source_id": 7,
        "event_type_id": 3,
        "physical_table": table,
    }


class _FakeRegistry:
    def __init__(self, fail_event_ids: Optional[set] = None) -> None:
        self.ensure_calls: List[tuple] = []
        self.inserts: List[tuple] = []
        self.fail_event_ids = fail_event_ids or set()

    async def ensure_columns_from_typed_values(self, table, typed_values):
        self.ensure_calls.append((table, dict(typed_values)))

    async def insert_typed_row(
        self, table, event_id, event_timestamp, source_id, typed_values, *, ensure_columns=True
    ):
        if event_id in self.fail_event_ids:
            raise RuntimeError("boom")
        self.inserts.append((table, event_id, source_id, dict(typed_values), ensure_columns))

    async def get_storage_table(self, event_type_id):  # не должен вызываться
        raise AssertionError("get_storage_table must not be used per row")


def _make_worker(pool, registry=None, new_last=900, new_processed=850):
    gateway = SimpleNamespace(backfill_events_batch=_FakeGateway(new_last, new_processed))
    return EventsBackfillWorker(
        SCHEMA,
        pool,
        gateway,
        registry or _FakeRegistry(),
        lambda data: dict(data),
        query_timeout_sec=30.0,
    )


class _FakeGateway:
    def __init__(self, new_last: int, new_processed: int) -> None:
        self.new_last = new_last
        self.new_processed = new_processed
        self.kwargs: Dict[str, Any] = {}

    async def __call__(self, batch_size, last_legacy_id, rows_processed, **kwargs):
        self.kwargs = kwargs
        return self.new_last, self.new_processed


# --- ProcedureGateway ----------------------------------------------------


@pytest.mark.asyncio
async def test_gateway_batch_is_a_single_sql_call():
    pool = _RecordingPool()
    gateway = ProcedureGateway(SCHEMA, pool)

    last, processed = await gateway.backfill_events_batch(500, 400, 380, timeout=30.0)

    assert (last, processed) == (900, 850)
    assert len(pool.calls) == 1
    query, args, timeout = pool.calls[0]
    assert f'"{SCHEMA}".uapg_backfill_events_batch($1, $2, $3)' in query
    assert args == (500, 400, 380)
    assert timeout == 30.0
    # Ни построчных проверок, ни построчных вставок.
    assert "SELECT 1 FROM" not in query
    assert "INSERT INTO" not in query


@pytest.mark.asyncio
async def test_gateway_batch_returns_input_when_function_gives_no_row():
    pool = _RecordingPool()
    pool.batch_result = None
    gateway = ProcedureGateway(SCHEMA, pool)

    assert await gateway.backfill_events_batch(500, 400, 380) == (400, 380)


# --- ведущий запрос typed-бэкфила ---------------------------------------


@pytest.mark.asyncio
async def test_typed_query_uses_ascending_cursor():
    pool = _RecordingPool(
        typed_rows=[_typed_row(10, 101)],
        payloads=[{"id": 10, "event_data": {"Severity": 500}}],
        typed_cursor=5,
    )
    await _make_worker(pool)._backfill_typed_rows(2)

    driving = next(q for q in pool.queries if "event_type_storage" in q)
    assert "et.legacy_row_id > $1" in driving
    assert "ORDER BY et.legacy_row_id\n" in driving
    assert "DESC" not in driving
    assert "information_schema" not in driving
    assert "to_regclass" in driving
    # Курсор из uapg_backfill_state подставлен как есть.
    args = next(a for q, a, _ in pool.calls if "event_type_storage" in q)
    assert args == (5, SCHEMA, 2)


@pytest.mark.asyncio
async def test_typed_payloads_fetched_in_one_query():
    rows = [_typed_row(10, 101), _typed_row(11, 102), _typed_row(12, 103)]
    pool = _RecordingPool(
        typed_rows=rows,
        payloads=[{"id": i, "event_data": {"Severity": 1}} for i in (10, 11, 12)],
    )
    inserted, failed = await _make_worker(pool)._backfill_typed_rows(3)

    assert (inserted, failed) == (3, 0)
    payload_calls = [(q, a) for q, a, _ in pool.calls if "event_data" in q]
    assert len(payload_calls) == 1
    assert "id = ANY($1::bigint[])" in payload_calls[0][0]
    assert payload_calls[0][1] == ([10, 11, 12],)
    # Построчной проверки существования в typed-таблице больше нет.
    assert not any("FROM \"uapg\".\"evt_alarm\"" in q for q in pool.queries)


@pytest.mark.asyncio
async def test_columns_ensured_once_per_table_per_batch():
    rows = [_typed_row(10, 101), _typed_row(11, 102), _typed_row(12, 103, "evt_other")]
    pool = _RecordingPool(
        typed_rows=rows,
        payloads=[
            {"id": 10, "event_data": {"Severity": 1}},
            {"id": 11, "event_data": {"Message": "x"}},
            {"id": 12, "event_data": {"Severity": 2}},
        ],
    )
    registry = _FakeRegistry()
    await _make_worker(pool, registry)._backfill_typed_rows(3)

    # Одна таблица — один вызов, по объединению ключей батча.
    assert len(registry.ensure_calls) == 2
    by_table = dict(registry.ensure_calls)
    assert set(by_table["evt_alarm"]) == {"Severity", "Message"}
    assert set(by_table["evt_other"]) == {"Severity"}
    # Вставки уже не повторяют DDL-проверку.
    assert all(call[4] is False for call in registry.inserts)


# --- курсор --------------------------------------------------------------


@pytest.mark.asyncio
async def test_full_batch_advances_cursor_to_max_legacy_id():
    rows = [_typed_row(10, 101), _typed_row(42, 102)]
    pool = _RecordingPool(
        typed_rows=rows,
        payloads=[{"id": 10, "event_data": {}}, {"id": 42, "event_data": {}}],
    )
    await _make_worker(pool)._backfill_typed_rows(2)

    state_writes = [a for q, a, _ in pool.calls if "ON CONFLICT (domain)" in q]
    assert state_writes == [("events_typed", 42, 0)]


@pytest.mark.asyncio
async def test_short_batch_restarts_cursor():
    """Хвост достигнут: следующий круг подхватит типы, чья таблица появилась позже."""
    pool = _RecordingPool(
        typed_rows=[_typed_row(10, 101)],
        payloads=[{"id": 10, "event_data": {}}],
        typed_cursor=7,
    )
    await _make_worker(pool)._backfill_typed_rows(5)

    state_writes = [a for q, a, _ in pool.calls if "ON CONFLICT (domain)" in q]
    assert state_writes == [("events_typed", 0, 0)]


@pytest.mark.asyncio
async def test_empty_batch_resets_cursor_only_when_moved():
    pool = _RecordingPool(typed_rows=[], typed_cursor=7)
    assert await _make_worker(pool)._backfill_typed_rows(5) == (0, 0)
    assert [a for q, a, _ in pool.calls if "ON CONFLICT (domain)" in q] == [
        ("events_typed", 0, 0)
    ]

    pool = _RecordingPool(typed_rows=[], typed_cursor=0)
    assert await _make_worker(pool)._backfill_typed_rows(5) == (0, 0)
    assert not any("ON CONFLICT (domain)" in q for q in pool.queries)


# --- устойчивость --------------------------------------------------------


@pytest.mark.asyncio
async def test_failing_row_does_not_abort_batch():
    rows = [_typed_row(10, 101), _typed_row(11, 102), _typed_row(12, 103)]
    pool = _RecordingPool(
        typed_rows=rows,
        payloads=[{"id": i, "event_data": {}} for i in (10, 11, 12)],
    )
    registry = _FakeRegistry(fail_event_ids={102})

    inserted, failed = await _make_worker(pool, registry)._backfill_typed_rows(3)

    assert (inserted, failed) == (2, 1)
    assert [call[1] for call in registry.inserts] == [101, 103]


@pytest.mark.asyncio
async def test_missing_legacy_payload_is_skipped():
    rows = [_typed_row(10, 101), _typed_row(11, 102)]
    pool = _RecordingPool(typed_rows=rows, payloads=[{"id": 10, "event_data": {}}])
    registry = _FakeRegistry()

    inserted, failed = await _make_worker(pool, registry)._backfill_typed_rows(2)

    assert (inserted, failed) == (1, 0)
    assert [call[1] for call in registry.inserts] == [101]


@pytest.mark.asyncio
async def test_run_batch_reports_typed_counters_and_passes_timeout():
    rows = [_typed_row(10, 101)]
    pool = _RecordingPool(typed_rows=rows, payloads=[{"id": 10, "event_data": {}}])
    worker = _make_worker(pool)

    stats = await worker.run_batch(5)

    assert stats["typed_rows_inserted"] == 1
    assert stats["typed_rows_failed"] == 0
    assert stats["last_legacy_id"] == 900
    assert stats["rows_processed"] == 850
    assert worker._gateway.backfill_events_batch.kwargs == {"timeout": 30.0}
    assert all(t == 30.0 for _, _, t in pool.calls)


# --- миграция 005 --------------------------------------------------------


def test_migration_005_registered_and_renders():
    assert "005_events_v2_backfill.sql" in MIGRATION_ORDER
    assert MIGRATION_ORDER.index("005_events_v2_backfill.sql") > MIGRATION_ORDER.index(
        "003_events_v2_functions.sql"
    )

    sql = load_migration_sql("005_events_v2_backfill.sql", "opcua_history")
    assert "{schema}" not in sql
    assert 'DROP PROCEDURE IF EXISTS "opcua_history".uapg_backfill_events_batch' in sql
    assert 'CREATE OR REPLACE FUNCTION "opcua_history".uapg_backfill_events_batch' in sql
    assert "RETURNS TABLE (last_legacy_id BIGINT, rows_processed BIGINT, rows_inserted BIGINT)" in sql
    assert "VALUES ('events_typed', 0, 0)" in sql
    # Никакого построчного FOR ... LOOP (комментарии, где он упомянут, не считаются).
    body = "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )
    assert "LOOP" not in body
    assert "INSERT INTO \"opcua_history\".events_ts" in body
