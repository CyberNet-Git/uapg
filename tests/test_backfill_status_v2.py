"""Регрессия: прогресс бэкфила V2 не считается полным anti-join по events_history.

Инцидент 30.09: старт писал `fetchval timed out after 30.0 seconds` и следом
`will reconnect without retrying` — полный anti-join events_history × events_ts
не укладывался в db_query_timeout_sec и рвал пул ради косметического узла
EventsBackfillComplete. Плюс на один expose приходилось два refresh.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from uapg.history_timescale_v2 import HistoryTimescaleV2
from uapg.v2.backfill_worker import EventsBackfillWorker
from uapg.v2.storage_mode import StorageMode


class _FakeConn:
    def __init__(self, pool: "_FakePool") -> None:
        self._pool = pool

    async def fetchval(self, query, *args, timeout=None):
        self._pool.queries.append((query, args, timeout))
        if self._pool.raises is not None:
            raise self._pool.raises
        return self._pool.result


class _FakeAcquire:
    def __init__(self, pool: "_FakePool") -> None:
        self._pool = pool

    async def __aenter__(self):
        return _FakeConn(self._pool)

    async def __aexit__(self, *exc_info):
        return False


class _FakePool:
    _closed = False
    _closing = False

    def __init__(self, result=False, raises=None) -> None:
        self.result = result
        self.raises = raises
        self.queries: list = []
        self.acquire_timeouts: list = []

    def acquire(self, timeout=None):
        self.acquire_timeouts.append(timeout)
        return _FakeAcquire(self)

    async def close(self) -> None:
        return None

    def terminate(self) -> None:
        return None


class _FakeNode:
    """Минимальный узел asyncua: get_child / add_object / add_variable / write_value."""

    def __init__(self, name: str = "root", parent: "_FakeNode | None" = None) -> None:
        self.name = name
        self.parent = parent
        self.children: dict = {}
        self.value = None

    async def get_child(self, path):
        key = path[0] if isinstance(path, (list, tuple)) else path
        if key in self.children:
            return self.children[key]
        raise RuntimeError(f"no child {key}")

    async def get_parent(self):
        return self.parent

    async def add_object(self, idx, name):
        node = _FakeNode(name, self)
        self.children[f"{idx}:{name}"] = node
        return node

    async def add_variable(self, idx, name, value):
        node = _FakeNode(name, self)
        node.value = value
        self.children[f"{idx}:{name}"] = node
        return node

    async def write_value(self, value):
        self.value = value


def _make_history(pool: _FakePool, **kwargs) -> HistoryTimescaleV2:
    history = HistoryTimescaleV2(
        user="u",
        password="p",
        database="d",
        host="h",
        port=5432,
        events_storage_mode=StorageMode.DUAL,
        **kwargs,
    )
    history._pool = pool
    history._v2_ready = True
    return history


def _probe_queries(pool: _FakePool) -> list:
    return [q for q, _, _ in pool.queries if "uapg_backfill_state" in q]


@pytest.mark.asyncio
async def test_expose_runs_single_backfill_probe():
    """До исправления refresh (и с ним anti-join) выполнялся дважды за один expose."""
    pool = _FakePool(result=False)
    history = _make_history(pool)
    server_node = _FakeNode("Server")
    server = SimpleNamespace(nodes=SimpleNamespace(server=server_node))

    with patch.object(HistoryTimescaleV2, "_timescaledb_available", AsyncMock(return_value=False)):
        await history.expose_history_settings_nodes(server, 2)

    assert len(_probe_queries(pool)) == 1
    assert not history._defer_settings_refresh
    # Узлы capability заполнены значениями, а не остались на initial.
    settings = server_node.children["2:History"].children["2:HistorySettings"]
    assert settings.children["2:EventsBackfillComplete"].value.Value is True
    assert settings.children["2:EventsStorageMode"].value.Value == "dual"


@pytest.mark.asyncio
async def test_probe_sql_is_bounded_and_watermark_based():
    pool = _FakePool(result=False)
    history = _make_history(pool)

    await history._is_events_backfill_complete()

    query, args, timeout = pool.queries[0]
    assert "uapg_backfill_state" in query
    assert "LIMIT $1" in query
    assert args == (1000,)
    # Ни одного count(*) по всей events_history.
    assert "count(*)" not in query
    # Служебная проба ограничена собственным коротким бюджетом, а не 30 с.
    assert timeout == 5.0
    assert pool.acquire_timeouts == [5.0]


@pytest.mark.asyncio
async def test_probe_result_is_cached_until_ttl_and_forceable():
    pool = _FakePool(result=False)
    history = _make_history(pool, events_backfill_status_ttl_sec=600.0)

    assert await history._is_events_backfill_complete() is True
    assert await history._is_events_backfill_complete() is True
    assert len(pool.queries) == 1

    assert await history._is_events_backfill_complete(force=True) is True
    assert len(pool.queries) == 2


@pytest.mark.asyncio
async def test_pending_rows_report_incomplete():
    pool = _FakePool(result=True)
    history = _make_history(pool)

    assert await history._is_events_backfill_complete() is False


@pytest.mark.asyncio
async def test_probe_timeout_does_not_tear_down_pool():
    """Раньше таймаут этого запроса шёл через _fetchval и пересоздавал пул."""
    pool = _FakePool(raises=asyncio.TimeoutError())
    history = _make_history(pool)
    reconnect = AsyncMock()

    with patch.object(HistoryTimescaleV2, "_force_reconnect", reconnect):
        assert await history._is_events_backfill_complete() is False
        # Неудачная проба тоже попадает под TTL: медленная БД не добавляет свой
        # таймаут к каждому HistoryRead.
        assert await history._is_events_backfill_complete() is False

    reconnect.assert_not_awaited()
    assert history._performance_counters.get("events_backfill_probe_failures_total") == 1
    assert len(pool.queries) == 1


@pytest.mark.asyncio
async def test_probe_failure_keeps_last_known_value():
    pool = _FakePool(result=False)
    history = _make_history(pool, events_backfill_status_ttl_sec=0.0)
    assert await history._is_events_backfill_complete() is True

    pool.raises = asyncio.TimeoutError()
    assert await history._is_events_backfill_complete() is True


@pytest.mark.asyncio
async def test_no_probe_when_v2_not_ready():
    pool = _FakePool(result=True)
    history = _make_history(pool)
    history._v2_ready = False

    assert await history._is_events_backfill_complete() is True
    assert pool.queries == []


@pytest.mark.asyncio
async def test_read_event_history_uses_cached_global_probe():
    """На каждый HistoryRead был anti-join по всем строкам источника."""
    pool = _FakePool(result=False)
    history = _make_history(pool)
    history._backfill_worker = SimpleNamespace(_pool=pool)
    read_events = AsyncMock(return_value=([], None, False))
    history._event_store = SimpleNamespace(read_events=read_events)

    with patch.object(HistoryTimescaleV2, "_resolve_source_db_id", AsyncMock(return_value=7)):
        for _ in range(3):
            await history.read_event_history(None, None, None, 10, None)

    assert len(pool.queries) == 1
    assert "eh.source_id" not in pool.queries[0][0]
    assert read_events.await_args.kwargs["partial"] is False


class _StatsPool:
    def __init__(self, lag_rows: int, min_id, max_id) -> None:
        self.row = {"lag_rows": lag_rows, "min_id": min_id, "max_id": max_id}
        self.queries: list = []
        self.timeouts: list = []

    async def fetchrow(self, query, *args, timeout=None):
        self.queries.append(query)
        self.timeouts.append(timeout)
        if "uapg_backfill_state" in query:
            return {"last_legacy_id": 100, "rows_processed": 100}
        return self.row

    async def fetch(self, query, *args, timeout=None):
        self.queries.append(query)
        self.timeouts.append(timeout)
        return []

    async def execute(self, query, *args, timeout=None):
        self.queries.append(query)
        self.timeouts.append(timeout)
        return "INSERT 0 1"


def _make_worker(pool, new_last: int) -> EventsBackfillWorker:
    gateway = SimpleNamespace(
        backfill_events_batch=AsyncMock(return_value=(new_last, new_last))
    )
    return EventsBackfillWorker(
        "uapg",
        pool,
        gateway,
        SimpleNamespace(),
        lambda data: {},
        query_timeout_sec=30.0,
    )


@pytest.mark.asyncio
async def test_run_batch_reports_progress_without_full_scans():
    pool = _StatsPool(lag_rows=0, min_id=1, max_id=514000)
    stats = await _make_worker(pool, 514000).run_batch(500)

    assert set(stats) == {
        "last_legacy_id",
        "rows_processed",
        "backfill_lag_rows",
        "v2_coverage_pct",
        "typed_rows_inserted",
        "typed_rows_failed",
    }
    assert stats["backfill_lag_rows"] == 0
    assert stats["v2_coverage_pct"] == 100.0
    # Ни anti-join, ни count(*) по всей таблице: счёт ограничен watermark.
    assert not any("legacy_row_id = eh.id" in q for q in pool.queries)
    stats_query = next(q for q in pool.queries if "lag_rows" in q)
    assert 'FROM "uapg".events_history WHERE id > $1' in stats_query
    assert stats_query.count("count(*)") == 1
    # Каждый запрос воркера ограничен по времени.
    assert all(t == 30.0 for t in pool.timeouts)


@pytest.mark.asyncio
async def test_run_batch_coverage_tracks_watermark_within_id_range():
    pool = _StatsPool(lag_rows=250_000, min_id=1_000_001, max_id=1_500_000)
    stats = await _make_worker(pool, 1_250_000).run_batch(500)

    assert stats["backfill_lag_rows"] == 250_000
    assert stats["v2_coverage_pct"] == 50.0


@pytest.mark.parametrize(
    "last_legacy_id,lag_rows,min_id,max_id,expected",
    [
        (0, 514_000, 1, 514_000, 0.0),
        (514_000, 0, 1, 514_000, 100.0),
        (0, 0, None, None, 100.0),
        (0, 5, None, None, 100.0),
        (999, 5, 1_000, 1_000, 0.0),
    ],
)
def test_coverage_pct_edge_cases(last_legacy_id, lag_rows, min_id, max_id, expected):
    assert (
        EventsBackfillWorker._coverage_pct(last_legacy_id, lag_rows, min_id, max_id)
        == expected
    )


@pytest.mark.asyncio
async def test_metrics_expose_probe_failures():
    pool = _FakePool(raises=asyncio.TimeoutError())
    history = _make_history(pool)

    with patch.object(HistoryTimescaleV2, "_force_reconnect", AsyncMock()):
        await history._is_events_backfill_complete()

    events_v2 = history.get_performance_metrics()["events_v2"]
    assert events_v2["backfill_probe_failures_total"] == 1
    assert events_v2["storage_mode"] == "dual"
    assert events_v2["backfill_probe_rows"] == 1000
    # Все значения скалярные и стабильного типа: узлы HistoryMetrics создаются один раз.
    flat = HistoryTimescaleV2._flatten_metrics({"events_v2": events_v2})
    assert all(isinstance(v, (bool, int, float, str)) for v in flat.values())
