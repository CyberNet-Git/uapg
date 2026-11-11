"""Тесты инварианта variables_last_value (0.2.26).

Инвариант: у каждой зарегистрированной переменной есть строка в variables_last_value,
поэтому read_last_values не обращается к hypertable variables_history. До 0.2.26 он
был объявлен в DDL-комментарии и нигде не исполнялся: new_historized_node(s) писали
только в variable_metadata, а дорогой фоллбэк по истории не мог сам себя закрыть
(находил строки только у переменных с историей) и повторялся на каждом старте.

Каждый тест ниже проверен мутацией: правка снята — тест краснеет.
"""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock, Mock

import pytest
from asyncua import ua
from asyncua.ua.ua_binary import variant_to_binary

import uapg.history_timescale as ht
from uapg.history_timescale import DEFAULT_LAST_VALUES_LOOKBACK, HistoryTimescale
from uapg.status_code import encode_status


def _history(**kwargs) -> HistoryTimescale:
    """Экземпляр без обращения к БД: конструктор инертен, init() не вызываем."""
    return HistoryTimescale(schema="history", **kwargs)


class _Conn:
    def __init__(self) -> None:
        self.executed: list[str] = []

    async def execute(self, sql, *args, **kwargs):
        self.executed.append(sql)


class _Acquire:
    def __init__(self, conn) -> None:
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class _Pool:
    def __init__(self, conn) -> None:
        self._conn = conn

    def acquire(self, timeout=None):
        return _Acquire(self._conn)


class TestRegistrationSeedsLastValue:
    @pytest.mark.asyncio
    async def test_bulk_registration_seeds_new_nodes_in_one_batch(self):
        history = _history()
        nodes = [ua.NodeId(f"v{i}", 2) for i in range(3)]
        ids = {history._format_node_id(n): 100 + i for i, n in enumerate(nodes)}
        history._fetch = AsyncMock(
            return_value=[{"node_id": k, "variable_id": v} for k, v in ids.items()]
        )
        history.seed_last_values = AsyncMock(return_value=3)

        await history.new_historized_nodes(nodes, None)

        history.seed_last_values.assert_awaited_once()
        (items,), _ = history.seed_last_values.await_args
        assert [n for n, _ in items] == nodes
        # Без значения: seed_last_values подставит Null и BadWaitingForInitialData.
        assert all(dv is None for _, dv in items)

    @pytest.mark.asyncio
    async def test_bulk_registration_does_not_reseed_cached_nodes(self):
        history = _history()
        node = ua.NodeId("known", 2)
        history._variable_metadata_cache[history._format_node_id(node)] = 5
        history._fetch = AsyncMock()
        history.seed_last_values = AsyncMock()

        await history.new_historized_nodes([node], None)

        history.seed_last_values.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_single_registration_seeds_only_when_metadata_is_new(self):
        history = _history()
        history._save_variable_metadata = AsyncMock(return_value=11)
        history.seed_last_values = AsyncMock(return_value=1)
        new = ua.NodeId("new", 2)
        cached = ua.NodeId("cached", 2)
        history._variable_metadata_cache[history._format_node_id(cached)] = 12

        await history.new_historized_node(new, None)
        await history.new_historized_node(cached, None)

        history.seed_last_values.assert_awaited_once()
        (items,), _ = history.seed_last_values.await_args
        assert [n for n, _ in items] == [new]

    @pytest.mark.asyncio
    async def test_seed_failure_does_not_fail_registration(self):
        history = _history()
        history._save_variable_metadata = AsyncMock(return_value=11)
        history.seed_last_values = AsyncMock(side_effect=RuntimeError("db down"))
        node = ua.NodeId("new", 2)

        await history.new_historized_node(node, None)

        assert history._datachanges_period[node][2] == 11


class TestStartupRepair:
    @pytest.mark.asyncio
    async def test_repair_seeds_with_encoded_bad_waiting_and_null_variant(self):
        history = _history()
        history._fetchval = AsyncMock(return_value=9692)

        inserted = await history._repair_last_value_rows()

        assert inserted == 9692
        sql, statuscode, varianttype, binary = history._fetchval.await_args.args
        assert "LEFT JOIN" in sql and "lv.variable_id IS NULL" in sql
        assert "ON CONFLICT (variable_id) DO NOTHING" in sql
        # Не хардкод: те же функции, что у seed_last_values.
        assert statuscode == encode_status(ua.StatusCodes.BadWaitingForInitialData)
        assert varianttype == 0
        assert binary == variant_to_binary(ua.Variant(None))
        assert history.get_cache_stats()["last_values_seeded"] == 9692

    @pytest.mark.asyncio
    async def test_repair_is_idempotent_second_run_inserts_nothing(self):
        history = _history()
        history._fetchval = AsyncMock(side_effect=[5, 0])

        assert await history._repair_last_value_rows() == 5
        assert await history._repair_last_value_rows() == 0
        assert history.get_cache_stats()["last_values_seeded"] == 5


class TestLookbackDefault:
    def test_default_is_seven_days(self):
        assert _history()._history_last_values_lookback == DEFAULT_LAST_VALUES_LOOKBACK
        assert DEFAULT_LAST_VALUES_LOOKBACK == timedelta(days=7)

    def test_unset_argument_uses_constructor_value_explicit_none_means_full_history(self):
        history = _history(history_last_values_lookback=timedelta(days=2))
        assert history._effective_lookback(ht._LOOKBACK_UNSET) == timedelta(days=2)
        assert history._effective_lookback(None) is None
        assert history._effective_lookback(timedelta(hours=1)) == timedelta(hours=1)

    def test_constructor_none_disables_window_and_values_are_clamped(self):
        assert _history(history_last_values_lookback=None)._history_last_values_lookback is None
        assert _history(history_last_values_lookback=timedelta(0))._history_last_values_lookback == timedelta(minutes=1)
        h = _history(
            history_last_values_fallback_chunk_size=0,
            history_last_values_fallback_batch_timeout_sec=0,
            history_last_values_fallback_budget_sec=-5,
        )
        assert h._history_last_values_fallback_chunk_size == 1
        assert h._history_last_values_fallback_batch_timeout_sec == 1.0
        assert h._history_last_values_fallback_budget_sec == 1.0


def _ready_for_fallback(history: HistoryTimescale) -> _Conn:
    conn = _Conn()
    history._ensure_pool = AsyncMock()
    history._pool = _Pool(conn)
    history._force_reconnect = AsyncMock()
    return conn


class TestBoundedFallback:
    @pytest.mark.asyncio
    async def test_timeout_returns_collected_rows_and_does_not_reconnect(self):
        history = _history(history_last_values_fallback_chunk_size=2)
        _ready_for_fallback(history)
        calls = []

        async def fake(conn, ids, since, timeout):
            calls.append(list(ids))
            if len(calls) == 2:
                raise TimeoutError()
            return [{"variable_id": ids[0]}]

        history._fetch_last_from_history = fake

        rows, searched = await history._bounded_history_fallback([1, 2, 3, 4, 5, 6], None)

        assert rows == [{"variable_id": 1}]
        assert searched == {1, 2}
        assert len(calls) == 2  # после таймаута остальные пачки не пробуются
        stats = history.get_cache_stats()
        assert stats["last_values_fallback_timeouts"] == 1
        assert stats["last_values_fallback_unprocessed"] == 4
        history._force_reconnect.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_total_budget_stops_fallback_with_partial_result(self, monkeypatch):
        history = _history(
            history_last_values_fallback_chunk_size=1,
            history_last_values_fallback_batch_timeout_sec=30.0,
            history_last_values_fallback_budget_sec=60.0,
        )
        _ready_for_fallback(history)
        clock = {"now": 1000.0}
        monkeypatch.setattr(ht.time, "monotonic", lambda: clock["now"])

        async def fake(conn, ids, since, timeout):
            clock["now"] += 40.0  # каждая пачка «съедает» 40 с из бюджета 60
            return [{"variable_id": ids[0]}]

        history._fetch_last_from_history = fake

        rows, searched = await history._bounded_history_fallback([1, 2, 3], None)

        assert searched == {1, 2}  # третья пачка не стартует: бюджет исчерпан
        assert len(rows) == 2
        assert history.get_cache_stats()["last_values_fallback_unprocessed"] == 1

    @pytest.mark.asyncio
    async def test_batch_timeout_is_capped_by_remaining_budget(self, monkeypatch):
        history = _history(
            history_last_values_fallback_batch_timeout_sec=30.0,
            history_last_values_fallback_budget_sec=10.0,
        )
        _ready_for_fallback(history)
        seen = []

        async def fake(conn, ids, since, timeout):
            seen.append(timeout)
            return []

        history._fetch_last_from_history = fake
        await history._bounded_history_fallback([1], None)

        assert seen and seen[0] <= 10.0


def _mapped(history: HistoryTimescale, count: int):
    nodes = [ua.NodeId(f"v{i}", 2) for i in range(count)]
    for i, n in enumerate(nodes):
        history._datachanges_period[n] = (None, 0, 100 + i)
    return nodes


class TestReadLastValuesFallback:
    @pytest.mark.asyncio
    async def test_searched_but_unfound_are_seeded_and_returned_as_bad_waiting(self):
        history = _history()
        nodes = _mapped(history, 2)
        history._fetch = AsyncMock(return_value=[])  # в variables_last_value ничего нет
        history._bounded_history_fallback = AsyncMock(return_value=([], {100, 101}))
        history.seed_last_values = AsyncMock(return_value=2)

        result = await history.read_last_values(nodes)

        history.seed_last_values.assert_awaited_once()
        for n in nodes:
            assert result[n] is not None
            assert result[n].StatusCode_.name == "BadWaitingForInitialData"
            assert result[n].Value.Value is None

    @pytest.mark.asyncio
    async def test_unsearched_nodes_stay_none_and_are_not_seeded(self):
        history = _history()
        nodes = _mapped(history, 2)
        history._fetch = AsyncMock(return_value=[])
        # просмотрена только первая переменная, на второй бюджет кончился
        history._bounded_history_fallback = AsyncMock(return_value=([], {100}))
        history.seed_last_values = AsyncMock(return_value=1)

        result = await history.read_last_values(nodes)

        assert result[nodes[0]] is not None
        assert result[nodes[1]] is None
        (items,), _ = history.seed_last_values.await_args
        assert [n for n, _ in items] == [nodes[0]]

    @pytest.mark.asyncio
    async def test_default_lookback_reaches_fallback_and_explicit_none_overrides_it(self):
        history = _history()
        nodes = _mapped(history, 1)
        history._fetch = AsyncMock(return_value=[])
        history._bounded_history_fallback = AsyncMock(return_value=([], set()))

        await history.read_last_values(nodes)
        assert history._bounded_history_fallback.await_args.args[1] == timedelta(days=7)

        await history.read_last_values(nodes, history_lookback=None)
        assert history._bounded_history_fallback.await_args.args[1] is None

    @pytest.mark.asyncio
    async def test_allow_history_fallback_false_never_touches_history(self):
        history = _history()
        nodes = _mapped(history, 1)
        history._fetch = AsyncMock(return_value=[])
        history._bounded_history_fallback = AsyncMock()

        await history.read_last_values(nodes, allow_history_fallback=False)

        history._bounded_history_fallback.assert_not_awaited()


class TestBackfillWindow:
    async def _run(self, lookback):
        history = _history()
        conn = _ready_for_fallback(history)
        history._fetch = AsyncMock(return_value=[{"variable_id": 1, "node_id": "ns=2;s=a"},
                                                  {"variable_id": 2, "node_id": "ns=2;s=b"}])
        history._fetch_last_from_history = AsyncMock(return_value=[])
        kwargs = {} if lookback == "unset" else {"history_lookback": lookback}
        stats = await history.backfill_last_values(pause_sec=0, **kwargs)
        return history, conn, stats

    @pytest.mark.asyncio
    async def test_window_limited_search_does_not_confirm_seeds(self):
        history, conn, stats = await self._run("unset")  # умолчание: 7 суток

        assert history._fetch_last_from_history.await_args.args[2] is not None  # since
        assert not any("SET is_seed = FALSE" in sql for sql in conn.executed)
        assert stats["confirmed_defaults"] == 0
        assert stats["unresolved_in_window"] == 2

    @pytest.mark.asyncio
    async def test_full_history_search_confirms_seeds(self):
        history, conn, stats = await self._run(None)

        assert history._fetch_last_from_history.await_args.args[2] is None
        assert any("SET is_seed = FALSE" in sql for sql in conn.executed)
        assert stats["confirmed_defaults"] == 2
        assert stats["unresolved_in_window"] == 0


class TestMetrics:
    def test_last_values_section_is_exposed(self):
        history = _history()
        history._cache_stats["last_values_fallback_timeouts"] = 3
        metrics = history.get_performance_metrics()

        section = metrics["last_values"]
        assert section["fallback_timeouts"] == 3
        for key in ("history_fallbacks", "fallback_unprocessed", "seeded", "table_hits"):
            assert key in section
        assert metrics["config"]["history_last_values_lookback_sec"] == 7 * 86400.0
