"""Лимит ответа HistoryRead и continuation point.

До 0.2.23 `_get_bounds` отдавал `limit = nb_values if nb_values else 10000`, то есть
клиентский `NumValuesPerNode` уходил в SQL `LIMIT` без потолка (запрос на 50 000 000
значений выполнялся буквально), а `nb_values == 0` — «лимит игнорировать» в терминах
asyncua — трактовался как жёсткая 10000. Атрибут `max_history_data_response_size`
присваивался и не читался ни разу.

Вместе с этим были два дефекта соответствия спецификации:

* метка продолжения бралась из последней **отданной** строки, хотя по контракту asyncua
  это метка последней **отвергнутой**. Следующий запрос приходит с `start = cont`, а
  `WHERE ... BETWEEN` включающий, поэтому страницы дублировали значение на стыке;
* у событий условие продолжения считалось по числу событий ПОСЛЕ `EventFilter`:
  стоило фильтру отбросить хотя бы одно событие, и клиенту сообщалось, что история
  исчерпана, хотя за границей окна оставались подходящие строки.

Главная проверка здесь — прогон через настоящий `HistoryManager` asyncua
(`test_paging_through_history_manager_*`): он упаковывает метку в байты и распаковывает
обратно, поэтому ловушка с переворотом направления воспроизводится как на живом сервере.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pytest
from asyncua import ua
from asyncua.server.history import HistoryManager
from asyncua.ua.ua_binary import variant_to_binary

from uapg import history_timescale as ht_module
from uapg.history_timescale import HistoryTimescale

T0 = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc)
VARIABLE_ID = 7
SOURCE_ID = 3


# --------------------------------------------------------------------------- переменные


def _variable_rows(count: int) -> List[Dict[str, Any]]:
    rows = []
    for i in range(count):
        ts = T0 + timedelta(seconds=i)
        rows.append(
            {
                "servertimestamp": ts,
                "sourcetimestamp": ts,
                "statuscode": 0,
                "value": str(float(i)),
                "varianttype": int(ua.VariantType.Double),
                "variantbinary": variant_to_binary(ua.Variant(float(i), ua.VariantType.Double)),
            }
        )
    return rows


def _history_with_rows(rows: List[Dict[str, Any]], **kwargs: Any) -> HistoryTimescale:
    """Экземпляр, у которого _fetch отдаёт строки по семантике самого SQL-запроса."""
    history = HistoryTimescale(schema="history", **kwargs)
    node_id = ua.NodeId("Bench.Double", 2)
    history._datachanges_period[node_id] = (timedelta(days=1), 0, VARIABLE_ID)
    history.node_id = node_id  # type: ignore[attr-defined]
    history.fetch_calls = []  # type: ignore[attr-defined]

    async def _fetch(sql: str, *args: Any) -> List[Dict[str, Any]]:
        variable_id, start, end, limit = args
        history.fetch_calls.append({"limit": limit, "start": start, "end": end, "sql": sql})
        assert variable_id == VARIABLE_ID
        selected = [r for r in rows if start <= r["sourcetimestamp"] <= end]
        selected.sort(key=lambda r: r["sourcetimestamp"], reverse="DESC" in sql)
        return selected[:limit]

    history._fetch = _fetch  # type: ignore[assignment]
    return history


class TestVariableResponseLimit:
    @pytest.mark.asyncio
    async def test_client_limit_is_capped_by_server(self):
        history = _history_with_rows(_variable_rows(50), history_read_max_response_rows=10)

        results, cont = await history.read_node_history(
            history.node_id, T0, T0 + timedelta(hours=1), 5_000_000
        )

        assert len(results) == 10
        # В SQL уходит страница плюс одна пробная строка, а не 5 000 000.
        assert history.fetch_calls[0]["limit"] == 11
        assert cont is not None

    @pytest.mark.asyncio
    async def test_continuation_is_first_rejected_row(self):
        rows = _variable_rows(50)
        history = _history_with_rows(rows, history_read_max_response_rows=10)

        results, cont = await history.read_node_history(
            history.node_id, T0, T0 + timedelta(hours=1), 0
        )

        assert len(results) == 10
        # Последняя отданная — rows[9]; метка обязана указывать на rows[10].
        assert results[-1].SourceTimestamp == rows[9]["sourcetimestamp"]
        assert cont == rows[10]["sourcetimestamp"]
        assert cont != rows[9]["sourcetimestamp"]

    @pytest.mark.asyncio
    async def test_exhausted_page_exactly_at_limit_has_no_continuation(self):
        """Выборка ровно в размер страницы и исчерпанная — продолжать нечего.

        Прежний код отдавал метку по `len(results) == limit` и стоил клиенту лишнего
        пустого round-trip.
        """
        history = _history_with_rows(_variable_rows(10), history_read_max_response_rows=10)

        results, cont = await history.read_node_history(
            history.node_id, T0, T0 + timedelta(hours=1), 0
        )

        assert len(results) == 10
        assert cont is None

    @pytest.mark.asyncio
    async def test_zero_nb_values_means_no_client_limit(self):
        history = _history_with_rows(_variable_rows(50), history_read_max_response_rows=25)

        results, _ = await history.read_node_history(
            history.node_id, T0, T0 + timedelta(hours=1), 0
        )

        assert len(results) == 25

    @pytest.mark.asyncio
    async def test_client_limit_below_cap_wins_and_closes_pagination(self):
        """Клиент попросил меньше потолка — отдаём ровно это и не продолжаем."""
        history = _history_with_rows(_variable_rows(50), history_read_max_response_rows=25)

        results, cont = await history.read_node_history(
            history.node_id, T0, T0 + timedelta(hours=1), 4
        )

        assert len(results) == 4
        assert history.fetch_calls[0]["limit"] == 5
        assert cont is None


# ------------------------------------------------------- прогон через HistoryManager


def _details(start: datetime, end: datetime, nb_values: int) -> Any:
    details = ua.ReadRawModifiedDetails()
    details.IsReadModified = False
    details.StartTime = start
    details.EndTime = end
    details.NumValuesPerNode = nb_values
    details.ReturnBounds = False
    return details


async def _page_through(
    history: HistoryTimescale,
    details: Any,
    *,
    max_pages: int = 50,
) -> List[Any]:
    """Пройти все страницы через настоящий HistoryManager, как это делает клиент."""
    manager = HistoryManager(None)
    manager.set_storage(history)

    collected: List[Any] = []
    cont: Optional[bytes] = None
    for _ in range(max_pages):
        rv = ua.HistoryReadValueId()
        rv.NodeId = history.node_id
        rv.ContinuationPoint = cont
        values, cont = await manager._read_datavalue_history(rv, details)
        collected.extend(values)
        if not cont:
            return collected
    raise AssertionError("пагинация не завершилась: метка продолжения не обнулилась")


class TestPagingThroughHistoryManager:
    @pytest.mark.asyncio
    async def test_ascending_pages_have_no_duplicates_or_gaps(self):
        rows = _variable_rows(47)
        history = _history_with_rows(rows, history_read_max_response_rows=10)

        collected = await _page_through(
            history, _details(T0, T0 + timedelta(hours=1), 0)
        )

        stamps = [dv.SourceTimestamp for dv in collected]
        assert stamps == [r["sourcetimestamp"] for r in rows]
        assert len(stamps) == len(set(stamps))

    @pytest.mark.asyncio
    async def test_descending_read_keeps_direction_across_pages(self):
        """Ловушка с переворотом направления.

        Чтение «последние N до момента E» приходит как StartTime = win_epoch, и
        `_get_bounds` выбирает DESC. На продолжении asyncua подставляет start = cont,
        оставляя end прежним, поэтому условие `start < end` раньше выбирало ASC: вторая
        страница шла с другого конца окна и повторяла первую в обратном порядке.
        """
        rows = _variable_rows(47)
        history = _history_with_rows(rows, history_read_max_response_rows=10)

        collected = await _page_through(
            history,
            _details(ua.get_win_epoch(), T0 + timedelta(hours=1), 0),
        )

        stamps = [dv.SourceTimestamp for dv in collected]
        expected = [r["sourcetimestamp"] for r in reversed(rows)]
        assert stamps == expected, "страницы должны идти строго от новых к старым"
        assert len(stamps) == len(set(stamps))

    @pytest.mark.asyncio
    async def test_num_values_per_node_is_a_total_not_a_per_response_quota(self):
        """По OPC UA Part 11 NumValuesPerNode — максимум за всё чтение."""
        history = _history_with_rows(_variable_rows(100), history_read_max_response_rows=10)

        collected = await _page_through(
            history, _details(T0, T0 + timedelta(hours=1), 25)
        )

        assert len(collected) == 25

    @pytest.mark.asyncio
    async def test_continuation_store_miss_degrades_without_duplicates(self):
        """Промах по хранилищу (рестарт, протухание) не должен терять или дублировать.

        Направление в этом случае восстановить невозможно, но ASC-чтение обязано
        остаться корректным: метка — это первая отвергнутая строка, окно включающее.
        """
        rows = _variable_rows(47)
        history = _history_with_rows(rows, history_read_max_response_rows=10)
        details = _details(T0, T0 + timedelta(hours=1), 0)

        manager = HistoryManager(None)
        manager.set_storage(history)
        collected: List[Any] = []
        cont: Optional[bytes] = None
        for _ in range(50):
            rv = ua.HistoryReadValueId()
            rv.NodeId = history.node_id
            rv.ContinuationPoint = cont
            values, cont = await manager._read_datavalue_history(rv, details)
            collected.extend(values)
            history._read_continuations.clear()  # имитируем потерю состояния
            if not cont:
                break

        stamps = [dv.SourceTimestamp for dv in collected]
        assert stamps == [r["sourcetimestamp"] for r in rows]


# ------------------------------------------------------------------------------ события


def _event_rows(history: HistoryTimescale, count: int) -> List[Dict[str, Any]]:
    rows = []
    for i in range(count):
        ts = T0 + timedelta(seconds=i)
        payload = history._event_to_binary_map(
            {
                "Message": ua.Variant(f"событие {i}", ua.VariantType.String),
                "Severity": ua.Variant(100 + i, ua.VariantType.UInt16),
            }
        )
        rows.append({"event_timestamp": ts, "event_type_id": 1, "event_data": payload})
    return rows


def _event_history(count: int, **kwargs: Any) -> HistoryTimescale:
    history = HistoryTimescale(schema="history", **kwargs)
    source_node = ua.NodeId("Line.Faults", 2)
    history._datachanges_period[source_node] = (timedelta(days=1), 0, SOURCE_ID, [])
    history.source_node = source_node  # type: ignore[attr-defined]
    rows = _event_rows(history, count)
    history.fetch_calls = []  # type: ignore[attr-defined]

    async def _fetch(sql: str, *args: Any) -> List[Dict[str, Any]]:
        # Курсорный вариант запроса идёт пятым параметром, бескурсорный его не имеет.
        source_db_id, start, end, limit = args[:4]
        cursor = args[4] if len(args) > 4 else None
        has_cursor_predicate = (
            "AND event_timestamp >" in sql or "AND event_timestamp <" in sql
        )
        assert (cursor is not None) == has_cursor_predicate
        history.fetch_calls.append({"limit": limit, "cursor": cursor})
        assert source_db_id == SOURCE_ID
        desc = "event_timestamp DESC" in sql
        selected = [r for r in rows if start <= r["event_timestamp"] <= end]
        if cursor is not None:
            selected = [
                r
                for r in selected
                if (r["event_timestamp"] < cursor if desc else r["event_timestamp"] > cursor)
            ]
        selected.sort(key=lambda r: r["event_timestamp"], reverse=desc)
        return selected[:limit]

    history._fetch = _fetch  # type: ignore[assignment]
    return history


class TestEventPagination:
    @pytest.mark.asyncio
    async def test_filter_dropping_everything_still_continues(self, monkeypatch):
        """Фильтр, отбросивший всю первую выборку, не должен обрывать пагинацию.

        Это тот самый дефект: `len(results)` считался после фильтра и сравнивался с
        лимитом строк SQL, поэтому `cont` становился None при живых данных в БД.
        """
        history = _event_history(40, history_read_max_response_rows=5)
        # Первая выборка (события 0..4) отбрасывается фильтром целиком, вторая проходит.
        kept = set(range(5, 15))

        def _fake_filter(events: List[Any], evfilter: Any) -> List[Any]:
            out = []
            for event in events:
                severity = int(getattr(event, "Severity", 0))
                if (severity - 100) in kept:
                    out.append(event)
            return out

        monkeypatch.setattr(ht_module, "apply_event_filter", _fake_filter)

        results, cont = await history.read_event_history(
            history.source_node, T0, T0 + timedelta(hours=1), 0, object()
        )

        # Первая выборка целиком отброшена фильтром — но страница до-набрана второй.
        assert len(history.fetch_calls) == 2
        assert history.fetch_calls[1]["cursor"] is not None, "до-набор идёт по курсору"
        assert len(results) == 5
        assert cont is not None

    @pytest.mark.asyncio
    async def test_underfilled_page_still_returns_continuation(self, monkeypatch):
        """Недобранная страница при неисчерпанной выборке обязана отдать метку."""
        history = _event_history(100, history_read_max_response_rows=5)

        monkeypatch.setattr(ht_module, "apply_event_filter", lambda events, evfilter: [])

        results, cont = await history.read_event_history(
            history.source_node, T0, T0 + timedelta(hours=1), 0, object()
        )

        assert results == []
        assert cont is not None, "выборка не исчерпана — клиент должен иметь возможность продолжить"

    @pytest.mark.asyncio
    async def test_exhausted_source_closes_pagination(self):
        history = _event_history(3, history_read_max_response_rows=5)

        results, cont = await history.read_event_history(
            history.source_node, T0, T0 + timedelta(hours=1), 0, None
        )

        assert len(results) == 3
        assert cont is None

    @pytest.mark.asyncio
    async def test_event_pages_cover_source_without_duplicates(self):
        history = _event_history(23, history_read_max_response_rows=5)

        collected: List[Any] = []
        cont: Optional[datetime] = None
        start = T0
        for _ in range(20):
            results, cont = await history.read_event_history(
                history.source_node, start, T0 + timedelta(hours=1), 0, None
            )
            collected.extend(results)
            if cont is None:
                break
            start = cont
        else:
            raise AssertionError("пагинация событий не завершилась")

        severities = [int(event.Severity) for event in collected]
        assert severities == list(range(100, 123))
        assert len(severities) == len(set(severities))
