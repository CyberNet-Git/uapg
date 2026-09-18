"""Границы чтения истории и точки продолжения.

Поведение задано OPC UA Part 11 и должно совпадать с 0.2.15: клиенты, уже
работающие с этим сервером, не должны заметить смены реализации.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from asyncua import ua

from uapg.opcua.reads import (
    DEFAULT_READ_LIMIT,
    continuation_point,
    limit_values,
    resolve_window,
)

START = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
END = datetime(2026, 9, 1, 13, 0, tzinfo=timezone.utc)


class TestWindowDirection:
    def test_normal_range_reads_oldest_first(self) -> None:
        window = resolve_window(START, END, 100)
        assert (window.start, window.end, window.order) == (START, END, "ASC")

    def test_missing_start_reads_newest_first(self) -> None:
        """Без времени начала клиент просит последние значения, а не первые."""
        window = resolve_window(None, END, 100)
        assert window.order == "DESC"
        assert window.end == END

    def test_epoch_start_reads_newest_first(self) -> None:
        """Клиенты присылают начало эпохи вместо отсутствующего времени."""
        window = resolve_window(ua.get_win_epoch(), END, 100)
        assert window.order == "DESC"

    def test_inverted_range_reads_newest_first(self) -> None:
        window = resolve_window(END, START, 100)
        assert (window.start, window.end, window.order) == (START, END, "DESC")

    def test_missing_end_extends_into_future(self) -> None:
        """Значение с временем источника чуть впереди часов сервера не должно пропасть."""
        window = resolve_window(START, None, 100)
        assert window.end > datetime.now(timezone.utc)

    def test_both_missing(self) -> None:
        window = resolve_window(None, None, 100)
        assert window.order == "DESC"
        assert window.start == ua.get_win_epoch()


class TestLimit:
    def test_explicit_limit(self) -> None:
        assert resolve_window(START, END, 42).limit == 42

    def test_zero_means_server_default(self) -> None:
        """Ноль в OPC UA — «без ограничения от клиента», но ответ ограничить обязан сервер."""
        assert resolve_window(START, END, 0).limit == DEFAULT_READ_LIMIT

    def test_none_means_server_default(self) -> None:
        assert resolve_window(START, END, None).limit == DEFAULT_READ_LIMIT


class TestContinuationPoint:
    def test_full_page_returns_marker(self) -> None:
        rows = [{"ts": START + timedelta(seconds=i)} for i in range(3)]
        assert continuation_point(rows, returned=3, limit=3, field="ts") == rows[-1]["ts"]

    def test_partial_page_has_no_marker(self) -> None:
        """Страница короче лимита означает, что читать больше нечего."""
        rows = [{"ts": START}]
        assert continuation_point(rows, returned=1, limit=10, field="ts") is None

    def test_empty_result_has_no_marker(self) -> None:
        assert continuation_point([], returned=0, limit=10, field="ts") is None

    def test_filtered_page_has_no_marker(self) -> None:
        """Если фильтр сократил выборку, лимит не достигнут и продолжения нет."""
        rows = [{"ts": START + timedelta(seconds=i)} for i in range(5)]
        assert continuation_point(rows, returned=2, limit=5, field="ts") is None


def test_limit_values() -> None:
    assert limit_values([1, 2, 3], 2) == [1, 2]
    assert limit_values([1, 2], 5) == [1, 2]
    assert limit_values([1, 2], 0) == [1, 2]
