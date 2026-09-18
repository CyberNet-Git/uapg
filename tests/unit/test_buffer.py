"""Буфер записи: батчинг, backpressure и надзор за воркером.

Каждый случай здесь взят из разбора реальных остановок историзации, поэтому
проверяется не только штатный путь, но и то, как буфер ведёт себя, когда БД
отвечать перестала.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, List, Optional

import pytest

from uapg.core.buffer import HistoryWriteBuffer, QueueOverflow
from uapg.core.metrics import BufferStats


@dataclass
class Item:
    value: int
    future: Optional[asyncio.Future] = field(default=None, compare=False, repr=False)


class Recorder:
    """Флаш, поведением которого управляет тест."""

    def __init__(self) -> None:
        self.batches: List[List[Item]] = []
        self.delay = 0.0
        self.error: Optional[Exception] = None
        self.hang = False

    async def __call__(self, batch: List[Item]) -> None:
        if self.hang:
            await asyncio.sleep(3600)
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.error is not None:
            raise self.error
        self.batches.append(list(batch))

    @property
    def flushed(self) -> List[int]:
        return [item.value for batch in self.batches for item in batch]


def _buffer(flush: Recorder, stats: BufferStats, **kwargs: Any) -> HistoryWriteBuffer[Item]:
    params: dict[str, Any] = {
        "max_batch_size": 10,
        "max_batch_interval_sec": 0.01,
        "queue_max_size": 100,
        "durability_mode": "async",
        "flush_timeout_sec": 1.0,
        "stall_timeout_sec": 0.2,
        "drop_log_interval_sec": 60.0,
        "restart_max_backoff_sec": 0.05,
        "logger": logging.getLogger("test.buffer"),
    }
    params.update(kwargs)
    return HistoryWriteBuffer("test", flush, stats, **params)


async def _wait_for(predicate: Any, timeout: float = 2.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("условие не наступило за отведённое время")


class TestBatching:
    async def test_items_are_written_in_batches(self) -> None:
        flush, stats = Recorder(), BufferStats()
        buffer = _buffer(flush, stats)
        buffer.start()
        try:
            for i in range(5):
                await buffer.enqueue(Item(i))
            await _wait_for(lambda: len(flush.flushed) == 5)
        finally:
            await buffer.stop()

        assert sorted(flush.flushed) == [0, 1, 2, 3, 4]
        assert stats.flush_batches_total >= 1

    async def test_batch_size_is_capped(self) -> None:
        flush, stats = Recorder(), BufferStats()
        buffer = _buffer(flush, stats, max_batch_size=3)
        buffer.start()
        try:
            for i in range(9):
                await buffer.enqueue(Item(i))
            await _wait_for(lambda: len(flush.flushed) == 9)
        finally:
            await buffer.stop()

        assert all(len(batch) <= 3 for batch in flush.batches)

    async def test_pending_items_are_written_on_stop(self) -> None:
        """Остановка сервера не должна терять то, что уже принято в очередь."""
        flush, stats = Recorder(), BufferStats()
        buffer = _buffer(flush, stats, max_batch_interval_sec=0.5)
        buffer.start()
        await buffer.enqueue(Item(1))
        await buffer.stop()

        assert flush.flushed == [1]


class TestBackpressure:
    async def test_overflow_drops_and_counts(self) -> None:
        flush, stats = Recorder(), BufferStats()
        flush.hang = True
        buffer = _buffer(flush, stats, queue_max_size=3)
        buffer.start()
        try:
            for i in range(20):
                await buffer.enqueue(Item(i))
        finally:
            await buffer.stop()

        assert stats.dropped_total > 0
        assert stats.enqueue_attempts_total == 20

    async def test_overflow_is_logged_once_per_interval(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Сообщение на каждый отброшенный элемент в одном инциденте дало 618 МБ лога."""
        flush, stats = Recorder(), BufferStats()
        flush.hang = True
        buffer = _buffer(flush, stats, queue_max_size=2, drop_log_interval_sec=3600.0)
        buffer.start()
        with caplog.at_level(logging.ERROR, logger="test.buffer"):
            try:
                for i in range(50):
                    await buffer.enqueue(Item(i))
            finally:
                await buffer.stop()

        drop_messages = [r for r in caplog.records if "очередь заполнена" in r.getMessage()]
        assert len(drop_messages) == 1
        assert stats.dropped_total > 10, "счёт отброшенных должен остаться точным"

    async def test_sync_mode_waits_for_write(self) -> None:
        flush, stats = Recorder(), BufferStats()
        buffer = _buffer(flush, stats, durability_mode="sync")
        buffer.start()
        try:
            await buffer.enqueue(Item(1))
            assert flush.flushed == [1], "синхронный режим обязан дождаться записи"
        finally:
            await buffer.stop()

    async def test_sync_mode_does_not_hang_forever(self) -> None:
        """Полная очередь со залипшим воркером не должна останавливать сервер навсегда."""
        flush, stats = Recorder(), BufferStats()
        flush.hang = True
        buffer = _buffer(flush, stats, queue_max_size=1, flush_timeout_sec=0.05)
        buffer.start()
        try:
            with pytest.raises(QueueOverflow):
                for i in range(10):
                    await buffer.enqueue(Item(i), sync=True)
        finally:
            await buffer.stop()

    async def test_sync_caller_sees_flush_error(self) -> None:
        flush, stats = Recorder(), BufferStats()
        flush.error = RuntimeError("БД недоступна")
        buffer = _buffer(flush, stats)
        buffer.start()
        try:
            with pytest.raises(RuntimeError, match="БД недоступна"):
                await buffer.enqueue(Item(1), sync=True)
        finally:
            await buffer.stop()


class TestWorkerSupervision:
    async def test_flush_error_does_not_stop_worker(self) -> None:
        """Одна неудачная пачка не должна прекращать разбор очереди."""
        flush, stats = Recorder(), BufferStats()
        flush.error = RuntimeError("временный сбой")
        buffer = _buffer(flush, stats)
        buffer.start()
        try:
            await buffer.enqueue(Item(1))
            await _wait_for(lambda: stats.flush_errors_total >= 1)

            flush.error = None
            await buffer.enqueue(Item(2))
            await _wait_for(lambda: flush.flushed == [2])
            assert buffer.is_worker_alive()
        finally:
            await buffer.stop()

    async def test_dead_worker_is_restarted(self, caplog: pytest.LogCaptureFixture) -> None:
        """Воркер, умерший молча, оставлял единственный след — «queue is full»."""
        flush, stats = Recorder(), BufferStats()
        buffer = _buffer(flush, stats)
        buffer.start()
        try:
            with caplog.at_level(logging.CRITICAL, logger="test.buffer"):
                buffer._task.cancel()  # type: ignore[union-attr]
                await asyncio.sleep(0.05)
                assert stats.last_worker_exit_reason is not None
                await _wait_for(buffer.is_worker_alive)

            await buffer.enqueue(Item(7))
            await _wait_for(lambda: flush.flushed == [7])
        finally:
            await buffer.stop()

        assert stats.worker_restarts_total >= 1

    async def test_stalled_worker_is_replaced(self, caplog: pytest.LogCaptureFixture) -> None:
        """Задача жива, ошибок нет, очередь растёт — залипание видно только по флашу."""
        flush, stats = Recorder(), BufferStats()
        flush.hang = True
        buffer = _buffer(flush, stats, stall_timeout_sec=0.05, flush_timeout_sec=0.0)
        buffer.start()
        try:
            await buffer.enqueue(Item(1))
            await _wait_for(lambda: stats.as_dict()["seconds_in_current_flush"] >= 0.05)

            with caplog.at_level(logging.CRITICAL, logger="test.buffer"):
                await buffer.enqueue(Item(2))

            assert stats.worker_stall_restarts_total >= 1
            assert any("снимается" in r.getMessage() for r in caplog.records)
        finally:
            flush.hang = False
            await buffer.stop()

    async def test_worker_not_started_is_not_resurrected_by_enqueue(self) -> None:
        """Буфер, который ещё не запускали, не должен начинать писать сам по себе."""
        flush, stats = Recorder(), BufferStats()
        buffer = _buffer(flush, stats)
        await buffer.enqueue(Item(1))
        assert not buffer.is_worker_alive()
        assert flush.flushed == []


class TestStats:
    async def test_timeout_is_counted_separately(self) -> None:
        flush, stats = Recorder(), BufferStats()
        flush.delay = 1.0
        buffer = _buffer(flush, stats, flush_timeout_sec=0.05)
        buffer.start()
        try:
            await buffer.enqueue(Item(1))
            await _wait_for(lambda: stats.flush_timeouts_total >= 1)
        finally:
            await buffer.stop()

        assert stats.flush_errors_total >= 1
        assert stats.flush_dropped_items_total >= 1

    async def test_queue_size_is_reported(self) -> None:
        flush, stats = Recorder(), BufferStats()
        flush.hang = True
        buffer = _buffer(flush, stats, queue_max_size=10)
        buffer.start()
        try:
            for i in range(4):
                await buffer.enqueue(Item(i))
            await _wait_for(lambda: stats.as_dict()["queue_size"] > 0)
            assert stats.as_dict()["queue_fill_ratio"] > 0
        finally:
            await buffer.stop()
