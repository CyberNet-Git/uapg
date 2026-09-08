"""
Надзор за воркером HistoryWriteBuffer и ограничение неограниченных ожиданий.

Инцидент, из которого выросли эти тесты: воркер переставал сливать очередь,
очередь доходила до предела, и единственным следом оставался поток сообщений
«queue is full» — ни о смерти воркера, ни о залипании на замке следа не было.
"""
import asyncio
import logging
import sys
from unittest.mock import Mock

import pytest

sys.modules.setdefault("psycopg", Mock())

from uapg.history_timescale import HistoryTimescale, HistoryWriteBuffer


def make_buffer(flush_func, **kwargs):
    params = {
        "name": "variables",
        "logger": logging.getLogger("test"),
        "max_batch_size": 10,
        "max_batch_interval_sec": 0.01,
        "queue_max_size": 5,
        "durability_mode": "async",
        "flush_func": flush_func,
    }
    params.update(kwargs)
    return HistoryWriteBuffer(**params)


async def noop_flush(batch):
    return None


@pytest.mark.asyncio
async def test_worker_restarts_after_external_cancel():
    buffer = make_buffer(noop_flush)
    buffer.start()
    await asyncio.sleep(0.05)
    assert buffer.is_worker_alive() is True

    buffer._task.cancel()
    # done-callback ставит перезапуск через call_later с backoff от 1 с
    await asyncio.sleep(1.3)

    stats = buffer.get_stats()
    assert stats["worker_restarts_total"] == 1
    assert stats["last_worker_exit_reason"] == "cancelled"
    assert stats["worker_alive"] is True

    await buffer.stop()


@pytest.mark.asyncio
async def test_worker_restarts_after_unexpected_exception():
    class Boom(BaseException):
        """Не Exception: именно такие ошибки убивали воркер бесшумно."""

    async def exploding_flush(batch):
        raise Boom("worker killed")

    buffer = make_buffer(exploding_flush)
    buffer.start()
    await buffer.enqueue(object())
    await asyncio.sleep(1.3)

    stats = buffer.get_stats()
    assert stats["worker_restarts_total"] >= 1
    assert "Boom" in stats["last_worker_exit_reason"]

    buffer._stopped = True
    if buffer._restart_handle is not None:
        buffer._restart_handle.cancel()


@pytest.mark.asyncio
async def test_enqueue_revives_dead_worker():
    buffer = make_buffer(noop_flush)
    buffer.start()
    await asyncio.sleep(0.05)

    # Симулируем уже умерший воркер без запланированного перезапуска
    buffer._task.cancel()
    await asyncio.sleep(0.05)
    buffer._task = None
    if buffer._restart_handle is not None:
        buffer._restart_handle.cancel()
        buffer._restart_handle = None

    await buffer.enqueue(object())
    assert buffer.is_worker_alive() is True

    await buffer.stop()


@pytest.mark.asyncio
async def test_never_started_buffer_is_not_revived_by_enqueue():
    buffer = make_buffer(noop_flush)
    await buffer.enqueue(object())
    assert buffer.is_worker_alive() is False


@pytest.mark.asyncio
async def test_drop_logging_is_rate_limited_but_counter_is_exact(caplog):
    buffer = make_buffer(noop_flush, queue_max_size=1, drop_log_interval_sec=3600.0)

    with caplog.at_level(logging.ERROR):
        for _ in range(50):
            await buffer.enqueue(object())

    stats = buffer.get_stats()
    assert stats["dropped_total"] == 49
    drop_messages = [r for r in caplog.records if "queue is full" in r.getMessage()]
    assert len(drop_messages) == 1


@pytest.mark.asyncio
async def test_seconds_since_last_flush_tracks_successful_flush():
    buffer = make_buffer(noop_flush)
    assert buffer.get_stats()["seconds_since_last_flush"] == -1.0

    await buffer._flush_pending([object()])

    seconds = buffer.get_stats()["seconds_since_last_flush"]
    assert 0.0 <= seconds < 1.0


@pytest.mark.asyncio
async def test_empty_buffer_metrics_match_get_stats_keys():
    """Узлы OPC UA создаются по набору ключей — расхождение ломает HistoryMetrics."""
    buffer = make_buffer(noop_flush)
    assert set(HistoryTimescale._empty_buffer_metrics()) == set(buffer.get_stats())


@pytest.mark.asyncio
async def test_ensure_pool_lock_wait_is_bounded():
    history = HistoryTimescale(db_lock_wait_timeout_sec=1.0)

    await history._pool_lock.acquire()
    try:
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(history._ensure_pool(), timeout=5.0)
    finally:
        history._pool_lock.release()

    metrics = history.get_performance_metrics()
    assert metrics["db"]["pool_wait_timeouts_total"] == 1


@pytest.mark.asyncio
async def test_pool_params_carry_connect_timeout():
    history = HistoryTimescale(db_pool_create_timeout_sec=7.0)
    assert history._build_pool_params()["timeout"] == 7.0
