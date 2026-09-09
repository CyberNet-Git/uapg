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


@pytest.mark.asyncio
async def test_hanging_flush_times_out_and_worker_keeps_draining():
    """
    Ключевой сценарий инцидента на АГК: флаш ушёл в await к мёртвому сокету и не
    вернулся. Без ограничения воркер оставался «живым» навсегда, и очередь
    переполнялась без единой ошибки в логе.
    """
    flush_calls = []

    async def hanging_flush(batch):
        flush_calls.append(len(batch))
        if len(flush_calls) == 1:
            await asyncio.sleep(3600)

    buffer = make_buffer(hanging_flush, flush_timeout_sec=0.2)
    buffer.start()
    await buffer.enqueue(object())
    await asyncio.sleep(0.5)

    stats = buffer.get_stats()
    assert stats["flush_timeouts_total"] == 1
    assert stats["flush_errors_total"] == 1
    assert "timed out" in stats["last_flush_error"]
    assert stats["worker_alive"] is True

    # Воркер должен продолжить работу, а не остаться в зависшем флаше
    await buffer.enqueue(object())
    await asyncio.sleep(0.2)
    assert buffer.get_stats()["flushed_items_total"] >= 1

    await buffer.stop()


@pytest.mark.asyncio
async def test_stalled_worker_is_restarted_from_enqueue_path():
    """Живой, но залипший воркер: task.done() остаётся False, и обычный надзор слеп."""

    async def hanging_flush(batch):
        await asyncio.sleep(3600)

    # Таймаут флаша выключен, чтобы проверить именно надзор за залипанием
    buffer = make_buffer(hanging_flush, flush_timeout_sec=0, stall_timeout_sec=0.3)
    buffer.start()
    await buffer.enqueue(object())
    await asyncio.sleep(0.1)

    stalled_task = buffer._task
    assert buffer.is_worker_alive() is True
    assert buffer.get_stats()["worker_stall_restarts_total"] == 0

    await asyncio.sleep(0.4)
    await buffer.enqueue(object())

    stats = buffer.get_stats()
    assert stats["worker_stall_restarts_total"] == 1
    assert "stalled in flush" in stats["last_worker_exit_reason"]
    assert buffer.is_worker_alive() is True
    assert buffer._task is not stalled_task

    await buffer.stop()


@pytest.mark.asyncio
async def test_seconds_in_current_flush_exposes_stuck_flush():
    async def hanging_flush(batch):
        await asyncio.sleep(3600)

    buffer = make_buffer(hanging_flush, flush_timeout_sec=0, stall_timeout_sec=0)
    assert buffer.get_stats()["seconds_in_current_flush"] == -1.0

    buffer.start()
    await buffer.enqueue(object())
    await asyncio.sleep(0.2)

    assert buffer.get_stats()["seconds_in_current_flush"] > 0.0

    buffer._stopped = True
    buffer._task.cancel()


@pytest.mark.asyncio
async def test_pool_params_carry_command_timeout_and_socket_setup():
    """command_timeout в asyncpg покрывает BEGIN/COMMIT, у которых нет своего timeout."""
    history = HistoryTimescale(db_command_timeout_sec=45.0)
    params = history._build_pool_params()

    assert params["command_timeout"] == 45.0
    assert params["init"] == history._configure_connection


@pytest.mark.asyncio
async def test_command_timeout_disabled_by_non_positive_value():
    history = HistoryTimescale(db_command_timeout_sec=0)
    assert history._build_pool_params()["command_timeout"] is None


@pytest.mark.asyncio
async def test_configure_connection_enables_tcp_keepalive():
    import socket as socket_module

    history = HistoryTimescale(
        db_tcp_keepalive_idle_sec=15,
        db_tcp_keepalive_interval_sec=5,
        db_tcp_keepalive_count=4,
        db_tcp_user_timeout_sec=30.0,
    )

    sock = socket_module.socket(socket_module.AF_INET, socket_module.SOCK_STREAM)
    conn = Mock()
    conn._transport.get_extra_info.return_value = sock
    try:
        await history._configure_connection(conn)

        assert sock.getsockopt(socket_module.SOL_SOCKET, socket_module.SO_KEEPALIVE) == 1
        assert sock.getsockopt(socket_module.IPPROTO_TCP, socket_module.TCP_KEEPIDLE) == 15
        assert sock.getsockopt(socket_module.IPPROTO_TCP, socket_module.TCP_KEEPINTVL) == 5
        assert sock.getsockopt(socket_module.IPPROTO_TCP, socket_module.TCP_KEEPCNT) == 4
        assert sock.getsockopt(socket_module.IPPROTO_TCP, socket_module.TCP_USER_TIMEOUT) == 30000
    finally:
        sock.close()


@pytest.mark.asyncio
async def test_configure_connection_survives_broken_socket():
    """Настройка сокета — не повод ронять подключение к БД."""
    history = HistoryTimescale()
    conn = Mock()
    conn._transport.get_extra_info.side_effect = RuntimeError("no transport")

    await history._configure_connection(conn)
