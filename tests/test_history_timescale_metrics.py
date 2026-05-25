import asyncio
import logging
import sys
from unittest.mock import Mock

sys.modules.setdefault("psycopg", Mock())

from uapg.history_timescale import HistoryTimescale, HistoryWriteBuffer


def test_performance_metrics_initial_snapshot():
    history = HistoryTimescale()

    metrics = history.get_performance_metrics()

    assert metrics["write"]["variables"]["save_node_value_calls_total"] == 0
    assert metrics["write"]["variables"]["queue_size"] == 0
    assert metrics["write"]["events"]["save_event_calls_total"] == 0
    assert metrics["db"]["timeouts_total"] == 0
    assert metrics["retention"]["per_variable_cleanup_enabled"] is False
    assert metrics["retention"]["per_event_cleanup_enabled"] is False
    assert metrics["config"]["history_write_batch_enabled"] is True


def test_reset_performance_metrics_resets_counters_and_buffers():
    history = HistoryTimescale()
    history._perf_inc("save_node_value_calls_total", 3)
    history._perf_observe_ms("variable_flush_total", 25.0)
    history._value_write_buffer = HistoryWriteBuffer(
        name="variables",
        logger=logging.getLogger("test"),
        max_batch_size=10,
        max_batch_interval_sec=1.0,
        queue_max_size=10,
        durability_mode="async",
        flush_func=lambda batch: None,
    )
    history._value_write_buffer._stats["enqueued_total"] = 4

    history.reset_performance_metrics()
    metrics = history.get_performance_metrics()

    assert metrics["write"]["variables"]["save_node_value_calls_total"] == 0
    assert metrics["write"]["variables"]["flush_count"] == 0
    assert metrics["write"]["variables"]["enqueued_total"] == 0


def test_write_buffer_enqueue_and_drop_stats():
    async def flush_func(batch):
        return None

    async def run_test():
        buffer = HistoryWriteBuffer(
            name="variables",
            logger=logging.getLogger("test"),
            max_batch_size=10,
            max_batch_interval_sec=1.0,
            queue_max_size=1,
            durability_mode="async",
            flush_func=flush_func,
        )

        await buffer.enqueue(object())
        await buffer.enqueue(object())
        return buffer.get_stats()

    stats = asyncio.run(run_test())
    assert stats["enqueue_attempts_total"] == 2
    assert stats["enqueued_total"] == 1
    assert stats["dropped_total"] == 1
    assert stats["queue_size"] == 1
    assert stats["queue_fill_ratio"] == 1.0


def test_write_buffer_flush_stats():
    async def flush_func(batch):
        return None

    async def run_test():
        buffer = HistoryWriteBuffer(
            name="variables",
            logger=logging.getLogger("test"),
            max_batch_size=10,
            max_batch_interval_sec=1.0,
            queue_max_size=10,
            durability_mode="async",
            flush_func=flush_func,
        )

        await buffer._flush_pending([object(), object(), object()])
        return buffer.get_stats()

    stats = asyncio.run(run_test())
    assert stats["flush_batches_total"] == 1
    assert stats["flushed_items_total"] == 3
    assert stats["last_batch_size"] == 3
    assert stats["max_batch_size_seen"] == 3
    assert stats["last_flush_duration_ms"] >= 0.0
    assert stats["avg_flush_duration_ms"] >= 0.0
