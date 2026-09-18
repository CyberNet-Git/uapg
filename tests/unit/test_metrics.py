"""Метрики: структура снимка и поведение счётчиков.

Набор ключей сверяется с замороженным контрактом, потому что из него строятся
имена узлов OPC UA: переименование ключа ломает адресное пространство сервера
молча, без единой ошибки в логе.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from uapg.core.config import (
    CacheSettings,
    ConnectionSettings,
    Keepalive,
    StorageSettings,
    Timeouts,
    WriteSettings,
)
from uapg.core.metrics import NEVER, BufferStats, CacheStats, MetricsRegistry, Timing

BASELINE_API = Path(__file__).parents[1] / "contract" / "baseline" / "api.json"


def _paths(value: Any, prefix: str = "") -> List[str]:
    if not isinstance(value, dict):
        return [prefix]
    paths: List[str] = []
    for key in sorted(value):
        child = f"{prefix}.{key}" if prefix else str(key)
        paths.extend(_paths(value[key], child))
    return paths


def _settings() -> StorageSettings:
    return StorageSettings(
        connection=ConnectionSettings.build(),
        timeouts=Timeouts.build(),
        keepalive=Keepalive.build(),
        write=WriteSettings.build(),
        cache=CacheSettings.build(),
    )


def test_snapshot_paths_match_contract() -> None:
    baseline: Dict[str, Any] = json.loads(BASELINE_API.read_text())
    registry = MetricsRegistry()
    snapshot = registry.snapshot(_settings().metrics_snapshot())
    assert _paths(snapshot) == baseline["metric_paths"]


def test_value_types_match_contract() -> None:
    """Тип значения выбирает тип данных узла OPC UA; другой тип в узел не запишется."""
    from tests.contract.api_introspect import metric_types

    baseline = json.loads(BASELINE_API.read_text())["metric_types"]
    current = metric_types(MetricsRegistry().snapshot(_settings().metrics_snapshot()))
    assert current == baseline


def test_cache_keys_match_contract() -> None:
    baseline = json.loads(BASELINE_API.read_text())
    assert sorted(CacheStats().as_dict()) == baseline["cache_stat_keys"]


class TestTiming:
    def test_aggregates(self) -> None:
        timing = Timing()
        timing.observe(10.0)
        timing.observe(20.0)
        assert (timing.count, timing.total_ms, timing.last_ms, timing.max_ms) == (2, 30.0, 20.0, 20.0)
        assert timing.avg_ms == 15.0

    def test_empty_average_is_zero_not_error(self) -> None:
        assert Timing().avg_ms == 0.0

    def test_flattened_keys(self) -> None:
        assert set(Timing().as_dict("flush")) == {
            "flush_count",
            "flush_total_ms",
            "flush_last_ms",
            "flush_max_ms",
            "flush_avg_ms",
        }


class TestBufferStats:
    def test_unknown_moments_report_never(self) -> None:
        stats = BufferStats().as_dict()
        assert stats["seconds_since_last_flush"] == NEVER
        assert stats["seconds_since_last_enqueue"] == NEVER
        assert stats["seconds_in_current_flush"] == NEVER

    def test_current_flush_duration_visible_while_running(self) -> None:
        """Залипший флаш отличается от простоя только этой величиной."""
        stats = BufferStats()
        stats.flush_started(batch_size=5)
        assert stats.as_dict()["seconds_in_current_flush"] >= 0.0

        stats.flush_succeeded(batch_size=5, duration_ms=12.0, queue_size=0)
        assert stats.as_dict()["seconds_in_current_flush"] == NEVER
        assert stats.as_dict()["seconds_since_last_flush"] >= 0.0

    def test_fill_ratio(self) -> None:
        stats = BufferStats(queue_max_size=200)
        stats.record_enqueued(queue_size=50)
        assert stats.as_dict()["queue_fill_ratio"] == 0.25

    def test_fill_ratio_without_limit_is_zero(self) -> None:
        assert BufferStats().as_dict()["queue_fill_ratio"] == 0.0

    def test_timeout_counted_separately_from_errors(self) -> None:
        stats = BufferStats()
        stats.flush_failed("boom", timeout=True, dropped_items=3)
        snapshot = stats.as_dict()
        assert snapshot["flush_errors_total"] == 1
        assert snapshot["flush_timeouts_total"] == 1
        assert snapshot["flush_dropped_items_total"] == 3
        assert snapshot["last_flush_error"] == "boom"

    def test_reset_keeps_queue_and_worker_state(self) -> None:
        """Сброс счётчиков не должен врать про текущее состояние очереди."""
        stats = BufferStats(queue_max_size=10)
        stats.worker_alive = True
        stats.record_enqueued(queue_size=4)
        stats.record_dropped(2)
        stats.reset()
        snapshot = stats.as_dict()
        assert snapshot["dropped_total"] == 0
        assert snapshot["queue_size"] == 4
        assert snapshot["worker_alive"] is True


def test_registry_reset_clears_counters() -> None:
    registry = MetricsRegistry()
    registry.variables.record_call()
    registry.variables.record_error()
    registry.database.timeouts_total = 5
    registry.reset()

    snapshot = registry.snapshot(_settings().metrics_snapshot())
    assert snapshot["write"]["variables"]["save_node_value_calls_total"] == 0
    assert snapshot["write"]["variables"]["save_node_value_errors_total"] == 0
    assert snapshot["db"]["timeouts_total"] == 0


def test_events_have_no_last_value_timing() -> None:
    """У событий нет последнего значения, и лишних ключей в снимке быть не должно."""
    events = MetricsRegistry().events.as_dict()
    assert not [key for key in events if key.startswith("upsert_last_value")]
