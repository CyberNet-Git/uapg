"""Метрики историзации.

Снимок отдаётся без обращения к БД: его читают из горячего пути и публикуют в
адресном пространстве OPC UA. Имена узлов строятся из путей словаря
(``write.variables.queue_size`` → ``WriteVariablesQueueSize``), поэтому набор
ключей — часть публичного контракта, а не деталь реализации.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

NEVER = -1.0


def _seconds_since(marker: Optional[float]) -> float:
    return NEVER if marker is None else max(0.0, time.monotonic() - marker)


@dataclass
class Timing:
    """Счётчик длительностей одной операции."""

    count: int = 0
    total_ms: float = 0.0
    last_ms: float = 0.0
    max_ms: float = 0.0

    def observe(self, duration_ms: float) -> None:
        self.count += 1
        self.total_ms += duration_ms
        self.last_ms = duration_ms
        self.max_ms = max(self.max_ms, duration_ms)

    @property
    def avg_ms(self) -> float:
        return self.total_ms / self.count if self.count else 0.0

    def reset(self) -> None:
        self.count = 0
        self.total_ms = 0.0
        self.last_ms = 0.0
        self.max_ms = 0.0

    def as_dict(self, prefix: str) -> Dict[str, float]:
        return {
            f"{prefix}_count": int(self.count),
            f"{prefix}_total_ms": round(self.total_ms, 3),
            f"{prefix}_last_ms": round(self.last_ms, 3),
            f"{prefix}_max_ms": round(self.max_ms, 3),
            f"{prefix}_avg_ms": round(self.avg_ms, 3),
        }


@dataclass
class BufferStats:
    """Состояние очереди записи.

    Разделение «жив ли воркер» и «сколько идёт текущий флаш» не косметическое:
    в инциденте на стенде задача воркера оставалась живой, ошибок не было, а
    очередь переполнялась — залипание видно только по второй величине.
    """

    queue_max_size: int = 0
    queue_size: int = 0
    enqueue_attempts_total: int = 0
    enqueued_total: int = 0
    dropped_total: int = 0
    flushed_items_total: int = 0
    flush_batches_total: int = 0
    last_batch_size: int = 0
    max_batch_size_seen: int = 0
    flush_errors_total: int = 0
    flush_timeouts_total: int = 0
    flush_dropped_items_total: int = 0
    # Пустая строка, а не None: по типу значения выбирается тип узла OPC UA.
    last_flush_error: str = ""
    worker_alive: bool = False
    worker_restarts_total: int = 0
    worker_stall_restarts_total: int = 0
    last_worker_exit_reason: str = ""
    flush_duration: Timing = field(default_factory=Timing)

    _last_flush_at: Optional[float] = None
    _last_enqueue_at: Optional[float] = None
    _flush_started_at: Optional[float] = None

    def record_enqueue_attempt(self) -> None:
        self.enqueue_attempts_total += 1
        self._last_enqueue_at = time.monotonic()

    def record_enqueued(self, queue_size: int) -> None:
        self.enqueued_total += 1
        self.queue_size = queue_size

    def record_dropped(self, count: int = 1) -> None:
        self.dropped_total += count

    def flush_started(self, batch_size: int) -> None:
        self._flush_started_at = time.monotonic()
        self.last_batch_size = batch_size
        self.max_batch_size_seen = max(self.max_batch_size_seen, batch_size)

    def flush_succeeded(self, batch_size: int, duration_ms: float, queue_size: int) -> None:
        self.flush_batches_total += 1
        self.flushed_items_total += batch_size
        self.flush_duration.observe(duration_ms)
        self.queue_size = queue_size
        self._last_flush_at = time.monotonic()
        self._flush_started_at = None

    def flush_failed(self, error: str, *, timeout: bool = False, dropped_items: int = 0) -> None:
        self.flush_errors_total += 1
        if timeout:
            self.flush_timeouts_total += 1
        if dropped_items:
            self.flush_dropped_items_total += dropped_items
        self.last_flush_error = error
        self._flush_started_at = None

    def reset(self) -> None:
        """Обнулить счётчики, сохранив текущее состояние очереди и воркера."""
        self.enqueue_attempts_total = 0
        self.enqueued_total = 0
        self.dropped_total = 0
        self.flushed_items_total = 0
        self.flush_batches_total = 0
        self.last_batch_size = 0
        self.max_batch_size_seen = 0
        self.flush_errors_total = 0
        self.flush_timeouts_total = 0
        self.flush_dropped_items_total = 0
        self.last_flush_error = ""
        self.worker_restarts_total = 0
        self.worker_stall_restarts_total = 0
        self.last_worker_exit_reason = ""
        self.flush_duration.reset()

    def as_dict(self) -> Dict[str, Any]:
        fill_ratio = (
            round(self.queue_size / self.queue_max_size, 4) if self.queue_max_size else 0.0
        )
        return {
            "queue_size": self.queue_size,
            "queue_max_size": self.queue_max_size,
            "queue_fill_ratio": fill_ratio,
            "enqueue_attempts_total": self.enqueue_attempts_total,
            "enqueued_total": self.enqueued_total,
            "dropped_total": self.dropped_total,
            "flushed_items_total": self.flushed_items_total,
            "flush_batches_total": self.flush_batches_total,
            "last_batch_size": self.last_batch_size,
            "max_batch_size_seen": self.max_batch_size_seen,
            "flush_errors_total": self.flush_errors_total,
            "flush_timeouts_total": self.flush_timeouts_total,
            "flush_dropped_items_total": self.flush_dropped_items_total,
            "last_flush_error": str(self.last_flush_error),
            "last_flush_duration_ms": round(self.flush_duration.last_ms, 3),
            "max_flush_duration_ms": round(self.flush_duration.max_ms, 3),
            "avg_flush_duration_ms": round(self.flush_duration.avg_ms, 3),
            "total_flush_duration_ms": round(self.flush_duration.total_ms, 3),
            "worker_alive": self.worker_alive,
            "worker_restarts_total": self.worker_restarts_total,
            "worker_stall_restarts_total": self.worker_stall_restarts_total,
            "last_worker_exit_reason": str(self.last_worker_exit_reason),
            "seconds_since_last_flush": round(_seconds_since(self._last_flush_at), 3),
            "seconds_since_last_enqueue": round(_seconds_since(self._last_enqueue_at), 3),
            # Длительность текущего незавершённого флаша отличает залипание от простоя.
            "seconds_in_current_flush": round(_seconds_since(self._flush_started_at), 3),
        }


@dataclass
class DomainMetrics:
    """Метрики одного домена записи — переменных или событий."""

    calls_key: str
    errors_key: str
    with_last_value_timing: bool
    calls_total: int = 0
    errors_total: int = 0
    buffer: BufferStats = field(default_factory=BufferStats)
    flush: Timing = field(default_factory=Timing)
    insert_history: Timing = field(default_factory=Timing)
    upsert_last_value: Timing = field(default_factory=Timing)

    def record_call(self) -> None:
        self.calls_total += 1

    def record_error(self) -> None:
        self.errors_total += 1

    def reset(self) -> None:
        self.calls_total = 0
        self.errors_total = 0
        self.buffer.reset()
        self.flush.reset()
        self.insert_history.reset()
        self.upsert_last_value.reset()

    def as_dict(self) -> Dict[str, Any]:
        snapshot: Dict[str, Any] = {
            self.calls_key: self.calls_total,
            self.errors_key: self.errors_total,
        }
        snapshot.update(self.buffer.as_dict())
        snapshot.update(self.flush.as_dict("flush"))
        snapshot.update(self.insert_history.as_dict("insert_history"))
        if self.with_last_value_timing:
            snapshot.update(self.upsert_last_value.as_dict("upsert_last_value"))
        return snapshot


@dataclass
class DatabaseMetrics:
    """Доступность БД: по этим счётчикам отличают проблемы сети от проблем нагрузки."""

    timeouts_total: int = 0
    reconnects_total: int = 0
    reconnects_skipped_total: int = 0
    pool_wait_timeouts_total: int = 0

    def reset(self) -> None:
        self.timeouts_total = 0
        self.reconnects_total = 0
        self.reconnects_skipped_total = 0
        self.pool_wait_timeouts_total = 0

    def as_dict(self) -> Dict[str, int]:
        return {
            "timeouts_total": self.timeouts_total,
            "reconnects_total": self.reconnects_total,
            "reconnects_skipped_total": self.reconnects_skipped_total,
            "pool_wait_timeouts_total": self.pool_wait_timeouts_total,
        }


class CacheStats:
    """Счётчики попаданий в кэши; набор ключей фиксирован контрактом."""

    KEYS = (
        "last_values_memory_hits",
        "last_values_memory_misses",
        "last_values_table_hits",
        "last_values_table_misses",
        "last_values_history_fallbacks",
        "variable_metadata_hits",
        "variable_metadata_misses",
        "event_source_hits",
        "event_source_misses",
        "event_type_hits",
        "event_type_misses",
    )

    def __init__(self) -> None:
        self._counters: Dict[str, int] = {key: 0 for key in self.KEYS}

    def hit(self, key: str, amount: int = 1) -> None:
        # Необязательные счётчики (например, пропущенные фоллбэки по истории)
        # появляются в снимке только после первого срабатывания, как в 0.2.15.
        self._counters[key] = self._counters.get(key, 0) + amount

    def reset(self) -> None:
        for key in self._counters:
            self._counters[key] = 0

    def as_dict(self) -> Dict[str, int]:
        return dict(self._counters)


class MetricsRegistry:
    """Единая точка сбора метрик и единственное место, где собирается их снимок."""

    def __init__(self) -> None:
        self.variables = DomainMetrics(
            calls_key="save_node_value_calls_total",
            errors_key="save_node_value_errors_total",
            with_last_value_timing=True,
        )
        self.events = DomainMetrics(
            calls_key="save_event_calls_total",
            errors_key="save_event_errors_total",
            with_last_value_timing=False,
        )
        self.database = DatabaseMetrics()
        self.cache = CacheStats()

    def reset(self) -> None:
        self.variables.reset()
        self.events.reset()
        self.database.reset()

    def snapshot(self, config: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "write": {
                "variables": self.variables.as_dict(),
                "events": self.events.as_dict(),
            },
            "db": self.database.as_dict(),
            # Очистка по каждой переменной и каждому событию в пути записи не
            # выполняется: старые данные удаляет глобальная политика TimescaleDB.
            "retention": {
                "per_variable_cleanup_enabled": False,
                "per_event_cleanup_enabled": False,
            },
            "config": config,
        }
