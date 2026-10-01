"""HistoryTimescale с включённым поиском событий.

Вся логика живёт в ``HistoryTimescale``. Этот класс отличается ровно одним:
если режим хранения событий не передан явно, он берётся из переменной
окружения ``UAPG_EVENTS_STORAGE_MODE`` (по умолчанию ``dual``), тогда как
голый ``HistoryTimescale`` остаётся в ``legacy`` и окружение не читает.
Именно так вели себя два класса в 0.2.15, и opc-vibro-iot-server выбирает
между ними по ``HISTORY_STORAGE_VERSION`` — поэтому имя и поведение сохранены.
"""

from typing import Any, Dict, Optional

from asyncua import ua

from .history_timescale import HistoryTimescale
from .storage.events_config import EventsV2Config
from .storage.storage_mode import StorageMode, get_events_storage_mode


class HistoryTimescaleV2(HistoryTimescale):
    """Историзация с типизированным хранением и поиском событий."""

    _publish_event_capabilities = True

    def __init__(
        self,
        *args: Any,
        events_storage_mode: Optional[StorageMode] = None,
        events_v2_config: Optional[EventsV2Config] = None,
        events_backfill_probe_rows: int = 1000,
        events_backfill_status_ttl_sec: float = 30.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._configure_events(
            events_storage_mode or get_events_storage_mode(),
            events_v2_config,
            backfill_probe_rows=events_backfill_probe_rows,
            backfill_status_ttl_sec=events_backfill_status_ttl_sec,
        )

    @property
    def events_storage_mode(self) -> StorageMode:
        return self._events_mode

    def get_performance_metrics(self) -> dict:
        metrics = super().get_performance_metrics()
        metrics["events_v2"] = {
            "storage_mode": self._events_mode.value,
            "storage_ready": bool(self._v2_ready),
            "backfill_probe_failures_total": (
                self._event_search.probe_failures if self._event_search is not None else 0
            ),
            "backfill_probe_rows": int(self._events_backfill_probe_rows),
            "backfill_status_ttl_sec": float(self._events_backfill_status_ttl_sec),
        }
        return metrics

    async def run_events_backfill(self, batch_size: int = 500) -> Dict[str, Any]:
        """Перенести очередную порцию событий из устаревшего хранения в слой поиска."""
        return await self._run_events_backfill(batch_size)

    async def explain_event_filter(
        self,
        source_id: ua.NodeId,
        start: Any,
        end: Any,
        nb_values: Optional[int],
        evfilter: Any,
    ) -> str:
        """План выполнения запроса истории событий — для диагностики фильтров."""
        return await self._explain_event_filter(source_id, start, end, nb_values, evfilter)
