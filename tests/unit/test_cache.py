"""Кэши в памяти процесса."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from asyncua import ua

from uapg.core.metrics import CacheStats
from uapg.storage.cache import Caches, LastValueCache, LookupCache

MOMENT = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)


def _datavalue(value: float, moment: datetime) -> ua.DataValue:
    return ua.DataValue(Value=ua.Variant(value, ua.VariantType.Double), SourceTimestamp=moment)


class TestLookupCache:
    def test_hit_and_miss_are_counted(self) -> None:
        stats = CacheStats()
        cache = LookupCache[str](stats, "variable_metadata_hits", "variable_metadata_misses")

        assert cache.get("ns=2;i=1") is None
        cache.put("ns=2;i=1", 7)
        assert cache.get("ns=2;i=1") == 7

        counters = stats.as_dict()
        assert counters["variable_metadata_misses"] == 1
        assert counters["variable_metadata_hits"] == 1

    def test_peek_does_not_count(self) -> None:
        stats = CacheStats()
        cache = LookupCache[str](stats, "variable_metadata_hits", "variable_metadata_misses")
        cache.put("a", 1)
        assert cache.peek("a") == 1
        assert stats.as_dict()["variable_metadata_hits"] == 0


class TestLastValueCache:
    def test_stores_and_returns(self) -> None:
        cache = LastValueCache(CacheStats())
        cache.put(1, _datavalue(1.0, MOMENT))
        value = cache.get(1)
        assert value is not None and value.Value.Value == 1.0

    def test_late_value_does_not_rewind(self) -> None:
        """Значения приходят не по порядку; запоздавшее не должно вытеснить свежее."""
        cache = LastValueCache(CacheStats())
        cache.put(1, _datavalue(2.0, MOMENT + timedelta(seconds=10)))
        cache.put(1, _datavalue(1.0, MOMENT))

        value = cache.get(1)
        assert value is not None and value.Value.Value == 2.0

    def test_disabled_cache_stores_nothing(self) -> None:
        cache = LastValueCache(CacheStats(), enabled=False)
        cache.put(1, _datavalue(1.0, MOMENT))
        assert cache.get(1) is None
        assert len(cache) == 0

    def test_values_without_timestamps_are_accepted(self) -> None:
        cache = LastValueCache(CacheStats())
        without = ua.DataValue(Value=ua.Variant(1.0, ua.VariantType.Double))
        cache.put(1, without)
        cache.put(1, _datavalue(2.0, MOMENT))
        value = cache.get(1)
        assert value is not None and value.Value.Value == 2.0


def test_caches_share_stats() -> None:
    stats = CacheStats()
    caches = Caches(stats)
    caches.variables.get("missing")
    caches.event_sources.get("missing")
    caches.event_types.get("missing")

    counters = stats.as_dict()
    assert counters["variable_metadata_misses"] == 1
    assert counters["event_source_misses"] == 1
    assert counters["event_type_misses"] == 1


def test_clear_empties_everything() -> None:
    caches = Caches(CacheStats())
    caches.variables.put("a", 1)
    caches.last_values.put(1, _datavalue(1.0, MOMENT))
    caches.clear()
    assert len(caches.variables) == 0
    assert len(caches.last_values) == 0
