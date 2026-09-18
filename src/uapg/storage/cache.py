"""Кэши в памяти процесса.

Историзация обращается к одним и тем же отображениям на каждой записи: узел в
идентификатор переменной, источник события в идентификатор источника. Без кэша
это лишний запрос к БД на каждое значение.

Кэши намеренно простые: словарь и счётчики попаданий. Они не вытесняют записи и
не истекают — отображения не меняются в течение жизни процесса, а их число
ограничено числом историзуемых узлов.
"""

from __future__ import annotations

from typing import Dict, Generic, Optional, TypeVar

from asyncua import ua

from ..core.metrics import CacheStats

K = TypeVar("K")


class LookupCache(Generic[K]):
    """Отображение ключа в идентификатор БД со счётом попаданий."""

    def __init__(self, stats: CacheStats, hit_key: str, miss_key: str) -> None:
        self._values: Dict[K, int] = {}
        self._stats = stats
        self._hit_key = hit_key
        self._miss_key = miss_key

    def get(self, key: K) -> Optional[int]:
        value = self._values.get(key)
        self._stats.hit(self._hit_key if value is not None else self._miss_key)
        return value

    def peek(self, key: K) -> Optional[int]:
        """Посмотреть без учёта в статистике."""
        return self._values.get(key)

    def put(self, key: K, value: int) -> None:
        self._values[key] = value

    def update(self, values: Dict[K, int]) -> None:
        self._values.update(values)

    def clear(self) -> None:
        self._values.clear()

    def __contains__(self, key: K) -> bool:
        return key in self._values

    def __len__(self) -> int:
        return len(self._values)


class LastValueCache:
    """Последнее значение каждой переменной.

    Нужен, чтобы чтение последнего значения сразу после записи видело именно
    записанное: при асинхронном режиме запись доезжает до БД позже.
    """

    def __init__(self, stats: CacheStats, enabled: bool = True) -> None:
        self._values: Dict[int, ua.DataValue] = {}
        self._stats = stats
        self._enabled = enabled

    @property
    def enabled(self) -> bool:
        return self._enabled

    def get(self, variable_id: int) -> Optional[ua.DataValue]:
        if not self._enabled:
            return None
        value = self._values.get(variable_id)
        self._stats.hit(
            "last_values_memory_hits" if value is not None else "last_values_memory_misses"
        )
        return value

    def put(self, variable_id: int, datavalue: ua.DataValue) -> None:
        """Запомнить значение, не откатывая кэш назад по времени источника."""
        if not self._enabled:
            return
        current = self._values.get(variable_id)
        if current is not None and _is_older(datavalue, current):
            return
        self._values[variable_id] = datavalue

    def update(self, values: Dict[int, ua.DataValue]) -> None:
        if self._enabled:
            self._values.update(values)

    def clear(self) -> None:
        self._values.clear()

    def __len__(self) -> int:
        return len(self._values)


def _is_older(candidate: ua.DataValue, current: ua.DataValue) -> bool:
    """Значения приходят не по порядку: запоздавшее не должно вытеснять свежее."""
    new_time = candidate.SourceTimestamp
    old_time = current.SourceTimestamp
    if new_time is None or old_time is None:
        return False
    return new_time < old_time


class Caches:
    """Все кэши историзации и их общая статистика."""

    def __init__(self, stats: CacheStats, *, last_values_enabled: bool = True) -> None:
        self.stats = stats
        self.variables = LookupCache[str](
            stats, "variable_metadata_hits", "variable_metadata_misses"
        )
        self.event_sources = LookupCache[str](stats, "event_source_hits", "event_source_misses")
        self.event_types = LookupCache[str](stats, "event_type_hits", "event_type_misses")
        self.last_values = LastValueCache(stats, enabled=last_values_enabled)

    def clear(self) -> None:
        self.variables.clear()
        self.event_sources.clear()
        self.event_types.clear()
        self.last_values.clear()
