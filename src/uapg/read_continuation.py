"""Состояние выданных continuation points для HistoryRead.

Зачем нужно состояние, хотя пагинация задумана stateless. `HistoryManager` asyncua
передаёт бэкенду только метку времени: `_read_datavalue_history` распаковывает
`rv.ContinuationPoint` в `starttime` и отдаёт его как `start`, а на выходе снова
упаковывает нашу метку через `Primitives.DateTime.pack`. Восемь байт, больше ничего
протащить нельзя. Из-за этого ломаются три вещи.

1. **Направление.** Чтение «последние N значений до момента E» клиент присылает как
   `StartTime = win_epoch`, `EndTime = E`, и `_get_bounds` выбирает `DESC`. На
   продолжении asyncua подставит `start = cont`, а `end` оставит прежним, то есть
   придёт `cont < E` — и `_get_bounds` выберет уже `ASC`. Вторая страница пойдёт с
   другого конца окна и повторит первую в обратном порядке. Закодировать направление
   в самой метке невозможно: любая метка — валидное время.

2. **`NumValuesPerNode`.** По OPC UA Part 11 это максимум значений за всё чтение, а не
   за один ответ. Сколько уже отдано, в метке времени не сохранить (та же дырка есть и
   в собственном `HistoryDict` asyncua).

3. **Тай-брейк.** Для `variables_history` и `events_history` метка однозначна: там есть
   UNIQUE по (переменная, время) и (источник, время), и запись идёт через
   `ON CONFLICT ... DO NOTHING`. Но в `events_ts` (v2) ключ — `(source_id,
   event_timestamp, event_id)`, и на одну метку может быть несколько событий. Курсор
   `EventStoreV2` носит пару `(ts, event_id)`, а наружу уходит только `ts`.

Поэтому выданные continuation points запоминаются здесь: ключ — (узел, метка),
значение — направление, исходное окно, остаток по `NumValuesPerNode` и необязательный
`event_id` для v2. Хранилище ограничено по размеру и по времени жизни; промах (рестарт
сервера, вытеснение, протухание) не ошибка — чтение деградирует к трактовке «что
прислал клиент, то и читаем», как было до этого.

Ложное совпадение практически исключено: ключ включает узел, а метки времени
микросекундные и берутся из самих данных.
"""

from __future__ import annotations

import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

DEFAULT_MAX_ENTRIES = 1024
DEFAULT_TTL_SEC = 300.0


@dataclass(frozen=True)
class ReadContinuation:
    """Что мы помним про выданную клиенту метку продолжения.

    Attributes:
        order: Направление исходного чтения, 'ASC' или 'DESC'.
        window_start: Начало окна исходного запроса (после нормализации _get_bounds).
        window_end: Конец окна исходного запроса.
        remaining: Сколько значений ещё можно отдать по исходному NumValuesPerNode;
            None — клиент лимита не задавал.
        event_id: Вторая половина keyset-курсора для v2-событий, где метка не уникальна.
    """

    order: str
    window_start: datetime
    window_end: datetime
    remaining: Optional[int] = None
    event_id: Optional[int] = None


@dataclass(frozen=True)
class ReadWindow:
    """Окно и лимит одной страницы HistoryRead.

    Attributes:
        order: Направление чтения, 'ASC' или 'DESC'.
        start_time: Начало окна этой страницы.
        end_time: Конец окна этой страницы.
        page_limit: Сколько значений разрешено отдать в этом ответе.
        total_remaining: Сколько значений ещё можно отдать по исходному
            NumValuesPerNode за всё чтение; None — клиент лимита не задавал.
        window_start: Начало исходного окна (не сужается продолжениями).
        window_end: Конец исходного окна.
        resumed: True, если это продолжение по известной нам метке.
        cursor_event_id: event_id из keyset-курсора v2-событий, если был сохранён.
    """

    order: str
    start_time: datetime
    end_time: datetime
    page_limit: int
    total_remaining: Optional[int]
    window_start: datetime
    window_end: datetime
    resumed: bool = False
    cursor_event_id: Optional[int] = None

    def next_remaining(self, returned: int) -> Optional[int]:
        """Остаток по NumValuesPerNode после отдачи `returned` значений."""
        if self.total_remaining is None:
            return None
        return max(0, self.total_remaining - returned)

    def exhausted_by_client_limit(self, returned: int) -> bool:
        """Клиент получил всё, что просил: продолжать нечего, даже если данные есть."""
        return self.total_remaining is not None and self.next_remaining(returned) == 0


class ReadContinuationStore:
    """Ограниченный по размеру и времени жизни склад выданных continuation points."""

    def __init__(
        self,
        max_entries: int = DEFAULT_MAX_ENTRIES,
        ttl_sec: float = DEFAULT_TTL_SEC,
    ) -> None:
        self._max_entries = max(1, int(max_entries))
        self._ttl_sec = max(0.0, float(ttl_sec))
        # key -> (момент записи, состояние); OrderedDict даёт вытеснение по LRU.
        self._entries: "OrderedDict[Tuple[str, datetime], Tuple[float, ReadContinuation]]" = (
            OrderedDict()
        )
        self._stats: Dict[str, int] = {"issued": 0, "hits": 0, "misses": 0, "evicted": 0, "expired": 0}

    def put(self, key_prefix: str, cont_ts: datetime, state: ReadContinuation) -> None:
        """Запомнить метку, которую сейчас отдаём клиенту."""
        key = (key_prefix, cont_ts)
        self._entries.pop(key, None)
        self._entries[key] = (time.monotonic(), state)
        self._stats["issued"] += 1
        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)
            self._stats["evicted"] += 1

    def take(self, key_prefix: str, cont_ts: Optional[datetime]) -> Optional[ReadContinuation]:
        """Забрать состояние по метке, если она наша и не протухла.

        Запись извлекается, а не читается: одна выданная метка обслуживает одно
        продолжение. Если клиент повторит тот же запрос, он получит деградированное,
        но корректное по данным поведение вместо устаревшего остатка.
        """
        if cont_ts is None:
            return None
        key = (key_prefix, cont_ts)
        found = self._entries.pop(key, None)
        if found is None:
            self._stats["misses"] += 1
            return None
        stored_at, state = found
        if self._ttl_sec and (time.monotonic() - stored_at) > self._ttl_sec:
            self._stats["expired"] += 1
            self._stats["misses"] += 1
            return None
        self._stats["hits"] += 1
        return state

    def clear(self) -> None:
        self._entries.clear()

    def get_stats(self) -> Dict[str, Any]:
        return {"entries": len(self._entries), **self._stats}
