"""Границы чтения истории и точки продолжения.

Правила заданы OPC UA Part 11 и реализованы так же, как в 0.2.15: клиент,
который уже умеет читать историю из этого сервера, не должен заметить смены
реализации.

Существенное здесь — направление выборки. Отсутствующее или равное началу эпохи
время начала означает «дай последние значения», то есть чтение идёт от свежих к
старым; перевёрнутый диапазон означает то же самое. Клиент, запросивший
последние N значений, иначе получил бы первые N за всю историю.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional, Sequence

from asyncua import ua

# Столько значений отдаётся, когда клиент не назвал их число. Ноль трактуется
# так же, как отсутствие: в OPC UA это «без ограничения со стороны клиента»,
# но сервер обязан чем-то ограничить ответ.
DEFAULT_READ_LIMIT = 10000

ORDER_ASC = "ASC"
ORDER_DESC = "DESC"


@dataclass(frozen=True)
class ReadWindow:
    """Разобранные параметры запроса истории."""

    start: datetime
    end: datetime
    order: str
    limit: int

    @property
    def newest_first(self) -> bool:
        return self.order == ORDER_DESC


def _is_unset(moment: Optional[datetime]) -> bool:
    return moment is None or moment == ua.get_win_epoch()


def resolve_window(
    start: Optional[datetime],
    end: Optional[datetime],
    nb_values: Optional[int],
) -> ReadWindow:
    """Привести параметры HistoryRead к диапазону, направлению и лимиту."""
    order = ORDER_ASC

    if _is_unset(start):
        # Начало не задано: клиенту нужны последние значения.
        order = ORDER_DESC
        start = ua.get_win_epoch()

    if _is_unset(end):
        # Конец не задан: читаем включительно по «сейчас» с запасом, чтобы
        # значения с временем источника чуть впереди часов сервера не пропали.
        end = datetime.now(timezone.utc) + timedelta(days=1)

    assert start is not None and end is not None
    if start < end:
        window_start, window_end = start, end
    else:
        # Перевёрнутый диапазон — тоже запрос «от свежих к старым».
        order = ORDER_DESC
        window_start, window_end = end, start

    return ReadWindow(
        start=window_start,
        end=window_end,
        order=order,
        limit=int(nb_values) if nb_values else DEFAULT_READ_LIMIT,
    )


def continuation_point(
    rows: Sequence[Any],
    returned: int,
    limit: int,
    field: str,
) -> Optional[datetime]:
    """Метка времени для продолжения чтения.

    Возвращается только тогда, когда страница заполнена ровно до лимита: иначе
    читать больше нечего. Это метка уже отданной строки — клиент продолжает
    чтение с неё, и граница диапазона включающая, поэтому одна строка приходит
    повторно. Поведение сохранено от 0.2.15, потому что на него опираются
    клиенты, уже работающие с этим сервером.
    """
    if returned != limit or not rows:
        return None
    value = rows[-1][field]
    return value if isinstance(value, datetime) else None


def limit_values(values: List[Any], limit: int) -> List[Any]:
    """Обрезать выборку до лимита."""
    return values[:limit] if limit and len(values) > limit else values
