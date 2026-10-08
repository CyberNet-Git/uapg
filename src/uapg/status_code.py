"""Хранение OPC UA StatusCode в колонке `statuscode INTEGER`.

OPC UA `StatusCode` — это **UInt32**, а колонка объявлена `INTEGER`, то есть signed int32.
Любой код семейства Bad имеет установленный старший бит и в неё не влезает:

    Good                     0x00000000           0  влезает: да
    UncertainInitialValue    0x40920000  1083310080  влезает: да
    BadNoData                0x809B0000  2157641728  влезает: НЕТ
    BadOutOfService          0x808D0000  2156724224  влезает: НЕТ
    BadWaitingForInitialData 0x80320000  2150760448  влезает: НЕТ

asyncpg отвергал такой параметр, `INSERT` поднимал исключение, широкий `except` в
`save_node_value` его проглатывал — значение терялось. В батчевом режиме
(`_flush_variable_batch`, `executemany`) одно такое значение роняло весь батч, то есть один
узел «не на связи» съедал чужие записи. Отсюда и наблюдение «статус всегда 0»: это было
следствие дефекта, а не свойством данных.

**Почему знаковое представление, а не `BIGINT`.** `ALTER COLUMN TYPE BIGINT` по семантике
PostgreSQL переписывает таблицу целиком (int4 и int8 не binary-coercible) и держит на это
время `ACCESS EXCLUSIVE`. На большой живой истории это блокировка записи на всю перезапись,
причём повторяющаяся на каждом старте, если она не успевает в `db_query_timeout_sec`.
Знаковая проекция UInt32 — биекция без потерь, не требует ни DDL, ни блокировок и работает
на уже существующих базах сразу.

Цена решения: Bad-коды в колонке выглядят отрицательными числами. Внешний SQL-запрос должен
приводить их обратно:

    SELECT CASE WHEN statuscode < 0 THEN statuscode + 4294967296 ELSE statuscode END

Строки, записанные до этой правки, содержат только Good и Uncertain — оба неотрицательные,
поэтому `decode_status` возвращает их без изменений, и старые данные читаются как раньше.
"""

from __future__ import annotations

from typing import Any, Optional

from asyncua import ua

#: Чем заменяется «статуса нет». По спецификации неизвестное качество — это Bad-код;
#: фабриковать Good нельзя, иначе клиент получит заведомо ложное «значение достоверно».
UNKNOWN_STATUS: int = int(ua.StatusCodes.BadWaitingForInitialData)

_UINT32_SPAN = 1 << 32
_INT32_MIN = -(1 << 31)
_INT32_MAX = (1 << 31) - 1


def encode_status(value: Optional[int]) -> int:
    """UInt32 статуса → значение для колонки `statuscode INTEGER`.

    Результат всегда укладывается в signed int32 — именно на этом инварианте держится отказ
    от миграции колонки. `None` трактуется как «статуса нет» и становится
    ``UNKNOWN_STATUS``.
    """
    if value is None:
        value = UNKNOWN_STATUS
    value = int(value) & 0xFFFFFFFF
    return value - _UINT32_SPAN if value > _INT32_MAX else value


def decode_status(raw: Optional[int]) -> int:
    """Значение колонки `statuscode` → UInt32 статуса.

    Обратна `encode_status`. Неотрицательные значения возвращаются как есть, поэтому строки,
    записанные до перехода на знаковое представление, читаются без изменений. NULL в колонке
    (она nullable) даёт ``UNKNOWN_STATUS``, а не `ua.StatusCode(None)` — у такого объекта
    `.is_good()` и `.name` бросают `TypeError`.
    """
    if raw is None:
        return UNKNOWN_STATUS
    raw = int(raw)
    return raw + _UINT32_SPAN if raw < 0 else raw


def status_from_datavalue(datavalue: Any) -> int:
    """Статус из `ua.DataValue` → значение для колонки.

    Заменяет голый `datavalue.StatusCode.value`, который бросал `AttributeError`, когда поле
    статуса `None` (в asyncua 1.x оно объявлено `Optional[StatusCode]`), а исключение затем
    гасил широкий `except` — и значение молча не записывалось.
    """
    status = getattr(datavalue, "StatusCode", None) if datavalue is not None else None
    return encode_status(getattr(status, "value", None))
