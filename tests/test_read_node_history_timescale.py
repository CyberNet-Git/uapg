"""Тесты чтения истории переменной из HistoryTimescale.

До 0.2.22 у `HistoryTimescale.read_node_history` не было ни одного теста, и это
дорого обошлось: открытая граница `asyncua>=1.1.8` затащила в окружение asyncua 2.0,
где поле `DataValue.StatusCode_` переименовано в `StatusCode`. Конструктор начал
бросать `TypeError`, широкий `except Exception` на `history_timescale.py:3909` гасил
его и возвращал `([], None)` — то есть любое чтение истории выглядело как «данных в
диапазоне нет», и видно это было только в логе.

Поэтому главная проверка здесь — `len(results) == 1`: при сломанном конструкторе
`DataValue` метод вернёт пустой список и тест покраснеет. Проверено мутацией.

Байты значения строятся настоящим `variant_to_binary`, а не произвольным литералом:
единственный прежний аналог (`test_history_pgsql.py::test_read_node_history`) подаёт
`b'test_binary_data'`, из-за чего декод падает, `except` его гасит, и проверки
`isinstance(results, list)` проходят при любом поведении метода.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest
from asyncua import ua
from asyncua.ua.ua_binary import variant_to_binary

from uapg.history_timescale import HistoryTimescale
from uapg.status_code import UNKNOWN_STATUS, encode_status

VARIABLE_ID = 7


def _history() -> HistoryTimescale:
    """Экземпляр без обращения к БД: конструктор инертен, init() не вызываем."""
    return HistoryTimescale(schema="history")


def _row(
    value: ua.Variant,
    source_ts: datetime,
    server_ts: datetime,
    statuscode: int | None = 0,
) -> dict:
    """Строка в том виде, в каком её читает read_node_history — по строковым ключам."""
    return {
        "servertimestamp": server_ts,
        "sourcetimestamp": source_ts,
        "statuscode": statuscode,
        "value": str(value.Value),
        "varianttype": int(value.VariantType),
        "variantbinary": variant_to_binary(value),
    }


class TestReadNodeHistory:
    @pytest.mark.asyncio
    async def test_decodes_row_into_datavalue(self):
        history = _history()
        node_id = ua.NodeId("TestVariable", 1)
        # 3-кортеж (period, count, variable_id) — иначе пошёл бы запрос в variable_metadata.
        history._datachanges_period[node_id] = (timedelta(days=1), 1000, VARIABLE_ID)

        source_ts = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc)
        server_ts = source_ts + timedelta(milliseconds=5)
        variant = ua.Variant(42.5, ua.VariantType.Double)
        history._fetch = AsyncMock(return_value=[_row(variant, source_ts, server_ts)])

        results, cont = await history.read_node_history(
            node_id, source_ts - timedelta(hours=1), source_ts + timedelta(hours=1), 100
        )

        # Если конструктор DataValue несовместим с установленной asyncua, здесь будет 0.
        assert len(results) == 1
        datavalue = results[0]
        assert datavalue.Value.Value == pytest.approx(42.5)
        assert datavalue.Value.VariantType == ua.VariantType.Double
        assert datavalue.SourceTimestamp == source_ts
        assert datavalue.ServerTimestamp == server_ts
        assert datavalue.StatusCode.value == 0
        # Строк меньше лимита — продолжать нечего. Поведение cont на границе
        # len(results) == limit здесь намеренно не проверяется: там сейчас дефект
        # (отдаётся таймстемп последней отданной строки вместо первой отвергнутой),
        # и тест не должен закреплять неверную семантику.
        assert cont is None

    @pytest.mark.asyncio
    async def test_variable_id_resolved_from_metadata_cache(self):
        """Вторая сеть получения variable_id — кэш метаданных, без _datachanges_period."""
        history = _history()
        node_id = ua.NodeId("TestVariable", 1)
        history._variable_metadata_cache[history._format_node_id(node_id)] = VARIABLE_ID

        source_ts = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc)
        variant = ua.Variant(-1, ua.VariantType.Int32)
        history._fetch = AsyncMock(return_value=[_row(variant, source_ts, source_ts)])

        results, _ = await history.read_node_history(
            node_id, source_ts - timedelta(hours=1), source_ts + timedelta(hours=1), 100
        )

        assert len(results) == 1
        assert results[0].Value.Value == -1
        # variable_id уходит первым параметром запроса.
        assert history._fetch.await_args.args[1] == VARIABLE_ID

    @pytest.mark.asyncio
    async def test_statuscode_from_column_is_preserved(self):
        """Ненулевой статус должен доезжать до DataValue, а не подменяться на Good."""
        history = _history()
        node_id = ua.NodeId("TestVariable", 1)
        history._datachanges_period[node_id] = (timedelta(days=1), 1000, VARIABLE_ID)

        source_ts = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc)
        uncertain = int(ua.StatusCodes.UncertainInitialValue)
        variant = ua.Variant(1.0, ua.VariantType.Double)
        history._fetch = AsyncMock(
            return_value=[_row(variant, source_ts, source_ts, statuscode=uncertain)]
        )

        results, _ = await history.read_node_history(
            node_id, source_ts - timedelta(hours=1), source_ts + timedelta(hours=1), 100
        )

        assert len(results) == 1
        assert results[0].StatusCode.value == uncertain

    @pytest.mark.asyncio
    async def test_bad_status_stored_signed_is_decoded_back(self):
        """Bad-коды лежат в колонке как отрицательные числа (см. uapg.status_code)."""
        history = _history()
        node_id = ua.NodeId("TestVariable", 1)
        history._datachanges_period[node_id] = (timedelta(days=1), 1000, VARIABLE_ID)

        source_ts = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc)
        bad = int(ua.StatusCodes.BadOutOfService)
        stored = encode_status(bad)
        assert stored < 0, "иначе тест не проверяет то, ради чего написан"
        history._fetch = AsyncMock(
            return_value=[
                _row(ua.Variant(1.0, ua.VariantType.Double), source_ts, source_ts, statuscode=stored)
            ]
        )

        results, _ = await history.read_node_history(
            node_id, source_ts - timedelta(hours=1), source_ts + timedelta(hours=1), 100
        )

        assert len(results) == 1
        assert results[0].StatusCode.value == bad
        assert results[0].StatusCode.name == "BadOutOfService"

    @pytest.mark.asyncio
    async def test_null_statuscode_does_not_break_read(self):
        """Колонка nullable; `ua.StatusCode(None)` сломался бы при кодировании ответа."""
        history = _history()
        node_id = ua.NodeId("TestVariable", 1)
        history._datachanges_period[node_id] = (timedelta(days=1), 1000, VARIABLE_ID)

        source_ts = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc)
        history._fetch = AsyncMock(
            return_value=[
                _row(ua.Variant(1.0, ua.VariantType.Double), source_ts, source_ts, statuscode=None)
            ]
        )

        results, _ = await history.read_node_history(
            node_id, source_ts - timedelta(hours=1), source_ts + timedelta(hours=1), 100
        )

        assert len(results) == 1
        assert results[0].StatusCode.value == UNKNOWN_STATUS
        assert results[0].StatusCode.name  # не бросает TypeError

    @pytest.mark.asyncio
    async def test_unknown_node_returns_empty_without_touching_history(self):
        history = _history()
        history._fetchval = AsyncMock(return_value=None)
        history._fetch = AsyncMock()

        now = datetime.now(timezone.utc)
        results, cont = await history.read_node_history(
            ua.NodeId("Unregistered", 1), now - timedelta(hours=1), now, 100
        )

        assert results == []
        assert cont is None
        history._fetch.assert_not_awaited()


def test_asyncua_datavalue_uses_statuscode_underscore_field():
    """Страховка от возврата asyncua 2.x в окружение.

    uapg собирает `DataValue(StatusCode_=...)` в семи местах history_timescale.py.
    В asyncua 2.x поле переименовано, конструктор бросает TypeError, а широкий except
    на путях чтения превращает это в пустой результат вместо ошибки. Обработчики мы
    намеренно не сужали, поэтому несовместимость должна ловиться здесь — тестами CI
    в репозитории не прикрыт (единственный workflow только собирает и публикует).
    """
    datavalue = ua.DataValue(
        Value=ua.Variant(1.0, ua.VariantType.Double),
        StatusCode_=ua.StatusCode(0),
    )

    assert datavalue.StatusCode.value == 0
