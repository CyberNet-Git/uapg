"""Знаковое представление StatusCode в колонке `statuscode INTEGER`.

OPC UA StatusCode — UInt32, колонка — signed int32, поэтому до 0.2.24 ни один Bad-код не
записывался: asyncpg отвергал параметр, `INSERT` падал, широкий `except` возвращал успех, и
значение терялось (в батчевом режиме — вместе со всем батчем). Миграцию на BIGINT решили не
делать: `ALTER COLUMN TYPE` переписывает таблицу под ACCESS EXCLUSIVE.

Главный инвариант, на котором держится отказ от миграции, — `encode_status` всегда возвращает
значение в диапазоне int4. Он проверяется здесь на границах, а не только на «удобных» кодах.
"""

from __future__ import annotations

import pytest
from asyncua import ua

from uapg.status_code import (
    UNKNOWN_STATUS,
    decode_status,
    encode_status,
    status_from_datavalue,
)

INT32_MIN = -(1 << 31)
INT32_MAX = (1 << 31) - 1
UINT32_MAX = (1 << 32) - 1

REAL_CODES = [
    int(ua.StatusCodes.Good),
    int(ua.StatusCodes.UncertainInitialValue),
    int(ua.StatusCodes.UncertainLastUsableValue),
    int(ua.StatusCodes.BadNoData),
    int(ua.StatusCodes.BadOutOfService),
    int(ua.StatusCodes.BadWaitingForInitialData),
]


class TestRoundTrip:
    @pytest.mark.parametrize("value", [0, 1, INT32_MAX, INT32_MAX + 1, UINT32_MAX])
    def test_boundaries_round_trip(self, value):
        assert decode_status(encode_status(value)) == value

    @pytest.mark.parametrize("value", REAL_CODES)
    def test_real_status_codes_round_trip(self, value):
        assert decode_status(encode_status(value)) == value

    @pytest.mark.parametrize("value", [0, 1, INT32_MAX, INT32_MAX + 1, UINT32_MAX] + REAL_CODES)
    def test_encoded_value_always_fits_int4(self, value):
        """Инвариант, который заменяет миграцию колонки на BIGINT."""
        encoded = encode_status(value)
        assert INT32_MIN <= encoded <= INT32_MAX

    def test_bad_codes_are_stored_negative(self):
        """Именно это и не влезало в колонку до правки."""
        assert encode_status(int(ua.StatusCodes.BadNoData)) < 0
        assert encode_status(int(ua.StatusCodes.Good)) == 0
        # Uncertain влезал в int4 и раньше — представление для него не меняется.
        assert encode_status(int(ua.StatusCodes.UncertainInitialValue)) > 0

    def test_status_survives_asyncua_round_trip(self):
        """Декодированное значение обязано давать тот же StatusCode, включая .name."""
        for value in REAL_CODES:
            restored = ua.StatusCode(decode_status(encode_status(value)))
            assert restored == ua.StatusCode(value)
            assert restored.name == ua.StatusCode(value).name


class TestLegacyAndMissingValues:
    @pytest.mark.parametrize("raw", [0, 1, 1083310080, INT32_MAX])
    def test_non_negative_column_values_are_returned_as_is(self, raw):
        """Строки, записанные до перехода, содержат только неотрицательные значения."""
        assert decode_status(raw) == raw

    def test_none_becomes_unknown_status(self):
        assert decode_status(None) == UNKNOWN_STATUS
        assert decode_status(encode_status(None)) == UNKNOWN_STATUS

    def test_unknown_status_is_a_bad_code_not_good(self):
        """Фабриковать Good для неизвестного качества нельзя."""
        assert UNKNOWN_STATUS == int(ua.StatusCodes.BadWaitingForInitialData)
        assert ua.StatusCode(UNKNOWN_STATUS).is_bad()

    def test_null_column_does_not_produce_broken_statuscode(self):
        """`ua.StatusCode(None)` — живой объект, у которого .name бросает TypeError."""
        with pytest.raises(TypeError):
            ua.StatusCode(None).name
        assert ua.StatusCode(decode_status(None)).name


class TestStatusFromDataValue:
    def test_takes_status_from_datavalue(self):
        datavalue = ua.DataValue(
            Value=ua.Variant(1.0, ua.VariantType.Double),
            StatusCode_=ua.StatusCode(ua.StatusCodes.BadOutOfService),
        )

        assert decode_status(status_from_datavalue(datavalue)) == int(
            ua.StatusCodes.BadOutOfService
        )

    def test_missing_status_becomes_unknown_instead_of_raising(self):
        """Раньше здесь был голый datavalue.StatusCode.value → AttributeError."""
        datavalue = ua.DataValue(
            Value=ua.Variant(1.0, ua.VariantType.Double), StatusCode_=None
        )

        assert decode_status(status_from_datavalue(datavalue)) == UNKNOWN_STATUS

    def test_none_datavalue_becomes_unknown(self):
        assert decode_status(status_from_datavalue(None)) == UNKNOWN_STATUS

    def test_default_datavalue_is_good(self):
        """У asyncua 1.x поле статуса имеет default_factory=StatusCode, то есть Good."""
        datavalue = ua.DataValue(Value=ua.Variant(1.0, ua.VariantType.Double))

        assert decode_status(status_from_datavalue(datavalue)) == int(ua.StatusCodes.Good)
