"""История переменных против живой TimescaleDB.

Тесты проходят полный путь значения: из ua.DataValue в таблицу и обратно.
Раньше этот путь не проверялся ничем — все тесты работали на моках, поэтому
ни SQL, ни формат хранения не имели подтверждения.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List

import pytest
from asyncua import ua

from tests.conftest import connect_kwargs
from uapg.codec import encode_variant, value_text
from uapg.core.config import ConnectionSettings, Keepalive, Timeouts
from uapg.core.database import Database
from uapg.core.metrics import MetricsRegistry
from uapg.storage.bootstrap import SchemaBootstrap
from uapg.storage.items import VariableWriteItem
from uapg.storage.variables import VariableRepository

pytestmark = pytest.mark.integration

SCHEMA = "public"
BASE_TIME = datetime(2026, 9, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
async def repo(pg_database: str):
    database = Database(
        connection=ConnectionSettings.build(**connect_kwargs(pg_database), schema=SCHEMA),
        timeouts=Timeouts.build(),
        keepalive=Keepalive.build(),
        metrics=MetricsRegistry().database,
    )
    await database.start()
    await SchemaBootstrap(database, SCHEMA).ensure_core_schema()
    registry = MetricsRegistry()
    try:
        yield VariableRepository(database, SCHEMA, registry.variables)
    finally:
        await database.stop()


def _item(
    variable_id: int,
    value: ua.Variant,
    moment: datetime,
    *,
    status: int = 0,
) -> VariableWriteItem:
    datavalue = ua.DataValue(Value=value)
    return VariableWriteItem(
        variable_id=variable_id,
        node_id_str=f"ns=2;i={variable_id}",
        source_timestamp=moment,
        server_timestamp=moment,
        status_code=status,
        value_str=value_text(value),
        variant_type=int(value.VariantType),
        variant_binary=encode_variant(value),
        group_key="g",
        datavalue=datavalue,
    )


class TestMetadata:
    async def test_registration_is_idempotent(self, repo: VariableRepository) -> None:
        """Повторная регистрация узла не должна плодить идентификаторы."""
        first = await repo.ensure_metadata("ns=2;i=1", retention_period=timedelta(days=30))
        second = await repo.ensure_metadata("ns=2;i=1", retention_period=timedelta(days=30))
        assert first == second

    async def test_batch_registration(self, repo: VariableRepository) -> None:
        node_ids = [f"ns=2;i={i}" for i in range(50)]
        mapping = await repo.ensure_metadata_many(node_ids, retention_period=timedelta(days=7))
        assert set(mapping) == set(node_ids)
        assert len(set(mapping.values())) == 50, "идентификаторы обязаны быть разными"

    async def test_batch_registration_matches_single(self, repo: VariableRepository) -> None:
        single = await repo.ensure_metadata("ns=2;i=100")
        batch = await repo.ensure_metadata_many(["ns=2;i=100", "ns=2;i=101"])
        assert batch["ns=2;i=100"] == single

    async def test_metadata_cache_load(self, repo: VariableRepository) -> None:
        await repo.ensure_metadata_many([f"ns=2;i={i}" for i in range(5)])
        cache = await repo.load_metadata_cache(1000)
        assert len(cache) == 5

    async def test_retention_period_is_stored(self, repo: VariableRepository) -> None:
        await repo.ensure_metadata("ns=2;i=7", retention_period=timedelta(days=3), max_records=42)
        row = await repo.find_metadata("ns=2;i=7")
        assert row is not None
        assert row["retention_period"] == timedelta(days=3)
        assert row["max_records"] == 42


class TestWriteAndRead:
    async def test_value_survives_roundtrip(self, repo: VariableRepository) -> None:
        variable_id = await repo.ensure_metadata("ns=2;i=1")
        value = ua.Variant(42.125, ua.VariantType.Double)
        await repo.flush([_item(variable_id, value, BASE_TIME)])

        history = await repo.read_history(
            variable_id, BASE_TIME - timedelta(hours=1), BASE_TIME + timedelta(hours=1), 10, "ASC"
        )
        assert len(history) == 1
        assert history[0].Value.Value == 42.125
        assert history[0].Value.VariantType == ua.VariantType.Double
        assert history[0].SourceTimestamp == BASE_TIME

    @pytest.mark.parametrize(
        "variant",
        [
            ua.Variant(True, ua.VariantType.Boolean),
            ua.Variant(-2147483648, ua.VariantType.Int32),
            ua.Variant(18446744073709551615, ua.VariantType.UInt64),
            ua.Variant("привет", ua.VariantType.String),
            ua.Variant(b"\x00\xff", ua.VariantType.ByteString),
            ua.Variant([1, 2, 3], ua.VariantType.Int32),
            ua.Variant(None),
        ],
        ids=["bool", "int32", "uint64", "string", "bytes", "array", "null"],
    )
    async def test_all_variant_types_survive(
        self, repo: VariableRepository, variant: ua.Variant
    ) -> None:
        variable_id = await repo.ensure_metadata(f"ns=2;s={variant.VariantType}")
        await repo.flush([_item(variable_id, variant, BASE_TIME)])

        history = await repo.read_history(
            variable_id, BASE_TIME - timedelta(hours=1), BASE_TIME + timedelta(hours=1), 10, "ASC"
        )
        assert history[0].Value.Value == variant.Value

    async def test_duplicate_timestamp_is_ignored(self, repo: VariableRepository) -> None:
        """Источник может прислать значение дважды — это не ошибка записи."""
        variable_id = await repo.ensure_metadata("ns=2;i=1")
        item = _item(variable_id, ua.Variant(1.0, ua.VariantType.Double), BASE_TIME)
        await repo.flush([item])
        await repo.flush([item])

        history = await repo.read_history(
            variable_id, BASE_TIME - timedelta(hours=1), BASE_TIME + timedelta(hours=1), 10, "ASC"
        )
        assert len(history) == 1

    async def test_order_and_limit(self, repo: VariableRepository) -> None:
        variable_id = await repo.ensure_metadata("ns=2;i=1")
        items = [
            _item(variable_id, ua.Variant(float(i), ua.VariantType.Double),
                  BASE_TIME + timedelta(seconds=i))
            for i in range(10)
        ]
        await repo.flush(items)

        window = (BASE_TIME - timedelta(hours=1), BASE_TIME + timedelta(hours=1))
        ascending = await repo.read_history(variable_id, *window, 3, "ASC")
        descending = await repo.read_history(variable_id, *window, 3, "DESC")

        assert [v.Value.Value for v in ascending] == [0.0, 1.0, 2.0]
        assert [v.Value.Value for v in descending] == [9.0, 8.0, 7.0]

    async def test_range_bounds_are_inclusive(self, repo: VariableRepository) -> None:
        variable_id = await repo.ensure_metadata("ns=2;i=1")
        await repo.flush([_item(variable_id, ua.Variant(1.0, ua.VariantType.Double), BASE_TIME)])

        history = await repo.read_history(variable_id, BASE_TIME, BASE_TIME, 10, "ASC")
        assert len(history) == 1, "границы диапазона включаются в выборку"

    async def test_status_code_is_preserved(self, repo: VariableRepository) -> None:
        """Плохое качество должно доезжать до клиента, а не теряться по дороге."""
        variable_id = await repo.ensure_metadata("ns=2;i=1")
        uncertain = 0x40000000
        await repo.flush(
            [_item(variable_id, ua.Variant(1.0, ua.VariantType.Double), BASE_TIME, status=uncertain)]
        )
        history = await repo.read_history(
            variable_id, BASE_TIME - timedelta(hours=1), BASE_TIME + timedelta(hours=1), 10, "ASC"
        )
        assert history[0].StatusCode.value == uncertain

    @pytest.mark.parametrize(
        "code",
        [0, 0x40000000, 0x80000000, 0x808D0000, 0xFFFFFFFF],
        ids=["good", "uncertain", "bad", "bad_out_of_service", "max"],
    )
    async def test_any_status_code_can_be_written(
        self, repo: VariableRepository, code: int
    ) -> None:
        """В 0.2.15 значение с кодом Bad не записывалось и уносило с собой всю пачку."""
        variable_id = await repo.ensure_metadata(f"ns=2;s=status{code}")
        await repo.flush(
            [_item(variable_id, ua.Variant(1.0, ua.VariantType.Double), BASE_TIME, status=code)]
        )

        history = await repo.read_history(
            variable_id, BASE_TIME - timedelta(hours=1), BASE_TIME + timedelta(hours=1), 10, "ASC"
        )
        assert history[0].StatusCode.value == code

    async def test_bad_value_does_not_poison_the_batch(self, repo: VariableRepository) -> None:
        """Один плохой отсчёт не должен уносить пачку из хороших значений."""
        variable_id = await repo.ensure_metadata("ns=2;i=900")
        items = [
            _item(variable_id, ua.Variant(float(i), ua.VariantType.Double),
                  BASE_TIME + timedelta(seconds=i),
                  status=0x80000000 if i == 3 else 0)
            for i in range(10)
        ]
        await repo.flush(items)

        history = await repo.read_history(
            variable_id, BASE_TIME - timedelta(hours=1), BASE_TIME + timedelta(hours=1), 50, "ASC"
        )
        assert len(history) == 10

    async def test_large_batch(self, repo: VariableRepository) -> None:
        variable_id = await repo.ensure_metadata("ns=2;i=1")
        items = [
            _item(variable_id, ua.Variant(float(i), ua.VariantType.Double),
                  BASE_TIME + timedelta(milliseconds=i))
            for i in range(1000)
        ]
        await repo.flush(items)

        history = await repo.read_history(
            variable_id, BASE_TIME - timedelta(hours=1), BASE_TIME + timedelta(hours=1), 2000, "ASC"
        )
        assert len(history) == 1000

    async def test_delete_history(self, repo: VariableRepository) -> None:
        variable_id = await repo.ensure_metadata("ns=2;i=1")
        items = [
            _item(variable_id, ua.Variant(float(i), ua.VariantType.Double),
                  BASE_TIME + timedelta(seconds=i))
            for i in range(5)
        ]
        await repo.flush(items)

        removed = await repo.delete_history(variable_id, BASE_TIME, BASE_TIME + timedelta(seconds=2))
        assert removed == 3
        remaining = await repo.read_history(
            variable_id, BASE_TIME - timedelta(hours=1), BASE_TIME + timedelta(hours=1), 10, "ASC"
        )
        assert len(remaining) == 2


class TestLastValues:
    async def test_last_value_follows_writes(self, repo: VariableRepository) -> None:
        variable_id = await repo.ensure_metadata("ns=2;i=1")
        await repo.flush([_item(variable_id, ua.Variant(1.0, ua.VariantType.Double), BASE_TIME)])
        await repo.flush(
            [_item(variable_id, ua.Variant(2.0, ua.VariantType.Double),
                   BASE_TIME + timedelta(seconds=1))]
        )

        last = await repo.read_last_value(variable_id)
        assert last is not None and last.Value.Value == 2.0

    async def test_late_write_does_not_rewind_last_value(self, repo: VariableRepository) -> None:
        """Значения приходят не по порядку; последнее должно остаться последним по времени."""
        variable_id = await repo.ensure_metadata("ns=2;i=1")
        await repo.flush(
            [_item(variable_id, ua.Variant(2.0, ua.VariantType.Double),
                   BASE_TIME + timedelta(seconds=10))]
        )
        await repo.flush([_item(variable_id, ua.Variant(1.0, ua.VariantType.Double), BASE_TIME)])

        last = await repo.read_last_value(variable_id)
        assert last is not None and last.Value.Value == 2.0

    async def test_seed_creates_placeholder(self, repo: VariableRepository) -> None:
        variable_id = await repo.ensure_metadata("ns=2;i=1")
        created = await repo.seed_last_values([(variable_id, ua.DataValue(Value=ua.Variant(None)))])
        assert created == 1

        last = await repo.read_last_value(variable_id)
        assert last is not None and last.Value.Value is None

    async def test_real_write_replaces_seed(self, repo: VariableRepository) -> None:
        """Заглушка не должна пережить настоящее значение, даже более раннее."""
        variable_id = await repo.ensure_metadata("ns=2;i=1")
        await repo.seed_last_values(
            [(variable_id, ua.DataValue(Value=ua.Variant(None), SourceTimestamp=BASE_TIME))]
        )
        await repo.flush(
            [_item(variable_id, ua.Variant(5.0, ua.VariantType.Double),
                   BASE_TIME - timedelta(days=1))]
        )

        last = await repo.read_last_value(variable_id)
        assert last is not None and last.Value.Value == 5.0

    async def test_seed_does_not_overwrite_existing(self, repo: VariableRepository) -> None:
        variable_id = await repo.ensure_metadata("ns=2;i=1")
        await repo.flush([_item(variable_id, ua.Variant(7.0, ua.VariantType.Double), BASE_TIME)])
        await repo.seed_last_values([(variable_id, ua.DataValue(Value=ua.Variant(None)))])

        last = await repo.read_last_value(variable_id)
        assert last is not None and last.Value.Value == 7.0

    async def test_read_many(self, repo: VariableRepository) -> None:
        mapping = await repo.ensure_metadata_many([f"ns=2;i={i}" for i in range(3)])
        for index, variable_id in enumerate(mapping.values()):
            await repo.flush(
                [_item(variable_id, ua.Variant(float(index), ua.VariantType.Double), BASE_TIME)]
            )

        values = await repo.read_last_values(list(mapping.values()))
        assert len(values) == 3

    async def test_cache_load_is_paged(self, repo: VariableRepository) -> None:
        mapping = await repo.ensure_metadata_many([f"ns=2;i={i}" for i in range(25)])
        for variable_id in mapping.values():
            await repo.flush(
                [_item(variable_id, ua.Variant(1.0, ua.VariantType.Double), BASE_TIME)]
            )

        values = await repo.iter_last_values(batch_size=10)
        assert len(values) == 25

    async def test_fallback_to_history(self, repo: VariableRepository) -> None:
        variable_id = await repo.ensure_metadata("ns=2;i=1")
        await repo.flush([_item(variable_id, ua.Variant(3.5, ua.VariantType.Double), BASE_TIME)])
        await repo._db.execute("DELETE FROM variables_last_value")

        assert await repo.read_last_value(variable_id) is None
        fallback = await repo.read_latest_from_history(variable_id)
        assert fallback is not None and fallback.Value.Value == 3.5
