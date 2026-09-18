"""История событий против живой TimescaleDB."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from asyncua import ua

from tests.conftest import connect_kwargs
from uapg.codec import encode_event_fields
from uapg.core.config import ConnectionSettings, Keepalive, Timeouts
from uapg.core.database import Database
from uapg.core.metrics import MetricsRegistry
from uapg.storage.bootstrap import SchemaBootstrap
from uapg.storage.events import EventRepository
from uapg.storage.items import EventWriteItem
from uapg.storage.migrations import SqlMigrator

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
    await SqlMigrator(database, SCHEMA).apply_all()
    try:
        yield EventRepository(database, SCHEMA, MetricsRegistry().events)
    finally:
        await database.stop()


def _event_item(
    source_id: int,
    type_id: int,
    moment: datetime,
    *,
    severity: int = 500,
    message: str = "Событие",
) -> EventWriteItem:
    fields = {
        "Message": ua.Variant(ua.LocalizedText(message, "ru"), ua.VariantType.LocalizedText),
        "Severity": ua.Variant(severity, ua.VariantType.UInt16),
        "SourceName": ua.Variant("Sensor-1", ua.VariantType.String),
    }
    return EventWriteItem(
        source_db_id=source_id,
        event_type_id=type_id,
        event_timestamp=moment,
        event_data_json=json.dumps(encode_event_fields(fields)),
        group_key="g",
    )


class TestMetadata:
    async def test_source_registration_is_idempotent(self, repo: EventRepository) -> None:
        first = await repo.ensure_source("ns=2;i=100", retention_period=timedelta(days=30))
        second = await repo.ensure_source("ns=2;i=100", retention_period=timedelta(days=30))
        assert first == second

    async def test_type_registration_is_idempotent(self, repo: EventRepository) -> None:
        first = await repo.ensure_type("ns=2;s=Events.VibroIoTEvent")
        second = await repo.ensure_type("ns=2;s=Events.VibroIoTEvent")
        assert first == second

    async def test_lookup_returns_none_for_unknown(self, repo: EventRepository) -> None:
        assert await repo.find_source("ns=2;i=999") is None
        assert await repo.find_type("ns=2;s=Nope") is None

    async def test_caches_load(self, repo: EventRepository) -> None:
        await repo.ensure_source("ns=2;i=1")
        await repo.ensure_type("ns=2;s=T")
        assert len(await repo.load_source_cache()) == 1
        assert len(await repo.load_type_cache()) == 1


class TestWriteAndRead:
    async def test_event_fields_survive_roundtrip(self, repo: EventRepository) -> None:
        source_id = await repo.ensure_source("ns=2;i=1")
        type_id = await repo.ensure_type("ns=2;s=T")
        await repo.flush([_event_item(source_id, type_id, BASE_TIME, severity=700)])

        rows = await repo.read_history_rows(
            source_id, BASE_TIME - timedelta(hours=1), BASE_TIME + timedelta(hours=1), 10, "ASC"
        )
        assert len(rows) == 1

        payloads = await repo.read_payloads([int(rows[0]["id"])])
        fields = payloads[int(rows[0]["id"])]
        assert fields["Severity"].Value == 700
        assert fields["Message"].Value.Text == "Событие"

    async def test_same_timestamp_keeps_first_event(self, repo: EventRepository) -> None:
        """Два события источника в один момент неразличимы по уникальному индексу."""
        source_id = await repo.ensure_source("ns=2;i=1")
        type_id = await repo.ensure_type("ns=2;s=T")
        await repo.flush([_event_item(source_id, type_id, BASE_TIME, message="первое")])
        await repo.flush([_event_item(source_id, type_id, BASE_TIME, message="второе")])

        rows = await repo.read_history_rows(
            source_id, BASE_TIME - timedelta(hours=1), BASE_TIME + timedelta(hours=1), 10, "ASC"
        )
        assert len(rows) == 1
        payloads = await repo.read_payloads([int(rows[0]["id"])])
        assert payloads[int(rows[0]["id"])]["Message"].Value.Text == "первое"

    async def test_order_and_limit(self, repo: EventRepository) -> None:
        source_id = await repo.ensure_source("ns=2;i=1")
        type_id = await repo.ensure_type("ns=2;s=T")
        await repo.flush(
            [
                _event_item(source_id, type_id, BASE_TIME + timedelta(seconds=i), severity=i)
                for i in range(10)
            ]
        )

        window = (BASE_TIME - timedelta(hours=1), BASE_TIME + timedelta(hours=1))
        ascending = await repo.read_history_rows(source_id, *window, 3, "ASC")
        descending = await repo.read_history_rows(source_id, *window, 3, "DESC")

        assert ascending[0]["event_timestamp"] < ascending[-1]["event_timestamp"]
        assert descending[0]["event_timestamp"] > descending[-1]["event_timestamp"]
        assert len(ascending) == 3

    async def test_events_of_other_sources_are_not_returned(self, repo: EventRepository) -> None:
        first = await repo.ensure_source("ns=2;i=1")
        second = await repo.ensure_source("ns=2;i=2")
        type_id = await repo.ensure_type("ns=2;s=T")
        await repo.flush([_event_item(first, type_id, BASE_TIME)])
        await repo.flush([_event_item(second, type_id, BASE_TIME)])

        rows = await repo.read_history_rows(
            first, BASE_TIME - timedelta(hours=1), BASE_TIME + timedelta(hours=1), 10, "ASC"
        )
        assert len(rows) == 1

    async def test_delete_history(self, repo: EventRepository) -> None:
        source_id = await repo.ensure_source("ns=2;i=1")
        type_id = await repo.ensure_type("ns=2;s=T")
        await repo.flush(
            [
                _event_item(source_id, type_id, BASE_TIME + timedelta(seconds=i))
                for i in range(5)
            ]
        )

        removed = await repo.delete_history(source_id, BASE_TIME, BASE_TIME + timedelta(seconds=2))
        assert removed == 3

    async def test_large_batch(self, repo: EventRepository) -> None:
        source_id = await repo.ensure_source("ns=2;i=1")
        type_id = await repo.ensure_type("ns=2;s=T")
        await repo.flush(
            [
                _event_item(source_id, type_id, BASE_TIME + timedelta(milliseconds=i))
                for i in range(500)
            ]
        )
        rows = await repo.read_history_rows(
            source_id, BASE_TIME - timedelta(hours=1), BASE_TIME + timedelta(hours=1), 1000, "ASC"
        )
        assert len(rows) == 500


class TestBackfillLag:
    async def test_lag_counts_rows_missing_from_search_layer(self, repo: EventRepository) -> None:
        """Отставание показывает, сколько событий ещё не попало в слой поиска."""
        source_id = await repo.ensure_source("ns=2;i=1")
        type_id = await repo.ensure_type("ns=2;s=T")
        await repo.flush(
            [
                _event_item(source_id, type_id, BASE_TIME + timedelta(seconds=i))
                for i in range(3)
            ]
        )

        assert await repo.backfill_lag() == 3
        assert await repo.backfill_lag(source_id) == 3
