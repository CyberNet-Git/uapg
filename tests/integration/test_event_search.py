"""Слой поиска событий против живой TimescaleDB.

Здесь проверяется то, ради чего слой существует: фильтр должен выполняться в
SQL, а не отсеивать строки в памяти после LIMIT. Иначе клиент, ищущий редкое
событие в длинной истории, получает пустой ответ.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pytest
from asyncua import ua

from tests.conftest import connect_kwargs
from uapg.codec import encode_event_fields
from uapg.codec.node_id import format_node_id
from uapg.core.config import ConnectionSettings, Keepalive, Timeouts
from uapg.core.database import Database
from uapg.core.metrics import MetricsRegistry
from uapg.storage.bootstrap import SchemaBootstrap
from uapg.storage.event_search import EventSearchStore
from uapg.storage.events import EventRepository
from uapg.storage.events_config import EventsV2Config
from uapg.storage.items import EventWriteItem
from uapg.storage.migrations import SqlMigrator
from uapg.storage.typed_events import EventSchemaRegistry, TypedEventTables

pytestmark = pytest.mark.integration

SCHEMA = "public"
BASE = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
WINDOW = (BASE - timedelta(days=1), BASE + timedelta(days=1))

SENSOR_TYPE = ua.NodeId("Events.SensorEvent", 2)
ALARM_TYPE = ua.NodeId("Events.AlarmEvent", 2)


class Harness:
    def __init__(self, db: Database) -> None:
        self.db = db
        config = EventsV2Config.from_csv(indexed="dev_eui")
        self.events = EventRepository(db, SCHEMA, MetricsRegistry().events)
        self.tables = TypedEventTables(db, SCHEMA, config)
        self.registry = EventSchemaRegistry(db, SCHEMA, self.tables)
        self.store = EventSearchStore(db, SCHEMA, self.events, self.tables, self.registry)
        self.source_id = 0
        self.type_ids: Dict[str, int] = {}

    async def setup(self) -> None:
        self.source_id = await self.events.ensure_source("ns=2;i=1")
        for key, node, fields in (
            ("sensor", SENSOR_TYPE, ["Message", "Severity", "dev_eui"]),
            ("alarm", ALARM_TYPE, ["Message", "Severity"]),
        ):
            type_id = await self.events.ensure_type(format_node_id(node))
            table, version = await self.registry.sync_event_type(
                type_id, node, self.registry.describe_fields(fields)
            )
            self.store.remember_type(type_id, table, version)
            self.type_ids[key] = type_id

    def item(self, kind: str, moment: datetime, **fields: Any) -> EventWriteItem:
        event_type = fields.get("event_type") or (SENSOR_TYPE if kind == "sensor" else ALARM_TYPE)
        variants = {
            "Message": ua.Variant(ua.LocalizedText(fields.get("message", "m"), "ru"),
                                  ua.VariantType.LocalizedText),
            "Severity": ua.Variant(fields.get("severity", 100), ua.VariantType.UInt16),
            "EventType": ua.Variant(event_type, ua.VariantType.NodeId),
            "Time": ua.Variant(moment, ua.VariantType.DateTime),
        }
        if "dev_eui" in fields:
            variants["dev_eui"] = ua.Variant(fields["dev_eui"], ua.VariantType.String)
        return EventWriteItem(
            source_db_id=self.source_id,
            event_type_id=self.type_ids[kind],
            event_timestamp=moment,
            event_data_json=json.dumps(encode_event_fields(variants)),
            group_key="g",
        )


@pytest.fixture
async def h(pg_database: str):
    db = Database(
        ConnectionSettings.build(**connect_kwargs(pg_database), schema=SCHEMA),
        Timeouts.build(),
        Keepalive.build(),
        MetricsRegistry().database,
    )
    await db.start()
    await SchemaBootstrap(db, SCHEMA).ensure_core_schema()
    await SqlMigrator(db, SCHEMA).apply_all()
    harness = Harness(db)
    await harness.setup()
    try:
        yield harness
    finally:
        await db.stop()


def _like(field: str, pattern: str, type_node: ua.NodeId = SENSOR_TYPE) -> ua.EventFilter:
    element = ua.ContentFilterElement()
    element.FilterOperator = ua.FilterOperator.Like
    element.FilterOperands = [
        ua.SimpleAttributeOperand(
            TypeDefinitionId=type_node,
            BrowsePath=[ua.QualifiedName(field)],
            AttributeId=ua.AttributeIds.Value,
        ),
        ua.LiteralOperand(ua.Variant(pattern)),
    ]
    content = ua.ContentFilter()
    content.Elements = [element]
    event_filter = ua.EventFilter()
    event_filter.WhereClause = content
    return event_filter


def _messages(events: List[Any]) -> List[str]:
    return [event.Message.Text for event in events]


class TestWrite:
    async def test_event_lands_in_all_three_places(self, h: Harness) -> None:
        await h.store.flush([h.item("sensor", BASE, dev_eui="AA01")])
        assert await h.db.fetchval("SELECT count(*) FROM events_history") == 1
        assert await h.db.fetchval("SELECT count(*) FROM events_ts") == 1
        assert await h.db.fetchval('SELECT "dev_eui" FROM evt_t_2_events_sensorevent') == "AA01"

    async def test_unknown_field_gets_a_column(self, h: Harness) -> None:
        """Поле, которого не было при регистрации типа, не должно теряться для поиска."""
        await h.store.flush([h.item("alarm", BASE, dev_eui="BB02")])
        columns = await h.tables.known_columns("evt_t_2_events_alarmevent")
        assert "dev_eui" in columns


class TestRead:
    async def test_payload_is_restored(self, h: Harness) -> None:
        await h.store.flush([h.item("sensor", BASE, message="полный текст", severity=700)])
        result = await h.store.read(h.source_id, *WINDOW, 10, "ASC", None)
        assert _messages(result.events) == ["полный текст"]
        assert result.events[0].Severity == 700

    async def test_rare_event_is_found_beyond_limit(self, h: Harness) -> None:
        """Главное свойство слоя: фильтр в SQL находит событие, которого нет среди первых N."""
        items = [
            h.item("sensor", BASE + timedelta(seconds=i), dev_eui="COMMON", message=f"c{i}")
            for i in range(200)
        ]
        items.append(h.item("sensor", BASE - timedelta(hours=5), dev_eui="RARE", message="нужное"))
        await h.store.flush(items)

        result = await h.store.read(h.source_id, *WINDOW, 5, "DESC", _like("dev_eui", "RARE"))
        assert _messages(result.events) == ["нужное"]

    async def test_field_filter_without_event_type_spans_types(self, h: Harness) -> None:
        """Поиск по полю без указания типа обязан смотреть во все типы, где поле есть.

        Это сценарий, который в 0.2.15 проверялся только на моках и был помечен
        как невыполненный; здесь он проверяется на реальной базе.
        """
        await h.store.flush(
            [
                h.item("sensor", BASE, dev_eui="X-721ec733", message="датчик"),
                h.item("alarm", BASE + timedelta(seconds=1), dev_eui="X-721ec733", message="тревога"),
                h.item("alarm", BASE + timedelta(seconds=2), dev_eui="OTHER", message="чужое"),
            ]
        )
        result = await h.store.read(h.source_id, *WINDOW, 10, "ASC", _like("dev_eui", "%721ec733%"))
        assert _messages(result.events) == ["датчик", "тревога"]

    async def test_types_without_the_field_do_not_match(self, h: Harness) -> None:
        """У типа нет поля — значит, условие на это поле для него ложно, а не снято."""
        await h.store.flush(
            [
                h.item("sensor", BASE, dev_eui="Z1", message="датчик"),
                h.item("alarm", BASE + timedelta(seconds=1), message="тревога без поля"),
            ]
        )
        result = await h.store.read(h.source_id, *WINDOW, 10, "ASC", _like("dev_eui", "Z1"))
        assert _messages(result.events) == ["датчик"]

    async def test_cursor_paginates_without_duplicates(self, h: Harness) -> None:
        await h.store.flush(
            [h.item("sensor", BASE + timedelta(seconds=i), dev_eui="P", message=f"e{i}")
             for i in range(7)]
        )
        seen: List[str] = []
        cursor = None
        for _ in range(5):
            page = await h.store.read(
                h.source_id, *WINDOW, 3, "ASC", _like("dev_eui", "P"), cursor=cursor
            )
            seen.extend(_messages(page.events))
            cursor = page.cursor
            if cursor is None:
                break
        assert seen == [f"e{i}" for i in range(7)]

    async def test_unknown_event_type_returns_nothing(self, h: Harness) -> None:
        await h.store.flush([h.item("sensor", BASE, dev_eui="A")])
        element = ua.ContentFilterElement()
        element.FilterOperator = ua.FilterOperator.InList
        element.FilterOperands = [
            ua.SimpleAttributeOperand(
                TypeDefinitionId=ua.NodeId(ua.ObjectIds.BaseEventType),
                BrowsePath=[ua.QualifiedName("EventType")],
                AttributeId=ua.AttributeIds.Value,
            ),
            ua.LiteralOperand(ua.Variant(ua.NodeId("Events.NoSuchEvent", 2))),
        ]
        content = ua.ContentFilter()
        content.Elements = [element]
        event_filter = ua.EventFilter()
        event_filter.WhereClause = content

        result = await h.store.read(h.source_id, *WINDOW, 10, "ASC", event_filter)
        assert result.events == []


class TestBackfill:
    async def test_legacy_events_become_searchable(self, h: Harness) -> None:
        """События, записанные до слоя поиска, после переноса находятся фильтром."""
        legacy = [h.item("sensor", BASE + timedelta(seconds=i), dev_eui=f"L{i}") for i in range(5)]
        await h.events.flush(legacy)
        assert await h.events.backfill_lag() == 5

        stats = await h.store.backfill(batch_size=100)
        assert stats["backfill_lag_rows"] == 0
        assert stats["v2_coverage_pct"] == 100.0

        result = await h.store.read(h.source_id, *WINDOW, 10, "ASC", _like("dev_eui", "L3"))
        assert len(result.events) == 1

    async def test_typed_backfill_progresses_past_first_batch(self, h: Harness) -> None:
        """В 0.2.15 перенос типизированных строк застревал на первой порции навсегда."""
        legacy = [h.item("sensor", BASE + timedelta(seconds=i), dev_eui=f"B{i}") for i in range(12)]
        await h.events.flush(legacy)

        for _ in range(4):
            await h.store.backfill(batch_size=5)

        count = await h.db.fetchval("SELECT count(*) FROM evt_t_2_events_sensorevent")
        assert count == 12


class TestStandardClientFilter:
    async def test_subtype_list_with_numeric_node_ids_is_resolved(self, h: Harness) -> None:
        """Стандартный клиент перечисляет подтипы числовыми NodeId пространства 0.

        Идентификатор узла OPC UA — не event_type_id в базе; в 0.2.15 их
        путали, и такой запрос возвращал пустую историю.
        """
        base_type = await h.events.ensure_type(format_node_id(ua.NodeId(ua.ObjectIds.BaseEventType)))
        h.store.remember_type(base_type, None, 1)
        item = h.item("sensor", BASE, dev_eui="Q", event_type=ua.NodeId(ua.ObjectIds.BaseEventType))
        item.event_type_id = base_type
        await h.store.flush([item])

        element = ua.ContentFilterElement()
        element.FilterOperator = ua.FilterOperator.InList
        element.FilterOperands = [
            ua.SimpleAttributeOperand(
                TypeDefinitionId=ua.NodeId(ua.ObjectIds.BaseEventType),
                BrowsePath=[ua.QualifiedName("EventType")],
                AttributeId=ua.AttributeIds.Value,
            ),
            ua.LiteralOperand(ua.Variant(ua.NodeId(2782))),
            ua.LiteralOperand(ua.Variant(ua.NodeId(ua.ObjectIds.BaseEventType))),
            ua.LiteralOperand(ua.Variant(ua.NodeId(base_type))),
        ]
        content = ua.ContentFilter()
        content.Elements = [element]
        event_filter = ua.EventFilter()
        event_filter.WhereClause = content

        result = await h.store.read(h.source_id, *WINDOW, 10, "ASC", event_filter)
        assert len(result.events) == 1


class TestTypedDdl:
    async def test_no_advisory_lock_is_left_behind(self, h: Harness) -> None:
        """Замок снимался через пул и мог попасть не на то соединение — и оставался навсегда."""
        await asyncio.gather(
            *(
                h.tables.ensure_table("evt_t_2_events_sensorevent", [f"extra_{i}"])
                for i in range(8)
            )
        )
        held = await h.db.fetchval("SELECT count(*) FROM pg_locks WHERE locktype = 'advisory'")
        assert held == 0
        columns = await h.tables._load_columns("evt_t_2_events_sensorevent")
        assert {f"extra_{i}" for i in range(8)} <= columns
