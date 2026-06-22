"""Unit tests for EventStoreV2 keyset pagination and multi-type reads."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, Tuple
from unittest.mock import AsyncMock, MagicMock, patch

from asyncua.common.events import Event

from uapg.v2.event_store import EventStoreV2
from uapg.v2.events_config import EventsV2Config
from uapg.v2.procedure_gateway import ProcedureGateway
from uapg.v2.schema_registry import EventSchemaRegistry


def _row(event_id: int, ts: datetime, legacy_row_id: int = 1) -> Dict[str, Any]:
    return {
        "event_id": event_id,
        "event_timestamp": ts,
        "event_type_id": 10,
        "legacy_row_id": legacy_row_id,
    }


def _make_store() -> Tuple[EventStoreV2, AsyncMock, AsyncMock]:
    pool = AsyncMock()
    pool.fetch = AsyncMock(return_value=[])
    pool.fetchval = AsyncMock(return_value=None)

    events_config = EventsV2Config.from_csv(
        indexed="serial,dev_eui",
        filterable="serial,dev_eui,EventType",
    )
    registry = MagicMock(spec=EventSchemaRegistry)
    registry.events_config = events_config
    registry.get_allowed_fields = AsyncMock(return_value={"serial", "dev_eui"})
    registry.get_storage_table = AsyncMock(return_value=None)

    gateway = MagicMock(spec=ProcedureGateway)
    gateway.read_events_v2 = AsyncMock(return_value=[])

    store = EventStoreV2("hist", pool, registry, gateway)
    return store, gateway, pool


def test_read_events_passes_keyset_cursor_to_gateway() -> None:
    asyncio.run(_read_events_passes_keyset_cursor_to_gateway())


async def _read_events_passes_keyset_cursor_to_gateway() -> None:
    store, gateway, _pool = _make_store()
    ts = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)
    gateway.read_events_v2.return_value = [_row(5, ts)]

    cont_in = (ts, 5)
    _events, cont_out, _partial = await store.read_events(
        1,
        datetime(2026, 6, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 6, 2, 0, 0, tzinfo=timezone.utc),
        10,
        "DESC",
        None,
        lambda _data: {},
        continuation=cont_in,
    )

    gateway.read_events_v2.assert_awaited_once()
    args = gateway.read_events_v2.await_args.args
    assert args[6] == ts
    assert args[7] == 5
    assert cont_out is None


def test_read_events_returns_ts_event_id_continuation() -> None:
    asyncio.run(_read_events_returns_ts_event_id_continuation())


async def _read_events_returns_ts_event_id_continuation() -> None:
    store, gateway, pool = _make_store()
    ts1 = datetime(2026, 6, 1, 12, 1, tzinfo=timezone.utc)
    gateway.read_events_v2.return_value = [_row(2, ts1)]
    pool.fetch.return_value = []

    _events, cont, _partial = await store.read_events(
        1,
        datetime(2026, 6, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 6, 2, 0, 0, tzinfo=timezone.utc),
        1,
        "DESC",
        None,
        lambda _data: {},
    )

    assert cont == (ts1, 2)


def test_read_events_refill_when_post_filter_under_delivers() -> None:
    asyncio.run(_read_events_refill_when_post_filter_under_delivers())


async def _read_events_refill_when_post_filter_under_delivers() -> None:
    store, gateway, pool = _make_store()
    ts_a = datetime(2026, 6, 1, 12, 2, tzinfo=timezone.utc)
    ts_b = datetime(2026, 6, 1, 12, 1, tzinfo=timezone.utc)
    ts_c = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)

    batches = [
        [_row(3, ts_a), _row(99, ts_b)],
        [_row(2, ts_b), _row(1, ts_c)],
    ]
    gateway.read_events_v2.side_effect = batches
    pool.fetch.return_value = []

    import uapg.v2.event_store as event_store_mod
    from asyncua import ua

    evfilter = ua.EventFilter()
    evfilter.WhereClause = ua.ContentFilter()
    evfilter.WhereClause.Elements = []

    original = event_store_mod.apply_event_filter
    event_store_mod.apply_event_filter = lambda events, _evfilter: events[:1]
    try:
        events, _cont, _partial = await store.read_events(
            1,
            datetime(2026, 6, 1, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 6, 2, 0, 0, tzinfo=timezone.utc),
            2,
            "DESC",
            evfilter,
            lambda _data: {},
        )
    finally:
        event_store_mod.apply_event_filter = original

    assert gateway.read_events_v2.await_count == 2
    assert len(events) == 2


def test_read_typed_multi_rows_builds_union() -> None:
    asyncio.run(_read_typed_multi_rows_builds_union())


async def _read_typed_multi_rows_builds_union() -> None:
    store, _gateway, pool = _make_store()
    store._registry.get_storage_table = AsyncMock(side_effect=lambda tid: f"evt_{tid}")
    pool.fetch = AsyncMock(return_value=[_row(1, datetime(2026, 6, 1, tzinfo=timezone.utc))])

    await store._read_typed_multi_rows(
        1,
        [10, 11],
        datetime(2026, 6, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 6, 2, 0, 0, tzinfo=timezone.utc),
        5,
        "DESC",
        {"field": "serial", "op": "ilike", "value": "%x%"},
        {"serial"},
        None,
        None,
    )

    sql = pool.fetch.await_args.args[0]
    assert "UNION ALL" in sql
    assert "e.event_type_id = $4" in sql
    assert 't."serial" ILIKE $5' in sql
    assert "e.event_type_id = $6" in sql
    assert 't."serial" ILIKE $7' in sql


async def _read_events_keeps_event_type_ids_after_replan() -> None:
    store, gateway, pool = _make_store()
    ts = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)
    pool.fetchval = AsyncMock(return_value=42)
    store._registry.get_storage_table = AsyncMock(return_value="evt_sensor_inactive")
    pool.fetch = AsyncMock(return_value=[_row(1, ts, legacy_row_id=0)])

    from asyncua import ua

    inlist = ua.ContentFilterElement()
    inlist.FilterOperator = ua.FilterOperator.InList
    inlist.FilterOperands = [
        ua.SimpleAttributeOperand(
            TypeDefinitionId=ua.NodeId(ua.ObjectIds.BaseEventType),
            BrowsePath=[ua.QualifiedName("EventType")],
            AttributeId=ua.AttributeIds.Value,
        ),
        ua.LiteralOperand(ua.Variant(ua.NodeId("Events.SensorInactiveEvent", 2))),
    ]
    like = ua.ContentFilterElement()
    like.FilterOperator = ua.FilterOperator.Like
    like.FilterOperands = [
        ua.SimpleAttributeOperand(
            TypeDefinitionId=ua.NodeId("Events.SensorInactiveEvent", 2),
            BrowsePath=[ua.QualifiedName("dev_eui")],
            AttributeId=ua.AttributeIds.Value,
        ),
        ua.LiteralOperand(ua.Variant("%721ec73398d49f99%")),
    ]
    and_el = ua.ContentFilterElement()
    and_el.FilterOperator = ua.FilterOperator.And
    and_el.FilterOperands = [ua.ElementOperand(Index=0), ua.ElementOperand(Index=1)]
    cf = ua.ContentFilter()
    cf.Elements = [inlist, like, and_el]
    evfilter = ua.EventFilter()
    evfilter.WhereClause = cf

    resolve_calls = {"count": 0}
    original_resolve = store._resolve_event_type_ids

    async def resolve_side_effect(planner, plan):
        resolve_calls["count"] += 1
        if resolve_calls["count"] == 1:
            return [42]
        return None

    store._resolve_event_type_ids = resolve_side_effect  # type: ignore[method-assign]

    _events, _cont, _partial = await store.read_events(
        1,
        datetime(2026, 6, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 6, 2, 0, 0, tzinfo=timezone.utc),
        10,
        "DESC",
        evfilter,
        lambda _data: {},
    )

    store._resolve_event_type_ids = original_resolve  # type: ignore[method-assign]
    assert resolve_calls["count"] == 2
    gateway.read_events_v2.assert_not_awaited()
    sql = pool.fetch.await_args.args[0]
    assert "event_type_id = $2" in sql
    assert 't."dev_eui" ILIKE $5' in sql


def test_read_events_resolves_event_type_ids_for_field_only_filter() -> None:
    asyncio.run(_read_events_resolves_event_type_ids_for_field_only_filter())


async def _read_events_resolves_event_type_ids_for_field_only_filter() -> None:
    store, gateway, pool = _make_store()
    store._registry.get_storage_table = AsyncMock(side_effect=lambda tid: f"evt_{tid}")
    union_row = _row(1, datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc), legacy_row_id=0)
    pool.fetch = AsyncMock(side_effect=[
        [{"event_type_id": 10}, {"event_type_id": 11}],
        [union_row],
    ])

    from asyncua import ua

    like = ua.ContentFilterElement()
    like.FilterOperator = ua.FilterOperator.Like
    like.FilterOperands = [
        ua.SimpleAttributeOperand(
            TypeDefinitionId=ua.NodeId("Events.VibroIoTEvent", 2),
            BrowsePath=[ua.QualifiedName("dev_eui")],
            AttributeId=ua.AttributeIds.Value,
        ),
        ua.LiteralOperand(ua.Variant("%721ec73398d49f99%")),
    ]
    cf = ua.ContentFilter()
    cf.Elements = [like]
    evfilter = ua.EventFilter()
    evfilter.WhereClause = cf

    import uapg.v2.event_store as event_store_mod

    original = event_store_mod.apply_event_filter
    event_store_mod.apply_event_filter = lambda events, _evfilter: events
    try:
        with patch.object(Event, "from_field_dict", return_value=MagicMock()):
            await store.read_events(
                1,
                datetime(2026, 6, 1, 0, 0, tzinfo=timezone.utc),
                datetime(2026, 6, 2, 0, 0, tzinfo=timezone.utc),
                5,
                "DESC",
                evfilter,
                lambda _data: {},
            )
    finally:
        event_store_mod.apply_event_filter = original

    gateway.read_events_v2.assert_not_awaited()
    sql = pool.fetch.await_args_list[-1].args[0]
    assert "UNION ALL" in sql
    assert "dev_eui" in sql


def test_read_events_keeps_event_type_ids_after_replan() -> None:
    asyncio.run(_read_events_keeps_event_type_ids_after_replan())


def main() -> None:
    test_read_events_passes_keyset_cursor_to_gateway()
    test_read_events_returns_ts_event_id_continuation()
    test_read_events_refill_when_post_filter_under_delivers()
    test_read_typed_multi_rows_builds_union()
    test_read_events_resolves_event_type_ids_for_field_only_filter()
    test_read_events_keeps_event_type_ids_after_replan()
    print("OK")


if __name__ == "__main__":
    main()
