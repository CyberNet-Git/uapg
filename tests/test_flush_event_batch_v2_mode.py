"""Регрессия: mode=v2 батч должен сохранять event_data (не пустой events_ts)."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from uapg.history_timescale import EventWriteItem
from uapg.history_timescale_v2 import HistoryTimescaleV2
from uapg.v2.storage_mode import StorageMode


class _FakeConnCM:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *args):
        return False


class _FakeTxCM:
    async def __aenter__(self):
        return None

    async def __aexit__(self, *args):
        return False


@pytest.mark.asyncio
async def test_flush_event_batch_v2_mode_calls_save_event_dual():
    history = HistoryTimescaleV2(
        user="u",
        password="p",
        database="d",
        host="h",
        port=5432,
        events_storage_mode=StorageMode.V2,
    )
    history._event_store = MagicMock()
    history._registry = MagicMock()
    history._registry.get_storage_table = AsyncMock(return_value="evt_sensor")
    history._typed_tables = {}
    history._schema_versions = {7: 1}
    history._db_query_timeout_sec = 5.0
    history._typed_values_from_json = MagicMock(return_value={"serial": "X"})

    conn = MagicMock()
    conn.transaction = MagicMock(return_value=_FakeTxCM())
    pool = MagicMock()
    pool.acquire = MagicMock(return_value=_FakeConnCM(conn))
    history._pool = pool
    history._ensure_pool = AsyncMock()

    async def _run(coro, _name):
        return await coro

    history._run_db_operation = _run

    item = EventWriteItem(
        source_db_id=1,
        event_type_id=7,
        event_timestamp=datetime(2026, 9, 3, tzinfo=timezone.utc),
        event_data_json='{"serial":"base64:AAAA"}',
        group_key="g",
    )

    with patch("uapg.history_timescale_v2.EventStoreV2") as store_cls:
        store = MagicMock()
        store.save_event_dual = AsyncMock(return_value=(10, 20))
        store_cls.return_value = store
        with patch("uapg.history_timescale_v2.ProcedureGateway"):
            await history._flush_event_batch([item])

        assert store_cls.called, "EventStoreV2 was not constructed"
        store.save_event_dual.assert_awaited_once()
        args = store.save_event_dual.await_args.args
        assert args[0] == 1
        assert args[1] == 7
        assert args[3] == item.event_data_json
        conn.execute.assert_not_called()
