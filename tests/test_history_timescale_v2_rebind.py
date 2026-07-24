"""Регрессия: после _force_reconnect V2 helpers должны использовать новый pool."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from uapg.history_timescale_v2 import HistoryTimescaleV2
from uapg.v2.storage_mode import StorageMode


def _make_pool(name: str):
    pool = SimpleNamespace(name=name, _closed=False, _closing=False)
    pool.close = AsyncMock()
    pool.terminate = lambda: None
    return pool


@pytest.mark.asyncio
async def test_force_reconnect_rebinds_v2_pool_refs():
    history = HistoryTimescaleV2(
        user="u",
        password="p",
        database="d",
        host="h",
        port=5432,
        events_storage_mode=StorageMode.DUAL,
    )
    pool_a = _make_pool("A")
    pool_b = _make_pool("B")
    history._pool = pool_a
    history._v2_ready = True
    history._gateway = SimpleNamespace(_pool=pool_a)
    history._event_store = SimpleNamespace(_pool=pool_a)
    history._backfill_worker = SimpleNamespace(_pool=pool_a)

    with patch("asyncpg.create_pool", new=AsyncMock(return_value=pool_b)):
        await history._force_reconnect(pool_a)

    assert history._pool is pool_b
    assert history._gateway._pool is pool_b
    assert history._event_store._pool is pool_b
    assert history._backfill_worker._pool is pool_b
    pool_a.close.assert_awaited()


@pytest.mark.asyncio
async def test_rebind_v2_pool_noop_when_helpers_missing():
    history = HistoryTimescaleV2(
        user="u",
        password="p",
        database="d",
        host="h",
        port=5432,
        events_storage_mode=StorageMode.LEGACY,
    )
    history._pool = _make_pool("alive")
    history._rebind_v2_pool()  # не должно падать без gateway/store/worker
