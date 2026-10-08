"""
Повтор флаша после таймаута, видимый реконнект и безопасный init индексов.

Инцидент в логе 14.09: 79 циклов «will reconnect without retrying», пул не
пересоздавался, батч терялся; CREATE INDEX на горячей таблице ждал INSERT
старого контейнера.
"""
import asyncio
import logging
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock

import pytest

sys.modules.setdefault("psycopg", Mock())

from uapg.history_timescale import HistoryTimescale, VariableWriteItem


def _sample_item() -> VariableWriteItem:
    now = datetime.now(timezone.utc)
    return VariableWriteItem(
        variable_id=1,
        node_id_str="ns=2;s=test",
        source_timestamp=now,
        server_timestamp=now,
        status_code=0,
        variant_type=11,
        variant_binary=b"\x00",
        group_key="g",
        datavalue=Mock(),
    )


def _open_pool() -> Mock:
    pool = Mock()
    pool._closed = False
    pool._closing = False
    return pool


@pytest.mark.asyncio
async def test_flush_variable_batch_retries_after_timeout():
    history = HistoryTimescale()
    history._ensure_pool = AsyncMock()
    history._force_reconnect = AsyncMock()
    history._log_connection_restored_if_needed = Mock()
    history._update_last_values_cache = Mock()
    history._pool = _open_pool()

    calls = {"n": 0}

    @asynccontextmanager
    async def fake_conn(_pool):
        calls["n"] += 1
        if calls["n"] == 1:
            raise TimeoutError("flush variable batch timed out after 120.0s (layer=flush)")
        conn = Mock()
        conn.executemany = AsyncMock()
        yield conn

    history._flush_on_connection = fake_conn

    await history._flush_variable_batch([_sample_item()])

    assert calls["n"] == 2
    history._force_reconnect.assert_awaited_once()
    history._update_last_values_cache.assert_called_once()


@pytest.mark.asyncio
async def test_flush_variable_batch_drops_after_second_timeout():
    history = HistoryTimescale()
    history._ensure_pool = AsyncMock()
    history._force_reconnect = AsyncMock()
    history._pool = _open_pool()

    calls = {"n": 0}

    @asynccontextmanager
    async def always_timeout(_pool):
        calls["n"] += 1
        raise TimeoutError("still dead")
        yield Mock()  # pragma: no cover

    history._flush_on_connection = always_timeout

    with pytest.raises(TimeoutError, match="still dead"):
        await history._flush_variable_batch([_sample_item()])

    assert calls["n"] == 2
    history._force_reconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_force_reconnect_skip_is_warning(caplog):
    history = HistoryTimescale()
    old_pool = _open_pool()
    new_pool = _open_pool()
    history._pool = new_pool
    history._create_pool_with_timeout = AsyncMock()

    with caplog.at_level(logging.WARNING):
        await history._force_reconnect(old_pool)

    assert "pool was already replaced" in caplog.text
    history._create_pool_with_timeout.assert_not_called()
    assert history.get_performance_metrics()["db"]["reconnects_skipped_total"] == 1
    assert history._pool is new_pool


@pytest.mark.asyncio
async def test_flush_on_connection_terminates_on_timeout():
    history = HistoryTimescale()
    conn = Mock()
    transaction = Mock()
    transaction.start = AsyncMock()
    transaction.commit = AsyncMock()
    transaction.rollback = AsyncMock()
    conn.transaction.return_value = transaction
    conn.terminate = Mock()

    pool = Mock()
    pool.acquire = AsyncMock(return_value=conn)
    pool.release = AsyncMock()

    with pytest.raises(TimeoutError):
        async with history._flush_on_connection(pool):
            raise TimeoutError("cancelled")

    conn.terminate.assert_called_once()
    pool.release.assert_not_awaited()
    transaction.rollback.assert_not_awaited()


@pytest.mark.asyncio
async def test_ensure_index_skips_existing():
    history = HistoryTimescale()
    history._index_state = AsyncMock(return_value=True)
    history._execute = AsyncMock()

    await history._ensure_index("idx_foo", "CREATE INDEX idx_foo ON t(x)")

    history._execute.assert_not_called()


@pytest.mark.asyncio
async def test_ensure_index_timeout_does_not_fail_startup():
    history = HistoryTimescale()
    history._index_state = AsyncMock(return_value=None)
    history._execute = AsyncMock(side_effect=TimeoutError("lock wait"))

    await history._ensure_index("idx_foo", "CREATE INDEX idx_foo ON t(x)")


@pytest.mark.asyncio
async def test_ensure_index_rebuilds_invalid_index():
    history = HistoryTimescale(schema="h")
    history._index_state = AsyncMock(return_value=False)
    history._execute = AsyncMock()

    await history._ensure_index("idx_foo", "CREATE INDEX idx_foo ON t(x)")

    executed = [call.args[0] for call in history._execute.await_args_list]
    assert executed == ['DROP INDEX IF EXISTS "h"."idx_foo"', "CREATE INDEX idx_foo ON t(x)"]


@pytest.mark.asyncio
async def test_ensure_index_disabled_skips_populated_table_and_reports_online_ddl():
    history = HistoryTimescale(schema="h", ensure_indexes_on_startup=False)
    history._index_state = AsyncMock(return_value=None)
    history._table_is_empty = AsyncMock(return_value=False)
    history._is_hypertable = AsyncMock(return_value=True)
    history._execute = AsyncMock()

    await history._ensure_index("idx_events_timestamp", "CREATE INDEX ...")

    history._execute.assert_not_called()
    history._table_is_empty.assert_awaited_once_with("events_history")
    online = history._startup_indexes_missing["idx_events_timestamp"]
    assert "WITH (timescaledb.transaction_per_chunk)" in online
    metrics = history.get_performance_metrics()["indexes"]
    assert metrics == {
        "ensure_on_startup": False,
        "startup_missing": 1,
        "startup_obsolete": 0,
    }


@pytest.mark.asyncio
async def test_ensure_index_disabled_still_builds_on_empty_table():
    """Свежая установка: индекс по пустой таблице дёшев и нужен (UNIQUE для ON CONFLICT)."""
    history = HistoryTimescale(schema="h", ensure_indexes_on_startup=False)
    history._index_state = AsyncMock(return_value=None)
    history._table_is_empty = AsyncMock(return_value=True)
    history._execute = AsyncMock()

    await history._ensure_index("idx_variables_varid_sourcets", "CREATE UNIQUE INDEX ...")

    history._execute.assert_awaited_once_with("CREATE UNIQUE INDEX ...")
    assert history.get_performance_metrics()["indexes"]["startup_missing"] == 0


@pytest.mark.asyncio
async def test_terminate_stale_backends_filters_by_application_name():
    history = HistoryTimescale(db_application_name="uapg-history")
    history._fetch = AsyncMock(return_value=[{"pid": 11}, {"pid": 22}])
    history._execute = AsyncMock()

    await history._terminate_stale_backends()

    query, app_name = history._fetch.await_args.args
    assert "pg_stat_activity" in query
    assert app_name == "uapg-history"
    terminated = [call.args[1] for call in history._execute.await_args_list]
    assert terminated == [11, 22]


@pytest.mark.asyncio
async def test_pool_params_use_custom_application_name():
    history = HistoryTimescale(db_application_name="custom-history")
    params = history._build_pool_params()
    assert params["server_settings"]["application_name"] == "custom-history"
