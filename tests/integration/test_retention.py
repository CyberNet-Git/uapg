"""Политика хранения: слой поиска событий не должен переживать сами события.

Миграция 004 ставила ``events_ts`` свои 365 дней, не связанные с
``global_retention_period``. При меньшем глобальном периоде получалось окно, где
строка поиска есть, а события в ``events_history`` уже нет: фильтр находил
событие, а ``HistoryRead`` его не отдавал — и молча.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Dict, Optional

import asyncpg
import pytest

from tests.conftest import connect_kwargs
from uapg import HistoryTimescaleV2
from uapg.v2.storage_mode import StorageMode

pytestmark = pytest.mark.integration


async def _policies(dsn: str) -> Dict[str, Optional[timedelta]]:
    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch(
            """
            SELECT hypertable_name, (config->>'drop_after')::interval AS drop_after
            FROM timescaledb_information.jobs
            WHERE proc_name = 'policy_retention' AND hypertable_schema = 'public'
            """
        )
    finally:
        await conn.close()
    return {row["hypertable_name"]: row["drop_after"] for row in rows}


def _storage(dsn: str, period: Optional[timedelta]) -> HistoryTimescaleV2:
    return HistoryTimescaleV2(
        **connect_kwargs(dsn),
        global_retention_period=period,
        events_storage_mode=StorageMode.DUAL,
    )


async def test_event_search_retention_follows_global_period(pg_database: str) -> None:
    storage = _storage(pg_database, timedelta(days=30))
    await storage.init()
    await storage.stop()

    policies = await _policies(pg_database)
    assert policies.get("events_history") == timedelta(days=30)
    assert policies.get("events_ts") == timedelta(days=30), (
        "слой поиска обязан жить столько же, сколько события, а не зашитые 365 дней"
    )


async def test_stale_event_search_policy_is_corrected_on_restart(pg_database: str) -> None:
    """Базы, где миграция 004 уже применена, исправляются при обычном старте."""
    storage = _storage(pg_database, timedelta(days=30))
    await storage.init()
    await storage.stop()

    conn = await asyncpg.connect(pg_database)
    try:
        await conn.execute("SELECT remove_retention_policy('public.events_ts')")
        await conn.execute(
            "SELECT add_retention_policy('public.events_ts', INTERVAL '365 days')"
        )
    finally:
        await conn.close()
    assert (await _policies(pg_database))["events_ts"] == timedelta(days=365)

    storage = _storage(pg_database, timedelta(days=30))
    await storage.init()
    await storage.stop()

    assert (await _policies(pg_database))["events_ts"] == timedelta(days=30)


async def test_unset_global_period_removes_event_search_policy(pg_database: str) -> None:
    """Без глобального периода события хранятся вечно — и слой поиска тоже."""
    storage = _storage(pg_database, None)
    await storage.init()
    await storage.stop()

    policies = await _policies(pg_database)
    assert "events_history" not in policies
    assert "events_ts" not in policies, (
        "иначе поиск терял бы строки, пока сами события лежат бессрочно"
    )


async def test_reapply_updates_every_history_table(pg_database: str) -> None:
    storage = _storage(pg_database, timedelta(days=30))
    await storage.init()
    try:
        await storage.reapply_global_retention_policy(timedelta(days=7))
    finally:
        await storage.stop()

    policies = await _policies(pg_database)
    assert policies.get("variables_history") == timedelta(days=7)
    assert policies.get("events_history") == timedelta(days=7)
    assert policies.get("events_ts") == timedelta(days=7)


async def test_policy_is_not_recreated_when_it_already_matches(pg_database: str) -> None:
    """Лишний remove/add на каждом старте сбрасывал бы расписание фоновой задачи."""
    storage = _storage(pg_database, timedelta(days=30))
    await storage.init()
    await storage.stop()

    conn = await asyncpg.connect(pg_database)
    try:
        before = await conn.fetch(
            "SELECT job_id, hypertable_name FROM timescaledb_information.jobs"
            " WHERE proc_name = 'policy_retention' AND hypertable_schema = 'public'"
            " ORDER BY hypertable_name"
        )
    finally:
        await conn.close()

    storage = _storage(pg_database, timedelta(days=30))
    await storage.init()
    await storage.stop()

    conn = await asyncpg.connect(pg_database)
    try:
        after = await conn.fetch(
            "SELECT job_id, hypertable_name FROM timescaledb_information.jobs"
            " WHERE proc_name = 'policy_retention' AND hypertable_schema = 'public'"
            " ORDER BY hypertable_name"
        )
    finally:
        await conn.close()

    assert [tuple(r) for r in before] == [tuple(r) for r in after]
