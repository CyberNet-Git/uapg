"""Поведение под сбоями, ради которого писались 0.2.13–0.2.15.

Каждый тест соответствует разобранному инциденту: батч, выброшенный по
таймауту без повтора; ROLLBACK, ушедший в мёртвый сокет и не вернувшийся;
замок, которого ждали бесконечно; зависшие сессии после SIGKILL контейнера.
"""

from __future__ import annotations

import asyncio
import time

import asyncpg
import pytest

from tests.conftest import connect_kwargs
from uapg.core.config import ConnectionSettings, Keepalive, Timeouts
from uapg.core.database import Database
from uapg.core.errors import OperationTimeout
from uapg.core.metrics import DatabaseMetrics

pytestmark = pytest.mark.integration

APP = "uapg-resilience"


def _database(dsn: str, **timeouts: float) -> Database:
    return Database(
        ConnectionSettings.build(**connect_kwargs(dsn), application_name=APP),
        Timeouts.build(**timeouts),  # type: ignore[arg-type]
        Keepalive.build(),
        DatabaseMetrics(),
    )


async def _sleeping_backends(dsn: str) -> int:
    conn = await asyncpg.connect(dsn)
    try:
        return int(
            await conn.fetchval(
                "SELECT count(*) FROM pg_stat_activity "
                "WHERE application_name = $1 AND query LIKE '%pg_sleep%' AND state = 'active'",
                APP,
            )
        )
    finally:
        await conn.close()


async def test_flush_timeout_is_retried_once(pg_database: str) -> None:
    """Таймаут флаша — не повод выбрасывать батч: одна повторная попытка после реконнекта."""
    db = _database(pg_database, flush_sec=0.5)
    await db.start()
    attempts = 0
    try:

        async def operation(conn: asyncpg.Connection) -> str:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                await conn.execute("SELECT pg_sleep(5)")
            return "записано"

        assert await db.run_in_transaction(operation, name="флаш") == "записано"
        assert attempts == 2
        assert db._metrics.reconnects_total == 1
    finally:
        await db.stop()


async def test_second_flush_timeout_gives_up(pg_database: str) -> None:
    db = _database(pg_database, flush_sec=0.5)
    await db.start()
    try:

        async def operation(conn: asyncpg.Connection) -> None:
            await conn.execute("SELECT pg_sleep(5)")

        started = time.monotonic()
        with pytest.raises(OperationTimeout):
            await db.run_in_transaction(operation, name="флаш")
        # Две попытки по полсекунды плюс реконнект, а не пять секунд сна.
        assert time.monotonic() - started < 4
    finally:
        await db.stop()


async def test_timed_out_transaction_releases_the_backend(pg_database: str) -> None:
    """Отменённый флаш рвёт сокет: иначе backend продолжает держать запрос и блокировки."""
    db = _database(pg_database, flush_sec=0.5)
    await db.start()
    try:

        async def operation(conn: asyncpg.Connection) -> None:
            await conn.execute("SELECT pg_sleep(30)")

        with pytest.raises(OperationTimeout):
            await db.run_in_transaction(operation, name="флаш")

        for _ in range(50):
            if await _sleeping_backends(pg_database) == 0:
                break
            await asyncio.sleep(0.1)
        assert await _sleeping_backends(pg_database) == 0
    finally:
        await db.stop()


async def test_query_timeout_is_not_retried(pg_database: str) -> None:
    """Повтор зависшего запроса только удвоил бы ожидание."""
    db = _database(pg_database, query_sec=0.5)
    await db.start()
    try:
        started = time.monotonic()
        with pytest.raises(OperationTimeout):
            await db.fetchval("SELECT pg_sleep(5)")
        assert time.monotonic() - started < 2
    finally:
        await db.stop()


async def test_waiting_for_a_stuck_reconnect_is_bounded(pg_database: str) -> None:
    """Зависший реконнект держит замок; остальные ждут его не дольше lock_wait_sec."""
    db = _database(pg_database, lock_wait_sec=1.0)
    await db.start()
    try:
        await db._reconnect_lock.acquire()
        pool = db._pool
        db._pool = None  # пул «в процессе пересоздания»
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            await db.fetchval("SELECT 1")
        assert time.monotonic() - started < 3
        assert db._metrics.pool_wait_timeouts_total == 1
        db._pool = pool
    finally:
        db._reconnect_lock.release()
        await db.stop()


async def test_stale_sessions_of_same_application_are_terminated(pg_database: str) -> None:
    """После SIGKILL старые backend'ы держат INSERT часами; при старте они снимаются."""
    from uapg.history_timescale import HistoryTimescale

    stale = await asyncpg.connect(pg_database, server_settings={"application_name": APP})
    foreign = await asyncpg.connect(pg_database, server_settings={"application_name": "чужое"})
    storage = HistoryTimescale(**connect_kwargs(pg_database), db_application_name=APP)
    try:
        await storage.init()
        await asyncio.sleep(0.2)
        assert stale.is_closed() or await _is_dead(stale)
        assert await foreign.fetchval("SELECT 1") == 1, "чужие приложения трогать нельзя"
        assert await storage._db.fetchval("SELECT 1") == 1, "свои соединения пула должны жить"
    finally:
        await storage.stop()
        await foreign.close()
        if not stale.is_closed():
            stale.terminate()


async def _is_dead(conn: asyncpg.Connection) -> bool:
    try:
        await conn.fetchval("SELECT 1")
        return False
    except Exception:
        return True
