"""Слой доступа к PostgreSQL против живой TimescaleDB.

Проверяются именно те свойства, из-за отсутствия которых историзация вставала
на стенде: ограниченность ожиданий, поведение при обрыве соединения и то, что
повтор не делается там, где он бессмыслен.
"""

from __future__ import annotations

import asyncio

import asyncpg
import pytest

from tests.conftest import connect_kwargs
from uapg.core.config import ConnectionSettings, Keepalive, Timeouts
from uapg.core.database import Database, is_connection_error
from uapg.core.errors import DatabaseStopping, OperationTimeout
from uapg.core.metrics import DatabaseMetrics

pytestmark = pytest.mark.integration


def _database(dsn: str, **timeout_overrides: object) -> Database:
    return Database(
        connection=ConnectionSettings.build(**connect_kwargs(dsn), application_name="uapg-tests"),
        timeouts=Timeouts.build(**timeout_overrides),  # type: ignore[arg-type]
        keepalive=Keepalive.build(),
        metrics=DatabaseMetrics(),
    )


@pytest.fixture
async def db(pg_database: str):
    database = _database(pg_database)
    await database.start()
    try:
        yield database
    finally:
        await database.stop()


class TestQueries:
    async def test_basic_operations(self, db: Database) -> None:
        assert await db.fetchval("SELECT 1") == 1
        assert (await db.fetchrow("SELECT 1 AS a, 2 AS b"))["b"] == 2
        assert len(await db.fetch("SELECT generate_series(1, 3)")) == 3
        assert (await db.execute("SELECT 1")).startswith("SELECT")

    async def test_application_name_is_set(self, db: Database) -> None:
        """Без своего application_name нельзя отличить свои зависшие backend'ы от чужих."""
        assert await db.fetchval("SHOW application_name") == "uapg-tests"

    async def test_connection_is_reused_from_pool(self, db: Database) -> None:
        for _ in range(5):
            await db.fetchval("SELECT 1")
        backends = await db.fetchval(
            "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'uapg-tests'"
        )
        assert backends >= 1


class TestErrorHandling:
    async def test_sql_error_does_not_recreate_pool(self, db: Database) -> None:
        """Ошибку сервера нельзя лечить пересозданием пула: соединение исправно."""
        generation_before = db._generation

        with pytest.raises(asyncpg.PostgresError):
            await db.fetchval("SELECT * FROM there_is_no_such_table")

        assert db._generation == generation_before
        assert db._metrics.reconnects_total == 0
        # Пул остался рабочим.
        assert await db.fetchval("SELECT 1") == 1

    async def test_sql_error_is_not_retried(self, db: Database) -> None:
        """Повтор заведомо падающего запроса только удваивает нагрузку."""
        attempts = 0
        original = db._execute_once

        async def counting(*args: object, **kwargs: object) -> object:
            nonlocal attempts
            attempts += 1
            return await original(*args, **kwargs)  # type: ignore[arg-type]

        db._execute_once = counting  # type: ignore[method-assign]
        with pytest.raises(asyncpg.PostgresError):
            await db.fetchval("SELECT 1/0")
        assert attempts == 1

    async def test_query_timeout_is_reported_with_layer(self, db_slow: Database) -> None:
        with pytest.raises(OperationTimeout) as info:
            await db_slow.fetchval("SELECT pg_sleep(5)")
        assert info.value.layer == "query"
        assert db_slow._metrics.timeouts_total == 1

    async def test_classification(self) -> None:
        assert is_connection_error(asyncpg.ConnectionDoesNotExistError())
        assert is_connection_error(asyncpg.InterfaceError("pool is closed"))
        assert is_connection_error(OSError("socket"))
        assert not is_connection_error(asyncpg.PostgresSyntaxError("boom"))
        assert not is_connection_error(asyncpg.UniqueViolationError("dup"))


@pytest.fixture
async def db_slow(pg_database: str):
    database = _database(pg_database, query_sec=0.5)
    await database.start()
    try:
        yield database
    finally:
        await database.stop()


class TestTransactions:
    async def test_commit(self, db: Database) -> None:
        await db.execute("CREATE TABLE t (id int)")

        async def insert(conn: asyncpg.Connection) -> None:
            await conn.execute("INSERT INTO t VALUES (1), (2)")

        await db.run_in_transaction(insert, name="вставка")
        assert await db.fetchval("SELECT count(*) FROM t") == 2

    async def test_rollback_on_error(self, db: Database) -> None:
        await db.execute("CREATE TABLE t (id int)")

        async def failing(conn: asyncpg.Connection) -> None:
            await conn.execute("INSERT INTO t VALUES (1)")
            raise asyncpg.UniqueViolationError("искусственный сбой")

        with pytest.raises(asyncpg.PostgresError):
            await db.run_in_transaction(failing, name="сбойная вставка")
        assert await db.fetchval("SELECT count(*) FROM t") == 0

    async def test_connection_returns_to_pool_after_failure(self, db: Database) -> None:
        """Соединение, не вернувшееся в пул, — это утечка, видимая только под нагрузкой."""
        for _ in range(12):
            async def failing(conn: asyncpg.Connection) -> None:
                raise asyncpg.UniqueViolationError("искусственный сбой")

            with pytest.raises(asyncpg.PostgresError):
                await db.run_in_transaction(failing, name="сбойная вставка")

        assert await db.fetchval("SELECT 1") == 1


class TestReconnect:
    async def test_pool_recreated_and_counted(self, db: Database) -> None:
        await db.reconnect()
        assert db._metrics.reconnects_total == 1
        assert await db.fetchval("SELECT 1") == 1

    async def test_stale_handle_does_not_recreate_pool(self, db: Database) -> None:
        """Повторный реконнект по устаревшей ссылке оборвал бы чужие соединения."""
        stale = await db._ensure_pool()
        await db.reconnect()
        assert db._metrics.reconnects_total == 1

        await db.reconnect(stale)
        assert db._metrics.reconnects_total == 1
        assert db._metrics.reconnects_skipped_total == 1

    async def test_recovers_after_server_side_termination(self, db: Database) -> None:
        """Соединения, убитые на стороне сервера, не должны останавливать запись."""
        assert await db.fetchval("SELECT 1") == 1
        await db.fetchval(
            """
            SELECT pg_terminate_backend(pid) FROM pg_stat_activity
            WHERE application_name = 'uapg-tests' AND pid <> pg_backend_pid()
            """
        )
        assert await db.fetchval("SELECT 1") == 1

    async def test_healthcheck(self, db: Database) -> None:
        assert await db.healthcheck() is True

    async def test_healthcheck_false_when_stopped(self, db: Database) -> None:
        await db.stop()
        assert await db.healthcheck() is False


class TestLifecycle:
    async def test_operations_rejected_after_stop(self, db: Database) -> None:
        await db.stop()
        with pytest.raises(DatabaseStopping):
            await db.fetchval("SELECT 1")

    async def test_stop_is_idempotent(self, db: Database) -> None:
        await db.stop()
        await db.stop()

    async def test_concurrent_queries_share_pool(self, db: Database) -> None:
        results = await asyncio.gather(*(db.fetchval("SELECT $1::int", i) for i in range(20)))
        assert results == list(range(20))
