"""Общие фикстуры и заглушки импортов.

Интеграционные тесты (маркер ``integration``) работают против одноразовой
TimescaleDB из ``docker-compose.test.yml``. Если базы нет, они пропускаются,
поэтому юнит-тесты остаются запускаемыми где угодно.
"""

from __future__ import annotations

import asyncio
import os
import sys
import uuid
from typing import Any, Dict, Iterator
from unittest.mock import Mock
from urllib.parse import urlparse

import pytest

sys.modules.setdefault("psycopg", Mock())

DEFAULT_TEST_DSN = "postgresql://uapg_test:uapg_test@127.0.0.1:55432/uapg_test"


def connect_kwargs(dsn: str) -> Dict[str, Any]:
    """Разобрать DSN в набор аргументов, который принимает HistoryTimescale."""
    parsed = urlparse(dsn)
    return {
        "host": parsed.hostname or "127.0.0.1",
        "port": parsed.port or 5432,
        "user": parsed.username or "postgres",
        "password": parsed.password or "",
        "database": (parsed.path or "/postgres").lstrip("/"),
    }


def _probe(dsn: str) -> str | None:
    async def _try() -> str | None:
        import asyncpg

        try:
            conn = await asyncio.wait_for(asyncpg.connect(dsn), timeout=5)
        except Exception as exc:  # noqa: BLE001 - причина нужна только для сообщения о пропуске
            return str(exc)
        await conn.close()
        return None

    return asyncio.run(_try())


@pytest.fixture(scope="session")
def admin_dsn() -> str:
    """DSN административной базы; тесты пропускаются, если её нет."""
    dsn = os.environ.get("UAPG_TEST_DSN", DEFAULT_TEST_DSN)
    failure = _probe(dsn)
    if failure is not None:
        pytest.skip(
            f"TimescaleDB для интеграционных тестов недоступна ({dsn}): {failure}. "
            "Поднимите её: docker compose -f docker-compose.test.yml up -d",
            allow_module_level=True,
        )
    return dsn


@pytest.fixture
async def pg_database(admin_dsn: str) -> Any:
    """Свежая база под один тест; удаляется после него."""
    import asyncpg

    name = f"uapg_t_{uuid.uuid4().hex[:12]}"
    admin = await asyncpg.connect(admin_dsn)
    try:
        await admin.execute(f'CREATE DATABASE "{name}"')
    finally:
        await admin.close()

    dsn = admin_dsn.rsplit("/", 1)[0] + "/" + name
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
    finally:
        await conn.close()

    try:
        yield dsn
    finally:
        admin = await asyncpg.connect(admin_dsn)
        try:
            await admin.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1",
                name,
            )
            await admin.execute(f'DROP DATABASE IF EXISTS "{name}"')
        finally:
            await admin.close()
