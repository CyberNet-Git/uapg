"""Старт не должен ни блокировать живую запись, ни падать из-за вторичного.

Индекс, которого нет, создаётся по возможности: CREATE INDEX на горячей
таблице ждёт блокировку за живым INSERT. Keepalive включается на сокете, но
ошибка его настройки не должна мешать работе с БД.
"""

from __future__ import annotations

import socket
from typing import Any, List

from uapg.core.config import ConnectionSettings, Keepalive, Timeouts
from uapg.core.database import Database
from uapg.core.metrics import DatabaseMetrics
from uapg.storage.bootstrap import SchemaBootstrap


class FakeDb:
    def __init__(self, *, exists: bool = False, fail: Exception | None = None) -> None:
        self.exists = exists
        self.fail = fail
        self.executed: List[str] = []

    async def fetchval(self, sql: str, *args: Any) -> Any:
        return 1 if self.exists else None

    async def execute(self, sql: str, *args: Any) -> str:
        if self.fail is not None:
            raise self.fail
        self.executed.append(sql)
        return "CREATE INDEX"


async def test_existing_index_is_not_recreated() -> None:
    """Даже CREATE INDEX IF NOT EXISTS ждёт блокировку — поэтому сначала проверка."""
    db = FakeDb(exists=True)
    await SchemaBootstrap(db, "public")._ensure_index("idx", "CREATE INDEX idx ON t (a)")  # type: ignore[arg-type]
    assert db.executed == []


async def test_index_timeout_does_not_fail_startup() -> None:
    db = FakeDb(fail=TimeoutError("lock"))
    await SchemaBootstrap(db, "public")._ensure_index("idx", "CREATE INDEX idx ON t (a)")  # type: ignore[arg-type]


async def test_index_error_does_not_fail_startup() -> None:
    db = FakeDb(fail=RuntimeError("что угодно"))
    await SchemaBootstrap(db, "public")._ensure_index("idx", "CREATE INDEX idx ON t (a)")  # type: ignore[arg-type]


class _Transport:
    def __init__(self, sock: Any) -> None:
        self._sock = sock

    def get_extra_info(self, name: str) -> Any:
        if isinstance(self._sock, Exception):
            raise self._sock
        return self._sock


class _Conn:
    def __init__(self, sock: Any) -> None:
        self._transport = _Transport(sock)


def _database(keepalive: Keepalive) -> Database:
    return Database(ConnectionSettings.build(), Timeouts.build(), keepalive, DatabaseMetrics())


async def test_keepalive_is_enabled_on_socket() -> None:
    """Молчаливый обрыв ловится ОС: прикладная отмена уходит в тот же мёртвый сокет."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        await _database(Keepalive.build(idle_sec=15, interval_sec=4, count=2))._configure_connection(
            _Conn(sock)  # type: ignore[arg-type]
        )
        assert sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) == 1
        if hasattr(socket, "TCP_KEEPIDLE"):
            assert sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE) == 15
            assert sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT) == 2
        if hasattr(socket, "TCP_USER_TIMEOUT"):
            assert sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_USER_TIMEOUT) == 60000
    finally:
        sock.close()


async def test_keepalive_disabled_leaves_socket_untouched() -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        await _database(Keepalive.build(idle_sec=0))._configure_connection(_Conn(sock))  # type: ignore[arg-type]
        assert sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) == 0
    finally:
        sock.close()


async def test_broken_socket_does_not_break_connection_setup() -> None:
    await _database(Keepalive.build())._configure_connection(_Conn(OSError("нет сокета")))  # type: ignore[arg-type]


def test_pool_kwargs_carry_timeouts_and_application_name() -> None:
    database = Database(
        ConnectionSettings.build(application_name="my-app", sslmode="disable"),
        Timeouts.build(command_sec=60, flush_sec=120, pool_create_sec=15),
        Keepalive.build(),
        DatabaseMetrics(),
    )
    kwargs = database._pool_kwargs()
    assert kwargs["command_timeout"] == 120, "не короче бюджета флаша"
    assert kwargs["timeout"] == 15
    assert kwargs["server_settings"]["application_name"] == "my-app"
    assert kwargs["ssl"] is False
    assert kwargs["init"] == database._configure_connection
