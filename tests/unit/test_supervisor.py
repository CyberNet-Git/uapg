"""Надзор за соединением: переходы состояния и объём логирования."""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

import pytest

from uapg.core.config import ConnectionSettings
from uapg.core.supervisor import ConnectionSupervisor


class FakeDatabase:
    """Подделка БД: здоровье задаётся тестом, реконнект только считается."""

    def __init__(self, healthy: bool = True) -> None:
        self.healthy = healthy
        self.reconnects = 0
        self.reconnect_fails = False
        self.settings = ConnectionSettings.build(database="d", host="h", port=5432)

    async def healthcheck(self) -> bool:
        return self.healthy

    async def reconnect(self, handle: Optional[object] = None) -> None:
        self.reconnects += 1
        if self.reconnect_fails:
            raise ConnectionError("БД недоступна")
        self.healthy = True


def _supervisor(database: FakeDatabase, **kwargs: float) -> ConnectionSupervisor:
    supervisor = ConnectionSupervisor(
        database,  # type: ignore[arg-type]
        logger=logging.getLogger("test.supervisor"),
        poll_interval_sec=kwargs.get("poll_interval_sec", 0.01),
        outage_log_interval_sec=kwargs.get("outage_log_interval_sec", 60.0),
        min_retry_delay_sec=kwargs.get("min_retry_delay_sec", 0.01),
        max_retry_delay_sec=kwargs.get("max_retry_delay_sec", 0.05),
    )
    return supervisor


async def _run_briefly(supervisor: ConnectionSupervisor, seconds: float = 0.1) -> None:
    supervisor.start()
    await asyncio.sleep(seconds)
    await supervisor.stop()


async def test_healthy_database_is_not_reconnected() -> None:
    database = FakeDatabase(healthy=True)
    await _run_briefly(_supervisor(database))
    assert database.reconnects == 0


async def test_unhealthy_database_triggers_reconnect() -> None:
    database = FakeDatabase(healthy=False)
    await _run_briefly(_supervisor(database))
    assert database.reconnects >= 1
    assert database.healthy


async def test_outage_is_reported_once_per_interval(caplog: pytest.LogCaptureFixture) -> None:
    """Ночь недоступности не должна превратиться в сотни мегабайт лога."""
    database = FakeDatabase(healthy=False)
    database.reconnect_fails = True

    with caplog.at_level(logging.ERROR, logger="test.supervisor"):
        await _run_briefly(_supervisor(database, outage_log_interval_sec=3600.0), seconds=0.3)

    outage_messages = [r for r in caplog.records if "недоступен" in r.getMessage()]
    assert len(outage_messages) == 1, [r.getMessage() for r in outage_messages]


async def test_recovery_is_logged_with_duration(caplog: pytest.LogCaptureFixture) -> None:
    database = FakeDatabase(healthy=False)
    database.reconnect_fails = True
    supervisor = _supervisor(database)

    with caplog.at_level(logging.INFO, logger="test.supervisor"):
        supervisor.start()
        await asyncio.sleep(0.05)
        assert not supervisor.healthy
        database.reconnect_fails = False
        database.healthy = True
        await asyncio.sleep(0.15)
        await supervisor.stop()

    assert supervisor.healthy
    restored = [r for r in caplog.records if "восстановлена" in r.getMessage()]
    assert len(restored) == 1
    assert "недоступности" in restored[0].getMessage()


async def test_stop_is_idempotent_and_cancels_task() -> None:
    supervisor = _supervisor(FakeDatabase())
    supervisor.start()
    await supervisor.stop()
    await supervisor.stop()


async def test_supervisor_survives_unexpected_error(caplog: pytest.LogCaptureFixture) -> None:
    """Молчаливая смерть надзора оставила бы обрыв при простое незамеченным."""

    class BrokenDatabase(FakeDatabase):
        def __init__(self) -> None:
            super().__init__()
            self.calls = 0

        async def healthcheck(self) -> bool:
            self.calls += 1
            raise RuntimeError("неожиданный сбой")

    database = BrokenDatabase()
    with caplog.at_level(logging.ERROR, logger="test.supervisor"):
        await _run_briefly(_supervisor(database), seconds=0.1)

    assert database.calls >= 2, "цикл надзора остановился после первой ошибки"
    assert any("Сбой в надзоре" in r.getMessage() for r in caplog.records)
