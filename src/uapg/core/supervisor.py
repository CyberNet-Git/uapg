"""Фоновый надзор за соединением с PostgreSQL.

Нужен не для того, чтобы чинить соединение — это делает сама операция при
ошибке, — а чтобы обрыв был замечен и при простое, и чтобы в логе осталась
внятная картина: когда пропала связь, сколько её не было и когда вернулась.

Отдельная забота — не утопить лог. При многочасовой недоступности БД
посекундные сообщения о неудачных попытках дают сотни мегабайт, поэтому
повторяющееся состояние выводится не чаще раза в минуту.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Optional

from .database import RECONNECT_MAX_DELAY_SEC, RECONNECT_MIN_DELAY_SEC, Database

HEALTHY_POLL_INTERVAL_SEC = 5.0
OUTAGE_LOG_INTERVAL_SEC = 60.0


class ConnectionSupervisor:
    """Следит за доступностью БД и переподключается с нарастающей паузой."""

    def __init__(
        self,
        database: Database,
        logger: Optional[logging.Logger] = None,
        *,
        poll_interval_sec: float = HEALTHY_POLL_INTERVAL_SEC,
        outage_log_interval_sec: float = OUTAGE_LOG_INTERVAL_SEC,
        min_retry_delay_sec: float = RECONNECT_MIN_DELAY_SEC,
        max_retry_delay_sec: float = RECONNECT_MAX_DELAY_SEC,
    ) -> None:
        self._database = database
        self.logger = logger or logging.getLogger("uapg.supervisor")
        self._poll_interval_sec = poll_interval_sec
        self._outage_log_interval_sec = outage_log_interval_sec
        self._min_retry_delay_sec = min_retry_delay_sec
        self._max_retry_delay_sec = max_retry_delay_sec

        self._task: Optional[asyncio.Task[None]] = None
        self._stop_event = asyncio.Event()
        self._healthy = True
        self._unavailable_since: Optional[float] = None
        self._last_outage_log_at: Optional[float] = None

    @property
    def healthy(self) -> bool:
        return self._healthy

    @property
    def outage_seconds(self) -> float:
        if self._unavailable_since is None:
            return 0.0
        return time.monotonic() - self._unavailable_since

    def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stop_event = asyncio.Event()
        self._task = asyncio.create_task(self._run(), name="uapg-connection-supervisor")

    async def stop(self) -> None:
        self._stop_event.set()
        task, self._task = self._task, None
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def _run(self) -> None:
        delay = self._min_retry_delay_sec
        while not self._stop_event.is_set():
            try:
                if await self._database.healthcheck():
                    self._note_healthy()
                    delay = self._min_retry_delay_sec
                    await self._sleep(self._poll_interval_sec)
                    continue

                self._note_unhealthy()
                try:
                    await self._database.reconnect()
                    delay = self._min_retry_delay_sec
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.logger.error("Попытка переподключения не удалась: %s", exc)
                    await self._sleep(delay + random.uniform(0, 0.3 * delay))
                    delay = min(delay * 2, self._max_retry_delay_sec)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                # Надзор не имеет права умереть тихо: без него обрыв при простое
                # останется незамеченным до первой записи.
                self.logger.error("Сбой в надзоре за соединением: %r", exc, exc_info=True)
                await self._sleep(delay)

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    def _note_healthy(self) -> None:
        if self._healthy:
            return
        outage = self.outage_seconds
        self.logger.info(
            "Связь с PostgreSQL восстановлена (db=%s, host=%s, port=%s) после %.1f с недоступности",
            self._database.settings.database,
            self._database.settings.host,
            self._database.settings.port,
            outage,
        )
        self._healthy = True
        self._unavailable_since = None
        self._last_outage_log_at = None

    def _note_unhealthy(self) -> None:
        now = time.monotonic()
        if self._unavailable_since is None:
            self._unavailable_since = now

        if self._healthy:
            self._healthy = False
            self._last_outage_log_at = now
            self.logger.error(
                "PostgreSQL недоступен (db=%s, host=%s, port=%s), начинаем переподключение",
                self._database.settings.database,
                self._database.settings.host,
                self._database.settings.port,
            )
            return

        # Состояние не изменилось: напоминаем о нём не чаще раза в интервал,
        # иначе лог за ночь недоступности вырастает до сотен мегабайт.
        if (
            self._last_outage_log_at is None
            or now - self._last_outage_log_at >= self._outage_log_interval_sec
        ):
            self._last_outage_log_at = now
            self.logger.error(
                "PostgreSQL недоступен уже %.1f с (db=%s, host=%s), попытки продолжаются",
                now - self._unavailable_since,
                self._database.settings.database,
                self._database.settings.host,
            )
