"""Буфер батчевой записи истории.

Очередь ограничена намеренно: при недоступности БД лучше потерять часть
значений и сказать об этом счётчиком, чем съесть память процесса. Но прошлые
инциденты показали, что одной ограниченной очереди мало, и здесь закрыты все
три обнаруженные тогда дыры.

Первая: воркер умирал молча. Его задачу никто не ждал, done-callback'а не было,
и единственным следом оставался поток «queue is full». Теперь выход воркера
всегда попадает в лог с причиной, а сам он поднимается заново.

Вторая: воркер оставался живым, но залипал внутри флаша на мёртвом сокете.
``task.done()`` при этом возвращал False, ошибок не было, а очередь
переполнялась. Поэтому надзор смотрит не на «жива ли задача», а на длительность
текущего флаша, и залипшую задачу снимает, не дожидаясь её завершения — её
собственная уборка может висеть на том же сокете.

Третья: сообщение об отбрасывании писалось на каждый элемент и при штатной
нагрузке давало сотни мегабайт лога. Теперь оно выводится не чаще раза в
интервал и несёт число отброшенных за окно.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable, Generic, List, Optional, TypeVar

from .config import (
    DEFAULT_DROP_LOG_INTERVAL_SEC,
    DEFAULT_WORKER_RESTART_MAX_BACKOFF_SEC,
    DURABILITY_SYNC,
)
from .metrics import BufferStats

T = TypeVar("T")

FlushCallable = Callable[[List[T]], Awaitable[None]]


class QueueOverflow(RuntimeError):
    """Очередь заполнена, элемент не принят."""


class HistoryWriteBuffer(Generic[T]):
    """Очередь с фоновым воркером, который пишет накопленное пачками."""

    def __init__(
        self,
        name: str,
        flush: FlushCallable[T],
        stats: BufferStats,
        *,
        max_batch_size: int,
        max_batch_interval_sec: float,
        queue_max_size: int,
        durability_mode: str,
        flush_timeout_sec: float,
        stall_timeout_sec: float,
        drop_log_interval_sec: float = DEFAULT_DROP_LOG_INTERVAL_SEC,
        restart_max_backoff_sec: float = DEFAULT_WORKER_RESTART_MAX_BACKOFF_SEC,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._name = name
        self._flush = flush
        self._stats = stats
        self._max_batch_size = max(1, int(max_batch_size))
        self._max_batch_interval_sec = max(0.001, float(max_batch_interval_sec))
        self._durability_mode = durability_mode
        self._flush_timeout_sec = max(0.0, float(flush_timeout_sec))
        self._stall_timeout_sec = max(0.0, float(stall_timeout_sec))
        self._drop_log_interval_sec = max(0.0, float(drop_log_interval_sec))
        self._restart_max_backoff_sec = max(0.1, float(restart_max_backoff_sec))
        self.logger = logger or logging.getLogger(f"uapg.buffer.{name}")

        self._queue: asyncio.Queue[T] = asyncio.Queue(maxsize=max(1, int(queue_max_size)))
        self._stats.queue_max_size = self._queue.maxsize
        self._task: Optional[asyncio.Task[None]] = None
        self._started = False
        self._stopping = False
        self._restart_attempt = 0
        self._drops_since_last_log = 0
        self._drop_log_at: Optional[float] = None

    # ------------------------------------------------------------------ жизненный цикл

    def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping = False
        self._started = True
        self._task = asyncio.create_task(self._run(), name=f"uapg-buffer-{self._name}")
        self._task.add_done_callback(self._on_worker_done)
        self._stats.worker_alive = True

    async def stop(self) -> None:
        """Остановить воркер, дав ему дописать то, что уже в очереди."""
        self._stopping = True
        task, self._task = self._task, None
        self._stats.worker_alive = False
        if task is None:
            return
        task.remove_done_callback(self._on_worker_done)
        try:
            await asyncio.wait_for(task, timeout=self._drain_budget_sec())
        except asyncio.TimeoutError:
            self.logger.warning(
                "Буфер %s не успел дописать очередь за %.1f с, воркер снимается (%d элементов)",
                self._name,
                self._drain_budget_sec(),
                self._queue.qsize(),
            )
            task.cancel()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            self.logger.error("Буфер %s: воркер завершился с ошибкой: %r", self._name, exc)

    def _drain_budget_sec(self) -> float:
        return self._flush_timeout_sec if self._flush_timeout_sec > 0 else 30.0

    @property
    def queue_size(self) -> int:
        return self._queue.qsize()

    def is_worker_alive(self) -> bool:
        return self._task is not None and not self._task.done()

    # ------------------------------------------------------------------ приём элементов

    async def enqueue(self, item: T, *, sync: bool = False) -> None:
        """Поставить элемент в очередь.

        В синхронном режиме вызывающий ждёт, пока элемент действительно окажется
        в БД. Ожидание ограничено по времени: иначе полная очередь со залипшим
        воркером навсегда останавливает поток записи сервера.
        """
        self._stats.record_enqueue_attempt()
        self._ensure_worker_running()

        wait_for_flush = sync or self._durability_mode == DURABILITY_SYNC
        if not wait_for_flush:
            try:
                self._queue.put_nowait(item)
            except asyncio.QueueFull:
                self._stats.record_dropped()
                self._log_drop()
                return
            self._stats.record_enqueued(self._queue.qsize())
            return

        future: asyncio.Future[float] = asyncio.get_running_loop().create_future()
        setattr(item, "future", future)
        try:
            async with asyncio.timeout(self._sync_budget_sec()):
                await self._queue.put(item)
                self._stats.record_enqueued(self._queue.qsize())
                await future
        except asyncio.TimeoutError:
            if not future.done():
                future.cancel()
            self._stats.record_dropped()
            raise QueueOverflow(
                f"буфер {self._name}: запись не подтверждена за {self._sync_budget_sec():.1f} с"
            ) from None

    def _sync_budget_sec(self) -> float:
        """Потолок ожидания в синхронном режиме: две попытки флаша с запасом."""
        if self._flush_timeout_sec <= 0:
            return 60.0
        return 2.0 * self._flush_timeout_sec

    def _log_drop(self) -> None:
        self._drops_since_last_log += 1
        now = time.monotonic()
        last = self._drop_log_at

        if last is not None and self._drop_log_interval_sec > 0:
            if now - last < self._drop_log_interval_sec:
                return

        dropped_in_window = self._drops_since_last_log
        self._drops_since_last_log = 0
        self._drop_log_at = now

        if last is None:
            self.logger.error(
                "Буфер %s: очередь заполнена (%d), элементы отбрасываются; "
                "далее сообщение не чаще раза в %.0f с",
                self._name,
                self._queue.maxsize,
                self._drop_log_interval_sec,
            )
        else:
            self.logger.error(
                "Буфер %s: отброшено %d элементов за %.0f с (всего %d, воркер жив: %s)",
                self._name,
                dropped_in_window,
                now - last,
                self._stats.dropped_total,
                self.is_worker_alive(),
            )

    # ------------------------------------------------------------------ воркер

    def _ensure_worker_running(self) -> None:
        """Поднять воркер, если он умер или залип.

        Буфер, который ни разу не запускали, здесь не поднимается: иначе запись
        пошла бы раньше инициализации.
        """
        if not self._started or self._stopping:
            return
        if self._task is None or self._task.done():
            self.start()
            return
        self._restart_if_stalled()

    def _restart_if_stalled(self) -> None:
        if self._stall_timeout_sec <= 0:
            return
        in_flush = self._stats.as_dict()["seconds_in_current_flush"]
        if in_flush < 0 or in_flush < self._stall_timeout_sec:
            return

        self._stats.worker_stall_restarts_total += 1
        self.logger.critical(
            "Буфер %s: флаш идёт %.1f с (порог %.1f), воркер снимается и поднимается заново",
            self._name,
            in_flush,
            self._stall_timeout_sec,
        )
        stalled, self._task = self._task, None
        if stalled is not None:
            stalled.remove_done_callback(self._on_worker_done)
            # Ждать завершения нельзя: уборка отменённой задачи может висеть на
            # том же мёртвом сокете, из-за которого флаш и залип.
            stalled.cancel()
        self.start()

    def _on_worker_done(self, task: "asyncio.Task[None]") -> None:
        self._stats.worker_alive = False
        if self._stopping:
            return

        reason = "отменён"
        if not task.cancelled():
            error = task.exception()
            reason = f"{type(error).__name__}: {error}" if error else "завершился сам"
        self._stats.last_worker_exit_reason = reason
        self.logger.critical(
            "Буфер %s: воркер остановился (%s), в очереди %d элементов",
            self._name,
            reason,
            self._queue.qsize(),
        )
        self._schedule_restart()

    def _schedule_restart(self) -> None:
        self._restart_attempt += 1
        delay = min(
            self._restart_max_backoff_sec,
            2.0 ** min(self._restart_attempt - 1, 10),
        )
        self._stats.worker_restarts_total += 1

        async def _restart() -> None:
            await asyncio.sleep(delay)
            if self._stopping:
                return
            self.logger.warning("Буфер %s: поднимаем воркер заново", self._name)
            self.start()

        asyncio.create_task(_restart(), name=f"uapg-buffer-restart-{self._name}")

    async def _run(self) -> None:
        pending: List[T] = []
        try:
            while not self._stopping or not self._queue.empty():
                if not pending:
                    try:
                        pending.append(
                            await asyncio.wait_for(
                                self._queue.get(), timeout=self._max_batch_interval_sec
                            )
                        )
                    except asyncio.TimeoutError:
                        continue

                # Добираем пачку без ожидания: всё, что уже пришло, едет одним заходом.
                while len(pending) < self._max_batch_size:
                    try:
                        pending.append(self._queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break

                try:
                    await self._flush_batch(pending)
                except Exception:
                    # Ошибка одного батча не должна останавливать разбор очереди.
                    pass
                pending.clear()

            if pending:
                try:
                    await self._flush_batch(pending)
                except Exception:
                    pass
        except asyncio.CancelledError:
            if not self._stopping:
                self._stats.last_worker_exit_reason = "отменён во время работы"
                self.logger.critical(
                    "Буфер %s: воркер отменён на ходу, не записано %d элементов",
                    self._name,
                    len(pending) + self._queue.qsize(),
                )
            raise

    async def _flush_batch(self, batch: List[T]) -> None:
        if not batch:
            return

        batch_size = len(batch)
        self._stats.flush_started(batch_size)
        started_at = time.perf_counter()
        try:
            if self._flush_timeout_sec > 0:
                async with asyncio.timeout(self._flush_timeout_sec):
                    await self._flush(batch)
            else:
                await self._flush(batch)
        except Exception as exc:
            duration_ms = (time.perf_counter() - started_at) * 1000.0
            timed_out = isinstance(exc, (asyncio.TimeoutError, TimeoutError))
            self._stats.flush_duration.observe(duration_ms)
            self._stats.flush_failed(
                str(exc) or f"таймаут флаша после {duration_ms / 1000.0:.1f} с",
                timeout=timed_out,
                dropped_items=batch_size,
            )
            self.logger.error(
                "Буфер %s: флаш %d элементов не прошёл за %.1f с: %s",
                self._name,
                batch_size,
                duration_ms / 1000.0,
                exc,
                exc_info=not timed_out,
            )
            self._resolve(batch, error=exc)
            raise
        else:
            duration_ms = (time.perf_counter() - started_at) * 1000.0
            self._stats.flush_succeeded(batch_size, duration_ms, self._queue.qsize())
            self._restart_attempt = 0
            self._resolve(batch, error=None)

    @staticmethod
    def _resolve(batch: List[T], *, error: Optional[BaseException]) -> None:
        now = time.time()
        for item in batch:
            future: Optional[asyncio.Future[Any]] = getattr(item, "future", None)
            if future is None or future.done():
                continue
            if error is None:
                future.set_result(now)
            else:
                future.set_exception(error)
