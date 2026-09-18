"""Доступ к PostgreSQL: пул, таймауты, реконнект.

Объект пула наружу не отдаётся. Раньше вспомогательные классы получали
``asyncpg.Pool`` при инициализации и после пересоздания продолжали ходить в
закрытый пул — история переменных при этом работала, а чтение событий падало с
«pool is closed» до перезапуска процесса. Здесь любое обращение проходит через
методы этого класса, которые каждый раз берут актуальный пул, поэтому целый
класс таких ошибок становится невозможным.

Второе, что здесь обязано быть верным, — ограниченность ожиданий. Молчаливый
обрыв сокета не обнаруживается прикладной отменой: отмена зависшего запроса
уходит в тот же мёртвый сокет. Поэтому обрыв ловится на уровне ОС (keepalive и
TCP_USER_TIMEOUT), а все ожидания — замка, создания пула, самой операции —
ограничены по времени.
"""

from __future__ import annotations

import asyncio
import logging
import socket
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List, Optional, TypeVar, cast

import asyncpg

from .config import ConnectionSettings, Keepalive, Timeouts
from .errors import DatabaseStopping, OperationTimeout
from .metrics import DatabaseMetrics

T = TypeVar("T")

RECONNECT_MIN_DELAY_SEC = 1.0
RECONNECT_MAX_DELAY_SEC = 30.0


@dataclass(frozen=True)
class PoolHandle:
    """Пул и номер его поколения.

    Номер позволяет отличить «мой пул сломался» от «пул уже заменили без меня»,
    не сравнивая объекты и не давая вызывающему возможность сохранить ссылку.
    """

    pool: asyncpg.Pool
    generation: int


def _is_open(pool: Optional[asyncpg.Pool]) -> bool:
    return pool is not None and not pool._closed and not getattr(pool, "_closing", False)


def is_connection_error(exc: BaseException) -> bool:
    """Отличить обрыв соединения от ошибки, на которую ответил сам сервер.

    Если PostgreSQL прислал ошибку (синтаксис, нарушение ограничения, выход за
    диапазон типа), соединение живо: пересоздавать пул незачем, а повторять
    запрос бессмысленно — он упадёт так же. Пересоздание пула на каждой такой
    ошибке под нагрузкой заметно дороже самой ошибки.
    """
    if isinstance(exc, asyncpg.PostgresConnectionError):
        return True
    if isinstance(exc, asyncpg.PostgresError):
        return False
    return isinstance(
        exc,
        (
            asyncpg.InterfaceError,
            asyncpg.InternalClientError,
            OSError,
            ConnectionError,
            EOFError,
        ),
    )


class Database:
    """Единственная точка обращения к PostgreSQL."""

    def __init__(
        self,
        connection: ConnectionSettings,
        timeouts: Timeouts,
        keepalive: Keepalive,
        metrics: DatabaseMetrics,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._connection = connection
        self._timeouts = timeouts
        self._keepalive = keepalive
        self._metrics = metrics
        self.logger = logger or logging.getLogger("uapg.database")

        self._pool: Optional[asyncpg.Pool] = None
        self._generation = 0
        self._pool_lock = asyncio.Lock()
        self._reconnect_lock = asyncio.Lock()
        self._stopping = False

    # ------------------------------------------------------------------ жизненный цикл

    async def start(self) -> None:
        self._stopping = False
        await self._ensure_pool()

    async def stop(self) -> None:
        self._stopping = True
        async with self._pool_lock:
            pool, self._pool = self._pool, None
        await self._close_pool(pool, "остановка")

    @property
    def is_connected(self) -> bool:
        return _is_open(self._pool)

    @property
    def settings(self) -> ConnectionSettings:
        return self._connection

    # ------------------------------------------------------------------ запросы

    async def execute(self, sql: str, *args: Any) -> str:
        return cast(str, await self._query("execute", sql, args))

    async def fetch(self, sql: str, *args: Any) -> List[asyncpg.Record]:
        return cast(List[asyncpg.Record], await self._query("fetch", sql, args))

    async def fetchrow(self, sql: str, *args: Any) -> Optional[asyncpg.Record]:
        return await self._query("fetchrow", sql, args)

    async def fetchval(self, sql: str, *args: Any) -> Any:
        return await self._query("fetchval", sql, args)

    async def _query(self, kind: str, sql: str, args: tuple[Any, ...]) -> Any:
        timeout = self._timeouts.query_sec

        async def _operation(conn: asyncpg.Connection) -> Any:
            method = getattr(conn, kind)
            return await method(sql, *args, timeout=timeout)

        # Таймаут запроса повтором не лечится: если БД не ответила за отведённое
        # время, второй такой же запрос сразу после реконнекта только удвоит
        # ожидание. Прочие ошибки чаще всего означают мёртвое соединение.
        return await self._run_on_connection(
            _operation,
            name=kind,
            layer="query",
            timeout=timeout,
            retry_on_timeout=False,
        )

    # ------------------------------------------------------------------ транзакции

    async def run_in_transaction(
        self,
        operation: Callable[[asyncpg.Connection], Awaitable[T]],
        *,
        name: str,
    ) -> T:
        """Выполнить операцию в транзакции с одной повторной попыткой.

        В отличие от одиночного запроса, здесь повтор делается и по таймауту:
        батч, потерянный при обрыве соединения, восстановить больше неоткуда.
        """
        return await self._run_on_connection(
            operation,
            name=name,
            layer="flush",
            timeout=self._timeouts.flush_operation_sec,
            retry_on_timeout=True,
            in_transaction=True,
        )

    # ------------------------------------------------------------------ здоровье

    async def healthcheck(self) -> bool:
        try:
            handle = await self._ensure_pool()

            async def _operation(conn: asyncpg.Connection) -> Any:
                return await conn.fetchval("SELECT 1", timeout=self._timeouts.query_sec)

            value = await self._execute_once(
                handle,
                _operation,
                name="healthcheck",
                layer="query",
                timeout=self._timeouts.query_sec,
                in_transaction=False,
            )
            return bool(value == 1)
        except Exception as exc:
            self.logger.debug(
                "Проверка PostgreSQL не прошла (db=%s, host=%s, port=%s): %r",
                self._connection.database,
                self._connection.host,
                self._connection.port,
                exc,
            )
            return False

    # ------------------------------------------------------------------ внутреннее

    async def _run_on_connection(
        self,
        operation: Callable[[asyncpg.Connection], Awaitable[T]],
        *,
        name: str,
        layer: str,
        timeout: Optional[float],
        retry_on_timeout: bool,
        in_transaction: bool = False,
    ) -> T:
        handle = await self._ensure_pool()
        try:
            return await self._execute_once(
                handle, operation, name=name, layer=layer, timeout=timeout,
                in_transaction=in_transaction,
            )
        except Exception as exc:
            if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
                if not retry_on_timeout:
                    self.logger.error("%s: таймаут, переподключаемся без повтора: %s", name, exc)
                    await self.reconnect(handle)
                    raise
            elif not is_connection_error(exc):
                # Сервер ответил ошибкой — соединение исправно, повтор даст то же самое.
                raise
            self.logger.error("%s: ошибка, переподключаемся и повторяем: %s", name, exc)
            await self.reconnect(handle)

        retry_handle = await self._ensure_pool()
        try:
            return await self._execute_once(
                retry_handle, operation, name=f"{name} (повтор)", layer=layer,
                timeout=timeout, in_transaction=in_transaction,
            )
        except Exception as exc:
            self.logger.error("%s: ошибка и после переподключения: %s", name, exc)
            raise

    async def _execute_once(
        self,
        handle: PoolHandle,
        operation: Callable[[asyncpg.Connection], Awaitable[T]],
        *,
        name: str,
        layer: str,
        timeout: Optional[float],
        in_transaction: bool,
    ) -> T:
        async def _run() -> T:
            if in_transaction:
                async with self._transaction(handle) as conn:
                    return await operation(conn)
            async with handle.pool.acquire(timeout=timeout) as conn:
                return await operation(conn)

        return await self._with_timeout(_run(), name=name, layer=layer, timeout=timeout)

    @asynccontextmanager
    async def _transaction(self, handle: PoolHandle) -> AsyncIterator[asyncpg.Connection]:
        """Транзакция, которая при таймауте рвёт сокет, а не ждёт ROLLBACK.

        asyncpg при выходе из транзакции отправляет ROLLBACK, и на мёртвом
        соединении это ожидание не заканчивается — отмена уходит в тот же сокет.
        """
        conn = await handle.pool.acquire(timeout=self._timeouts.flush_operation_sec)
        terminated = False
        transaction = conn.transaction()
        try:
            await transaction.start()
            try:
                yield conn
                await transaction.commit()
            except (asyncio.TimeoutError, TimeoutError, asyncio.CancelledError):
                terminated = True
                self._terminate(conn)
                raise
            except Exception:
                try:
                    await transaction.rollback()
                except Exception as rollback_error:
                    self.logger.warning(
                        "Откат не прошёл (%r), соединение закрывается принудительно",
                        rollback_error,
                    )
                    terminated = True
                    self._terminate(conn)
                raise
        finally:
            if not terminated:
                try:
                    await handle.pool.release(conn)
                except Exception as release_error:
                    self.logger.warning(
                        "Соединение не вернулось в пул (%r), закрывается принудительно",
                        release_error,
                    )
                    self._terminate(conn)

    def _terminate(self, conn: Any) -> None:
        try:
            conn.terminate()
        except Exception as exc:
            self.logger.warning("Соединение не удалось закрыть принудительно: %r", exc)

    async def _with_timeout(
        self,
        awaitable: Awaitable[T],
        *,
        name: str,
        layer: str,
        timeout: Optional[float],
    ) -> T:
        if timeout is None or timeout <= 0:
            return await awaitable
        try:
            return await asyncio.wait_for(awaitable, timeout=timeout)
        except asyncio.TimeoutError:
            self._metrics.timeouts_total += 1
            self.logger.error("%s: таймаут %.1f с (слой %s)", name, timeout, layer)
            raise OperationTimeout(name, timeout, layer) from None

    # ------------------------------------------------------------------ пул

    async def _ensure_pool(self) -> PoolHandle:
        if self._stopping:
            raise DatabaseStopping("бэкенд историзации останавливается")
        pool = self._pool
        if _is_open(pool):
            assert pool is not None
            return PoolHandle(pool, self._generation)

        # Если реконнект уже идёт, дожидаемся его вместо создания конкурирующего пула.
        if self._reconnect_lock.locked():
            async with self._bounded_lock(self._reconnect_lock, "замок реконнекта"):
                pass
            pool = self._pool
            if _is_open(pool):
                assert pool is not None
                return PoolHandle(pool, self._generation)

        async with self._bounded_lock(self._pool_lock, "замок пула"):
            if _is_open(self._pool):
                assert self._pool is not None
                return PoolHandle(self._pool, self._generation)
            self._pool = await self._create_pool("создание пула")
            self._generation += 1
            assert self._pool is not None
            return PoolHandle(self._pool, self._generation)

    async def reconnect(self, handle: Optional[PoolHandle] = None) -> None:
        """Пересоздать пул.

        Если пул уже заменён другой задачей, повторный реконнект только оборвёт
        чужие работающие соединения, поэтому он пропускается — но заметно, с
        предупреждением и счётчиком: молчаливый пропуск в своё время стоил суток
        разбирательств.
        """
        async with self._bounded_lock(self._reconnect_lock, "замок реконнекта"):
            if self._stopping:
                raise DatabaseStopping("бэкенд историзации останавливается")

            old_pool: Optional[asyncpg.Pool] = None
            async with self._bounded_lock(self._pool_lock, "замок пула"):
                if (
                    handle is not None
                    and handle.generation != self._generation
                    and _is_open(self._pool)
                ):
                    self._metrics.reconnects_skipped_total += 1
                    self.logger.warning(
                        "Реконнект пропущен: пул уже заменён (поколение %d, текущее %d)",
                        handle.generation,
                        self._generation,
                    )
                    return
                old_pool, self._pool = self._pool, None

            # Старый пул закрывается вне замка, но до создания нового: иначе
            # asyncpg ловит гонки вида «another operation is in progress».
            await self._close_pool(old_pool, "реконнект")

            async with self._bounded_lock(self._pool_lock, "замок пула"):
                try:
                    self._pool = await self._create_pool("реконнект")
                    self._generation += 1
                    self._metrics.reconnects_total += 1
                    self.logger.info("Пул соединений пересоздан")
                except Exception as exc:
                    self.logger.critical("Переподключиться не удалось, БД недоступна: %s", exc)
                    raise

    async def _create_pool(self, reason: str) -> asyncpg.Pool:
        try:
            async with asyncio.timeout(self._timeouts.pool_create_sec):
                return await asyncpg.create_pool(**self._pool_kwargs())
        except TimeoutError:
            self._metrics.pool_wait_timeouts_total += 1
            self.logger.error(
                "Пул не создан за %.1f с (%s)", self._timeouts.pool_create_sec, reason
            )
            raise

    async def _close_pool(self, pool: Optional[asyncpg.Pool], reason: str) -> None:
        if pool is None:
            return
        try:
            await asyncio.wait_for(pool.close(), timeout=self._timeouts.pool_close_sec)
        except asyncio.TimeoutError:
            self.logger.warning(
                "Пул не закрылся за %.1f с (%s), соединения обрываются",
                self._timeouts.pool_close_sec,
                reason,
            )
            pool.terminate()
        except Exception as exc:
            self.logger.warning("Ошибка закрытия пула (%s): %r", reason, exc)

    @asynccontextmanager
    async def _bounded_lock(self, lock: asyncio.Lock, what: str) -> AsyncIterator[None]:
        """Захват замка с ограничением по времени.

        Без него ожидание замка, удерживаемого зависшим реконнектом, не
        заканчивается никогда, а снаружи это выглядит как «запись молча
        прекратилась».
        """
        try:
            async with asyncio.timeout(self._timeouts.lock_wait_sec):
                await lock.acquire()
        except TimeoutError:
            self._metrics.pool_wait_timeouts_total += 1
            self.logger.error(
                "Не дождались %s за %.1f с, операция отменена",
                what,
                self._timeouts.lock_wait_sec,
            )
            raise
        try:
            yield
        finally:
            lock.release()

    def _pool_kwargs(self) -> Dict[str, Any]:
        settings = self._connection
        kwargs: Dict[str, Any] = {
            "user": settings.user,
            "password": settings.password,
            "database": settings.database,
            "host": settings.host,
            "port": settings.port,
            "min_size": settings.min_size,
            "max_size": settings.max_size,
            # Ограничение установки одного соединения; общий бюджет создания
            # пула задаётся отдельно через asyncio.timeout.
            "timeout": self._timeouts.pool_create_sec,
            "command_timeout": self._timeouts.effective_command_sec,
            "init": self._configure_connection,
        }
        kwargs.update(settings.extra)

        if settings.sslmode == "disable":
            kwargs["ssl"] = False
        elif settings.sslmode in ("require", "verify-ca", "verify-full"):
            kwargs["ssl"] = True

        server_settings = dict(kwargs.get("server_settings") or {})
        server_settings["application_name"] = settings.application_name
        kwargs["server_settings"] = server_settings
        return kwargs

    async def _configure_connection(self, conn: asyncpg.Connection) -> None:
        """Включить keepalive на сокете соединения.

        Ошибка настройки сокета не должна мешать работе с БД: без keepalive
        обрыв обнаружится позже, но запись продолжит работать.
        """
        if not self._keepalive.enabled:
            return
        try:
            sock = conn._transport.get_extra_info("socket")
            if sock is None or sock.family not in (socket.AF_INET, socket.AF_INET6):
                return

            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            for option_name, value in (
                ("TCP_KEEPIDLE", self._keepalive.idle_sec),
                ("TCP_KEEPINTVL", self._keepalive.interval_sec),
                ("TCP_KEEPCNT", self._keepalive.count),
            ):
                option = getattr(socket, option_name, None)
                if option is not None:
                    sock.setsockopt(socket.IPPROTO_TCP, option, value)

            user_timeout = getattr(socket, "TCP_USER_TIMEOUT", None)
            if user_timeout is not None and self._keepalive.user_timeout_sec > 0:
                sock.setsockopt(
                    socket.IPPROTO_TCP,
                    user_timeout,
                    int(self._keepalive.user_timeout_sec * 1000),
                )
        except Exception as exc:
            self.logger.warning("Keepalive на сокете не включён: %r", exc)
