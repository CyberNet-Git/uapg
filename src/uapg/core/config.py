"""Настройки историзации.

Раньше три десятка параметров конструктора разъезжались по атрибутам класса
вперемешку с рантайм-состоянием, а правила нормализации («таймаут <= 0 значит
отключён», «ожидание замка не короче секунды») были разбросаны по месту
использования. Здесь они собраны в неизменяемые группы, и это единственное
место, где живут значения по умолчанию и границы.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Dict, Mapping, Optional

DEFAULT_FLUSH_TIMEOUT_SEC = 120.0
DEFAULT_WORKER_STALL_TIMEOUT_SEC = 300.0
DEFAULT_DB_APPLICATION_NAME = "uapg-history"
DEFAULT_DROP_LOG_INTERVAL_SEC = 60.0
DEFAULT_WORKER_RESTART_MAX_BACKOFF_SEC = 30.0

DURABILITY_ASYNC = "async"
DURABILITY_SYNC = "sync"
CONSISTENCY_LOCAL = "local"
CONSISTENCY_GLOBAL = "global"


def _optional_positive(value: Optional[float]) -> Optional[float]:
    """Неположительный таймаут означает «без ограничения», а не «мгновенно»."""
    if value is None:
        return None
    number = float(value)
    return number if number > 0 else None


@dataclass(frozen=True)
class Timeouts:
    """Границы ожиданий. Ноль или None означают отсутствие ограничения."""

    query_sec: Optional[float] = 30.0
    command_sec: Optional[float] = 60.0
    pool_close_sec: float = 5.0
    pool_create_sec: float = 30.0
    lock_wait_sec: float = 60.0
    flush_sec: float = DEFAULT_FLUSH_TIMEOUT_SEC
    worker_stall_sec: float = DEFAULT_WORKER_STALL_TIMEOUT_SEC

    @classmethod
    def build(
        cls,
        *,
        query_sec: Optional[float] = 30.0,
        command_sec: Optional[float] = 60.0,
        pool_close_sec: float = 5.0,
        pool_create_sec: float = 30.0,
        lock_wait_sec: float = 60.0,
        flush_sec: float = DEFAULT_FLUSH_TIMEOUT_SEC,
        worker_stall_sec: float = DEFAULT_WORKER_STALL_TIMEOUT_SEC,
    ) -> "Timeouts":
        return cls(
            query_sec=_optional_positive(query_sec),
            command_sec=_optional_positive(command_sec),
            pool_close_sec=max(0.1, float(pool_close_sec)),
            # Пересоздание пула держит замки, пока создаёт соединения: без нижней
            # границы флаш, вошедший в ожидание, стоит там неограниченно долго.
            pool_create_sec=max(1.0, float(pool_create_sec)),
            lock_wait_sec=max(1.0, float(lock_wait_sec)),
            flush_sec=max(0.0, float(flush_sec)),
            worker_stall_sec=max(0.0, float(worker_stall_sec)),
        )

    @property
    def effective_command_sec(self) -> Optional[float]:
        """command_timeout пула не должен быть короче бюджета флаша.

        Иначе asyncpg оборвёт INSERT раньше, чем сработает прикладной таймаут,
        и в логе останется ошибка не того слоя.
        """
        candidates = [v for v in (self.command_sec, self.flush_sec) if v and v > 0]
        return max(candidates) if candidates else None

    @property
    def flush_operation_sec(self) -> Optional[float]:
        return self.flush_sec if self.flush_sec > 0 else self.query_sec

    @property
    def buffer_flush_sec(self) -> float:
        """Внешний потолок буфера: две попытки флаша плюс ожидание замка реконнекта."""
        if self.flush_sec <= 0:
            return 0.0
        return 2.0 * self.flush_sec + self.lock_wait_sec


@dataclass(frozen=True)
class Keepalive:
    """TCP keepalive: обрыв соединения должен обнаруживаться на уровне ОС.

    Прикладной отмены недостаточно — отмена зависшего запроса уходит в тот же
    мёртвый сокет, и ожидание не заканчивается никогда.
    """

    idle_sec: int = 30
    interval_sec: int = 10
    count: int = 3
    # Ограничивает время неподтверждённой отправки и потому ловит обрыв даже
    # посреди активной записи, когда keepalive не помогает.
    user_timeout_sec: float = 60.0

    @classmethod
    def build(
        cls,
        *,
        idle_sec: int = 30,
        interval_sec: int = 10,
        count: int = 3,
        user_timeout_sec: float = 60.0,
    ) -> "Keepalive":
        return cls(
            idle_sec=max(0, int(idle_sec)),
            interval_sec=max(1, int(interval_sec)),
            count=max(1, int(count)),
            user_timeout_sec=max(0.0, float(user_timeout_sec)),
        )

    @property
    def enabled(self) -> bool:
        return self.idle_sec > 0


@dataclass(frozen=True)
class WriteSettings:
    """Батчевая запись истории."""

    batch_enabled: bool = True
    max_batch_size: int = 500
    max_batch_interval_sec: float = 1.0
    queue_max_size: int = 10000
    durability_mode: str = DURABILITY_ASYNC
    read_consistency_mode: str = CONSISTENCY_LOCAL

    @classmethod
    def build(
        cls,
        *,
        batch_enabled: bool = True,
        max_batch_size: int = 500,
        max_batch_interval_sec: float = 1.0,
        queue_max_size: int = 10000,
        durability_mode: str = DURABILITY_ASYNC,
        read_consistency_mode: str = CONSISTENCY_LOCAL,
    ) -> "WriteSettings":
        return cls(
            batch_enabled=bool(batch_enabled),
            max_batch_size=int(max_batch_size),
            max_batch_interval_sec=float(max_batch_interval_sec),
            queue_max_size=int(queue_max_size),
            durability_mode=str(durability_mode),
            read_consistency_mode=str(read_consistency_mode),
        )

    @property
    def wait_for_flush(self) -> bool:
        """Режим global: вызывающий ждёт, пока запись станет видимой для чтения."""
        return self.read_consistency_mode == CONSISTENCY_GLOBAL


@dataclass(frozen=True)
class CacheSettings:
    """Кэши последних значений и метаданных."""

    enabled: bool = True
    last_values_enabled: bool = True
    # Параметр принят ради совместимости конструктора, но размер кэша не
    # ограничивается — так вело себя и 0.2.15.
    last_values_max_size_mb: int = 100
    last_values_init_batch_size: int = 1000
    metadata_enabled: bool = True
    metadata_init_max_rows: int = 500000

    @classmethod
    def build(
        cls,
        *,
        enabled: bool = True,
        last_values_enabled: bool = True,
        last_values_max_size_mb: int = 100,
        last_values_init_batch_size: int = 1000,
        metadata_enabled: bool = True,
        metadata_init_max_rows: int = 500000,
    ) -> "CacheSettings":
        return cls(
            enabled=bool(enabled),
            last_values_enabled=bool(last_values_enabled),
            last_values_max_size_mb=int(last_values_max_size_mb),
            last_values_init_batch_size=int(last_values_init_batch_size),
            metadata_enabled=bool(metadata_enabled),
            metadata_init_max_rows=int(metadata_init_max_rows),
        )


@dataclass(frozen=True)
class ConnectionSettings:
    """Параметры подключения и пула."""

    user: str = "postgres"
    password: str = "postmaster"
    database: str = "opcua"
    host: str = "localhost"
    port: int = 5432
    min_size: int = 1
    max_size: int = 10
    schema: str = "public"
    sslmode: Optional[str] = None
    # Позволяет отличить свои зависшие backend'ы от чужих и завершить только их.
    application_name: str = DEFAULT_DB_APPLICATION_NAME
    # Прочие аргументы asyncpg, переданные через **kwargs конструктора.
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def build(
        cls,
        *,
        user: str = "postgres",
        password: str = "postmaster",
        database: str = "opcua",
        host: str = "localhost",
        port: int = 5432,
        min_size: int = 1,
        max_size: int = 10,
        schema: str = "public",
        sslmode: Optional[str] = None,
        application_name: str = DEFAULT_DB_APPLICATION_NAME,
        extra: Optional[Mapping[str, Any]] = None,
    ) -> "ConnectionSettings":
        return cls(
            user=user,
            password=password,
            database=database,
            host=host,
            port=int(port),
            min_size=int(min_size),
            max_size=int(max_size),
            schema=schema,
            sslmode=sslmode,
            application_name=str(application_name or "").strip() or DEFAULT_DB_APPLICATION_NAME,
            extra=dict(extra or {}),
        )

    def with_overrides(self, values: Mapping[str, Any]) -> "ConnectionSettings":
        """Применить значения из расшифрованной конфигурации поверх текущих."""
        known = {"user", "password", "database", "host", "port", "schema", "sslmode"}
        overrides = {key: value for key, value in values.items() if key in known}
        if not overrides:
            return self
        merged: Dict[str, Any] = {
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "host": self.host,
            "port": self.port,
            "min_size": self.min_size,
            "max_size": self.max_size,
            "schema": self.schema,
            "sslmode": self.sslmode,
            "application_name": self.application_name,
            "extra": self.extra,
        }
        merged.update(overrides)
        return ConnectionSettings.build(**merged)


@dataclass(frozen=True)
class StorageSettings:
    """Полный набор настроек бэкенда историзации."""

    connection: ConnectionSettings = field(default_factory=ConnectionSettings)
    timeouts: Timeouts = field(default_factory=Timeouts)
    keepalive: Keepalive = field(default_factory=Keepalive)
    write: WriteSettings = field(default_factory=WriteSettings)
    cache: CacheSettings = field(default_factory=CacheSettings)
    # Глобальная политика TimescaleDB и верхняя граница для per-node ретенции.
    global_retention_period: Optional[timedelta] = None

    @property
    def schema(self) -> str:
        return self.connection.schema

    def metrics_snapshot(self) -> Dict[str, Any]:
        """Раздел config в get_performance_metrics(); набор ключей — часть контракта."""
        return {
            "history_write_batch_enabled": self.write.batch_enabled,
            "history_write_max_batch_size": self.write.max_batch_size,
            "history_write_max_batch_interval_sec": self.write.max_batch_interval_sec,
            "history_write_queue_max_size": self.write.queue_max_size,
            "history_write_durability_mode": self.write.durability_mode,
            "history_write_read_consistency_mode": self.write.read_consistency_mode,
            "db_query_timeout_sec": self.timeouts.query_sec,
            "db_pool_create_timeout_sec": self.timeouts.pool_create_sec,
            "db_lock_wait_timeout_sec": self.timeouts.lock_wait_sec,
            "db_command_timeout_sec": self.timeouts.command_sec,
            "db_tcp_keepalive_idle_sec": self.keepalive.idle_sec,
            "db_tcp_user_timeout_sec": self.keepalive.user_timeout_sec,
            "history_flush_timeout_sec": self.timeouts.flush_sec,
            "history_worker_stall_timeout_sec": self.timeouts.worker_stall_sec,
            "db_application_name": self.connection.application_name,
        }
