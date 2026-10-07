"""Apply versioned SQL migrations bundled with uapg."""

from __future__ import annotations

import logging
from importlib import resources
from pathlib import Path
from typing import Any, Callable, Coroutine, Dict, List, Optional

import asyncpg

_logger = logging.getLogger(__name__)

MIGRATION_ORDER = [
    "001_core_migrations.sql",
    "002_events_v2_tables.sql",
    "003_events_v2_functions.sql",
    "004_events_v2_timescale.sql",
    "005_events_v2_backfill.sql",
    "006_events_v2_read.sql",
    "101_variables_v2_tables.sql",
    "102_variables_v2_functions.sql",
]

# Только ускоряют чтение, не меняя сигнатур и данных: если роль приложения не
# может их применить (объект принадлежит другой роли), старт продолжается на
# прежней версии, а миграция остаётся неприменённой до `uapg migrations apply`.
DEFERRABLE_MIGRATIONS = frozenset({"006_events_v2_read"})

_PRIVILEGE_ERRORS = (asyncpg.exceptions.InsufficientPrivilegeError,)


class MigrationPrivilegeError(RuntimeError):
    """Обязательная миграция не применена: у роли нет прав на объекты схемы."""


def privilege_hint(schema: str) -> str:
    return (
        f'objects in schema "{schema}" are owned by another role; apply pending migrations '
        "as the owner role (uapg migrations apply --user <owner>) or transfer ownership "
        "to the application role (ALTER FUNCTION/TABLE ... OWNER TO <app_role>)"
    )


def _migration_version(filename: str) -> str:
    return Path(filename).stem


def load_migration_sql(filename: str, schema: str) -> str:
    package = "uapg.sql.migrations"
    with resources.files(package).joinpath(filename).open("r", encoding="utf-8") as fh:
        sql = fh.read()
    return sql.replace("{schema}", schema)


class SqlMigrator:
    """Runs bundled SQL migrations idempotently."""

    def __init__(
        self,
        schema: str,
        execute: Callable[..., Coroutine[Any, Any, Any]],
        fetch: Callable[..., Coroutine[Any, Any, List[Any]]],
        fetchval: Callable[..., Coroutine[Any, Any, Any]],
        logger: Optional[logging.Logger] = None,
        *,
        include_variables: bool = True,
    ) -> None:
        self._schema = schema
        self._execute = execute
        self._fetch = fetch
        self._fetchval = fetchval
        self._logger = logger or _logger
        self._include_variables = include_variables
        self.deferred: Dict[str, str] = {}

    async def ensure_migrations_table(self) -> None:
        await self._execute(
            f'''
            CREATE TABLE IF NOT EXISTS "{self._schema}".uapg_schema_migrations (
                version TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            '''
        )

    async def _is_applied(self, version: str) -> bool:
        row = await self._fetchval(
            f'''
            SELECT 1 FROM "{self._schema}".uapg_schema_migrations
            WHERE version = $1
            LIMIT 1
            ''',
            version,
        )
        return row is not None

    async def _mark_applied(self, version: str) -> None:
        await self._execute(
            f'''
            INSERT INTO "{self._schema}".uapg_schema_migrations (version)
            VALUES ($1)
            ON CONFLICT (version) DO NOTHING
            ''',
            version,
        )

    def _files_to_apply(self) -> List[str]:
        files = list(MIGRATION_ORDER)
        if not self._include_variables:
            files = [f for f in files if not f.startswith("101_") and not f.startswith("102_")]
        return files

    async def pending(self) -> List[str]:
        await self.ensure_migrations_table()
        return [
            _migration_version(f)
            for f in self._files_to_apply()
            if not await self._is_applied(_migration_version(f))
        ]

    async def apply_all(self, *, defer_on_privilege_error: bool = True) -> List[str]:
        """Применить неприменённые миграции по порядку.

        ``defer_on_privilege_error=False`` — для ручного запуска под ролью-владельцем:
        любая ошибка прерывает применение.
        """
        await self.ensure_migrations_table()
        self.deferred = {}
        applied: List[str] = []
        for filename in self._files_to_apply():
            version = _migration_version(filename)
            if await self._is_applied(version):
                continue
            sql = load_migration_sql(filename, self._schema)
            self._logger.info("Applying uapg migration %s to schema %s", version, self._schema)
            try:
                await self._execute(sql)
            except _PRIVILEGE_ERRORS as e:
                if defer_on_privilege_error and version in DEFERRABLE_MIGRATIONS:
                    self.deferred[version] = str(e)
                    self._logger.warning(
                        "uapg migration %s deferred: %s; %s",
                        version,
                        e,
                        privilege_hint(self._schema),
                    )
                    continue
                raise MigrationPrivilegeError(
                    f"uapg migration {version} failed: {e}; {privilege_hint(self._schema)}"
                ) from e
            await self._mark_applied(version)
            applied.append(version)
        return applied

    async def detect_v2_ready(self) -> bool:
        row = await self._fetchval(
            f'''
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = 'events_ts'
            LIMIT 1
            ''',
            self._schema,
        )
        return row is not None
