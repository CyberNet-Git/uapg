"""Версионированные миграции слоя типизированных событий.

В отличие от ядра, которое приводится к нужному виду идемпотентным DDL, здесь
есть функции и процедуры: их нельзя «создать, если нет», их надо применять по
порядку и помнить, что уже применено. Отметки лежат в ``uapg_schema_migrations``.

Контрольных сумм нет намеренно, как и раньше: уже применённый файл не
перечитывается, поэтому править вышедшую миграцию бесполезно — изменения
оформляются новым файлом.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional, Sequence

from ..core.database import Database
from ..core.sql import load_sql

PACKAGE = "uapg.sql.migrations"

# Миграции 101/102 (переменные v2 по ADR-003) удалены: они создавали в каждой
# базе гипертаблицу и функции, которых не касалась ни одна строка Python. В
# базах, где они уже применены, объекты остаются и ничему не мешают.
EVENT_MIGRATIONS: Sequence[str] = (
    "001_core_migrations.sql",
    "002_events_v2_tables.sql",
    "003_events_v2_functions.sql",
    "004_events_v2_timescale.sql",
    "005_events_v2_backfill.sql",
)

# Признак готовности слоя v2. Проверка намеренно простая и не должна меняться:
# переименование таблицы молча вернуло бы все существующие базы к legacy-пути.
V2_ANCHOR_TABLE = "events_ts"


class SqlMigrator:
    """Применяет SQL-миграции, поставляемые вместе с пакетом."""

    def __init__(
        self,
        database: Database,
        schema: str,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._db = database
        self._schema = schema
        self.logger = logger or logging.getLogger("uapg.migrations")

    def _files(self) -> List[str]:
        return list(EVENT_MIGRATIONS)

    async def apply_all(self) -> List[str]:
        """Применить недостающие миграции; вернуть список применённых."""
        await self._ensure_bookkeeping()
        applied: List[str] = []
        for filename in self._files():
            version = Path(filename).stem
            if await self._is_applied(version):
                continue
            self.logger.info("Применяется миграция %s", version)
            await self._db.execute(load_sql(filename, self._schema, package=PACKAGE))
            await self._mark_applied(version)
            applied.append(version)
        return applied

    async def _ensure_bookkeeping(self) -> None:
        await self._db.execute(
            f'''
            CREATE TABLE IF NOT EXISTS "{self._schema}".uapg_schema_migrations (
                version TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            '''
        )

    async def _is_applied(self, version: str) -> bool:
        found = await self._db.fetchval(
            f'SELECT 1 FROM "{self._schema}".uapg_schema_migrations WHERE version = $1',
            version,
        )
        return found is not None

    async def _mark_applied(self, version: str) -> None:
        await self._db.execute(
            f'''
            INSERT INTO "{self._schema}".uapg_schema_migrations (version)
            VALUES ($1) ON CONFLICT (version) DO NOTHING
            ''',
            version,
        )

    async def detect_v2_ready(self) -> bool:
        """Готова ли база к работе со слоем типизированных событий."""
        found = await self._db.fetchval(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = $2
            """,
            self._schema,
            V2_ANCHOR_TABLE,
        )
        return found is not None
