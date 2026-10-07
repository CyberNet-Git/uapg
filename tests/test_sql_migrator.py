"""SqlMigrator: отложенные оптимизационные миграции и понятная ошибка прав."""

from typing import List, Optional, Set

import asyncpg
import pytest

from uapg.v2.sql_migrator import (
    DEFERRABLE_MIGRATIONS,
    MIGRATION_ORDER,
    MigrationPrivilegeError,
    SqlMigrator,
)


class FakeDb:
    def __init__(self, applied: Set[str], fail_on: Optional[str] = None) -> None:
        self.applied = set(applied)
        self.fail_on = fail_on
        self.executed: List[str] = []

    async def execute(self, sql: str, *args):
        if sql.lstrip().startswith("INSERT INTO") and "uapg_schema_migrations" in sql:
            self.applied.add(args[0])
            return
        if self.fail_on and f"-- marker:{self.fail_on}" in sql:
            raise asyncpg.exceptions.InsufficientPrivilegeError("must be owner of function f")
        self.executed.append(sql)

    async def fetch(self, sql: str, *args):
        return []

    async def fetchval(self, sql: str, *args):
        return 1 if args and args[0] in self.applied else None


@pytest.fixture(autouse=True)
def _marked_sql(monkeypatch):
    monkeypatch.setattr(
        "uapg.v2.sql_migrator.load_migration_sql",
        lambda filename, schema: f"-- marker:{filename[:-4]}\nSELECT 1",
    )


def _migrator(db: FakeDb) -> SqlMigrator:
    return SqlMigrator("history", db.execute, db.fetch, db.fetchval)


ALL_BEFORE_006 = {f[:-4] for f in MIGRATION_ORDER if f < "006"}


def test_006_is_the_only_deferrable_migration() -> None:
    assert DEFERRABLE_MIGRATIONS == frozenset({"006_events_v2_read"})


@pytest.mark.asyncio
async def test_deferrable_migration_privilege_error_does_not_stop_startup() -> None:
    db = FakeDb(ALL_BEFORE_006, fail_on="006_events_v2_read")
    migrator = _migrator(db)
    applied = await migrator.apply_all()
    assert "006_events_v2_read" not in db.applied
    assert "006_events_v2_read" in migrator.deferred
    assert applied == ["101_variables_v2_tables", "102_variables_v2_functions"]
    assert await migrator.pending() == ["006_events_v2_read"]


@pytest.mark.asyncio
async def test_manual_apply_does_not_defer() -> None:
    db = FakeDb(ALL_BEFORE_006, fail_on="006_events_v2_read")
    with pytest.raises(MigrationPrivilegeError, match="uapg migrations apply"):
        await _migrator(db).apply_all(defer_on_privilege_error=False)


@pytest.mark.asyncio
async def test_required_migration_privilege_error_is_explained() -> None:
    db = FakeDb(set(), fail_on="003_events_v2_functions")
    with pytest.raises(MigrationPrivilegeError) as exc:
        await _migrator(db).apply_all()
    assert "003_events_v2_functions" in str(exc.value)
    assert "OWNER TO" in str(exc.value)
    assert "003_events_v2_functions" not in db.applied


@pytest.mark.asyncio
async def test_deferred_migration_applies_on_next_run() -> None:
    db = FakeDb(ALL_BEFORE_006, fail_on="006_events_v2_read")
    migrator = _migrator(db)
    await migrator.apply_all()
    db.fail_on = None
    assert await migrator.apply_all() == ["006_events_v2_read"]
    assert migrator.deferred == {}
    assert await migrator.pending() == []
