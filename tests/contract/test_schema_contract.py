"""Схема БД обязана совпадать с эталоном 0.2.15.

Существующие базы переезжают на новый код без миграции, поэтому расхождение в
DDL — это либо тихая потеря данных, либо отказ записи на проде. Тест поднимает
пустую базу, прогоняет инициализацию и сравнивает результат с замороженным
снимком; каждое допустимое отступление перечислено в ``divergences.py``.
"""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List

import asyncpg
import pytest

from tests.conftest import connect_kwargs
from tests.contract.divergences import SCHEMA as ALLOWED
from tests.contract.divergences import SCHEMA_DEAD_VARIABLES_V2
from tests.contract.schema_introspect import snapshot_schema

BASELINE_DIR = Path(__file__).parent / "baseline"
SCHEMA = "public"

pytestmark = pytest.mark.integration


_INTERVAL_UNITS = {
    "year": 365 * 86400,
    "years": 365 * 86400,
    "mon": 30 * 86400,
    "mons": 30 * 86400,
    "month": 30 * 86400,
    "months": 30 * 86400,
    "week": 7 * 86400,
    "weeks": 7 * 86400,
    "day": 86400,
    "days": 86400,
    "hour": 3600,
    "hours": 3600,
    "min": 60,
    "mins": 60,
    "minute": 60,
    "minutes": 60,
    "sec": 1,
    "secs": 1,
    "second": 1,
    "seconds": 1,
}


def _interval_seconds(value: Any) -> Any:
    """Длительность интервала PostgreSQL в секундах.

    Месяц и год считаются как 30 и 365 дней: политики хранения такими единицами
    не задаются, а сравнение должно быть полным, а не падать на незнакомом виде.
    """
    if not isinstance(value, str):
        return value
    total = 0.0
    tokens = value.replace("@", " ").split()
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if ":" in token:
            sign = -1 if token.startswith("-") else 1
            parts = token.lstrip("+-").split(":")
            scale = (3600, 60, 1)
            total += sign * sum(float(p) * s for p, s in zip(parts, scale[: len(parts)]))
            index += 1
            continue
        unit = tokens[index + 1].rstrip(",") if index + 1 < len(tokens) else ""
        if unit not in _INTERVAL_UNITS:
            return value
        total += float(token) * _INTERVAL_UNITS[unit]
        index += 2
    return total


def _diff_named(kind: str, baseline: Dict[str, Any], current: Dict[str, Any]) -> List[str]:
    diffs = []
    for key in sorted(set(baseline) - set(current)):
        diffs.append(f"{kind} removed: {key}")
    for key in sorted(set(current) - set(baseline)):
        diffs.append(f"{kind} added: {key}")
    return diffs


def _diff_lists(kind: str, baseline: Dict[str, List[str]], current: Dict[str, List[str]]) -> List[str]:
    diffs = _diff_named(kind, baseline, current)
    for key in sorted(set(baseline) & set(current)):
        for item in sorted(set(baseline[key]) - set(current[key])):
            diffs.append(f"{kind} {key} removed: {item}")
        for item in sorted(set(current[key]) - set(baseline[key])):
            diffs.append(f"{kind} {key} added: {item}")
    return diffs


def diff_schema(baseline: Dict[str, Any], current: Dict[str, Any]) -> List[str]:
    diffs: List[str] = []
    diffs += _diff_named("tables", baseline["tables"], current["tables"])
    for table in sorted(set(baseline["tables"]) & set(current["tables"])):
        if baseline["tables"][table] != current["tables"][table]:
            diffs.append(
                f"tables {table} columns changed:\n"
                f"  эталон: {json.dumps(baseline['tables'][table], ensure_ascii=False)}\n"
                f"  сейчас: {json.dumps(current['tables'][table], ensure_ascii=False)}"
            )
    diffs += _diff_lists("indexes", baseline["indexes"], current["indexes"])
    diffs += _diff_lists("constraints", baseline["constraints"], current["constraints"])
    diffs += _diff_named("routines", baseline["routines"], current["routines"])
    diffs += _diff_named("views", baseline["views"], current["views"])

    base_ts, cur_ts = baseline["timescale"], current["timescale"]
    for item in sorted(set(base_ts["hypertables"]) - set(cur_ts["hypertables"])):
        diffs.append(f"hypertable removed: {item}")
    for item in sorted(set(cur_ts["hypertables"]) - set(base_ts["hypertables"])):
        diffs.append(f"hypertable added: {item}")

    base_dims = {json.dumps(d, sort_keys=True, ensure_ascii=False) for d in base_ts["dimensions"]}
    cur_dims = {json.dumps(d, sort_keys=True, ensure_ascii=False) for d in cur_ts["dimensions"]}
    for item in sorted(base_dims - cur_dims):
        diffs.append(f"dimension removed: {item}")
    for item in sorted(cur_dims - base_dims):
        diffs.append(f"dimension added: {item}")

    # hypertable_id в config зависит от порядка создания и к контракту не относится.
    def _job_key(job: Dict[str, Any]) -> str:
        config = dict(job["config"] or {})
        config.pop("hypertable_id", None)
        # drop_after сравнивается по длительности, а не по написанию: «365 days»
        # и «8760:00:00» — одна и та же политика, а вот 30 дней вместо 365 —
        # уже расхождение, и его тест обязан увидеть.
        if "drop_after" in config:
            config["drop_after"] = _interval_seconds(config["drop_after"])
        return json.dumps(
            {"proc": job["proc"], "hypertable": job["hypertable"], "config": config},
            sort_keys=True,
            ensure_ascii=False,
        )

    base_jobs = {_job_key(j) for j in base_ts["jobs"]}
    cur_jobs = {_job_key(j) for j in cur_ts["jobs"]}
    for item in sorted(base_jobs - cur_jobs):
        diffs.append(f"job removed: {item}")
    for item in sorted(cur_jobs - base_jobs):
        diffs.append(f"job added: {item}")
    return diffs


def _is_allowed(diff: str) -> bool:
    if diff in ALLOWED:
        return True
    # Мёртвые объекты variables v2 (ADR-003) в новой схеме не создаются.
    return "removed" in diff and any(name in diff for name in SCHEMA_DEAD_VARIABLES_V2)


async def _snapshot(dsn: str) -> Dict[str, Any]:
    conn = await asyncpg.connect(dsn)
    try:
        return await snapshot_schema(conn, SCHEMA)
    finally:
        await conn.close()


def _assert_matches(baseline_name: str, current: Dict[str, Any]) -> None:
    baseline = json.loads((BASELINE_DIR / baseline_name).read_text())
    unexpected = [d for d in diff_schema(baseline, current) if not _is_allowed(d)]
    assert not unexpected, (
        f"Схема разошлась с эталоном {baseline_name}:\n" + "\n".join(unexpected)
    )


async def test_core_schema_matches_baseline(pg_database: str) -> None:
    from uapg.history_timescale import HistoryTimescale

    storage = HistoryTimescale(
        **connect_kwargs(pg_database),
        schema=SCHEMA,
        global_retention_period=timedelta(days=365),
    )
    await storage.init()
    await storage.stop()

    _assert_matches("schema_core.json", await _snapshot(pg_database))


async def test_v2_schema_matches_baseline(pg_database: str) -> None:
    from uapg.history_timescale_v2 import HistoryTimescaleV2
    from uapg.v2.storage_mode import StorageMode

    storage = HistoryTimescaleV2(
        **connect_kwargs(pg_database),
        schema=SCHEMA,
        global_retention_period=timedelta(days=365),
        events_storage_mode=StorageMode.DUAL,
    )
    await storage.init()
    await storage.stop()

    _assert_matches("schema_v2.json", await _snapshot(pg_database))


def _new_database(dsn: str, schema: str = SCHEMA):
    from uapg.core.config import ConnectionSettings, Keepalive, Timeouts
    from uapg.core.database import Database
    from uapg.core.metrics import DatabaseMetrics

    return Database(
        connection=ConnectionSettings.build(**connect_kwargs(dsn), schema=schema),
        timeouts=Timeouts.build(),
        keepalive=Keepalive.build(),
        metrics=DatabaseMetrics(),
    )


async def _bootstrap_new_schema(dsn: str, *, with_v2: bool = False) -> None:
    from uapg.storage.bootstrap import SchemaBootstrap
    from uapg.storage.migrations import SqlMigrator

    database = _new_database(dsn)
    await database.start()
    try:
        await SchemaBootstrap(database, SCHEMA).ensure_core_schema(
            global_retention=timedelta(days=365)
        )
        if with_v2:
            await SqlMigrator(database, SCHEMA).apply_all()
    finally:
        await database.stop()


async def test_new_bootstrap_matches_baseline(pg_database: str) -> None:
    """Главная проверка переноса схемы в SQL-файлы: она обязана совпасть с 0.2.15."""
    await _bootstrap_new_schema(pg_database)
    _assert_matches("schema_core.json", await _snapshot(pg_database))


async def test_new_migrations_match_baseline(pg_database: str) -> None:
    """Слой типизированных событий тоже обязан совпасть с 0.2.15."""
    await _bootstrap_new_schema(pg_database, with_v2=True)
    _assert_matches("schema_v2.json", await _snapshot(pg_database))


async def test_migrations_are_applied_once(pg_database: str) -> None:
    """Повторный запуск не должен переприменять функции и процедуры."""
    from uapg.storage.bootstrap import SchemaBootstrap
    from uapg.storage.migrations import SqlMigrator

    database = _new_database(pg_database)
    await database.start()
    try:
        await SchemaBootstrap(database, SCHEMA).ensure_core_schema()
        migrator = SqlMigrator(database, SCHEMA)

        first = await migrator.apply_all()
        assert first, "первый запуск обязан применить миграции"
        assert await migrator.apply_all() == []
        assert await migrator.detect_v2_ready() is True
    finally:
        await database.stop()


async def test_v2_not_ready_on_bare_core_schema(pg_database: str) -> None:
    """Признак готовности должен честно говорить «нет» до миграций."""
    from uapg.storage.bootstrap import SchemaBootstrap
    from uapg.storage.migrations import SqlMigrator

    database = _new_database(pg_database)
    await database.start()
    try:
        await SchemaBootstrap(database, SCHEMA).ensure_core_schema()
        assert await SqlMigrator(database, SCHEMA).detect_v2_ready() is False
    finally:
        await database.stop()


async def test_new_bootstrap_is_idempotent(pg_database: str) -> None:
    """Сервер перезапускают часто: повторный запуск не должен менять схему."""
    await _bootstrap_new_schema(pg_database)
    first = await _snapshot(pg_database)
    await _bootstrap_new_schema(pg_database)
    assert await _snapshot(pg_database) == first


async def test_new_bootstrap_works_in_custom_schema(pg_database: str) -> None:
    """Схема настраивается параметром, и всё должно оказаться именно в ней."""
    import asyncpg

    from uapg.core.config import ConnectionSettings, Keepalive, Timeouts
    from uapg.core.database import Database
    from uapg.core.metrics import DatabaseMetrics
    from uapg.storage.bootstrap import SchemaBootstrap

    database = Database(
        connection=ConnectionSettings.build(**connect_kwargs(pg_database), schema="opcua_hist"),
        timeouts=Timeouts.build(),
        keepalive=Keepalive.build(),
        metrics=DatabaseMetrics(),
    )
    await database.start()
    try:
        await SchemaBootstrap(database, "opcua_hist").ensure_core_schema()
    finally:
        await database.stop()

    conn = await asyncpg.connect(pg_database)
    try:
        snapshot = await snapshot_schema(conn, "opcua_hist")
    finally:
        await conn.close()

    assert "variables_history" in snapshot["tables"]
    assert snapshot["timescale"]["hypertables"] == ["events_history", "variables_history"]


async def test_schema_bootstrap_is_idempotent(pg_database: str) -> None:
    """Повторная инициализация не должна менять схему: сервер перезапускают часто."""
    from uapg.history_timescale_v2 import HistoryTimescaleV2
    from uapg.v2.storage_mode import StorageMode

    for _ in range(2):
        storage = HistoryTimescaleV2(
            **connect_kwargs(pg_database),
            schema=SCHEMA,
            global_retention_period=timedelta(days=365),
            events_storage_mode=StorageMode.DUAL,
        )
        await storage.init()
        await storage.stop()

    _assert_matches("schema_v2.json", await _snapshot(pg_database))
