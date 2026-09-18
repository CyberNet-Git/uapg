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
