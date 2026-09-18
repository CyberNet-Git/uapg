"""Снятие эталонного контракта с реализации uapg 0.2.15.

Работает только на дереве 0.2.15 (main, коммит 18df1cf): в v3 модулей, которые
он импортирует, больше нет. Оставлен как запись того, как получен эталон.

Запускается ОДИН раз против старого кода; результат коммитится в
``tests/contract/baseline/`` и дальше служит приёмкой для переписанной реализации.
После переписывания скрипт запускать нельзя — он снимет контракт с нового кода
и тем самым обессмыслит проверку (подробнее в ``baseline/README.md``).

    python -m tests.contract.capture_baseline all --dsn postgresql://...

Требуется живая TimescaleDB; база под каждый снимок создаётся заново.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock
from urllib.parse import urlparse

import asyncpg

# db_manager тянет psycopg, которого может не быть в окружении; для снятия контракта
# он не нужен (и уходит из пакета в v3). Заглушка ставится до импорта uapg.
sys.modules.setdefault("psycopg", MagicMock())

from tests.contract.api_introspect import api_snapshot  # noqa: E402
from tests.contract.schema_introspect import snapshot_schema  # noqa: E402
from tests.contract.wire_introspect import wire_snapshot  # noqa: E402

BASELINE_DIR = Path(__file__).parent / "baseline"
DEFAULT_DSN = "postgresql://uapg_test:uapg_test@127.0.0.1:55432/uapg_test"
SCHEMA = "public"


def _write(name: str, snapshot: Dict[str, Any]) -> None:
    BASELINE_DIR.mkdir(parents=True, exist_ok=True)
    path = BASELINE_DIR / name
    path.write_text(json.dumps(snapshot, indent=2, sort_keys=True, ensure_ascii=False) + "\n")
    print(f"written {path}")


def _conn_kwargs(dsn: str) -> Dict[str, Any]:
    parsed = urlparse(dsn)
    return {
        "host": parsed.hostname or "127.0.0.1",
        "port": parsed.port or 5432,
        "user": parsed.username or "postgres",
        "password": parsed.password or "",
        "database": (parsed.path or "/postgres").lstrip("/"),
    }


async def _recreate_database(admin_dsn: str, name: str) -> str:
    admin = await asyncpg.connect(admin_dsn)
    try:
        await admin.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1",
            name,
        )
        await admin.execute(f'DROP DATABASE IF EXISTS "{name}"')
        await admin.execute(f'CREATE DATABASE "{name}"')
    finally:
        await admin.close()

    dsn = admin_dsn.rsplit("/", 1)[0] + "/" + name
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
    finally:
        await conn.close()
    return dsn


async def _dump(dsn: str, filename: str) -> None:
    conn = await asyncpg.connect(dsn)
    try:
        snapshot = await snapshot_schema(conn, SCHEMA)
    finally:
        await conn.close()
    _write(filename, snapshot)


async def capture_schema(admin_dsn: str) -> None:
    from uapg.history_timescale import HistoryTimescale
    from uapg.history_timescale_v2 import HistoryTimescaleV2
    from uapg.v2.storage_mode import StorageMode

    # Ядро: обычный HistoryTimescale, события V2 не задействованы.
    core_dsn = await _recreate_database(admin_dsn, "uapg_contract_core")
    storage = HistoryTimescale(
        **_conn_kwargs(core_dsn),
        schema=SCHEMA,
        global_retention_period=timedelta(days=365),
    )
    await storage.init()
    await storage.stop()
    await _dump(core_dsn, "schema_core.json")

    # Слой V2: миграции применяются при mode != legacy.
    v2_dsn = await _recreate_database(admin_dsn, "uapg_contract_v2")
    storage_v2 = HistoryTimescaleV2(
        **_conn_kwargs(v2_dsn),
        schema=SCHEMA,
        global_retention_period=timedelta(days=365),
        events_storage_mode=StorageMode.DUAL,
    )
    await storage_v2.init()
    await storage_v2.stop()
    await _dump(v2_dsn, "schema_v2.json")


def capture_api() -> None:
    _write("api.json", api_snapshot())


def capture_wire() -> None:
    from uapg.history_timescale import HistoryTimescale

    storage = HistoryTimescale()
    _write(
        "wire.json",
        wire_snapshot(storage._event_to_binary_map, storage._binary_map_to_event_values),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("what", choices=["all", "schema", "api", "wire"])
    parser.add_argument("--dsn", default=DEFAULT_DSN)
    args = parser.parse_args()

    if args.what in ("all", "api"):
        capture_api()
    if args.what in ("all", "wire"):
        capture_wire()
    if args.what in ("all", "schema"):
        asyncio.run(capture_schema(args.dsn))


if __name__ == "__main__":
    main()
