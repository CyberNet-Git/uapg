"""CLI ``uapg indexes plan|apply``: онлайн-обслуживание индексов истории.

Обёртки деплоя передают ``defaults`` (подключение, схема, поля событий из своей
конфигурации), а пользователь переопределяет их аргументами командной строки.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any, Dict, List, Mapping, Optional, Sequence

from ..v2.events_config import EventsV2Config
from .online_indexes import (
    ALL_SCOPES,
    apply_plan,
    build_plan,
    connect_maintenance,
    format_plan,
)

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_PENDING = 2


def _parse_scopes(raw: str) -> List[str]:
    scopes = [part.strip() for part in str(raw or "all").split(",") if part.strip()]
    for scope in scopes:
        if scope != "all" and scope not in ALL_SCOPES:
            raise argparse.ArgumentTypeError(
                f"unknown scope {scope!r}; expected one of: all, {', '.join(ALL_SCOPES)}"
            )
    return scopes or ["all"]


def connection_parent() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    conn = common.add_argument_group("подключение")
    conn.add_argument("--dsn", help="postgresql://user@host:port/db (пароль лучше через PGPASSWORD)")
    conn.add_argument("--host")
    conn.add_argument("--port", type=int)
    conn.add_argument("--user")
    conn.add_argument("--password", help="по умолчанию берётся из PGPASSWORD")
    conn.add_argument("--database")
    conn.add_argument("--sslmode", choices=["disable", "allow", "prefer", "require", "verify-ca", "verify-full"])
    common.add_argument("--schema", default="public", help="схема истории (по умолчанию public)")
    common.add_argument("--lock-timeout", type=float, default=10.0, help="lock_timeout, с (0 — без ограничения)")
    common.add_argument("--json", action="store_true", help="вывод в JSON")
    common.add_argument("-v", "--verbose", action="store_true")
    return common


async def connect_from_args(args: argparse.Namespace):
    return await connect_maintenance(
        dsn=args.dsn,
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
        sslmode=args.sslmode,
    )


def build_parser(prog: str = "uapg indexes", defaults: Optional[Mapping[str, Any]] = None) -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False, parents=[connection_parent()])
    common.add_argument("--indexed-fields", default="", help="CSV полей событий для GIN trgm (indexed_fields)")
    common.add_argument("--field-aliases", default="", help="CSV пар filter_name:column_name")
    common.add_argument(
        "--scope",
        type=_parse_scopes,
        default=["all"],
        help="all или CSV из core,v2,trgm (по умолчанию all)",
    )
    common.add_argument("--fix-invalid", action="store_true", help="пересоздавать индексы INVALID от прерванной сборки")

    parser = argparse.ArgumentParser(
        prog=prog,
        description=(
            "План и онлайн-сборка недостающих индексов истории uapg без блокировки записи: "
            "CREATE INDEX CONCURRENTLY для обычных таблиц, "
            "WITH (timescaledb.transaction_per_chunk) для hypertable."
        ),
    )
    sub = parser.add_subparsers(dest="action", required=True)
    plan_p = sub.add_parser("plan", parents=[common], help="показать недостающие индексы (только чтение)")
    plan_p.add_argument("--sql", action="store_true", help="вывести SQL для ручного запуска в psql")
    plan_p.add_argument("--check", action="store_true", help=f"код выхода {EXIT_PENDING}, если есть недостающие")
    apply_p = sub.add_parser("apply", parents=[common], help="построить недостающие индексы онлайн")
    apply_p.add_argument("--dry-run", action="store_true", help="только показать, что будет выполнено")
    apply_p.add_argument(
        "--no-create-extension",
        action="store_true",
        help="не пытаться выполнить CREATE EXTENSION pg_trgm",
    )
    if defaults:
        plan_p.set_defaults(**defaults)
        apply_p.set_defaults(**defaults)
    return parser


def _events_config(args: argparse.Namespace) -> EventsV2Config:
    return EventsV2Config.from_csv(indexed=args.indexed_fields, aliases=args.field_aliases)


async def run(
    argv: Optional[Sequence[str]] = None,
    *,
    defaults: Optional[Mapping[str, Any]] = None,
    prog: str = "uapg indexes",
) -> int:
    args = build_parser(prog, defaults).parse_args(list(sys.argv[1:] if argv is None else argv))
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    conn = await connect_from_args(args)
    try:
        plan = await build_plan(conn, args.schema, scopes=args.scope, events_config=_events_config(args))
        if args.action == "plan":
            if args.sql:
                print(plan.sql_script(fix_invalid=args.fix_invalid, lock_timeout_sec=args.lock_timeout), end="")
            elif args.json:
                print(json.dumps(plan.to_dict(), ensure_ascii=False, indent=2))
            else:
                print(format_plan(plan))
            return EXIT_PENDING if args.check and plan.pending else EXIT_OK

        results = await apply_plan(
            conn,
            plan,
            lock_timeout_sec=args.lock_timeout,
            fix_invalid=args.fix_invalid,
            dry_run=args.dry_run,
            create_extension=not args.no_create_extension,
        )
        if args.json:
            payload: Dict[str, Any] = {
                "results": [r.__dict__ for r in results],
                "plan": plan.to_dict(),
            }
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            if not results:
                print("Недостающих индексов нет.")
            for r in results:
                suffix = f" error={r.error}" if r.error else ""
                print(f"{r.action:<8} {r.name} {r.duration_sec:.1f}s{suffix}")
                if r.action == "dry_run":
                    print(f"  {r.sql}")
        return EXIT_FAILED if any(not r.ok for r in results) else EXIT_OK
    finally:
        await conn.close()
