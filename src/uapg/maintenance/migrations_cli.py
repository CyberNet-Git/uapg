"""CLI ``uapg migrations status|apply``: ручное применение SQL-миграций uapg.

Нужен, когда роль приложения не владеет объектами схемы истории и миграция
отложена на старте (``DEFERRABLE_MIGRATIONS``): администратор применяет её
под ролью-владельцем, не перезапуская сервер.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any, Mapping, Optional, Sequence

import asyncpg

from ..v2.sql_migrator import DEFERRABLE_MIGRATIONS, MigrationPrivilegeError, SqlMigrator
from .indexes_cli import EXIT_FAILED, EXIT_OK, EXIT_PENDING, connect_from_args, connection_parent


def build_parser(prog: str = "uapg migrations", defaults: Optional[Mapping[str, Any]] = None) -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False, parents=[connection_parent()])
    common.add_argument(
        "--no-variables",
        action="store_true",
        help="без миграций variables V2 (10x), если они не используются",
    )
    parser = argparse.ArgumentParser(
        prog=prog,
        description="Статус и применение SQL-миграций uapg (events/variables V2) на работающей БД.",
    )
    sub = parser.add_subparsers(dest="action", required=True)
    status_p = sub.add_parser("status", parents=[common], help="показать неприменённые миграции (только чтение)")
    status_p.add_argument("--check", action="store_true", help=f"код выхода {EXIT_PENDING}, если есть неприменённые")
    apply_p = sub.add_parser("apply", parents=[common], help="применить неприменённые миграции")
    apply_p.add_argument("--dry-run", action="store_true", help="только показать, что будет применено")
    if defaults:
        status_p.set_defaults(**defaults)
        apply_p.set_defaults(**defaults)
    return parser


async def run(
    argv: Optional[Sequence[str]] = None,
    *,
    defaults: Optional[Mapping[str, Any]] = None,
    prog: str = "uapg migrations",
) -> int:
    args = build_parser(prog, defaults).parse_args(list(sys.argv[1:] if argv is None else argv))
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    conn = await connect_from_args(args)
    try:
        migrator = SqlMigrator(
            args.schema,
            conn.execute,
            conn.fetch,
            conn.fetchval,
            include_variables=not args.no_variables,
        )
        pending = await migrator.pending()
        if args.action == "status" or args.dry_run:
            current_user = await conn.fetchval("SELECT current_user")
            if args.json:
                print(json.dumps({"schema": args.schema, "user": current_user, "pending": pending}, indent=2))
            elif not pending:
                print(f'Схема "{args.schema}": все миграции uapg применены.')
            else:
                print(f'Схема "{args.schema}" (роль {current_user}): неприменённые миграции:')
                for version in pending:
                    mark = " (оптимизация, может быть отложена на старте)" if version in DEFERRABLE_MIGRATIONS else ""
                    print(f"  {version}{mark}")
            if args.action == "status":
                return EXIT_PENDING if args.check and pending else EXIT_OK
            return EXIT_OK

        if args.lock_timeout > 0:
            await conn.execute(f"SET lock_timeout = '{int(args.lock_timeout * 1000)}ms'")
        await conn.execute("SET statement_timeout = 0")
        try:
            applied = await migrator.apply_all(defer_on_privilege_error=False)
        except (MigrationPrivilegeError, asyncpg.PostgresError) as e:
            print(f"Ошибка: {e}", file=sys.stderr)
            return EXIT_FAILED
        if args.json:
            print(json.dumps({"schema": args.schema, "applied": applied}, indent=2))
        elif applied:
            for version in applied:
                print(f"applied  {version}")
        else:
            print("Неприменённых миграций нет.")
        return EXIT_OK
    finally:
        await conn.close()
