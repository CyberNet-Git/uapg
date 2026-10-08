"""План и сборка недостающих индексов истории без блокировки записи.

На старте uapg строит индексы обычным ``CREATE INDEX``: он держит ShareLock и на
время сборки останавливает запись в таблицу. На большой БД это делается заранее
этим модулем (или CLI ``uapg indexes``) на работающей системе:

* обычные таблицы — ``CREATE INDEX CONCURRENTLY``;
* hypertable TimescaleDB, где ``CONCURRENTLY`` не поддерживается, —
  ``WITH (timescaledb.transaction_per_chunk)``: чанки индексируются по одному,
  блокировка держится только на время одного чанка.

Прерванная онлайн-сборка оставляет индекс с ``indisvalid = false``; такой индекс
планировщик не использует, и план показывает его как ``invalid``.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from ..v2.events_config import EventsV2Config, expand_sql_filter_fields
from ..v2.schema_registry import TRGM_CANDIDATES_SQL, TRGM_COLUMN_TYPES, trgm_index_name

_logger = logging.getLogger(__name__)

SCOPE_CORE = "core"
SCOPE_V2 = "v2"
SCOPE_TRGM = "trgm"
ALL_SCOPES: Tuple[str, ...] = (SCOPE_CORE, SCOPE_V2, SCOPE_TRGM)

# Отдельное имя сессии: сервер при старте снимает «хвосты» по своему
# application_name, и сборка индекса с тем же именем была бы прервана.
MAINTENANCE_APPLICATION_NAME = "uapg-maintenance"

STATUS_PRESENT = "present"
STATUS_MISSING = "missing"
STATUS_INVALID = "invalid"
STATUS_NO_TABLE = "no_table"
# Индекс есть в БД, но uapg его больше не ожидает: удалять только по явному флагу.
STATUS_OBSOLETE = "obsolete"


@dataclass(frozen=True)
class IndexSpec:
    """Индекс, который uapg ожидает в схеме истории."""

    name: str
    table: str
    # Часть DDL после имени таблицы: "(col)", "USING GIN (col)", "(a, b DESC) INCLUDE (c)".
    definition: str
    unique: bool = False
    scope: str = SCOPE_CORE

    def create_sql(self, schema: str, *, online: bool = False, hypertable: bool = False) -> str:
        unique = "UNIQUE " if self.unique else ""
        concurrently = "CONCURRENTLY " if online and not hypertable else ""
        sql = (
            f'CREATE {unique}INDEX {concurrently}IF NOT EXISTS "{self.name}"'
            f' ON "{schema}"."{self.table}" {self.definition}'
        )
        if online and hypertable:
            sql += " WITH (timescaledb.transaction_per_chunk)"
        return sql

    def drop_sql(self, schema: str, *, online: bool = False, hypertable: bool = False) -> str:
        concurrently = "CONCURRENTLY " if online and not hypertable else ""
        return f'DROP INDEX {concurrently}IF EXISTS "{schema}"."{self.name}"'


def _core(name: str, table: str, definition: str, *, unique: bool = False) -> IndexSpec:
    return IndexSpec(name, table, definition, unique=unique, scope=SCOPE_CORE)


# Индексы единых таблиц HistoryTimescale; старт и инструмент берут их отсюда.
CORE_INDEX_SPECS: Tuple[IndexSpec, ...] = (
    _core("idx_variables_timestamp", "variables_history", "(sourcetimestamp)"),
    _core("idx_variables_varid_sourcets", "variables_history", "(variable_id, sourcetimestamp)", unique=True),
    _core("idx_events_source_id", "events_history", "(source_id)"),
    _core("idx_events_event_type_id", "events_history", "(event_type_id)"),
    _core("idx_events_timestamp", "events_history", "(event_timestamp)"),
    _core("idx_events_data_gin", "events_history", "USING GIN (event_data)"),
    _core("idx_events_sourceid_eventts", "events_history", "(source_id, event_timestamp)", unique=True),
    _core("idx_variable_metadata_variable_id", "variable_metadata", "(variable_id)"),
    _core("idx_event_sources_source_id", "event_sources", "(source_id)"),
    _core("idx_event_types_event_type_id", "event_types", "(event_type_id)"),
    _core("idx_event_sources_node_id", "event_sources", "(source_node_id)", unique=True),
    _core("idx_event_types_name", "event_types", "(event_type_name)", unique=True),
    _core("idx_variable_metadata_node_id", "variable_metadata", "(node_id)", unique=True),
    _core("idx_events_history_type_source", "events_history", "(event_type_id, source_id)"),
    _core("idx_variable_metadata_created", "variable_metadata", "(created_at)"),
    _core("idx_event_sources_created", "event_sources", "(created_at)"),
    _core("idx_event_types_created", "event_types", "(created_at)"),
    _core("idx_variables_last_value_updated", "variables_last_value", "(updated_at)"),
)

# Индексы, которые uapg создавал раньше и больше не ожидает. Каталог нужен потому, что
# удаление записи из CORE_INDEX_SPECS только перестаёт её создавать: build_plan фильтрует
# pg_index по именам из каталога, поэтому индекс, которого в каталоге нет, не попадает в
# выборку вовсе и остаётся в базе навсегда. Определение здесь — документация: drop_sql
# работает по имени.
OBSOLETE_INDEX_SPECS: Tuple[IndexSpec, ...] = (
    # Строгий префикс UNIQUE-индекса (variable_id, sourcetimestamp).
    _core("idx_variables_variable_id", "variables_history", "(variable_id)"),
    # Копия того же UNIQUE-индекса минус уникальность.
    _core(
        "idx_variables_history_variable_id_timestamp",
        "variables_history",
        "(variable_id, sourcetimestamp)",
    ),
    # Ни один запрос не фильтрует и не сортирует по servertimestamp.
    _core("idx_variables_server_timestamp", "variables_history", "(servertimestamp)"),
    # Покрывающий индекс обслуживал только фолбэк read_last_value, который теперь берёт
    # variantbinary тем же запросом; все массовые LATERAL-запросы тянут variantbinary и
    # index-only быть не могут в принципе. На стенде 130 МБ при idx_scan = 0.
    _core(
        "idx_variables_history_vid_ts_desc_covering",
        "variables_history",
        "(variable_id, sourcetimestamp DESC) INCLUDE (statuscode, varianttype, servertimestamp)",
    ),
    # Копия UNIQUE-индекса (source_id, event_timestamp) минус уникальность.
    _core("idx_events_history_source_timestamp", "events_history", "(source_id, event_timestamp)"),
    # Байт-в-байт то же, что idx_events_history_type_source: два имени, одно определение.
    _core("idx_events_history_event_type_source", "events_history", "(event_type_id, source_id)"),
)

# Индексы, которые добавляет HistoryTimescaleV2 поверх единых таблиц.
V2_INDEX_SPECS: Tuple[IndexSpec, ...] = (
    IndexSpec("idx_events_history_id", "events_history", "(id)", scope=SCOPE_V2),
)


def find_index_spec(name: str) -> Optional[IndexSpec]:
    for spec in (*CORE_INDEX_SPECS, *V2_INDEX_SPECS):
        if spec.name == name:
            return spec
    return None


def trgm_index_spec(table: str, column: str) -> IndexSpec:
    return IndexSpec(
        trgm_index_name(table, column),
        table,
        f'USING gin ("{column}" gin_trgm_ops)',
        scope=SCOPE_TRGM,
    )


@dataclass
class IndexPlanItem:
    spec: IndexSpec
    status: str
    hypertable: bool = False
    # Сколько раз планировщик воспользовался индексом (pg_stat_all_indexes.idx_scan).
    # None — статистики нет (индекса нет в БД). Для лишних индексов это главный довод
    # за или против удаления, и именно его мне не хватало, когда я принял покрывающий
    # индекс за мёртвый по idx_scan = 0 на синтетическом стенде.
    idx_scan: Optional[int] = None

    @property
    def pending(self) -> bool:
        """Надо построить. Лишние индексы сюда не входят: у них своя ветка."""
        return self.status in (STATUS_MISSING, STATUS_INVALID)

    @property
    def obsolete(self) -> bool:
        return self.status == STATUS_OBSOLETE

    def online_sql(self, schema: str) -> str:
        return self.spec.create_sql(schema, online=True, hypertable=self.hypertable)

    def drop_online_sql(self, schema: str) -> str:
        return self.spec.drop_sql(schema, online=True, hypertable=self.hypertable)

    def to_dict(self, schema: str) -> Dict[str, Any]:
        return {
            "name": self.spec.name,
            "table": self.spec.table,
            "scope": self.spec.scope,
            "status": self.status,
            "hypertable": self.hypertable,
            "idx_scan": self.idx_scan,
            "sql": self.drop_online_sql(schema) if self.obsolete else self.online_sql(schema),
        }


@dataclass
class IndexPlan:
    schema: str
    items: List[IndexPlanItem] = field(default_factory=list)
    timescaledb_available: bool = False
    trgm_extension_available: bool = False

    @property
    def pending(self) -> List[IndexPlanItem]:
        return [item for item in self.items if item.pending]

    @property
    def obsolete(self) -> List[IndexPlanItem]:
        """Индексы, которые есть в БД и больше не нужны. Удаляются только по флагу."""
        return [item for item in self.items if item.obsolete]

    def counts(self) -> Dict[str, int]:
        result: Dict[str, int] = {}
        for item in self.items:
            result[item.status] = result.get(item.status, 0) + 1
        return result

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema": self.schema,
            "timescaledb_available": self.timescaledb_available,
            "trgm_extension_available": self.trgm_extension_available,
            "counts": self.counts(),
            "items": [item.to_dict(self.schema) for item in self.items],
        }

    def sql_script(
        self,
        *,
        fix_invalid: bool = False,
        drop_obsolete: bool = False,
        lock_timeout_sec: float = 10.0,
    ) -> str:
        """SQL для ручного запуска в psql (autocommit, без --single-transaction)."""
        lines = [
            f"-- uapg: онлайн-сборка индексов схемы {self.schema}",
            "-- Выполнять в psql без -1/--single-transaction: CONCURRENTLY и",
            "-- transaction_per_chunk недопустимы внутри транзакции.",
            "SET statement_timeout = 0;",
            f"SET lock_timeout = '{_lock_timeout_ms(lock_timeout_sec)}ms';",
        ]
        obsolete = self.obsolete
        if obsolete:
            lines.append("")
            lines.append("-- Лишние индексы: uapg их больше не ожидает.")
            for item in obsolete:
                scans = "неизвестно" if item.idx_scan is None else str(item.idx_scan)
                lines.append(f"-- {item.spec.name} (использований idx_scan: {scans})")
                drop = item.drop_online_sql(self.schema)
                lines.append(
                    f"{drop};" if drop_obsolete else f"-- {drop};  -- нужен --drop-obsolete"
                )
        pending = self.pending
        if not pending:
            lines.append("")
            lines.append("-- Недостающих индексов нет.")
            return "\n".join(lines) + "\n"
        if any(i.spec.scope == SCOPE_TRGM for i in pending) and not self.trgm_extension_available:
            lines.append("-- Требует прав суперпользователя:")
            lines.append("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        for item in pending:
            kind = "hypertable" if item.hypertable else "table"
            lines.append(f"-- {item.spec.name} ({item.status}, {kind} {item.spec.table})")
            if item.status == STATUS_INVALID:
                drop = item.spec.drop_sql(self.schema, online=True, hypertable=item.hypertable)
                lines.append(f"{drop};" if fix_invalid else f"-- {drop};  -- нужен --fix-invalid")
                if not fix_invalid:
                    continue
            lines.append(f"{item.online_sql(self.schema)};")
        return "\n".join(lines) + "\n"


@dataclass
class IndexApplyResult:
    name: str
    action: str  # created | rebuilt | dropped | skipped | failed | dry_run
    sql: str
    duration_sec: float = 0.0
    error: str = ""

    @property
    def ok(self) -> bool:
        return self.action != "failed"


def _lock_timeout_ms(lock_timeout_sec: float) -> int:
    return max(0, int(float(lock_timeout_sec) * 1000))


def _normalize_scopes(scopes: Optional[Iterable[str]]) -> Tuple[str, ...]:
    if not scopes:
        return ALL_SCOPES
    result = []
    for scope in scopes:
        if scope == "all":
            return ALL_SCOPES
        if scope not in ALL_SCOPES:
            raise ValueError(f"Unknown index scope: {scope!r}")
        if scope not in result:
            result.append(scope)
    return tuple(result)


async def _extension_available(conn: Any, name: str) -> bool:
    return await conn.fetchval("SELECT 1 FROM pg_extension WHERE extname = $1", name) is not None


async def _relation_exists(conn: Any, schema: str, name: str) -> bool:
    return await conn.fetchval("SELECT to_regclass(format('%I.%I', $1::text, $2::text))", schema, name) is not None


async def _trgm_specs(conn: Any, schema: str, events_config: Optional[EventsV2Config]) -> List[IndexSpec]:
    if events_config is None:
        return []
    columns = expand_sql_filter_fields(set(events_config.indexed_fields), events_config.field_aliases)
    if not columns:
        return []
    if not await _relation_exists(conn, schema, "event_type_storage"):
        return []
    rows = await conn.fetch(
        TRGM_CANDIDATES_SQL.format(schema=schema),
        schema,
        sorted(columns),
        list(TRGM_COLUMN_TYPES),
    )
    return [trgm_index_spec(str(r["table_name"]), str(r["column_name"])) for r in rows]


async def build_plan(
    conn: Any,
    schema: str,
    *,
    scopes: Optional[Iterable[str]] = None,
    events_config: Optional[EventsV2Config] = None,
) -> IndexPlan:
    """Сравнить ожидаемые индексы с каталогом БД. Только чтение."""
    selected = _normalize_scopes(scopes)
    plan = IndexPlan(schema=schema)
    plan.timescaledb_available = await _extension_available(conn, "timescaledb")
    plan.trgm_extension_available = await _extension_available(conn, "pg_trgm")

    specs: List[IndexSpec] = []
    if SCOPE_CORE in selected:
        specs.extend(CORE_INDEX_SPECS)
    # V2-индексы и trgm имеют смысл только там, где развёрнута схема V2.
    v2_present = await _relation_exists(conn, schema, "events_ts")
    if SCOPE_V2 in selected and v2_present:
        specs.extend(V2_INDEX_SPECS)
    if SCOPE_TRGM in selected and v2_present:
        specs.extend(await _trgm_specs(conn, schema, events_config))
    if not specs:
        return plan

    tables = sorted({s.table for s in specs})
    table_rows = await conn.fetch(
        """
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relname = ANY($2::text[]) AND c.relkind IN ('r', 'p')
        """,
        schema,
        tables,
    )
    existing_tables = {str(r["relname"]) for r in table_rows}

    hypertables: set = set()
    if plan.timescaledb_available:
        ht_rows = await conn.fetch(
            "SELECT hypertable_name FROM timescaledb_information.hypertables WHERE hypertable_schema = $1",
            schema,
        )
        hypertables = {str(r["hypertable_name"]) for r in ht_rows}

    # Лишние индексы проверяются только в core-проходе: они относятся к единым таблицам.
    obsolete_specs: List[IndexSpec] = list(OBSOLETE_INDEX_SPECS) if SCOPE_CORE in selected else []

    index_rows = await conn.fetch(
        """
        SELECT c.relname, i.indisvalid, s.idx_scan
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_stat_all_indexes s ON s.indexrelid = i.indexrelid
        WHERE n.nspname = $1 AND c.relname = ANY($2::text[])
        """,
        schema,
        sorted({sp.name for sp in (*specs, *obsolete_specs)}),
    )
    index_valid = {str(r["relname"]): bool(r["indisvalid"]) for r in index_rows}
    index_scans = {
        str(r["relname"]): (None if r["idx_scan"] is None else int(r["idx_scan"]))
        for r in index_rows
    }

    for spec in obsolete_specs:
        if spec.name not in index_valid:
            continue  # уже удалён или никогда не создавался — докладывать не о чем
        plan.items.append(
            IndexPlanItem(
                spec=spec,
                status=STATUS_OBSOLETE,
                hypertable=spec.table in hypertables,
                idx_scan=index_scans.get(spec.name),
            )
        )

    for spec in specs:
        if spec.table not in existing_tables:
            status = STATUS_NO_TABLE
        elif spec.name not in index_valid:
            status = STATUS_MISSING
        elif not index_valid[spec.name]:
            status = STATUS_INVALID
        else:
            status = STATUS_PRESENT
        plan.items.append(
            IndexPlanItem(
                spec=spec,
                status=status,
                hypertable=spec.table in hypertables,
                idx_scan=index_scans.get(spec.name),
            )
        )
    return plan


async def apply_plan(
    conn: Any,
    plan: IndexPlan,
    *,
    lock_timeout_sec: float = 10.0,
    fix_invalid: bool = False,
    drop_obsolete: bool = False,
    dry_run: bool = False,
    create_extension: bool = True,
    logger: Optional[logging.Logger] = None,
) -> List[IndexApplyResult]:
    """Построить недостающие индексы плана по одному, онлайн.

    Соединение должно быть вне транзакции: CONCURRENTLY и transaction_per_chunk
    внутри транзакционного блока запрещены.
    """
    log = logger or _logger
    schema = plan.schema
    results: List[IndexApplyResult] = []
    pending = plan.pending
    obsolete = plan.obsolete
    if not pending and not obsolete:
        return results
    if not dry_run:
        in_tx = getattr(conn, "is_in_transaction", None)
        if callable(in_tx) and in_tx():
            raise RuntimeError("Online index build requires a connection outside of a transaction")
        await conn.execute("SET statement_timeout = 0")
        await conn.execute(f"SET lock_timeout = '{_lock_timeout_ms(lock_timeout_sec)}ms'")

    # Лишние индексы удаляются первыми: дальше недостающие строятся на уже
    # облегчённой таблице. Без явного флага только докладываем — удаление индекса
    # необратимо, и решение за оператором.
    for item in obsolete:
        drop_sql = item.drop_online_sql(schema)
        if not drop_obsolete:
            results.append(
                IndexApplyResult(
                    item.spec.name, "skipped", drop_sql, error="obsolete index; use drop_obsolete"
                )
            )
            continue
        if dry_run:
            results.append(IndexApplyResult(item.spec.name, "dry_run", drop_sql))
            continue
        started = time.monotonic()
        try:
            log.info("Dropping obsolete index: %s", drop_sql)
            await conn.execute(drop_sql)
        except Exception as e:
            results.append(
                IndexApplyResult(
                    item.spec.name, "failed", drop_sql, time.monotonic() - started, error=repr(e)
                )
            )
            log.warning("Obsolete index %s was not dropped: %r", item.spec.name, e)
            continue
        plan.items.remove(item)
        results.append(IndexApplyResult(item.spec.name, "dropped", drop_sql, time.monotonic() - started))

    if not pending:
        return results

    trgm_ok = plan.trgm_extension_available
    if not trgm_ok and create_extension and not dry_run and any(i.spec.scope == SCOPE_TRGM for i in pending):
        try:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
            trgm_ok = True
            plan.trgm_extension_available = True
        except Exception as e:
            log.warning("pg_trgm is not available (run as superuser: CREATE EXTENSION pg_trgm): %r", e)

    for item in pending:
        create_sql = item.online_sql(schema)
        drop_sql = item.spec.drop_sql(schema, online=True, hypertable=item.hypertable)
        if item.status == STATUS_INVALID and not fix_invalid:
            results.append(
                IndexApplyResult(item.spec.name, "skipped", drop_sql, error="invalid index; use fix_invalid")
            )
            continue
        if item.spec.scope == SCOPE_TRGM and not trgm_ok and not dry_run:
            results.append(IndexApplyResult(item.spec.name, "failed", create_sql, error="pg_trgm is not installed"))
            continue
        statements = [drop_sql, create_sql] if item.status == STATUS_INVALID else [create_sql]
        if dry_run:
            results.append(IndexApplyResult(item.spec.name, "dry_run", ";\n".join(statements)))
            continue
        started = time.monotonic()
        try:
            for sql in statements:
                log.info("Building index: %s", sql)
                await conn.execute(sql)
        except Exception as e:
            results.append(
                IndexApplyResult(
                    item.spec.name, "failed", create_sql, time.monotonic() - started, error=repr(e)
                )
            )
            log.warning("Index %s was not built: %r", item.spec.name, e)
            continue
        action = "rebuilt" if item.status == STATUS_INVALID else "created"
        item.status = STATUS_PRESENT
        results.append(IndexApplyResult(item.spec.name, action, create_sql, time.monotonic() - started))
    return results


async def connect_maintenance(
    *,
    dsn: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    database: Optional[str] = None,
    sslmode: Optional[str] = None,
    application_name: str = MAINTENANCE_APPLICATION_NAME,
    timeout: float = 30.0,
) -> Any:
    """Отдельное соединение для обслуживания (не из пула истории)."""
    import asyncpg

    kwargs: Dict[str, Any] = {
        "timeout": timeout,
        "server_settings": {"application_name": application_name},
    }
    if dsn:
        kwargs["dsn"] = dsn
    for key, value in (("host", host), ("port", port), ("user", user), ("password", password), ("database", database)):
        if value not in (None, ""):
            kwargs[key] = value
    if sslmode == "disable":
        kwargs["ssl"] = False
    elif sslmode in ("require", "verify-ca", "verify-full"):
        kwargs["ssl"] = True
    return await asyncpg.connect(**kwargs)


def format_plan(plan: IndexPlan) -> str:
    rows: Sequence[IndexPlanItem] = plan.items
    lines = [
        f"schema={plan.schema} timescaledb={plan.timescaledb_available} "
        f"pg_trgm={plan.trgm_extension_available}",
    ]
    for item in rows:
        kind = "hypertable" if item.hypertable else "table"
        # Для лишних индексов показываем idx_scan: это главный довод за удаление.
        scans = "" if item.idx_scan is None or not item.obsolete else f" idx_scan={item.idx_scan}"
        lines.append(
            f"  {item.status:<8} {item.spec.scope:<4} {item.spec.name} "
            f"({kind} {item.spec.table}){scans}"
        )
    counts = plan.counts()
    lines.append(
        "total={total} present={present} missing={missing} invalid={invalid} "
        "no_table={no_table} obsolete={obsolete}".format(
            total=len(rows),
            present=counts.get(STATUS_PRESENT, 0),
            missing=counts.get(STATUS_MISSING, 0),
            invalid=counts.get(STATUS_INVALID, 0),
            no_table=counts.get(STATUS_NO_TABLE, 0),
            obsolete=counts.get(STATUS_OBSOLETE, 0),
        )
    )
    if counts.get(STATUS_OBSOLETE):
        lines.append("Лишние индексы удаляются: uapg indexes apply --drop-obsolete")
    return "\n".join(lines)
