"""Слой поиска событий: запись в оба хранилища и чтение с фильтром в SQL.

Событие пишется одной процедурой ``uapg_save_event_v2``: полный состав полей
уходит в ``events_history``, строка поиска — в ``events_ts``. Затем, если у
типа есть типизированная таблица, туда кладутся поля, по которым ищут.

Чтение устроено наоборот: сначала по ``events_ts`` и типизированным таблицам
находятся нужные строки — с условием фильтра внутри SQL, — затем полный состав
полей подтягивается из ``events_history``. Фильтр в памяти после этого
применяется ещё раз: он страхует те части ContentFilter, которые в SQL не
переводятся.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import asyncpg
from asyncua.common.events import Event

from ..codec import decode_event_data
from ..codec.node_id import format_node_id
from ..core.database import Database
from ..core.sql import load_queries
from ..opcua.event_filter import apply_event_filter
from .events import EventRepository
from .events_config import expand_sql_filter_fields, typed_fields_supported
from .filter_plan import (
    EventFilterPlanner,
    FilterPlan,
    event_type_name_from_literal,
    render_filter,
)
from .items import EventWriteItem
from .typed_events import EventSchemaRegistry, TypedEventTables

# Сколько раз дочитывать, если фильтр в памяти отсеял часть строк. Без предела
# редкое событие в длинной истории превращает один HistoryRead в полный скан.
MAX_REFILL_ITERATIONS = 5

EventCursor = Tuple[datetime, int]


@dataclass
class SearchResult:
    events: List[Any]
    cursor: Optional[EventCursor]


class EventSearchStore:
    """Запись и чтение событий через слой поиска."""

    def __init__(
        self,
        database: Database,
        schema: str,
        events: EventRepository,
        tables: TypedEventTables,
        registry: EventSchemaRegistry,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._db = database
        self._schema = schema
        self._events = events
        self._tables = tables
        self._registry = registry
        self.logger = logger or logging.getLogger("uapg.storage.event_search")
        self._sql = load_queries("search.sql", schema)
        # Сколько раз служебная проба готовности переноса не выполнилась.
        self.probe_failures = 0
        # event_type_id -> (таблица, версия схемы), чтобы не спрашивать реестр на каждом событии.
        self._types: Dict[int, Tuple[Optional[str], int]] = {}

    @property
    def config(self) -> Any:
        return self._tables.config

    def remember_type(self, event_type_id: int, table: Optional[str], version: int) -> None:
        self._types[int(event_type_id)] = (table, int(version))

    async def _type_storage(self, event_type_id: int) -> Tuple[Optional[str], int]:
        known = self._types.get(int(event_type_id))
        if known is not None:
            return known
        table = await self._registry.storage_table(int(event_type_id))
        self._types[int(event_type_id)] = (table, 1)
        return table, 1

    # ------------------------------------------------------------------ запись

    async def flush(self, items: List[EventWriteItem]) -> None:
        """Записать пачку событий в оба слоя одной транзакцией."""
        if not items:
            return

        prepared: List[Tuple[EventWriteItem, Optional[str], Dict[str, Any], int]] = []
        for item in items:
            table, version = await self._type_storage(item.event_type_id)
            columns: Dict[str, Any] = {}
            if table:
                values = decode_event_data(json.loads(item.event_data_json))
                columns = self._tables.columns_for(values)
                # DDL — до транзакции записи: замок и ALTER нельзя держать
                # внутри батча, иначе один новый столбец тормозит всю запись.
                await self._tables.ensure_table(table, list(columns))
            prepared.append((item, table, columns, version))

        save_sql = self._sql["save_event"]
        tables = self._tables

        async def _write(conn: asyncpg.Connection) -> None:
            for item, table, columns, version in prepared:
                row = await conn.fetchrow(
                    save_sql,
                    item.source_db_id,
                    item.event_type_id,
                    item.event_timestamp,
                    item.event_data_json,
                    version,
                )
                if row is None or row["event_id"] is None or not table or not columns:
                    continue
                await conn.execute(
                    tables.insert_statement(table, list(columns)),
                    int(row["event_id"]),
                    item.event_timestamp,
                    item.source_db_id,
                    *columns.values(),
                )

        await self._db.run_in_transaction(_write, name="запись событий v2")

    # ------------------------------------------------------------------ чтение

    async def read(
        self,
        source_id: int,
        start: datetime,
        end: datetime,
        limit: int,
        order: str,
        evfilter: Any,
        *,
        cursor: Optional[EventCursor] = None,
    ) -> SearchResult:
        planner = EventFilterPlanner(field_aliases=self.config.field_aliases)
        plan = planner.build(evfilter)

        type_ids = await self._resolve_types(planner, plan)
        if planner.event_type_literals(plan) and not type_ids:
            # Клиент назвал тип, которого в базе нет: событий такого типа нет тоже.
            return SearchResult([], None)

        typed_fields = planner.typed_fields(plan)
        use_typed = bool(typed_fields) and typed_fields_supported(
            typed_fields, set(self.config.sql_filter_fields), self.config.field_aliases
        )
        branches = (
            await self._typed_branches(planner.without_event_type(plan), type_ids)
            if use_typed
            else None
        )

        matched: List[Any] = []
        last_cursor: Optional[EventCursor] = None
        exhausted = False
        position = cursor

        for _ in range(MAX_REFILL_ITERATIONS):
            if len(matched) >= limit:
                break
            want = limit - len(matched)
            if branches is not None:
                rows = await self._read_typed(source_id, start, end, want, order, branches, position)
            else:
                rows = await self._read_plain(source_id, start, end, want, order, type_ids, position)

            if not rows:
                exhausted = True
                break

            events = await self._hydrate(rows)
            if evfilter:
                events = apply_event_filter(events, evfilter)
            matched.extend(events)

            last_cursor = (rows[-1]["event_timestamp"], int(rows[-1]["event_id"]))
            position = last_cursor
            if len(rows) < want:
                exhausted = True
                break

        matched = matched[:limit]
        next_cursor = last_cursor if not exhausted and len(matched) >= limit else None
        return SearchResult(matched, next_cursor)

    async def _resolve_types(self, planner: EventFilterPlanner, plan: FilterPlan) -> List[int]:
        """Идентификаторы типов в базе по условию на EventType.

        NodeId сопоставляются по строковому ключу одним запросом: стандартный
        клиент присылает сотню подтипов, и запрос на каждый стоил бы дорого.
        Строковые литералы — короткие имена вроде «SensorInactive» — ищутся по
        суффиксу ключа, как и раньше.
        """
        resolved: List[int] = []
        nodes = planner.event_type_nodes(plan)
        if nodes:
            rows = await self._db.fetch(
                self._sql["resolve_types_by_key"], [format_node_id(node) for node in nodes]
            )
            resolved.extend(int(row["event_type_id"]) for row in rows)

        for literal in planner.event_type_literals(plan):
            if not isinstance(literal, str):
                continue
            name = event_type_name_from_literal(literal)
            if not name:
                continue
            value = await self._db.fetchval(
                self._sql["resolve_type"], name, f"%;s=Events.{name}", f"%;s={name}"
            )
            if value is not None:
                resolved.append(int(value))
        return sorted(set(resolved))

    async def _typed_branches(
        self, plan: FilterPlan, type_ids: Sequence[int]
    ) -> List[Tuple[int, str, str, List[Any]]]:
        """Ветви запроса: по одной на тип события, у которого есть таблица поиска.

        Условие строится под колонки конкретной таблицы. Ветвь, где условие
        заведомо ложно (у типа нет нужного поля), в запрос не попадает вовсе.
        """
        rows = await self._db.fetch(self._sql["types_with_storage"])
        storage = {int(row["event_type_id"]): str(row["physical_table"]) for row in rows}
        wanted = [int(i) for i in type_ids] if type_ids else sorted(storage)

        branches: List[Tuple[int, str, str, List[Any]]] = []
        for type_id in wanted:
            table = storage.get(type_id)
            if not table:
                continue
            columns: Set[str] = await self._tables.known_columns(table)
            if not columns:
                continue
            rendered = render_filter(plan, available_columns=columns, param_offset=1)
            if rendered.sql == "FALSE":
                continue
            branches.append((type_id, table, rendered.sql, rendered.params))
        return branches

    async def _read_typed(
        self,
        source_id: int,
        start: datetime,
        end: datetime,
        limit: int,
        order: str,
        branches: List[Tuple[int, str, str, List[Any]]],
        cursor: Optional[EventCursor],
    ) -> List[asyncpg.Record]:
        if not branches:
            return []
        direction = "DESC" if order.upper() == "DESC" else "ASC"
        args: List[Any] = [source_id, start, end]
        parts: List[str] = []

        for type_id, table, condition, params in branches:
            args.append(type_id)
            type_param = len(args)
            offset = len(args)
            # Параметры условия нумеровались с $1 — сдвигаем их за общие.
            shifted = _shift_params(condition, offset) if condition else ""
            args.extend(params)
            part = (
                f'SELECT e.event_id, e.event_timestamp, e.event_type_id, e.legacy_row_id '
                f'FROM "{self._schema}".events_ts e '
                f'JOIN "{self._schema}"."{table}" t '
                f"ON t.event_id = e.event_id AND t.event_timestamp = e.event_timestamp "
                f"WHERE e.source_id = $1 AND e.event_type_id = ${type_param} "
                f"AND e.event_timestamp BETWEEN $2 AND $3"
            )
            if shifted:
                part += f" AND {shifted}"
            parts.append(part)

        cursor_sql = ""
        if cursor is not None:
            args.extend(cursor)
            ts, event_id = len(args) - 1, len(args)
            comparison = "<" if direction == "DESC" else ">"
            cursor_sql = (
                f"WHERE (m.event_timestamp, m.event_id) {comparison} (${ts}, ${event_id})"
            )
        args.append(limit)

        sql = (
            f"SELECT m.event_id, m.event_timestamp, m.event_type_id, m.legacy_row_id "
            f"FROM ({' UNION ALL '.join(parts)}) m {cursor_sql} "
            f"ORDER BY m.event_timestamp {direction}, m.event_id {direction} "
            f"LIMIT ${len(args)}"
        )
        return await self._db.fetch(sql, *args)

    async def _read_plain(
        self,
        source_id: int,
        start: datetime,
        end: datetime,
        limit: int,
        order: str,
        type_ids: Sequence[int],
        cursor: Optional[EventCursor],
    ) -> List[asyncpg.Record]:
        return await self._db.fetch(
            self._sql["read_events"],
            source_id,
            start,
            end,
            limit,
            "DESC" if order.upper() == "DESC" else "ASC",
            list(type_ids) or None,
            cursor[0] if cursor else None,
            cursor[1] if cursor else None,
        )

    async def _hydrate(self, rows: Sequence[asyncpg.Record]) -> List[Any]:
        legacy_ids = [int(row["legacy_row_id"]) for row in rows if row["legacy_row_id"]]
        payloads = await self._events.read_payloads(legacy_ids)
        events: List[Any] = []
        for row in rows:
            fields = payloads.get(int(row["legacy_row_id"] or 0))
            if not fields:
                continue
            try:
                events.append(Event.from_field_dict(fields))
            except Exception as exc:
                self.logger.debug("Событие %s не собрано: %s", row["event_id"], exc)
        return events

    async def explain(
        self,
        source_id: int,
        start: datetime,
        end: datetime,
        limit: int,
        order: str,
        type_ids: Optional[Sequence[int]],
    ) -> str:
        value = await self._db.fetchval(
            self._sql["explain_filter"],
            source_id,
            start,
            end,
            limit,
            order,
            list(type_ids) if type_ids else None,
        )
        return str(value or "")

    def allowed_filter_fields(self) -> Set[str]:
        return expand_sql_filter_fields(set(self.config.sql_filter_fields), self.config.field_aliases)

    # ------------------------------------------------------------------ перенос

    # Домены курсоров в uapg_backfill_state: отдельный для переноса в events_ts и
    # отдельный для заполнения типизированных таблиц.
    EVENTS_DOMAIN = "events"
    TYPED_DOMAIN = "events_typed"

    async def pending_backfill(self, probe_rows: int, *, timeout: Optional[float] = None) -> Optional[bool]:
        """Осталась ли у переноса работа. ``None`` — проба не удалась.

        Ограниченная проба над watermark вместо полного anti-join: последний на
        проде разворачивался в Parallel Hash Anti Join по всем чанкам
        гипертаблицы, не укладывался в таймаут запроса и через общий путь
        обработки ошибок пересоздавал пул — ради косметического узла OPC UA.
        """
        value = await self._db.probe_fetchval(
            self._sql["backfill_pending"], max(1, int(probe_rows)), timeout=timeout
        )
        if value is None:
            self.probe_failures += 1
            return None
        return bool(value)

    async def _state(self, domain: str) -> Tuple[int, int]:
        row = await self._db.fetchrow(self._sql["backfill_state"], domain)
        if row is None:
            return 0, 0
        return int(row["last_legacy_id"]), int(row["rows_processed"])

    async def _set_state(self, domain: str, last_id: int, processed: int) -> None:
        await self._db.execute(self._sql["set_backfill_state"], domain, int(last_id), int(processed))

    async def backfill(self, batch_size: int = 500) -> Dict[str, Any]:
        """Перенести очередную порцию событий из устаревшего хранения в слой поиска."""
        last_id, processed = await self._state(self.EVENTS_DOMAIN)
        row = await self._db.fetchrow(self._sql["backfill_batch"], batch_size, last_id, processed)
        if row is not None:
            last_id = int(row["last_legacy_id"])
            processed = int(row["rows_processed"])

        typed_inserted, typed_failed = await self._backfill_typed(batch_size)

        stats = await self._db.fetchrow(self._sql["backfill_stats"], last_id)
        lag = int((stats["lag_rows"] if stats else 0) or 0)
        return {
            "last_legacy_id": last_id,
            "rows_processed": processed,
            # Строки, которых перенос ещё не касался. Надмножество точного
            # отставания: сюда попадают и свежие события двойной записи, но
            # дойдя до хвоста, значение остаётся около нуля.
            "backfill_lag_rows": lag,
            "v2_coverage_pct": _coverage_pct(
                last_id,
                lag,
                stats["min_id"] if stats else None,
                stats["max_id"] if stats else None,
            ),
            "typed_rows_inserted": typed_inserted,
            "typed_rows_failed": typed_failed,
        }

    async def _backfill_typed(self, batch_size: int) -> Tuple[int, int]:
        """Заполнить типизированные таблицы по восходящему курсору.

        Дойдя до хвоста, курсор сбрасывается в ноль: следующий круг подхватит
        события тех типов, у которых таблица поиска появилась позже. Повторная
        вставка безвредна — она идёт с ON CONFLICT DO NOTHING.
        """
        cursor, _ = await self._state(self.TYPED_DOMAIN)
        rows = await self._db.fetch(
            self._sql["typed_backfill_rows"], int(cursor), self._schema, int(batch_size)
        )
        if not rows:
            if cursor:
                await self._set_state(self.TYPED_DOMAIN, 0, 0)
            return 0, 0

        # Поля всех событий батча — одним запросом, а не по одному на событие.
        payloads = await self._events.read_payloads([int(r["legacy_row_id"]) for r in rows])

        inserted = 0
        failed = 0
        for row in rows:
            fields = payloads.get(int(row["legacy_row_id"]))
            if not fields:
                continue
            try:
                await self._tables.insert_row(
                    str(row["physical_table"]),
                    event_id=int(row["event_id"]),
                    event_timestamp=row["event_timestamp"],
                    source_id=int(row["source_id"]),
                    values=fields,
                )
                inserted += 1
            except Exception as exc:
                failed += 1
                self.logger.warning(
                    "Событие %s не перенесено в таблицу поиска: %s", row["event_id"], exc
                )

        await self._set_state(self.TYPED_DOMAIN, int(rows[-1]["legacy_row_id"]), inserted)
        return inserted, failed


def _coverage_pct(
    last_legacy_id: int,
    lag_rows: int,
    min_id: Optional[int],
    max_id: Optional[int],
) -> float:
    """Где стоит watermark в диапазоне идентификаторов events_history.

    Минимум учитывается из-за политики хранения: без него покрытие завышалось бы
    на величину уже удалённого хвоста.
    """
    if max_id is None or min_id is None or lag_rows == 0:
        return 100.0
    span = int(max_id) - int(min_id) + 1
    if span <= 0:
        return 100.0
    done = min(max(last_legacy_id - int(min_id) + 1, 0), span)
    return round(100.0 * done / span, 2)


def _shift_params(sql: str, offset: int) -> str:
    """Сдвинуть номера параметров $N на offset."""
    return re.sub(r"\$(\d+)", lambda m: f"${int(m.group(1)) + offset}", sql)
