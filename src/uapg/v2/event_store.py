"""Events V2 read/write orchestration."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from asyncua.common.events import Event

from ..event_filter import apply_event_filter
from .events_config import expand_sql_filter_fields, typed_fields_supported
from .filter_planner import EventFilterPlanner, sql_where_from_plan
from .procedure_gateway import ProcedureGateway
from .schema_registry import EventSchemaRegistry, python_value_to_sql

_logger = logging.getLogger(__name__)

MAX_REFILL_ITERATIONS = 5
EventContinuation = Tuple[datetime, int]


class EventStoreV2:
    """Coordinates dual-write and typed reads for events."""

    def __init__(
        self,
        schema: str,
        pool: Any,
        registry: EventSchemaRegistry,
        gateway: ProcedureGateway,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._schema = schema
        self._pool = pool
        self._registry = registry
        self._gateway = gateway
        self._logger = logger or _logger

    async def save_event_dual(
        self,
        source_db_id: int,
        event_type_id: int,
        event_timestamp: datetime,
        event_data_json: str,
        typed_values: Dict[str, Any],
        physical_table: Optional[str],
        schema_version: int = 1,
    ) -> Tuple[Optional[int], Optional[int]]:
        legacy_row_id, event_id = await self._gateway.save_event_v2(
            source_db_id,
            event_type_id,
            event_timestamp,
            event_data_json,
            schema_version,
        )
        if event_id and physical_table and typed_values:
            await self._registry.insert_typed_row(
                physical_table,
                int(event_id),
                event_timestamp,
                source_db_id,
                typed_values,
            )
        return legacy_row_id, event_id

    async def read_events(
        self,
        source_db_id: int,
        start: datetime,
        end: datetime,
        limit: int,
        order: str,
        evfilter: Any,
        binary_map_to_values: Any,
        *,
        continuation: Optional[EventContinuation] = None,
        partial: bool = False,
    ) -> Tuple[List[Any], Optional[EventContinuation], bool]:
        planner = EventFilterPlanner(
            field_aliases=self._registry.events_config.field_aliases,
        )
        plan = planner.build(evfilter)
        event_type_ids = await self._resolve_event_type_ids(planner, plan)
        pinned_event_type_ids = event_type_ids
        if planner.extract_event_type_names(plan) and not event_type_ids:
            return [], None, partial

        allowed = None
        if event_type_ids:
            allowed = await self._registry.get_allowed_fields(event_type_ids)
            if allowed:
                planner = EventFilterPlanner(
                    allowed_fields=allowed,
                    field_aliases=self._registry.events_config.field_aliases,
                )
                plan = planner.build(evfilter)
                event_type_ids = await self._resolve_event_type_ids(planner, plan) or pinned_event_type_ids

        typed_fields = planner._collect_typed_fields(plan)
        if typed_fields and not event_type_ids:
            event_type_ids = await self._resolve_pushdown_event_type_ids(typed_fields)

        matched: List[Any] = []
        cursor = continuation
        db_exhausted = False
        last_cont: Optional[EventContinuation] = None

        for _ in range(MAX_REFILL_ITERATIONS):
            if len(matched) >= limit:
                break

            fetch_limit = limit - len(matched)
            rows, path_partial = await self._fetch_rows(
                source_db_id,
                start,
                end,
                fetch_limit,
                order,
                plan,
                event_type_ids,
                typed_fields,
                cursor,
            )
            if path_partial:
                partial = True

            if not rows:
                db_exhausted = True
                break

            events = await self._hydrate_events(rows, binary_map_to_values)
            if evfilter:
                events = apply_event_filter(events, evfilter)
            matched.extend(events)

            last_row = rows[-1]
            last_cont = (
                last_row["event_timestamp"],
                int(last_row["event_id"]),
            )
            cursor = last_cont

            if len(rows) < fetch_limit:
                db_exhausted = True
                break

        matched = matched[:limit]
        cont: Optional[EventContinuation] = None
        if not db_exhausted and last_cont is not None and len(matched) >= limit:
            cont = last_cont
        return matched, cont, partial

    async def _fetch_rows(
        self,
        source_db_id: int,
        start: datetime,
        end: datetime,
        limit: int,
        order: str,
        plan: Dict[str, Any],
        event_type_ids: Optional[List[int]],
        typed_fields: Set[str],
        cursor: Optional[EventContinuation],
    ) -> Tuple[List[Any], bool]:
        cursor_ts = cursor[0] if cursor else None
        cursor_event_id = cursor[1] if cursor else None

        if typed_fields and event_type_ids:
            if len(event_type_ids) == 1:
                return await self._read_typed_rows(
                    source_db_id,
                    int(event_type_ids[0]),
                    start,
                    end,
                    limit,
                    order,
                    plan,
                    cursor_ts,
                    cursor_event_id,
                )
            # Схемы полей читаются одним запросом: раньше _common_pushdown_fields и
            # _event_type_ids_with_fields делали по запросу на тип, каждый свой проход.
            schema_fields = await self._registry.get_schema_fields_for_event_types(
                event_type_ids
            )
            common_fields = self._common_pushdown_fields(
                event_type_ids, typed_fields, schema_fields
            )
            if common_fields:
                eligible_type_ids = self._event_type_ids_with_fields(
                    event_type_ids,
                    common_fields,
                    schema_fields,
                )
                if eligible_type_ids:
                    tables = await self._registry.get_storage_tables(eligible_type_ids)
                    return await self._read_typed_multi_rows(
                        source_db_id,
                        eligible_type_ids,
                        start,
                        end,
                        limit,
                        order,
                        plan,
                        common_fields,
                        cursor_ts,
                        cursor_event_id,
                        tables=tables,
                    )

        rows = await self._gateway.read_events_v2(
            source_db_id,
            start,
            end,
            limit,
            order,
            event_type_ids,
            cursor_ts,
            cursor_event_id,
        )
        return list(rows), False

    def _common_pushdown_fields(
        self,
        event_type_ids: List[int],
        typed_fields: Set[str],
        schema_fields: Dict[int, Set[str]],
    ) -> Optional[Set[str]]:
        aliases = self._registry.events_config.field_aliases
        configured = expand_sql_filter_fields(
            set(self._registry.events_config.sql_filter_fields),
            aliases,
        )
        allowed_sets: List[Set[str]] = []
        for type_id in event_type_ids:
            fields = schema_fields.get(int(type_id)) or set()
            if not fields:
                return None
            allowed_sets.append(fields & configured if configured else fields)

        common = typed_fields.copy()
        for allowed in allowed_sets:
            common &= allowed
        return common if common else None

    def _event_type_ids_with_fields(
        self,
        event_type_ids: List[int],
        required_fields: Set[str],
        schema_fields: Dict[int, Set[str]],
    ) -> List[int]:
        return [
            int(type_id)
            for type_id in event_type_ids
            if required_fields.issubset(schema_fields.get(int(type_id)) or set())
        ]

    async def _resolve_event_type_ids(
        self,
        planner: EventFilterPlanner,
        plan: Dict[str, Any],
    ) -> Optional[List[int]]:
        numeric = planner.extract_event_type_ids(plan)
        if numeric:
            return numeric
        names = planner.extract_event_type_names(plan)
        if not names:
            return None
        ids: List[int] = []
        for name in names:
            row = await self._pool.fetchval(
                f'''
                SELECT event_type_id
                FROM "{self._schema}".event_types
                WHERE event_type_name = $1
                   OR event_type_name LIKE $2
                   OR event_type_name LIKE $3
                LIMIT 1
                ''',
                name,
                f"%;s=Events.{name}",
                f"%;s={name}",
            )
            if row is not None:
                ids.append(int(row))
        return ids if ids else None

    async def _resolve_pushdown_event_type_ids(self, typed_fields: Set[str]) -> Optional[List[int]]:
        """Все типы с typed-таблицами, когда в фильтре есть поля payload, но нет InList по EventType."""
        if not typed_fields:
            return None
        cfg = self._registry.events_config
        configured = set(cfg.sql_filter_fields)
        if configured and not typed_fields_supported(typed_fields, configured, cfg.field_aliases):
            return None
        rows = await self._pool.fetch(
            f'''
            SELECT et.event_type_id
            FROM "{self._schema}".event_types et
            INNER JOIN "{self._schema}".event_type_storage ets
              ON ets.event_type_id = et.event_type_id
            ORDER BY et.event_type_id
            '''
        )
        ids = [int(row["event_type_id"]) for row in rows]
        return ids or None

    def _cursor_clause(
        self,
        order: str,
        *,
        table_alias: str = "e",
        cursor_ts: Optional[datetime],
        cursor_event_id: Optional[int],
        param_index: int,
    ) -> Tuple[str, List[Any]]:
        if cursor_ts is None:
            return "", []
        order_sql = "DESC" if order.upper() == "DESC" else "ASC"
        ts_param = param_index
        id_param = param_index + 1
        if order_sql == "DESC":
            clause = f'''
              AND (
                  {table_alias}.event_timestamp < ${ts_param}
                  OR (
                      {table_alias}.event_timestamp = ${ts_param}
                      AND ${id_param}::bigint IS NOT NULL
                      AND {table_alias}.event_id < ${id_param}
                  )
              )
            '''
        else:
            clause = f'''
              AND (
                  {table_alias}.event_timestamp > ${ts_param}
                  OR (
                      {table_alias}.event_timestamp = ${ts_param}
                      AND ${id_param}::bigint IS NOT NULL
                      AND {table_alias}.event_id > ${id_param}
                  )
              )
            '''
        return clause, [cursor_ts, cursor_event_id]

    def _typed_branch_sql(
        self,
        table: str,
        event_type_param: int,
        plan: Dict[str, Any],
        *,
        order: str,
        limit_param: int,
        param_offset: int,
        cursor_ts: Optional[datetime],
        cursor_event_id: Optional[int],
    ) -> Tuple[str, List[Any], int]:
        """Одна ветка выборки, работающая только по typed-таблице.

        Фильтр, курсор, порядок и LIMIT стоят внутри ветки. Раньше ORDER BY и LIMIT
        были только снаружи UNION ALL (в ветки PostgreSQL их не заносит), а фильтр
        применялся после JOIN с events_ts — поэтому каждая ветка материализовала всё
        окно, а оба нужных индекса оказывались на разных отношениях.

        $1 — source_id, $2/$3 — границы окна; они общие для всех ветвей.
        """
        args: List[Any] = []
        where_sql, params, _ = sql_where_from_plan(
            plan, table_alias="t", param_offset=param_offset
        )
        branch = f'''
                SELECT t.event_id, t.event_timestamp, ${event_type_param}::bigint AS event_type_id
                FROM "{self._schema}"."{table}" t
                WHERE t.source_id = $1
                  AND t.event_timestamp BETWEEN $2 AND $3
        '''
        if where_sql:
            branch += f"                  AND {where_sql}\n"
            args.extend(params)
            param_offset += len(params)

        cursor_sql, cursor_params = self._cursor_clause(
            order,
            table_alias="t",
            cursor_ts=cursor_ts,
            cursor_event_id=cursor_event_id,
            param_index=param_offset,
        )
        if cursor_sql:
            branch += cursor_sql
            args.extend(cursor_params)
            param_offset += len(cursor_params)

        order_sql = "DESC" if order.upper() == "DESC" else "ASC"
        branch += (
            f"                ORDER BY t.event_timestamp {order_sql}, t.event_id {order_sql}\n"
            f"                LIMIT ${limit_param}\n"
        )
        return branch, args, param_offset

    def _merge_typed_branches(self, branches: List[str], order: str, limit_param: int) -> str:
        """Слияние ветвей и добор legacy_row_id из events_ts для ≤limit строк по PK."""
        order_sql = "DESC" if order.upper() == "DESC" else "ASC"
        union_sql = " UNION ALL ".join(f"({branch})" for branch in branches)
        return f'''
            SELECT m.event_id, m.event_timestamp, m.event_type_id, e.legacy_row_id
            FROM ({union_sql}) m
            INNER JOIN "{self._schema}".events_ts e
              ON e.source_id = $1
             AND e.event_timestamp = m.event_timestamp
             AND e.event_id = m.event_id
            ORDER BY m.event_timestamp {order_sql}, m.event_id {order_sql}
            LIMIT ${limit_param}
        '''

    async def _read_typed_rows(
        self,
        source_db_id: int,
        event_type_id: int,
        start: datetime,
        end: datetime,
        limit: int,
        order: str,
        plan: Dict[str, Any],
        cursor_ts: Optional[datetime],
        cursor_event_id: Optional[int],
    ) -> Tuple[List[Any], bool]:
        table = await self._registry.get_storage_table(event_type_id)
        if not table:
            return [], True

        typed_plan = EventFilterPlanner().strip_event_type(plan)
        # $1 source_id, $2/$3 окно, $4 limit, $5 event_type_id, дальше фильтр и курсор.
        args: List[Any] = [source_db_id, start, end, limit, int(event_type_id)]
        branch, branch_args, _ = self._typed_branch_sql(
            table,
            5,
            typed_plan,
            order=order,
            limit_param=4,
            param_offset=6,
            cursor_ts=cursor_ts,
            cursor_event_id=cursor_event_id,
        )
        args.extend(branch_args)
        sql = self._merge_typed_branches([branch], order, 4)
        rows = await self._pool.fetch(sql, *args)
        return list(rows), False

    async def _read_typed_multi_rows(
        self,
        source_db_id: int,
        event_type_ids: List[int],
        start: datetime,
        end: datetime,
        limit: int,
        order: str,
        plan: Dict[str, Any],
        common_fields: Set[str],
        cursor_ts: Optional[datetime],
        cursor_event_id: Optional[int],
        tables: Optional[Dict[int, str]] = None,
    ) -> Tuple[List[Any], bool]:
        typed_plan = EventFilterPlanner().strip_event_type(plan)
        filtered_plan = self._filter_plan_fields(typed_plan, common_fields)
        if tables is None:
            tables = await self._registry.get_storage_tables(event_type_ids)

        # $1 source_id, $2/$3 окно, $4 limit — общие для всех ветвей.
        args: List[Any] = [source_db_id, start, end, limit]
        param_offset = 5
        branches: List[str] = []
        for event_type_id in event_type_ids:
            table = tables.get(int(event_type_id))
            if not table:
                continue
            event_type_param = param_offset
            args.append(int(event_type_id))
            param_offset += 1
            branch, branch_args, param_offset = self._typed_branch_sql(
                table,
                event_type_param,
                filtered_plan,
                order=order,
                limit_param=4,
                param_offset=param_offset,
                cursor_ts=cursor_ts,
                cursor_event_id=cursor_event_id,
            )
            args.extend(branch_args)
            branches.append(branch)

        if not branches:
            return [], True

        sql = self._merge_typed_branches(branches, order, 4)
        rows = await self._pool.fetch(sql, *args)
        return list(rows), False

    def _filter_plan_fields(self, plan: Dict[str, Any], allowed: Set[str]) -> Dict[str, Any]:
        if not plan:
            return {}
        if "and" in plan:
            children = [self._filter_plan_fields(child, allowed) for child in plan["and"]]
            children = [child for child in children if child]
            return {"and": children} if children else {}
        if "or" in plan:
            children = [self._filter_plan_fields(child, allowed) for child in plan["or"]]
            children = [child for child in children if child]
            return {"or": children} if children else {}
        field = plan.get("field")
        if field and field not in allowed and field != "EventType":
            return {}
        return plan

    async def _hydrate_events(self, rows: List[Any], binary_map_to_values: Any) -> List[Any]:
        if not rows:
            return []
        legacy_ids = [r["legacy_row_id"] for r in rows if r["legacy_row_id"]]
        legacy_map: Dict[int, Dict[str, Any]] = {}
        if legacy_ids:
            legacy_rows = await self._pool.fetch(
                f'''
                SELECT id, event_data
                FROM "{self._schema}".events_history
                WHERE id = ANY($1::bigint[])
                ''',
                legacy_ids,
            )
            for lr in legacy_rows:
                data = lr["event_data"]
                if isinstance(data, str):
                    data = json.loads(data)
                legacy_map[int(lr["id"])] = data

        results: List[Any] = []
        for row in rows:
            data = legacy_map.get(int(row["legacy_row_id"] or 0), {})
            values = binary_map_to_values(data) if data else {}
            try:
                event = Event.from_field_dict(values)
                results.append(event)
            except Exception as exc:
                self._logger.debug("hydrate event fallback: %s", exc)
        return results

    def extract_typed_values(self, raw_event_data: Dict[str, Any]) -> Dict[str, Any]:
        typed: Dict[str, Any] = {}
        for key, variant in raw_event_data.items():
            if hasattr(variant, "Value"):
                typed[key] = python_value_to_sql(variant.Value)
            else:
                typed[key] = python_value_to_sql(variant)
        return typed
