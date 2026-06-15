"""Events V2 read/write orchestration."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from asyncua.common.events import Event

from .filter_planner import EventFilterPlanner, sql_where_from_plan
from .procedure_gateway import ProcedureGateway
from .schema_registry import EventSchemaRegistry, python_value_to_sql

_logger = logging.getLogger(__name__)


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
        partial: bool = False,
    ) -> Tuple[List[Any], Optional[datetime], bool]:
        planner = EventFilterPlanner(
            field_aliases=self._registry.events_config.field_aliases,
        )
        plan = planner.build(evfilter)
        event_type_ids = planner.extract_event_type_ids(plan)
        typed_fields = planner._collect_typed_fields(plan)

        if typed_fields and event_type_ids and len(event_type_ids) == 1:
            events, cont, is_partial = await self._read_typed(
                source_db_id,
                int(event_type_ids[0]),
                start,
                end,
                limit,
                order,
                plan,
                binary_map_to_values,
            )
            return events, cont, is_partial or partial

        rows = await self._gateway.read_events_v2(
            source_db_id, start, end, limit, order, event_type_ids
        )
        events = await self._hydrate_events(rows, binary_map_to_values)
        cont = rows[-1]["event_timestamp"] if len(rows) == limit and rows else None
        return events, cont, partial

    async def _read_typed(
        self,
        source_db_id: int,
        event_type_id: int,
        start: datetime,
        end: datetime,
        limit: int,
        order: str,
        plan: Dict[str, Any],
        binary_map_to_values: Any,
    ) -> Tuple[List[Any], Optional[datetime], bool]:
        table = await self._registry.get_storage_table(event_type_id)
        if not table:
            return [], None, True

        typed_plan = EventFilterPlanner().strip_event_type(plan)
        where_sql, params, _ = sql_where_from_plan(typed_plan, table_alias="t")
        order_sql = "DESC" if order.upper() == "DESC" else "ASC"
        sql = f'''
            SELECT e.event_id, e.event_timestamp, e.event_type_id, e.legacy_row_id
            FROM "{self._schema}".events_ts e
            INNER JOIN "{self._schema}"."{table}" t
              ON t.event_id = e.event_id AND t.event_timestamp = e.event_timestamp
            WHERE e.source_id = $1
              AND e.event_type_id = $2
              AND e.event_timestamp BETWEEN $3 AND $4
        '''
        args: List[Any] = [source_db_id, event_type_id, start, end]
        if where_sql:
            sql += f" AND {where_sql}"
            args.extend(params)
        sql += f" ORDER BY e.event_timestamp {order_sql}, e.event_id {order_sql} LIMIT ${len(args) + 1}"
        args.append(limit)

        rows = await self._pool.fetch(sql, *args)
        events = await self._hydrate_events(rows, binary_map_to_values)
        cont = rows[-1]["event_timestamp"] if len(rows) == limit and rows else None
        return events, cont, False

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
