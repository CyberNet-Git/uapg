"""Backfill legacy events_history into events_ts + typed tables."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional, Tuple

from .procedure_gateway import ProcedureGateway
from .schema_registry import EventSchemaRegistry

_logger = logging.getLogger(__name__)


class EventsBackfillWorker:
    """Batch worker for legacy → v2 migration."""

    def __init__(
        self,
        schema: str,
        pool: Any,
        gateway: ProcedureGateway,
        registry: EventSchemaRegistry,
        binary_map_to_values: Any,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._schema = schema
        self._pool = pool
        self._gateway = gateway
        self._registry = registry
        self._binary_map_to_values = binary_map_to_values
        self._logger = logger or _logger

    async def get_state(self) -> Tuple[int, int]:
        row = await self._pool.fetchrow(
            f'''
            SELECT last_legacy_id, rows_processed
            FROM "{self._schema}".uapg_backfill_state
            WHERE domain = 'events'
            '''
        )
        if row is None:
            return 0, 0
        return int(row["last_legacy_id"]), int(row["rows_processed"])

    async def run_batch(self, batch_size: int = 500) -> Dict[str, int]:
        last_id, processed = await self.get_state()
        new_last, new_processed = await self._gateway.backfill_events_batch(
            batch_size, last_id, processed
        )
        await self._backfill_typed_rows(batch_size)
        lag = await self._pool.fetchval(
            f'''
            SELECT count(*)::bigint
            FROM "{self._schema}".events_history eh
            WHERE NOT EXISTS (
                SELECT 1 FROM "{self._schema}".events_ts et
                WHERE et.legacy_row_id = eh.id
            )
            '''
        )
        total = await self._pool.fetchval(f'SELECT count(*)::bigint FROM "{self._schema}".events_history')
        coverage = 0.0
        if total:
            covered = int(total) - int(lag or 0)
            coverage = round(100.0 * covered / int(total), 2)
        return {
            "last_legacy_id": new_last,
            "rows_processed": new_processed,
            "backfill_lag_rows": int(lag or 0),
            "v2_coverage_pct": coverage,
        }

    async def _backfill_typed_rows(self, batch_size: int) -> None:
        rows = await self._pool.fetch(
            f'''
            SELECT et.event_id, et.event_timestamp, et.source_id, et.event_type_id, et.legacy_row_id
            FROM "{self._schema}".events_ts et
            LEFT JOIN "{self._schema}".event_type_storage ets ON ets.event_type_id = et.event_type_id
            WHERE et.legacy_row_id IS NOT NULL
              AND ets.physical_table IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM information_schema.tables t
                  WHERE t.table_schema = $1
                    AND t.table_name = ets.physical_table
              ) IS FALSE
            ORDER BY et.legacy_row_id DESC
            LIMIT $2
            ''',
            self._schema,
            batch_size,
        )
        for row in rows:
            table = await self._registry.get_storage_table(int(row["event_type_id"]))
            if not table:
                continue
            exists = await self._pool.fetchval(
                f'''
                SELECT 1 FROM "{self._schema}"."{table}"
                WHERE event_id = $1 AND event_timestamp = $2
                LIMIT 1
                ''',
                row["event_id"],
                row["event_timestamp"],
            )
            if exists:
                continue
            legacy = await self._pool.fetchrow(
                f'''
                SELECT event_data FROM "{self._schema}".events_history
                WHERE id = $1
                ''',
                row["legacy_row_id"],
            )
            if legacy is None:
                continue
            data = legacy["event_data"]
            if isinstance(data, str):
                data = json.loads(data)
            values = self._binary_map_to_values(data)
            typed = {k: (v.Value if hasattr(v, "Value") else v) for k, v in values.items()}
            await self._registry.insert_typed_row(
                table,
                int(row["event_id"]),
                row["event_timestamp"],
                int(row["source_id"]),
                typed,
            )
