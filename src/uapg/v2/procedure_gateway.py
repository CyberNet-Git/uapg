"""Gateway for SQL functions and procedures with Python fallback."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

_logger = logging.getLogger(__name__)


class ProcedureGateway:
    """Thin asyncpg wrapper for uapg SQL objects."""

    def __init__(self, schema: str, pool: Any, logger: Optional[logging.Logger] = None) -> None:
        self._schema = schema
        self._pool = pool
        self._logger = logger or _logger

    async def save_event_v2(
        self,
        source_id: int,
        event_type_id: int,
        event_timestamp: datetime,
        event_data_json: str,
        schema_version: int = 1,
    ) -> Tuple[Optional[int], Optional[int]]:
        row = await self._pool.fetchrow(
            f'''
            SELECT legacy_row_id, event_id
            FROM "{self._schema}".uapg_save_event_v2($1, $2, $3, $4::jsonb, $5)
            ''',
            source_id,
            event_type_id,
            event_timestamp,
            event_data_json,
            schema_version,
        )
        if row is None:
            return None, None
        return row["legacy_row_id"], row["event_id"]

    async def read_events_v2(
        self,
        source_id: int,
        start: datetime,
        end: datetime,
        limit: int,
        order: str,
        event_type_ids: Optional[List[int]] = None,
        cursor_ts: Optional[datetime] = None,
        cursor_event_id: Optional[int] = None,
    ) -> List[Any]:
        return await self._pool.fetch(
            f'''
            SELECT *
            FROM "{self._schema}".uapg_read_events_v2(
                $1, $2, $3, $4, $5, $6::bigint[], $7, $8
            )
            ''',
            source_id,
            start,
            end,
            limit,
            order,
            event_type_ids,
            cursor_ts,
            cursor_event_id,
        )

    async def explain_event_filter(
        self,
        source_id: int,
        start: datetime,
        end: datetime,
        limit: int,
        order: str,
        event_type_ids: Optional[List[int]] = None,
    ) -> str:
        plan = await self._pool.fetchval(
            f'''
            SELECT "{self._schema}".uapg_explain_event_filter($1, $2, $3, $4, $5, $6::bigint[])
            ''',
            source_id,
            start,
            end,
            limit,
            order,
            event_type_ids,
        )
        return str(plan or "")

    async def sync_event_type_schema(
        self,
        event_type_id: int,
        node_id: str,
        parent_node_id: Optional[str],
        fields: List[Dict[str, Any]],
        schema_version: int,
        physical_table: str,
    ) -> None:
        await self._pool.execute(
            f'CALL "{self._schema}".uapg_sync_event_type_schema($1, $2, $3, $4::jsonb, $5, $6)',
            event_type_id,
            node_id,
            parent_node_id,
            json.dumps(fields),
            schema_version,
            physical_table,
        )

    async def backfill_events_batch(
        self, batch_size: int, last_legacy_id: int, rows_processed: int
    ) -> Tuple[int, int]:
        rows = await self._pool.fetch(
            f'''
            SELECT eh.id, eh.source_id, eh.event_type_id, eh.event_timestamp
            FROM "{self._schema}".events_history eh
            WHERE eh.id > $1
            ORDER BY eh.id
            LIMIT $2
            ''',
            last_legacy_id,
            batch_size,
        )
        new_last = last_legacy_id
        new_processed = rows_processed
        for row in rows:
            exists = await self._pool.fetchval(
                f'''
                SELECT 1 FROM "{self._schema}".events_ts
                WHERE legacy_row_id = $1
                LIMIT 1
                ''',
                row["id"],
            )
            if not exists:
                await self._pool.execute(
                    f'''
                    INSERT INTO "{self._schema}".events_ts (
                        source_id, event_type_id, event_timestamp, schema_version, legacy_row_id
                    ) VALUES ($1, $2, $3, 1, $4)
                    ON CONFLICT DO NOTHING
                    ''',
                    row["source_id"],
                    row["event_type_id"],
                    row["event_timestamp"],
                    row["id"],
                )
                new_processed += 1
            new_last = int(row["id"])
        await self._pool.execute(
            f'''
            INSERT INTO "{self._schema}".uapg_backfill_state (domain, last_legacy_id, rows_processed)
            VALUES ('events', $1, $2)
            ON CONFLICT (domain) DO UPDATE SET
                last_legacy_id = EXCLUDED.last_legacy_id,
                rows_processed = EXCLUDED.rows_processed,
                updated_at = NOW()
            ''',
            new_last,
            new_processed,
        )
        return new_last, new_processed
