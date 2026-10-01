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
        self,
        batch_size: int,
        last_legacy_id: int,
        rows_processed: int,
        *,
        timeout: Optional[float] = None,
    ) -> Tuple[int, int]:
        """Один батч legacy → events_ts.

        Раньше метод повторял на Python логику SQL-объекта и делал это построчно:
        keyset-SELECT плюс `SELECT 1` и INSERT на каждую строку — 1+2N round-trip на батч.
        Функция `uapg_backfill_events_batch` (миграция 005) делает то же одним
        set-based INSERT ... SELECT и сама двигает uapg_backfill_state.
        """
        row = await self._pool.fetchrow(
            f'''
            SELECT last_legacy_id, rows_processed
            FROM "{self._schema}".uapg_backfill_events_batch($1, $2, $3)
            ''',
            batch_size,
            last_legacy_id,
            rows_processed,
            timeout=timeout,
        )
        if row is None:
            return last_legacy_id, rows_processed
        return int(row["last_legacy_id"]), int(row["rows_processed"])
