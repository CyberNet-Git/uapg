"""Backfill legacy events_history into events_ts + typed tables."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional, Tuple

from .procedure_gateway import ProcedureGateway
from .schema_registry import EventSchemaRegistry

_logger = logging.getLogger(__name__)

# Прогресс считается от watermark uapg_backfill_state, а не полным anti-join
# events_history × events_ts: последний на проде разворачивался в Parallel Hash
# Anti Join по всем чанкам гипертаблицы и не укладывался в таймаут запроса.
# Все три подзапроса обслуживаются индексом idx_events_history_id.
_BACKFILL_STATS_SQL = '''
SELECT
    (SELECT count(*)::bigint FROM "{schema}".events_history WHERE id > $1) AS lag_rows,
    (SELECT min(id)::bigint  FROM "{schema}".events_history)               AS min_id,
    (SELECT max(id)::bigint  FROM "{schema}".events_history)               AS max_id
'''


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
        query_timeout_sec: Optional[float] = None,
    ) -> None:
        self._schema = schema
        self._pool = pool
        self._gateway = gateway
        self._registry = registry
        self._binary_map_to_values = binary_map_to_values
        self._logger = logger or _logger
        # Воркер ходит в сырой пул, минуя _fetchval: без явного таймаута батч мог
        # висеть бесконечно.
        self._query_timeout_sec = (
            None
            if query_timeout_sec is None or float(query_timeout_sec) <= 0
            else float(query_timeout_sec)
        )

    async def get_state(self) -> Tuple[int, int]:
        row = await self._pool.fetchrow(
            f'''
            SELECT last_legacy_id, rows_processed
            FROM "{self._schema}".uapg_backfill_state
            WHERE domain = 'events'
            ''',
            timeout=self._query_timeout_sec,
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
        stats = await self._pool.fetchrow(
            _BACKFILL_STATS_SQL.format(schema=self._schema),
            int(new_last),
            timeout=self._query_timeout_sec,
        )
        lag = int((stats["lag_rows"] if stats else 0) or 0)
        return {
            "last_legacy_id": new_last,
            "rows_processed": new_processed,
            # Строки, которых sweep ещё не касался. Надмножество прежнего точного
            # anti-join: новые строки dual-write тоже попадают в счёт, но батч,
            # дошедший до хвоста, оставляет значение около нуля.
            "backfill_lag_rows": lag,
            "v2_coverage_pct": self._coverage_pct(
                int(new_last),
                lag,
                stats["min_id"] if stats else None,
                stats["max_id"] if stats else None,
            ),
        }

    @staticmethod
    def _coverage_pct(
        last_legacy_id: int,
        lag_rows: int,
        min_id: Optional[int],
        max_id: Optional[int],
    ) -> float:
        """Позиция watermark в диапазоне id events_history.

        min_id учитывает удаление старых строк retention-политикой: без него
        покрытие завышалось бы на величину уже вычищенного хвоста id.
        """
        if max_id is None or min_id is None or lag_rows == 0:
            return 100.0
        span = int(max_id) - int(min_id) + 1
        if span <= 0:
            return 100.0
        done = min(max(last_legacy_id - int(min_id) + 1, 0), span)
        return round(100.0 * done / span, 2)

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
            timeout=self._query_timeout_sec,
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
                timeout=self._query_timeout_sec,
            )
            if exists:
                continue
            legacy = await self._pool.fetchrow(
                f'''
                SELECT event_data FROM "{self._schema}".events_history
                WHERE id = $1
                ''',
                row["legacy_row_id"],
                timeout=self._query_timeout_sec,
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
