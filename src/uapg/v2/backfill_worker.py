"""Backfill legacy events_history into events_ts + typed tables."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional, Tuple

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

# Ведущий запрос typed-бэкфила. До 0.2.17 здесь было `ORDER BY legacy_row_id DESC
# LIMIT n` без курсора: каждый вызов брал одни и те же свежие строки и до старых не
# доходил никогда. Восходящий курсор обслуживается частичным индексом
# idx_events_ts_legacy_row, physical_table приходит из JOIN, а существование таблицы
# проверяется to_regclass вместо коррелированного подзапроса к information_schema.
_TYPED_ROWS_SQL = '''
SELECT et.legacy_row_id, et.event_id, et.event_timestamp, et.source_id,
       et.event_type_id, ets.physical_table
FROM "{schema}".events_ts et
JOIN "{schema}".event_type_storage ets ON ets.event_type_id = et.event_type_id
WHERE et.legacy_row_id > $1
  AND to_regclass(format('%I.%I', $2::text, ets.physical_table)) IS NOT NULL
ORDER BY et.legacy_row_id
LIMIT $3
'''

_LEGACY_PAYLOAD_SQL = '''
SELECT id, event_data
FROM "{schema}".events_history
WHERE id = ANY($1::bigint[])
'''

_SET_STATE_SQL = '''
INSERT INTO "{schema}".uapg_backfill_state (domain, last_legacy_id, rows_processed)
VALUES ($1, $2, $3)
ON CONFLICT (domain) DO UPDATE SET
    last_legacy_id = EXCLUDED.last_legacy_id,
    rows_processed = EXCLUDED.rows_processed,
    updated_at = NOW()
'''

# Курсор typed-бэкфила живёт в отдельном domain уже существующей uapg_backfill_state.
_TYPED_DOMAIN = "events_typed"


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

    async def get_state(self, domain: str = "events") -> Tuple[int, int]:
        row = await self._pool.fetchrow(
            f'''
            SELECT last_legacy_id, rows_processed
            FROM "{self._schema}".uapg_backfill_state
            WHERE domain = $1
            ''',
            domain,
            timeout=self._query_timeout_sec,
        )
        if row is None:
            return 0, 0
        return int(row["last_legacy_id"]), int(row["rows_processed"])

    async def _set_state(self, domain: str, last_legacy_id: int, rows_processed: int) -> None:
        await self._pool.execute(
            _SET_STATE_SQL.format(schema=self._schema),
            domain,
            int(last_legacy_id),
            int(rows_processed),
            timeout=self._query_timeout_sec,
        )

    async def run_batch(self, batch_size: int = 500) -> Dict[str, Any]:
        last_id, processed = await self.get_state()
        new_last, new_processed = await self._gateway.backfill_events_batch(
            batch_size, last_id, processed, timeout=self._query_timeout_sec
        )
        typed_inserted, typed_failed = await self._backfill_typed_rows(batch_size)
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
            "typed_rows_inserted": typed_inserted,
            "typed_rows_failed": typed_failed,
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

    async def _backfill_typed_rows(self, batch_size: int) -> Tuple[int, int]:
        """Перенос очередной порции events_ts → typed-таблицы.

        Возвращает (вставлено, не удалось). Проход идёт по восходящему курсору
        uapg_backfill_state[events_typed]; дойдя до хвоста, курсор сбрасывается в 0,
        чтобы следующий круг подхватил строки типов, у которых typed-таблица
        появилась позже. Повторная вставка безвредна: insert_typed_row делает
        ON CONFLICT (event_id, event_timestamp) DO NOTHING.
        """
        cursor, _ = await self.get_state(_TYPED_DOMAIN)
        rows = await self._pool.fetch(
            _TYPED_ROWS_SQL.format(schema=self._schema),
            int(cursor),
            self._schema,
            batch_size,
            timeout=self._query_timeout_sec,
        )
        if not rows:
            if cursor:
                await self._set_state(_TYPED_DOMAIN, 0, 0)
            return 0, 0

        # event_data на весь батч одним запросом вместо N отдельных WHERE id = $1
        # (индекс idx_events_history_id).
        payloads = await self._pool.fetch(
            _LEGACY_PAYLOAD_SQL.format(schema=self._schema),
            [int(r["legacy_row_id"]) for r in rows],
            timeout=self._query_timeout_sec,
        )
        payload_by_id = {int(r["id"]): r["event_data"] for r in payloads}

        decoded: List[Tuple[Any, str, Dict[str, Any]]] = []
        failed = 0
        for row in rows:
            data = payload_by_id.get(int(row["legacy_row_id"]))
            if data is None:
                continue
            try:
                if isinstance(data, str):
                    data = json.loads(data)
                values = self._binary_map_to_values(data)
                typed = {
                    k: (v.Value if hasattr(v, "Value") else v) for k, v in values.items()
                }
            except Exception as e:
                failed += 1
                self._logger.warning(
                    "Typed backfill: cannot decode event_data for legacy id=%s: %r",
                    row["legacy_row_id"],
                    e,
                )
                continue
            decoded.append((row, str(row["physical_table"]), typed))

        # Колонки досоздаются один раз на таблицу по объединению ключей батча:
        # иначе _ensure_physical_table (advisory lock + DDL + information_schema)
        # выполнялся бы на каждую строку.
        merged: Dict[str, Dict[str, Any]] = {}
        for _row, table, typed in decoded:
            merged.setdefault(table, {}).update(typed)
        for table, typed_values in merged.items():
            try:
                await self._registry.ensure_columns_from_typed_values(table, typed_values)
            except Exception as e:
                self._logger.warning(
                    "Typed backfill: cannot ensure columns of %s: %r", table, e
                )

        inserted = 0
        for row, table, typed in decoded:
            try:
                await self._registry.insert_typed_row(
                    table,
                    int(row["event_id"]),
                    row["event_timestamp"],
                    int(row["source_id"]),
                    typed,
                    ensure_columns=False,
                )
                inserted += 1
            except Exception as e:
                failed += 1
                self._logger.warning(
                    "Typed backfill: insert into %s failed for event_id=%s: %r",
                    table,
                    row["event_id"],
                    e,
                )

        if len(rows) < batch_size:
            # Хвост достигнут — начинаем круг заново.
            await self._set_state(_TYPED_DOMAIN, 0, 0)
        else:
            await self._set_state(
                _TYPED_DOMAIN, max(int(r["legacy_row_id"]) for r in rows), 0
            )
        return inserted, failed
