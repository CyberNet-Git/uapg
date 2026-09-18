"""Хранение истории событий.

Событие хранится в двух видах сразу. Полный состав полей лежит в
``events_history.event_data`` — только из него можно восстановить событие для
HistoryRead. Слой поиска (``events_ts`` и типизированные таблицы) хранит
отдельные поля колонками, чтобы фильтровать на стороне БД, а не в памяти после
выборки.

Из этого следует правило, которое легко нарушить: ``events_history`` — не
устаревшая таблица, от которой можно избавиться. Пока состав полей не
восстанавливается из типизированных колонок, она остаётся единственным местом,
где событие хранится целиком.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

import asyncpg

from ..codec import decode_event_data
from ..core.database import Database
from ..core.metrics import DomainMetrics
from ..core.sql import load_queries
from .items import EventWriteItem


class EventRepository:
    """Доступ к истории событий."""

    def __init__(
        self,
        database: Database,
        schema: str,
        metrics: DomainMetrics,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._db = database
        self._schema = schema
        self._metrics = metrics
        self.logger = logger or logging.getLogger("uapg.storage.events")
        self._sql = load_queries("events.sql", schema)

    # ------------------------------------------------------------------ метаданные

    async def ensure_source(
        self,
        source_node_id: str,
        *,
        retention_period: Optional[timedelta] = None,
        max_records: int = 0,
    ) -> int:
        source_id = await self._db.fetchval(
            self._sql["upsert_source"], source_node_id, retention_period, max_records
        )
        if source_id is None:
            source_id = await self._db.fetchval(self._sql["find_source"], source_node_id)
        return int(source_id)

    async def ensure_type(self, event_type_name: str) -> int:
        event_type_id = await self._db.fetchval(self._sql["upsert_type"], event_type_name)
        if event_type_id is None:
            event_type_id = await self._db.fetchval(self._sql["find_type"], event_type_name)
        return int(event_type_id)

    async def find_source(self, source_node_id: str) -> Optional[int]:
        value = await self._db.fetchval(self._sql["find_source"], source_node_id)
        return int(value) if value is not None else None

    async def find_type(self, event_type_name: str) -> Optional[int]:
        value = await self._db.fetchval(self._sql["find_type"], event_type_name)
        return int(value) if value is not None else None

    async def load_source_cache(self) -> Dict[str, int]:
        rows = await self._db.fetch(self._sql["load_source_cache"])
        return {row["source_node_id"]: int(row["source_id"]) for row in rows}

    async def load_type_cache(self) -> Dict[str, int]:
        rows = await self._db.fetch(self._sql["load_type_cache"])
        return {row["event_type_name"]: int(row["event_type_id"]) for row in rows}

    # ------------------------------------------------------------------ запись

    async def flush(self, items: List[EventWriteItem]) -> None:
        """Записать пачку событий в устаревшее хранение."""
        if not items:
            return

        rows = [
            (item.source_db_id, item.event_type_id, item.event_timestamp, item.event_data_json)
            for item in items
        ]
        insert_sql = self._sql["insert_history"]
        metrics = self._metrics

        async def _write(conn: asyncpg.Connection) -> None:
            started = time.perf_counter()
            await conn.executemany(insert_sql, rows)
            metrics.insert_history.observe((time.perf_counter() - started) * 1000.0)

        started_at = time.perf_counter()
        await self._db.run_in_transaction(_write, name="запись событий")
        metrics.flush.observe((time.perf_counter() - started_at) * 1000.0)

    async def delete_history(self, source_id: int, start: datetime, end: datetime) -> int:
        result = await self._db.execute(self._sql["delete_history"], source_id, start, end)
        try:
            return int(str(result).split()[-1])
        except (ValueError, IndexError):
            return 0

    # ------------------------------------------------------------------ чтение

    async def read_history_rows(
        self,
        source_id: int,
        start: datetime,
        end: datetime,
        limit: int,
        order: str,
    ) -> List[asyncpg.Record]:
        query = "read_history_desc" if order.upper() == "DESC" else "read_history_asc"
        return await self._db.fetch(self._sql[query], source_id, start, end, limit)

    async def read_payloads(self, legacy_ids: Sequence[int]) -> Dict[int, Dict[str, Any]]:
        """Состав полей событий по идентификаторам строк устаревшего хранения."""
        if not legacy_ids:
            return {}
        rows = await self._db.fetch(self._sql["read_payloads_by_legacy_ids"], list(legacy_ids))
        return {int(row["id"]): decode_payload(row["event_data"]) for row in rows}

    async def backfill_lag(self, source_id: Optional[int] = None) -> int:
        """Сколько событий ещё не попало в слой поиска."""
        value = await self._db.fetchval(self._sql["backfill_lag"], source_id)
        return int(value or 0)


def decode_payload(raw: Any) -> Dict[str, Any]:
    """Разобрать event_data в поля события.

    JSONB приходит строкой, пока не зарегистрирован кодек asyncpg, поэтому
    разбор делается здесь, а не в вызывающем коде.
    """
    if raw is None:
        return {}
    data = json.loads(raw) if isinstance(raw, str) else raw
    if not isinstance(data, dict):
        return {}
    return decode_event_data(data)
