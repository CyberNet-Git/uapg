"""Хранение истории переменных.

Здесь собрано всё обращение к ``variables_history``, ``variables_last_value`` и
``variable_metadata``: регистрация переменных, батчевая запись, чтение
диапазона и последних значений.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple, cast

import asyncpg
from asyncua import ua

from ..codec import encode_variant, row_to_datavalue, status_code_to_column
from ..core.database import Database
from ..core.metrics import DomainMetrics
from ..core.sql import load_queries
from .items import VariableWriteItem


class VariableRepository:
    """Доступ к истории переменных."""

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
        self.logger = logger or logging.getLogger("uapg.storage.variables")
        self._sql = load_queries("variables.sql", schema)

    # ------------------------------------------------------------------ метаданные

    async def ensure_metadata(
        self,
        node_id_str: str,
        *,
        data_type: Optional[str] = None,
        retention_period: Optional[timedelta] = None,
        max_records: int = 0,
    ) -> int:
        variable_id = await self._db.fetchval(
            self._sql["upsert_metadata"],
            node_id_str,
            data_type,
            retention_period,
            max_records,
        )
        return int(variable_id)

    async def ensure_metadata_many(
        self,
        node_ids: Sequence[str],
        *,
        retention_period: Optional[timedelta] = None,
        max_records: int = 0,
    ) -> Dict[str, int]:
        """Зарегистрировать несколько переменных одним запросом."""
        if not node_ids:
            return {}
        rows = await self._db.fetch(
            self._sql["upsert_metadata_many"],
            list(node_ids),
            [None] * len(node_ids),
            [retention_period] * len(node_ids),
            [max_records] * len(node_ids),
        )
        return {row["node_id"]: int(row["variable_id"]) for row in rows}

    async def find_metadata(self, node_id_str: str) -> Optional[asyncpg.Record]:
        return await self._db.fetchrow(self._sql["find_metadata"], node_id_str)

    async def load_metadata_cache(self, max_rows: int) -> Dict[str, int]:
        rows = await self._db.fetch(self._sql["load_metadata_cache"], max_rows)
        return {row["node_id"]: int(row["variable_id"]) for row in rows}

    # ------------------------------------------------------------------ запись

    async def flush(self, items: List[VariableWriteItem]) -> None:
        """Записать пачку значений.

        История и кэш последних значений обновляются одной транзакцией: иначе
        при обрыве между ними кэш начинает противоречить истории.
        """
        if not items:
            return

        history_rows = [
            (
                item.variable_id,
                item.server_timestamp,
                item.source_timestamp,
                _status_for_column(item.status_code),
                item.value_str,
                item.variant_type,
                item.variant_binary,
            )
            for item in items
        ]
        last_value_rows = [
            (
                item.variable_id,
                item.source_timestamp,
                item.server_timestamp,
                _status_for_column(item.status_code),
                item.variant_type,
                item.variant_binary,
            )
            for item in items
        ]

        insert_sql = self._sql["insert_history"]
        upsert_sql = self._sql["upsert_last_value"]
        metrics = self._metrics

        async def _write(conn: asyncpg.Connection) -> None:
            started = time.perf_counter()
            await conn.executemany(insert_sql, history_rows)
            metrics.insert_history.observe((time.perf_counter() - started) * 1000.0)

            started = time.perf_counter()
            await conn.executemany(upsert_sql, last_value_rows)
            metrics.upsert_last_value.observe((time.perf_counter() - started) * 1000.0)

        started_at = time.perf_counter()
        await self._db.run_in_transaction(_write, name="запись значений")
        metrics.flush.observe((time.perf_counter() - started_at) * 1000.0)

    async def write_one(self, item: VariableWriteItem) -> None:
        """Записать одно значение без батчинга."""
        await self.flush([item])

    async def seed_last_values(self, items: Sequence[Tuple[int, ua.DataValue]]) -> int:
        """Создать строки-заглушки для переменных, у которых значения ещё не было."""
        if not items:
            return 0

        empty = encode_variant(ua.Variant(None))
        now = datetime.now(tz=None).astimezone()
        variable_ids: List[int] = []
        source_ts: List[datetime] = []
        server_ts: List[datetime] = []
        status_codes: List[int] = []
        variant_types: List[int] = []
        payloads: List[bytes] = []

        for variable_id, datavalue in items:
            variant = datavalue.Value if datavalue is not None else None
            variable_ids.append(int(variable_id))
            source_ts.append((datavalue.SourceTimestamp if datavalue else None) or now)
            server_ts.append((datavalue.ServerTimestamp if datavalue else None) or now)
            status_codes.append(
                status_code_to_column(datavalue.StatusCode if datavalue else None)
            )
            variant_types.append(int(variant.VariantType) if variant is not None else 0)
            payloads.append(encode_variant(variant) if variant is not None else empty)

        result = await self._db.execute(
            self._sql["seed_last_values"],
            variable_ids,
            source_ts,
            server_ts,
            status_codes,
            variant_types,
            payloads,
        )
        return _affected_rows(result)

    async def delete_history(
        self,
        variable_id: int,
        start: datetime,
        end: datetime,
    ) -> int:
        result = await self._db.execute(self._sql["delete_history"], variable_id, start, end)
        return _affected_rows(result)

    # ------------------------------------------------------------------ чтение

    async def read_history(
        self,
        variable_id: int,
        start: datetime,
        end: datetime,
        limit: int,
        order: str,
    ) -> List[ua.DataValue]:
        query = "read_history_desc" if order.upper() == "DESC" else "read_history_asc"
        rows = await self._db.fetch(self._sql[query], variable_id, start, end, limit)
        return [row_to_datavalue(row) for row in rows]

    async def read_history_rows(
        self,
        variable_id: int,
        start: datetime,
        end: datetime,
        limit: int,
        order: str,
    ) -> List[asyncpg.Record]:
        """То же чтение, но строками: нужно там, где важна метка времени последней."""
        query = "read_history_desc" if order.upper() == "DESC" else "read_history_asc"
        return await self._db.fetch(self._sql[query], variable_id, start, end, limit)

    async def read_last_value(self, variable_id: int) -> Optional[ua.DataValue]:
        row = await self._db.fetchrow(self._sql["read_last_value"], variable_id)
        if row is None:
            return None
        return row_to_datavalue(row)

    async def read_last_values(self, variable_ids: Sequence[int]) -> Dict[int, ua.DataValue]:
        if not variable_ids:
            return {}
        rows = await self._db.fetch(self._sql["read_last_values_many"], list(variable_ids))
        return {int(row["variable_id"]): row_to_datavalue(row) for row in rows}

    async def read_latest_from_history(self, variable_id: int) -> Optional[ua.DataValue]:
        row = await self._db.fetchrow(self._sql["read_latest_from_history"], variable_id)
        return row_to_datavalue(row) if row is not None else None

    async def iter_last_values(self, batch_size: int) -> Dict[int, ua.DataValue]:
        """Загрузить кэш последних значений постранично.

        Постранично, а не одним запросом: на крупных инсталляциях строк сотни
        тысяч, и единичный fetch занимает и память, и соединение.
        """
        values: Dict[int, ua.DataValue] = {}
        last_id = 0
        while True:
            rows = await self._db.fetch(
                self._sql["load_last_values_page"], last_id, batch_size
            )
            if not rows:
                break
            for row in rows:
                variable_id = int(row["variable_id"])
                values[variable_id] = row_to_datavalue(row)
                last_id = variable_id
        return values


def _status_for_column(code: int) -> int:
    """Код качества из элемента очереди в вид, который принимает колонка."""
    return status_code_to_column(ua.StatusCode(cast(Any, int(code) & 0xFFFFFFFF)))


def _affected_rows(result: Any) -> int:
    """Число строк из статуса команды PostgreSQL вида 'INSERT 0 5'."""
    try:
        return int(str(result).split()[-1])
    except (ValueError, IndexError):
        return 0
