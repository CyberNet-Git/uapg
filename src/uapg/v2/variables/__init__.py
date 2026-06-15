"""Variables V2 roadmap skeleton."""

from __future__ import annotations

from typing import Any, Dict


class AggregatePlanBuilder:
    """Build AggregatePlan JSON from OPC UA ReadProcessedDetails (stub)."""

    @staticmethod
    def from_processed_details(details: Any, variable_id: int) -> Dict[str, Any]:
        aggregate = "Average"
        interval_ms = 60000
        if details is not None:
            if hasattr(details, "AggregateType") and details.AggregateType is not None:
                aggregate = str(details.AggregateType.name)
            if hasattr(details, "ProcessingInterval"):
                interval_ms = int(details.ProcessingInterval or interval_ms)
        return {
            "mode": "processed",
            "aggregate": aggregate,
            "variable_id": variable_id,
            "processing_interval_ms": interval_ms,
        }


class VariablesStoreV2:
    """Placeholder for variables dual-write (release 2+)."""

    def __init__(self, schema: str, pool: Any) -> None:
        self._schema = schema
        self._pool = pool

    async def is_ready(self) -> bool:
        row = await self._pool.fetchval(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = 'variables_ts'
            LIMIT 1
            """,
            self._schema,
        )
        return row is not None
