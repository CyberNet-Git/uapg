"""HistoryTimescale V2 facade with typed events storage."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

from asyncua import ua

from .history_timescale import EventWriteItem, HistoryTimescale
from .v2.backfill_worker import EventsBackfillWorker
from .v2.event_store import EventStoreV2
from .v2.events_config import EventsV2Config
from .v2.procedure_gateway import ProcedureGateway
from .v2.schema_registry import EventSchemaRegistry
from .v2.sql_migrator import SqlMigrator
from .v2.storage_mode import (
    StorageMode,
    get_events_storage_mode,
    should_read_v2,
    should_write_legacy,
    should_write_v2,
)


class HistoryTimescaleV2(HistoryTimescale):
    """Dual-write events storage with SQL filter push-down."""

    def __init__(
        self,
        *args: Any,
        events_storage_mode: Optional[StorageMode] = None,
        events_v2_config: Optional[EventsV2Config] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._events_storage_mode = events_storage_mode or get_events_storage_mode()
        self._events_v2_config = events_v2_config or EventsV2Config()
        self._v2_ready = False
        self._gateway: Optional[ProcedureGateway] = None
        self._registry: Optional[EventSchemaRegistry] = None
        self._event_store: Optional[EventStoreV2] = None
        self._backfill_worker: Optional[EventsBackfillWorker] = None
        self._typed_tables: Dict[int, str] = {}
        self._schema_versions: Dict[int, int] = {}

    @property
    def events_storage_mode(self) -> StorageMode:
        return self._events_storage_mode

    async def init(self) -> None:
        await super().init()
        if self._events_storage_mode == StorageMode.LEGACY:
            return
        migrator = SqlMigrator(
            self._schema,
            self._execute,
            self._fetch,
            self._fetchval,
            self.logger,
        )
        await migrator.apply_all()
        self._v2_ready = await migrator.detect_v2_ready()
        if not self._v2_ready:
            self.logger.warning("Events V2 tables not ready; falling back to legacy semantics")
            return
        await self._ensure_pool()
        self._gateway = ProcedureGateway(self._schema, self._pool, self.logger)
        self._registry = EventSchemaRegistry(
            self._schema,
            self._execute,
            self._fetch,
            self._fetchrow,
            self._fetchval,
            self.logger,
            events_config=self._events_v2_config,
        )
        self._event_store = EventStoreV2(
            self._schema,
            self._pool,
            self._registry,
            self._gateway,
            self.logger,
        )
        self._backfill_worker = EventsBackfillWorker(
            self._schema,
            self._pool,
            self._gateway,
            self._registry,
            self._binary_map_to_event_values,
            self.logger,
        )
        self.logger.info("HistoryTimescaleV2 initialized (mode=%s)", self._events_storage_mode.value)

    async def new_historized_event(
        self,
        source_id: ua.NodeId,
        evtypes: List[ua.NodeId],
        period: Any,
        count: int = 0,
    ) -> None:
        from .opc_node_id import coerce_node_id

        source_nid = coerce_node_id(source_id)
        evtypes_raw = evtypes
        evtypes_nid = [coerce_node_id(event_type) for event_type in evtypes_raw]
        await super().new_historized_event(source_nid, evtypes_nid, period, count)
        if not should_write_v2(self._events_storage_mode) or not self._registry or not self._gateway:
            return
        for raw_event_type in evtypes_raw:
            event_type_nid = coerce_node_id(raw_event_type)
            event_type_name = self._format_node_id(event_type_nid)
            event_db_id = self._event_type_cache.get(event_type_name)
            if event_db_id is None:
                event_db_id = await self._fetchval(
                    f'''
                    SELECT event_type_id FROM "{self._schema}".event_types
                    WHERE event_type_name = $1
                    LIMIT 1
                    ''',
                    event_type_name,
                )
            if event_db_id is None:
                continue
            fields = await self._registry.introspect_fields([raw_event_type], self._get_event_fields)
            table, schema_version = await self._registry.sync_event_type(
                int(event_db_id),
                event_type_nid,
                None,
                fields,
                self._gateway,
            )
            self._typed_tables[int(event_db_id)] = table
            self._schema_versions[int(event_db_id)] = schema_version

    async def _flush_event_batch(self, items: List[EventWriteItem]) -> None:
        if not items:
            return
        if not should_write_v2(self._events_storage_mode) or not self._event_store:
            await super()._flush_event_batch(items)
            return

        for attempt in (1, 2):
            await self._ensure_pool()
            failed_pool = self._pool
            try:

                async def _op() -> None:
                    async with failed_pool.acquire(timeout=self._db_query_timeout_sec) as conn:
                        async with conn.transaction():
                            for it in items:
                                if should_write_legacy(self._events_storage_mode):
                                    typed_values = self._typed_values_from_json(it.event_data_json)
                                    table = self._typed_tables.get(it.event_type_id)
                                    if table is None and self._registry:
                                        table = await self._registry.get_storage_table(it.event_type_id)
                                    schema_version = self._schema_versions.get(it.event_type_id, 1)
                                    gateway = ProcedureGateway(self._schema, conn, self.logger)
                                    store = EventStoreV2(
                                        self._schema,
                                        conn,
                                        self._registry,
                                        gateway,
                                        self.logger,
                                    )
                                    await store.save_event_dual(
                                        it.source_db_id,
                                        it.event_type_id,
                                        it.event_timestamp,
                                        it.event_data_json,
                                        typed_values,
                                        table,
                                        schema_version,
                                    )
                                else:
                                    await conn.execute(
                                        f'''
                                        INSERT INTO "{self._schema}".events_ts
                                        (source_id, event_type_id, event_timestamp, schema_version)
                                        VALUES ($1, $2, $3, $4)
                                        ''',
                                        it.source_db_id,
                                        it.event_type_id,
                                        it.event_timestamp,
                                        self._schema_versions.get(it.event_type_id, 1),
                                    )

                await self._run_db_operation(_op(), "flush event batch v2")
                return
            except Exception as e:
                if attempt == 1:
                    self.logger.error("Flush event batch v2 failed, reconnecting: %s", e)
                    await self._force_reconnect(failed_pool)
                else:
                    self.logger.error("Flush event batch v2 failed after reconnect: %s", e)
                    raise

    async def save_event(self, event: Any) -> None:
        if not should_write_v2(self._events_storage_mode) or not self._event_store:
            await super().save_event(event)
            return

        if event is None or not hasattr(event, "SourceNode") or event.SourceNode is None:
            self.logger.error("save_event: invalid event")
            return
        event_type = getattr(event, "EventType", None)
        if event_type is None:
            self.logger.error("save_event: event.EventType is None")
            return

        source_data = self._datachanges_period.get(event.SourceNode)
        source_db_id = None
        event_db_id = None
        if source_data and len(source_data) == 4:
            _, _, source_db_id, event_ids = source_data
            event_db_id = event_ids.get(event_type, (None, None))[1]

        if source_db_id is None or event_db_id is None:
            await super().save_event(event)
            return

        event_time = getattr(event, "Time", None) or getattr(event, "time", None) or datetime.now(timezone.utc)
        raw_event_data = (
            event.get_event_props_as_fields_dict()
            if hasattr(event, "get_event_props_as_fields_dict")
            else {}
        )
        bin_event_data = self._event_to_binary_map(raw_event_data)
        event_data_json = json.dumps(bin_event_data)
        typed_values = self._event_store.extract_typed_values(raw_event_data)
        table = self._typed_tables.get(int(event_db_id))
        if table is None and self._registry:
            table = await self._registry.get_storage_table(int(event_db_id))

        if self._history_write_batch_enabled and self._event_write_buffer is not None:
            source_node_id_str = self._format_node_id(event.SourceNode)
            group_key = self._build_group_key_from_node_id(source_node_id_str)
            item = EventWriteItem(
                source_db_id=source_db_id,
                event_type_id=event_db_id,
                event_timestamp=event_time,
                event_data_json=event_data_json,
                group_key=group_key,
            )
            sync = self._history_write_read_consistency_mode == "global"
            await self._event_write_buffer.enqueue(item, sync=sync)
            return

        await self._event_store.save_event_dual(
            source_db_id,
            event_db_id,
            event_time,
            event_data_json,
            typed_values,
            table,
            self._schema_versions.get(int(event_db_id), 1),
        )

    async def read_event_history(
        self,
        source_id: ua.NodeId,
        start: Any,
        end: Any,
        nb_values: Optional[int],
        evfilter: Any,
    ) -> Tuple[List[Any], Optional[datetime]]:
        if not should_read_v2(self._events_storage_mode) or not self._event_store:
            return await super().read_event_history(source_id, start, end, nb_values, evfilter)

        start_time, end_time, order, limit = self._get_bounds(start, end, nb_values)
        source_db_id = await self._resolve_source_db_id(source_id)
        if source_db_id is None:
            return [], None

        partial = False
        if self._backfill_worker:
            lag = await self._pool.fetchval(
                f'''
                SELECT count(*)::bigint
                FROM "{self._schema}".events_history eh
                WHERE eh.source_id = $1
                  AND NOT EXISTS (
                      SELECT 1 FROM "{self._schema}".events_ts et
                      WHERE et.legacy_row_id = eh.id
                  )
                ''',
                source_db_id,
            )
            partial = int(lag or 0) > 0

        results, cont, is_partial = await self._event_store.read_events(
            source_db_id,
            start_time,
            end_time,
            limit,
            order,
            evfilter,
            self._binary_map_to_event_values,
            continuation=None,
            partial=partial,
        )
        if is_partial and self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("read_event_history v2 partial=true (backfill incomplete)")
        opc_cont: Optional[Union[datetime, Tuple[datetime, int]]] = cont[0] if cont else None
        return results, opc_cont

    async def run_events_backfill(self, batch_size: int = 500) -> Dict[str, int]:
        if not self._backfill_worker:
            return {"backfill_lag_rows": -1, "v2_coverage_pct": 0.0}
        return await self._backfill_worker.run_batch(batch_size)

    async def explain_event_filter(
        self,
        source_id: ua.NodeId,
        start: Any,
        end: Any,
        nb_values: Optional[int],
        evfilter: Any,
    ) -> str:
        if not self._gateway:
            return ""
        start_time, end_time, order, limit = self._get_bounds(start, end, nb_values)
        source_db_id = await self._resolve_source_db_id(source_id)
        if source_db_id is None:
            return ""
        from .v2.filter_planner import EventFilterPlanner

        planner = EventFilterPlanner(field_aliases=self._events_v2_config.field_aliases)
        plan = planner.build(evfilter)
        event_type_ids = None
        pinned_event_type_ids = None
        if self._event_store:
            event_type_ids = await self._event_store._resolve_event_type_ids(planner, plan)
            pinned_event_type_ids = event_type_ids
        else:
            event_type_ids = planner.extract_event_type_ids(plan)
        allowed = None
        if self._registry and event_type_ids:
            allowed = await self._registry.get_allowed_fields(event_type_ids)
            planner = EventFilterPlanner(
                allowed_fields=allowed or None,
                field_aliases=self._events_v2_config.field_aliases,
            )
            plan = planner.build(evfilter)
            if self._event_store:
                event_type_ids = (
                    await self._event_store._resolve_event_type_ids(planner, plan)
                    or pinned_event_type_ids
                )
            else:
                event_type_ids = planner.extract_event_type_ids(plan)
        return await self._gateway.explain_event_filter(
            source_db_id, start_time, end_time, limit, order, event_type_ids
        )

    async def _resolve_source_db_id(self, source_id: ua.NodeId) -> Optional[int]:
        source_data = self._datachanges_period.get(source_id)
        if source_data and len(source_data) == 4:
            return int(source_data[2])
        source_node_id_str = self._format_node_id(source_id)
        cached_sid = self._event_source_cache.get(source_node_id_str)
        if cached_sid is not None:
            return int(cached_sid)
        source_db_id = await self._fetchval(
            f'''
            SELECT source_id FROM "{self._schema}".event_sources
            WHERE source_node_id = $1
            LIMIT 1
            ''',
            source_node_id_str,
        )
        if source_db_id is not None:
            self._event_source_cache[source_node_id_str] = int(source_db_id)
        return int(source_db_id) if source_db_id is not None else None

    def _typed_values_from_json(self, event_data_json: str) -> Dict[str, Any]:
        data = json.loads(event_data_json)
        values = self._binary_map_to_event_values(data)
        return {k: (v.Value if hasattr(v, "Value") else v) for k, v in values.items()}

    async def expose_history_settings_nodes(
        self,
        server: Any,
        namespace_index: int,
        *,
        parent: Any = None,
    ) -> None:
        await super().expose_history_settings_nodes(server, namespace_index, parent=parent)
        await self._expose_events_v2_capability_nodes(server, namespace_index, parent=parent)

    async def _expose_events_v2_capability_nodes(
        self,
        server: Any,
        namespace_index: int,
        *,
        parent: Any = None,
    ) -> None:
        if server is None:
            return
        idx = int(namespace_index)
        nodes = self._opcua_history_settings_nodes
        if not nodes:
            return

        async def _get_or_add_variable(parent_node: Any, name: str, initial: ua.Variant) -> Any:
            qn = f"{idx}:{name}"
            try:
                return await parent_node.get_child([qn])
            except Exception:
                return await parent_node.add_variable(idx, name, initial)

        settings_parent = None
        try:
            history_node = nodes.get("UapgVersion")
            if history_node is not None:
                settings_parent = await history_node.get_parent()
        except Exception:
            settings_parent = None
        if settings_parent is None:
            return

        cap_nodes = {
            "EventsStorageVersion": ua.Variant("v1", ua.VariantType.String),
            "EventsStorageMode": ua.Variant("legacy", ua.VariantType.String),
            "EventsSqlFilterSupported": ua.Variant(False, ua.VariantType.Boolean),
            "EventsSqlFilterFields": ua.Variant("", ua.VariantType.String),
            "EventsBackfillComplete": ua.Variant(True, ua.VariantType.Boolean),
        }
        for name, variant in cap_nodes.items():
            if name not in nodes:
                nodes[name] = await _get_or_add_variable(settings_parent, name, variant)

        await self.refresh_history_settings_nodes()

    async def refresh_history_settings_nodes(self) -> None:
        await super().refresh_history_settings_nodes()
        nodes = self._opcua_history_settings_nodes
        if not nodes:
            return
        sql_supported = (
            self._v2_ready
            and self._events_storage_mode != StorageMode.LEGACY
            and bool(self._events_v2_config.sql_filter_fields)
        )
        backfill_complete = True
        if self._v2_ready and self._pool:
            try:
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
                backfill_complete = int(lag or 0) == 0
            except Exception:
                backfill_complete = False

        values = {
            "EventsStorageVersion": ua.Variant("v2" if self._v2_ready else "v1", ua.VariantType.String),
            "EventsStorageMode": ua.Variant(self._events_storage_mode.value, ua.VariantType.String),
            "EventsSqlFilterSupported": ua.Variant(sql_supported, ua.VariantType.Boolean),
            "EventsSqlFilterFields": ua.Variant(
                self._events_v2_config.sql_filter_fields_csv() if sql_supported else "",
                ua.VariantType.String,
            ),
            "EventsBackfillComplete": ua.Variant(backfill_complete, ua.VariantType.Boolean),
        }
        for key, variant in values.items():
            node = nodes.get(key)
            if node is None:
                continue
            try:
                await node.write_value(variant)
            except Exception:
                continue
