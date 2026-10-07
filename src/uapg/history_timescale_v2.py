"""HistoryTimescale V2 facade with typed events storage."""

from __future__ import annotations

import json
import logging
import time
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
    should_write_v2,
)

# Проба «остались ли у бэкфила неперенесённые строки»: смотрим только первые
# probe_rows строк выше watermark — ровно то, что нашёл бы следующий батч
# run_events_backfill(). Стоимость O(probe_rows) по idx_events_history_id и не
# зависит от размера events_history.
#
# Голое «EXISTS (id > watermark)» здесь не годится: dual-write кладёт новое
# событие сразу и в events_history, и в events_ts, поэтому флаг сбрасывался бы
# в false после каждой записи, хотя переносить нечего.
_EVENTS_BACKFILL_PENDING_SQL = '''
SELECT EXISTS (
    SELECT 1
    FROM (
        SELECT eh.id
        FROM "{schema}".events_history eh
        WHERE eh.id > COALESCE((
            SELECT bs.last_legacy_id
            FROM "{schema}".uapg_backfill_state bs
            WHERE bs.domain = 'events'
        ), 0)
        ORDER BY eh.id
        LIMIT $1
    ) probe
    WHERE NOT EXISTS (
        SELECT 1 FROM "{schema}".events_ts et
        WHERE et.legacy_row_id = probe.id
    )
)
'''


class HistoryTimescaleV2(HistoryTimescale):
    """Dual-write events storage with SQL filter push-down."""

    def __init__(
        self,
        *args: Any,
        events_storage_mode: Optional[StorageMode] = None,
        events_v2_config: Optional[EventsV2Config] = None,
        events_backfill_probe_rows: int = 1000,
        events_backfill_status_ttl_sec: float = 30.0,
        events_trgm_index_enabled: bool = True,
        events_trgm_index_timeout_sec: float = 300.0,
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
        # Прогресс бэкфила оценивается по watermark uapg_backfill_state, а не
        # полным anti-join по events_history: на проде это Parallel Hash Anti Join
        # по всем чанкам гипертаблицы, который не укладывается в db_query_timeout_sec.
        self._events_backfill_probe_rows = max(1, int(events_backfill_probe_rows))
        self._events_backfill_status_ttl_sec = max(0.0, float(events_backfill_status_ttl_sec))
        self._events_backfill_complete: Optional[bool] = None
        self._events_backfill_checked_at = 0.0
        self._defer_settings_refresh = False
        # btree по текстовой колонке не обслуживает ILIKE '%...%': фильтр
        # push-down упирался в полный просмотр окна по каждому типу.
        self._events_trgm_index_enabled = bool(events_trgm_index_enabled)
        self._events_trgm_index_timeout_sec = max(1.0, float(events_trgm_index_timeout_sec))
        self._trgm_extension_available = False
        self._trgm_indexes_created = 0
        self._trgm_indexes_missing = 0

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
        # events_history.id — BIGSERIAL без PK: без индекса полным сканом по всем
        # чанкам идут и keyset-батч бэкфила, и гидрация чтений (WHERE id = ANY(...)),
        # и watermark-проба готовности. _ensure_index идемпотентен и переживает
        # таймаут/lock: недостающий индекс будет создан на следующем старте.
        await self._ensure_index(
            "idx_events_history_id",
            f'CREATE INDEX idx_events_history_id ON "{self._schema}".events_history (id)',
        )
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
            query_timeout_sec=self._db_query_timeout_sec,
        )
        await self._ensure_trgm_indexes()
        self.logger.info("HistoryTimescaleV2 initialized (mode=%s)", self._events_storage_mode.value)

    def _rebind_v2_pool(self) -> None:
        """Синхронизировать кэшированные pool-ссылки V2 с актуальным self._pool.

        EventStoreV2 / ProcedureGateway / EventsBackfillWorker получают объект pool
        при init и иначе продолжают ходить в закрытый пул после _force_reconnect.
        """
        pool = self._pool
        if self._gateway is not None:
            self._gateway._pool = pool
        if self._event_store is not None:
            self._event_store._pool = pool
        if self._backfill_worker is not None:
            self._backfill_worker._pool = pool

    async def _force_reconnect(self, failed_pool=None) -> None:
        try:
            await super()._force_reconnect(failed_pool)
        finally:
            # При неудаче super() уже закрыл старый пул и оставил self._pool = None.
            # Без перепривязки EventStoreV2 продолжает acquire на закрытом объекте.
            self._rebind_v2_pool()

    async def _ensure_pool(self) -> None:
        await super()._ensure_pool()
        # Пул для записи может появиться здесь, минуя успешный _force_reconnect.
        # Если self._pool уже открыт, базовый метод сразу выходит — ссылки V2 всё равно обновляем.
        self._rebind_v2_pool()

    def _backfill_probe_timeout_sec(self) -> float:
        """Короткий бюджет служебной пробы: она не должна занимать весь query timeout."""
        base = self._db_query_timeout_sec
        if base is None or float(base) <= 0:
            return 5.0
        return min(5.0, float(base))

    async def _probe_fetchval(self, query: str, *args: Any) -> Any:
        """Best-effort fetchval для служебных проб.

        Сознательно в обход _run_db_operation/_fetchval: значение нужно только для
        витрины настроек и DEBUG-лога, поэтому его таймаут не имеет права рвать пул
        через _force_reconnect и вешать на реконнект всю историзацию.
        """
        pool = self._pool
        if pool is None:
            return None
        timeout = self._backfill_probe_timeout_sec()
        try:
            async with pool.acquire(timeout=timeout) as conn:
                return await conn.fetchval(query, *args, timeout=timeout)
        except Exception as e:
            self._perf_inc("events_backfill_probe_failures_total")
            self.logger.debug("Events backfill probe skipped: %s", e)
            return None

    async def _best_effort_execute(self, sql: str, timeout: float) -> bool:
        """Служебный DDL со своим бюджетом, без _force_reconnect при неудаче.

        Сборка GIN на большой typed-таблице не укладывается в db_query_timeout_sec,
        а _execute на таймауте пересоздаёт пул и останавливает историзацию.
        """
        pool = self._pool
        if pool is None:
            return False
        try:
            async with pool.acquire(timeout=timeout) as conn:
                await conn.execute(sql, timeout=timeout)
            return True
        except Exception as e:
            self.logger.warning("Statement skipped (%s): %r", sql.split("\n", 1)[0][:120], e)
            return False

    async def _ensure_pg_trgm(self) -> bool:
        available = bool(
            await self._probe_fetchval("SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm'")
        )
        if available:
            return True
        await self._best_effort_execute(
            "CREATE EXTENSION IF NOT EXISTS pg_trgm",
            self._backfill_probe_timeout_sec(),
        )
        return bool(
            await self._probe_fetchval("SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm'")
        )

    async def _ensure_trgm_indexes(self) -> None:
        """Догоняющий проход: GIN trgm для текстовых колонок из indexed_fields.

        _ensure_physical_table создаёт индекс по колонке только когда колонка
        только что добавлена, поэтому для уже существующих колонок (как techplace
        на стенде) индекс сам не появится.
        """
        self._trgm_extension_available = False
        self._trgm_indexes_missing = 0
        if not self._v2_ready or self._registry is None:
            return
        if not self._events_trgm_index_enabled:
            self.logger.info("Events V2 trgm indexes disabled (events_trgm_index_enabled=False)")
            return
        try:
            planned = await self._registry.plan_trgm_indexes()
        except Exception as e:
            self.logger.warning("Cannot plan events V2 trgm indexes: %r", e)
            return
        self._trgm_extension_available = await self._ensure_pg_trgm()
        if not self._trgm_extension_available:
            self._trgm_indexes_missing = len(planned)
            if planned:
                self.logger.warning(
                    "pg_trgm is not installed: %d trgm index(es) not created, "
                    "substring search (ILIKE) will scan the whole window. "
                    "Run as superuser: CREATE EXTENSION pg_trgm;",
                    len(planned),
                )
            return
        if not planned:
            self.logger.debug("Events V2 trgm indexes already in place")
            return
        failed = 0
        for item in planned:
            ok = await self._best_effort_execute(
                item["ddl"], self._events_trgm_index_timeout_sec
            )
            if ok:
                self._trgm_indexes_created += 1
            else:
                failed += 1
                self._perf_inc("events_trgm_index_failures_total")
        self._trgm_indexes_missing = failed
        self.logger.info(
            "Events V2 trgm indexes: created %d, failed %d (of %d planned)",
            self._trgm_indexes_created,
            failed,
            len(planned),
        )
        if failed:
            self.logger.warning(
                "Some trgm indexes were not created; a hot stand can build them "
                "without blocking writes, e.g.: %s",
                planned[0]["ddl"].replace("CREATE INDEX", "CREATE INDEX CONCURRENTLY", 1),
            )

    async def _is_events_backfill_complete(self, *, force: bool = False) -> bool:
        """Есть ли у бэкфила работа. Результат кэшируется на events_backfill_status_ttl_sec."""
        if not self._v2_ready or self._pool is None:
            return True
        now = time.monotonic()
        if (
            not force
            and self._events_backfill_complete is not None
            and now - self._events_backfill_checked_at < self._events_backfill_status_ttl_sec
        ):
            return self._events_backfill_complete
        has_pending = await self._probe_fetchval(
            _EVENTS_BACKFILL_PENDING_SQL.format(schema=self._schema),
            int(self._events_backfill_probe_rows),
        )
        if has_pending is None:
            # Проба не удалась — не выдаём «готово» за факт. Отметку времени всё
            # равно обновляем, иначе на медленной БД каждый HistoryRead ждал бы
            # свой таймаут пробы заново.
            fallback = (
                self._events_backfill_complete
                if self._events_backfill_complete is not None
                else False
            )
            self._events_backfill_complete = fallback
            self._events_backfill_checked_at = now
            return fallback
        self._events_backfill_complete = not bool(has_pending)
        self._events_backfill_checked_at = now
        return self._events_backfill_complete

    def get_performance_metrics(self) -> dict:
        metrics = super().get_performance_metrics()
        metrics["events_v2"] = {
            "storage_mode": self._events_storage_mode.value,
            "storage_ready": bool(self._v2_ready),
            "backfill_probe_failures_total": self._performance_counters.get(
                "events_backfill_probe_failures_total", 0
            ),
            "backfill_probe_rows": int(self._events_backfill_probe_rows),
            "backfill_status_ttl_sec": float(self._events_backfill_status_ttl_sec),
            "trgm_index_enabled": bool(self._events_trgm_index_enabled),
            "trgm_extension_available": bool(self._trgm_extension_available),
            "trgm_indexes_created_total": int(self._trgm_indexes_created),
            "trgm_indexes_missing": int(self._trgm_indexes_missing),
            "trgm_index_failures_total": self._performance_counters.get(
                "events_trgm_index_failures_total", 0
            ),
        }
        return metrics

    async def new_historized_event(
        self,
        source_id: ua.NodeId,
        evtypes: List[ua.NodeId],
        period: Any,
        count: int = 0,
    ) -> None:
        from .opc_node_id import coerce_node_id

        evtypes_raw = evtypes
        evtypes_nid = [coerce_node_id(event_type) for event_type in evtypes_raw]
        # Legacy path introspects fields from asyncua Node (see HistoryTimescale.new_historized_event).
        await super().new_historized_event(source_id, evtypes_raw, period, count)
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

        flush_timeout = self._flush_op_timeout_sec()
        for attempt in (1, 2):
            await self._ensure_pool()
            failed_pool = self._pool
            try:

                async def _op() -> None:
                    async with self._flush_on_connection(failed_pool) as conn:
                        for it in items:
                            # Always persist OPC payload via uapg_save_event_v2
                            # (events_history.event_data + events_ts.legacy_row_id).
                            # Mode=v2 previously inserted only into events_ts without
                            # event_data → HistoryRead returned empty field values.
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

                await self._run_db_operation(
                    _op(),
                    "flush event batch v2",
                    timeout=flush_timeout,
                    layer="flush",
                )
                return
            except Exception as e:
                if attempt == 1:
                    self.logger.error(
                        "Flush event batch v2 failed, will reconnect and retry: %s",
                        e,
                    )
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

        # Чтение не ждёт следующего flush: само поднимает пул и перепривязывает EventStoreV2.
        await self._ensure_pool()

        start_time, end_time, order, limit = self._get_bounds(start, end, nb_values)
        source_db_id = await self._resolve_source_db_id(source_id)
        if source_db_id is None:
            return [], None

        partial = False
        if self._backfill_worker:
            # Раньше здесь был anti-join по всем строкам источника на каждый
            # HistoryRead. Оценка стала глобальной и кэшированной: она консервативнее
            # (partial=true, если отстал любой источник) и влияет только на DEBUG-лог.
            partial = not await self._is_events_backfill_complete()

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
        # Базовый expose сам зовёт refresh, и через виртуальную диспетчеризацию
        # попадает в V2-override — вместе с refresh после capability-узлов это
        # давало две пробы бэкфила на один старт. Откладываем до конца.
        self._defer_settings_refresh = True
        try:
            await super().expose_history_settings_nodes(server, namespace_index, parent=parent)
            await self._expose_events_v2_capability_nodes(server, namespace_index, parent=parent)
        finally:
            self._defer_settings_refresh = False
        await self.refresh_history_settings_nodes()

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
        if self._defer_settings_refresh:
            return
        await super().refresh_history_settings_nodes()
        nodes = self._opcua_history_settings_nodes
        if not nodes:
            return
        sql_supported = (
            self._v2_ready
            and self._events_storage_mode != StorageMode.LEGACY
            and bool(self._events_v2_config.sql_filter_fields)
        )
        backfill_complete = await self._is_events_backfill_complete()

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
