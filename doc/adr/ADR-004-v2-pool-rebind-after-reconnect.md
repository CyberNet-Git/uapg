# ADR-004: Rebind V2 pool refs after reconnect

## Status

Accepted — 2026-07-24

## Context

`HistoryTimescaleV2` создаёт `EventStoreV2`, `ProcedureGateway` и `EventsBackfillWorker` с прямой ссылкой на объект `asyncpg.Pool` при `init()`.

`HistoryTimescale._force_reconnect` закрывает старый pool и создаёт новый в `self._pool`. Value history идёт через `_fetch` / `_ensure_pool` и всегда берёт актуальный `self._pool`. Event history V2 ходит через кэшированные `_pool` в helpers → `InterfaceError: pool is closed` при живой истории переменных.

## Decision

После каждого `_force_reconnect` в `HistoryTimescaleV2` вызывать `_rebind_v2_pool()`, обновляя `_pool` у gateway / event_store / backfill_worker.

Lag-запросы перевести на `_fetchval`, чтобы использовать timeout/reconnect wrappers родителя.

## Consequences

- Event HistoryRead после timeout/reconnect снова работает без рестарта процесса.
- Класс бага «stale pool object» остаётся возможным для будущих helpers, если они снова закэшируют pool без rebind; долгосрочно предпочтительнее передавать accessors (`_fetch`/`_fetchval`), как у `EventSchemaRegistry`.
