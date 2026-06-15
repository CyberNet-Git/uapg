# ADR-001: History V2 — события (typed storage + SQL search)

## Статус

Принято

## Контекст

`events_history.event_data` хранит OPC UA Variant в base64 JSONB. SQL-фильтрация по полям (`serial`, `dev_eui`) невозможна; `EventFilter` применяется in-memory после `LIMIT`.

## Решение

- Hypertable `events_ts` как time-series anchor.
- Typed tables `evt_<slug>` per OPC UA event type (flatten supertype fields).
- Schema registry `event_type_schema` + auto DDL при `new_historized_event`.
- Dual-write: legacy `events_history` + v2 в одной транзакции.
- Read: `FilterPlan` JSON → SQL push-down; residual `apply_event_filter` in-memory.
- Без PostgreSQL INHERITS совместно с hypertable.

## Режимы

- `legacy` — только старый путь.
- `dual` — запись в оба слоя; чтение v2 first.
- `v2` — только v2 (legacy archive).

## Legacy fallback

При typed filter и незавершённом backfill: `partial=true`, строки только в legacy не участвуют в поиске по typed columns.

## Последствия

- Runtime DDL (`ADD COLUMN`) под advisory lock.
- Backfill worker обязателен для исторических данных.
