# Интеграция HistoryTimescaleV2 (opcvibroiot)

## opc-vibro-iot-server

- `create_history_backend()` в `src/opcua/history_backend.py` — выбор v1/v2 по `HISTORY_STORAGE_VERSION`.
- Env (VibroIoT defaults в `config.py`):
  - `HISTORY_STORAGE_VERSION=v1|v2` (default `v1`)
  - `UAPG_EVENTS_STORAGE_MODE=legacy|dual|v2`
  - `UAPG_EVENTS_INDEXED_FIELDS` — btree-индексы typed columns (CSV)
  - `UAPG_EVENTS_SQL_FILTER_FIELDS` — whitelist SQL push-down + OPC capability node
  - `UAPG_EVENTS_FIELD_ALIASES` — `api_name:column_name` (опционально)
  - `HISTORY_EVENTS_BACKFILL_ON_START`, `HISTORY_EVENTS_BACKFILL_BATCH_SIZE`
- Capability nodes (V2): `Server/History/HistorySettings/EventsSqlFilterFields` и др.

## uapg core

- Без product-specific хардкода: indexed/filterable fields задаёт потребитель через `EventsV2Config`.
- См. `src/uapg/v2/events_config.py`.

## opc-vibro-iot-client (ovic) — следующий этап

- `discover_history_capabilities()` — read capability nodes.
- WhereClause push-down только при `EventsSqlFilterSupported=true`.

## web-ui-api — следующий этап

- Post-filter остаётся safety net; scan cap снижается при confirmed server-side filters.

## ADR

- `opcvibroiot/services/opc-vibro-iot-server/doc/architecture/adrs/` — ссылка на uapg ADR-001.
