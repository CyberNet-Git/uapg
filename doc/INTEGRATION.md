# Интеграция HistoryTimescaleV2 (opcvibroiot)

## opc-vibro-iot-server

- `create_history_backend()` в `src/opcua/history_backend.py` — выбор v1/v2 по `HISTORY_STORAGE_VERSION`.
- Env (VibroIoT defaults в `config.py`):
  - `HISTORY_STORAGE_VERSION=v1|v2` (default `v1`)
  - `UAPG_EVENTS_STORAGE_MODE=legacy|dual|v2`
    - `dual` — рекомендуемый режим; запись payload + индекс/typed.
    - `v2` — тот же путь записи payload (`uapg_save_event_v2` → `events_history.event_data`); без payload HistoryRead отдаёт пустые поля.
    - Не путать с «только events_ts без JSONB» — гидратация HistoryRead всё ещё идёт через `legacy_row_id` → `events_history`.
  - `UAPG_EVENTS_INDEXED_FIELDS` — btree-индексы typed columns (CSV)
  - `UAPG_EVENTS_SQL_FILTER_FIELDS` — whitelist SQL push-down + OPC capability node
  - `UAPG_EVENTS_FIELD_ALIASES` — `api_name:column_name` (опционально)
  - `HISTORY_EVENTS_BACKFILL_ON_START`, `HISTORY_EVENTS_BACKFILL_BATCH_SIZE`
  - `HISTORY_DB_ENSURE_INDEXES_ON_START` → `ensure_indexes_on_startup` (uapg >= 0.2.20)
  - `HISTORY_EVENTS_TRGM_INDEX_ENABLED` → `events_trgm_index_enabled`, `HISTORY_EVENTS_TRGM_INDEX_TIMEOUT_SEC` → `events_trgm_index_timeout_sec`
- Capability nodes (V2): `Server/History/HistorySettings/EventsSqlFilterFields` и др.

### Онлайн-сборка индексов на стенде

Обёртка `src/tools/history_indexes.py` берёт подключение из `get_db_config()`, схему из `POSTGRES_SCHEMA_HISTORY`, поля из `UAPG_EVENTS_*` и вызывает `uapg.maintenance.indexes_cli`. Аргументы те же, что у `uapg indexes`.

```bash
# сервис vibroiot-server в prod-compose, server в dev
docker compose exec vibroiot-server python /service/src/tools/history_indexes.py plan
docker compose exec vibroiot-server python /service/src/tools/history_indexes.py plan --sql > indexes.sql
docker compose exec vibroiot-server python /service/src/tools/history_indexes.py apply
```

На prod-стенде при больших таблицах: `HISTORY_DB_ENSURE_INDEXES_ON_START=false`, `HISTORY_EVENTS_TRGM_INDEX_ENABLED=false`, индексы строятся `apply` на работающей БД до рестарта. Runbook — в админ-гайде opcvibroiot (`doc/adminguide/03-...env-файлы.md`).

Первый аргумент `migrations` переводит обёртку на `uapg.maintenance.migrations_cli`. Если `--user`/`--dsn` переопределяют роль, пароль приложения не подставляется (берётся `--password` или `PGPASSWORD`):

```bash
docker compose exec vibroiot-server python /service/src/tools/history_indexes.py migrations status
docker compose exec -e PGPASSWORD vibroiot-server python /service/src/tools/history_indexes.py migrations apply --user postgres
```

Сервер пишет WARNING-и uapg (отложенная миграция, пропущенные индексы с DDL) при `HISTORY_LOG_LEVEL=WARNING` (по умолчанию).

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
