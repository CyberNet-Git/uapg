# Changelog

## [0.2.0] - 2026-06-11

### Добавлено

- **HistoryTimescaleV2** — typed storage событий, dual-write с legacy, SQL-фильтрация по полям OPC UA
- SQL migrations (`events_ts`, schema registry, stored functions `uapg_*`)
- `FilterPlan` JSON и `EventFilterPlanner` для push-down фильтров
- Backfill worker legacy → v2 (`run_events_backfill`)
- ADR: events V2, platform, variables roadmap (`doc/adr/`)
- Skeleton variables V2 + aggregation SQL (релиз 2+)

### Конфигурация

- `UAPG_EVENTS_STORAGE_MODE=dual` по умолчанию

### Изменено (2026-06-11)

- **events V2 config:** убран product-specific хардкод `STRING_INDEX_FIELDS`; indexed/sql_filter fields и aliases задаются через `EventsV2Config` (runtime).
- **OPC UA capability nodes** в `HistoryTimescaleV2`: `EventsSqlFilterFields`, `EventsStorageVersion` и др.

## [Unreleased]

### Fixed
- Исправлено именование колонок: все колонки теперь используют нижний регистр
- Заменено `_EventTypeName` на `_eventtypename`
- Заменено `_Timestamp` на `_timestamp`
- Упрощены функции проверки структуры таблиц, убраны лишние проверки на дублирование колонок
- Упрощены функции работы с первичными ключами
- Улучшена обработка ошибок при переименовании колонок в TimescaleDB chunk'ах

### Changed
- Все SQL запросы теперь используют стандартные имена колонок в нижнем регистре
- Упрощена логика исправления дублирующихся колонок
- Убраны избыточные проверки существования колонок с разным регистром

## [Previous versions...] 