# Changelog

## [0.2.8] - 2026-07-17

### Добавлено

- **Инвариант `variables_last_value`:** у каждой зарегистрированной переменной должна быть строка последнего значения — тогда чтение последних значений никогда не обращается к таблице истории. Новая колонка `is_seed` (строка-дефолт, не сверенная с историей; миграция `ADD COLUMN IF NOT EXISTS` при init).
- **`seed_last_values(items)`:** батчевый засев дефолтных значений с `is_seed=TRUE` и `ON CONFLICT DO NOTHING` — существующие реальные значения не затрагиваются.
- **`backfill_last_values()`:** фоновая идемпотентная сверка с историей чанками — заполняет отсутствующие строки, замещает сиды реальными последними значениями, сиды без истории помечает сверенными. После первого полного прохода выполняется мгновенно.
- **`read_last_values(..., history_lookback=timedelta)`:** необязательное ограничение фоллбэк-чтения по времени. С условием `sourcetimestamp >= now() - lookback` TimescaleDB исключает старые чанки — переменные без данных больше не заставляют пробегать индексы всех чанков hypertable.
- **Самозалечивание `variables_last_value`:** значения, найденные фоллбэком по истории, батчево upsert'ятся в таблицу-кэш последних значений — при последующих чтениях и рестартах фоллбэк для этих переменных не выполняется.

### Изменено

- **Все пути записи последнего значения** (буферный flush, одиночный upsert, самозалечивание фоллбэка) выставляют `is_seed=FALSE` и перекрывают строку-сид независимо от её timestamp (`WHERE is_seed OR sourcetimestamp <= EXCLUDED.sourcetimestamp`).

## [0.2.7] - 2026-07-17

### Исправлено

- **`HistoryTimescale.read_last_values`:** фоллбэк-чтение последних значений из `variables_history` переведён с `SELECT DISTINCT ON (variable_id) ... WHERE variable_id = ANY(...)` (на hypertable с большим числом чанков — секунды на вызов) на `unnest + CROSS JOIN LATERAL ... ORDER BY sourcetimestamp DESC LIMIT 1` — точечный top-1 проход по индексу `(variable_id, sourcetimestamp DESC)`; `variantbinary` читается тем же запросом, убран дополнительный роундтрип на каждую найденную строку.

## [0.2.6] - 2026-07-17

### Добавлено

- **`HistoryTimescale.new_historized_nodes`:** батчевая регистрация узлов для историзации — один `INSERT ... SELECT unnest(...) ON CONFLICT` на все узлы, отсутствующие в кэше метаданных, вместо upsert-роундтрипа на каждый узел; при конфликте `data_type` не сбрасывается в `Unknown`.

### Изменено

- **`HistoryTimescale.new_historized_node`:** если `variable_id` уже есть в кэше метаданных (прогревается из БД при init), upsert в `variable_metadata` пропускается — на рестарте сервера это убирает по одному DB-роундтрипу на каждый историзируемый узел.

## [0.2.4] - 2026-07-01

### Исправлено

- **`EventStoreV2._common_pushdown_fields`:** пересечение typed-полей с учётом схемы каждого `event_type_id` — UNION ALL не ссылается на несуществующие колонки.
- **Убраны привязки к конкретному проекту:** хардкод `mountpoint`/`mountpoint_tag` заменён на generic `field_aliases`; удалена отладочная запись в пути opcvibroiot.

### Изменено

- **`expand_sql_filter_fields` / `typed_fields_supported`:** принимают `aliases` из конфигурации деплоя вместо захардкоженных имён полей.

## [0.2.3] - 2026-06-19

### Добавлено

- **Keyset-пагинация событий V2:** `EventStoreV2.read_events` передаёт `cursor_ts`/`cursor_event_id` в `uapg_read_events_v2`, возвращает continuation `(event_timestamp, event_id)`, цикл догрузки после post-filter (до 5 итераций).
- **Multi-type SQL push-down:** typed-чтение для нескольких `event_type_ids` через `UNION ALL` при общих indexed-полях; whitelist `allowed_fields` в read path.

### Исправлено

- **`HistoryTimescaleV2.read_event_history`:** убран ошибочный `sql_continuation` от нижней границы диапазона; пагинация DESC опирается на границы из `_get_bounds` и OPC continuation timestamp.
- **`EventStoreV2.read_events`:** сохранение `event_type_ids` при пересборке FilterPlan с `allowed_fields` — совместный фильтр тип+поле не теряет SQL-фильтр по типу.
- **`EventStoreV2.read_events`:** при фильтре только по полям payload (`dev_eui` и т.д.) без `EventType` — typed SQL push-down по всем типам с physical table (`UNION ALL`).
- **`EventStoreV2` typed SQL push-down:** исправлена нумерация SQL-параметров для фильтров по typed-полям; фильтр `dev_eui` больше не конфликтует с `event_type_id` и не вызывает `BadInternalError`.

## [0.2.2] - 2026-06-17

### Исправлено

- **История событий (V2):** `new_historized_event` принимает типы событий как `asyncua.Node` (как передаёт asyncua из `get_referenced_nodes`), а не только `NodeId`. Исправлено падение `'Node' object has no attribute 'NamespaceIndex'` при включении historization — из-за него не создавалась подписка и `save_event` не вызывался.
- **История событий (V2):** исправлен импорт `get_event_properties_from_type_node` из `asyncua.common.events` (раньше — из несуществующего `asyncua.server.history`, срабатывал fallback с пустым списком полей). Typed-таблицы создавались без колонок (`serial`, `dev_eui` и др.), из-за чего `insert_typed_row` падал с `UndefinedColumnError`.
- **История событий (V2):** `insert_typed_row` перед записью добавляет отсутствующие колонки в typed-таблицу по ключам события (lazy migration для таблиц с пустой схемой).
- **История событий (V2):** `python_value_to_sql` приводит числовые и булевы значения к строке для TEXT-колонок typed-таблиц (исправлена ошибка `expected str, got int` при flush).

## [0.2.1] - 2026-06-15

### Исправлено

- **filter_planner:** оператор OPC UA `InList` разбирает все литералы (`operands[1:]`), а не только первый; чтение истории с несколькими типами событий больше не сводится к фильтру по одному типу.

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
- **SQL migrations 002/101:** PK hypertable-таблиц включает space-partition column (`source_id` / `variable_id`); индексы создаются после `create_hypertable`.
- **filter_planner:** извлечение `EventType` из InList по строковым NodeId (без `int(Identifier)`); неизвестные типы → пустой результат вместо `BadInternalError`.

## [Unreleased]

### Исправлено

- **HistoryTimescaleV2.new_historized_event:** в legacy-ветку (`HistoryTimescale`) снова передаются исходные `asyncua.Node` из `historize_event`, а не только `NodeId`. Устранены предупреждения `_get_event_fields: Cannot introspect event fields from NodeId ... without server Node` при старте с `HISTORY_STORAGE_VERSION=v2` и пустой `_event_fields` для legacy-чтения.

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