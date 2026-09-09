# Changelog

## [0.2.14] - 2026-09-09

### Исправлено

- **Историзация вставала намертво при молчаливом обрыве соединения с PostgreSQL.** Разбор инцидента на стенде АГК: запись значений прекратилась в 11:57, через 20 минут очередь на 10000 элементов заполнилась, и следующие 23 часа все значения отбрасывались. За это время в логе не появилось ни одной ошибки от историзации — ни сбоя флаша, ни таймаута, ни реконнекта. Воркер оставался живым, event loop работал без запинки, а чтения из той же БД проходили успешно. Причина в том, что прикладной отмены оказалось недостаточно: когда сокет к БД умирает молча, отмена зависшего запроса уходит в тот же мёртвый сокет (asyncpg шлёт `ROLLBACK` при выходе из транзакции), и ожидание не заканчивается никогда. Обрыв теперь обнаруживается на уровне ОС.
- **`BEGIN`, `COMMIT` и `ROLLBACK` выполнялись без ограничения по времени.** Контекст транзакции asyncpg шлёт их без явного `timeout`, а в параметрах пула не было `command_timeout`. Добавлен параметр `db_command_timeout_sec` (60 с), который действует внутри asyncpg и покрывает в том числе служебные команды транзакции.

### Добавлено

- **TCP keepalive на соединениях с БД.** Параметры `db_tcp_keepalive_idle_sec` (30 с), `db_tcp_keepalive_interval_sec` (10 с), `db_tcp_keepalive_count` (3) и `db_tcp_user_timeout_sec` (60 с). Последний ограничивает время неподтверждённой отправки и потому ловит обрыв даже посреди активной записи, когда keepalive не помогает. Ошибка настройки сокета не мешает работе с БД, а лишь пишется в лог.
- **Ограничение времени флаша** параметром `history_flush_timeout_sec` (120 с). Превышение считается ошибкой флаша, попадает в отдельный счётчик `flush_timeouts_total`, и воркер продолжает разбирать очередь вместо того, чтобы остаться в зависшем ожидании.
- **Надзор за залипшим воркером.** Прежний надзор поднимал только мёртвую задачу, а в инциденте на АГК задача оставалась живой: `task.done()` возвращал `False`, ошибок не было, очередь при этом переполнялась. Теперь при флаше, идущем дольше `history_worker_stall_timeout_sec` (300 с), путь записи отменяет задачу и сразу поднимает новую, не дожидаясь завершения старой — её cleanup может висеть на том же мёртвом сокете. Срабатывания считаются в `worker_stall_restarts_total`.
- **Метрики `flush_timeouts_total`, `worker_stall_restarts_total` и `seconds_in_current_flush`** в `get_stats()`, а значит в `get_performance_metrics()` и узлах OPC UA `HistoryMetrics`. Последняя показывает длительность текущего незавершённого флаша и отличает залипание от простоя. В снимок конфигурации добавлены `db_command_timeout_sec`, `db_tcp_keepalive_idle_sec`, `db_tcp_user_timeout_sec`, `history_flush_timeout_sec` и `history_worker_stall_timeout_sec`.

## [0.2.13] - 2026-09-08

### Исправлено

- **Молчаливая смерть воркера `HistoryWriteBuffer`:** воркер завершался по `asyncio.CancelledError` без единой записи в лог, а исключения вне ветки `Exception` терялись целиком — никто не ожидал task и не было done-callback. Очередь после этого копилась до предела, и единственным следом оставался поток «queue is full». Теперь выход воркера логируется на уровне CRITICAL с причиной, а сам воркер поднимается автоматически (экспоненциальный backoff до 30 с). Путь записи дополнительно проверяет воркер при каждом `enqueue`: буфер, который ни разу не запускали, при этом не поднимается.
- **Неограниченные ожидания при пересоздании пула:** `_ensure_pool` ждал `_reconnect_lock`, `_pool_lock` и `asyncpg.create_pool` без каких-либо таймаутов, а `_force_reconnect` удерживает оба замка, пока создаёт пул. Зависший реконнект останавливал историзацию навсегда и выглядел снаружи как «запись молча прекратилась». Все три ожидания ограничены по времени, в `create_pool` передаётся `timeout` на установку соединения. Новые параметры `db_pool_create_timeout_sec` (30 с) и `db_lock_wait_timeout_sec` (60 с); срабатывание пишется в лог на уровне ERROR и считается в `db.pool_wait_timeouts_total`.
- **Разрастание лога при переполнении очереди:** сообщение «queue is full» писалось на каждый отброшенный элемент и при штатной нагрузке давало сотни мегабайт логов. Теперь оно выводится не чаще раза в `drop_log_interval_sec` (60 с) с числом отброшенных за окно; точный счёт по-прежнему в `dropped_total`.

### Добавлено

- **Диагностические метрики буфера** в `get_stats()` и, следовательно, в `get_performance_metrics()` и узлы OPC UA `HistoryMetrics`: `worker_alive`, `worker_restarts_total`, `last_worker_exit_reason`, `seconds_since_last_flush`, `seconds_since_last_enqueue`. Пара `worker_alive` и `seconds_since_last_flush` различает мёртвый воркер и живой, но залипший в ожидании БД — по прежнему набору метрик эти два случая были неотличимы.
- **`db.pool_wait_timeouts_total`** и `config.db_pool_create_timeout_sec` / `config.db_lock_wait_timeout_sec` в снимке метрик.

## [0.2.12] - 2026-09-03

### Исправлено

- **`HistoryTimescaleV2._flush_event_batch` при `UAPG_EVENTS_STORAGE_MODE=v2`:** батч больше не пишет «пустой» ряд только в `events_ts` без `event_data`/`legacy_row_id`. Чтение HistoryRead (UAExpert, ovic) гидратирует поля из `events_history` — без payload события приходили с незаполненными полями. Режим `v2` теперь тоже вызывает `save_event_dual` / `uapg_save_event_v2` (как `dual` и одиночный `save_event`). Уже записанные в сломанном режиме строки без `legacy_row_id` не восстанавливаются — нужны новые события после обновления.

## [0.2.11] - 2026-07-24

### Исправлено

- **HistoryTimescaleV2 / pool is closed:** после `_force_reconnect` обновляются кэшированные ссылки на asyncpg pool в `EventStoreV2`, `ProcedureGateway` и `EventsBackfillWorker`. Раньше value history продолжала работать через `_fetch`/`_ensure_pool`, а чтение event history V2 ходило в уже закрытый pool. Lag-запросы в `read_event_history` и `refresh_history_settings_nodes` переведены на `_fetchval` (timeout + reconnect).

## [0.2.10] - 2026-07-20

### Изменено

- **`backfill_last_values`:** lookback (`history_lookback`) для chunk exclusion TimescaleDB; дефолтный `chunk_size=100`; возвращает `restored_items` как `(node_id_str, DataValue)`; опциональный callback `on_chunk_restored` для применения в address space по мере нахождения значений. Обновляет in-memory last-values cache. Кандидаты — только `is_seed` (строки без last_value создаёт `seed_last_values` при historize; orphan metadata без истории больше не гоняет LATERAL).

## [0.2.9] - 2026-07-17

### Добавлено

- **`read_last_values(..., allow_history_fallback=True)`:** при `False` читает только memory/`variables_last_value`, без LATERAL по `variables_history`. Для bulk-restore при старте сервера: иначе тысячи переменных без строк в кэше упираются в `db_query_timeout` (30 с) и рвут пул (`Fetch timed out, will reconnect`).

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