# Changelog

## [0.2.19] - 2026-10-07

Чтение истории V2 на стенде (`history.events_history` ≈ 1525 МБ, `history.events_ts` ≈ 278 МБ) выбиралось медленно: сервер строил `UNION ALL` по typed-таблицам с `techplace ILIKE '%77-12-29%'`, затем сортировал и применял `LIMIT`. Причин было две, независимых друг от друга, и для устранения нужны обе правки. Замеры ниже — на живом PostgreSQL 16, 400k строк `events_ts`, два типа событий, prepared statements, `EXPLAIN (ANALYZE, BUFFERS)`.

### Исправлено

- **`ILIKE '%...%'` не обслуживался индексом.** Фильтр приходит из `ua.FilterOperator.Like`, превращается в `{"op": "ilike"}` и рендерится как `t."techplace" ILIKE $n`; btree по текстовой колонке при ведущем `%` не используется, поэтому при отсутствии совпадений просматривалось всё окно по каждому типу. Для текстовых колонок из `indexed_fields` теперь создаётся GIN-индекс `pg_trgm` (`USING gin (col gin_trgm_ops)`). Запрос без совпадений: **4130 буферов и 76 мс → 30 буферов и 0.34 мс**.
- **Индекс не появлялся для уже существующих колонок.** `_ensure_physical_table` создавал индекс по колонке только в ветке «колонка только что добавлена» (`if name in existing_names: continue`), так что на работающем стенде, где `techplace` давно есть, новый индекс не возник бы никогда. Добавлен догоняющий проход на старте, который находит недостающие trgm-индексы по `event_type_storage` × `information_schema.columns` и создаёт их.
- **`ORDER BY` и `LIMIT` стояли только снаружи `UNION ALL`.** PostgreSQL не заносит `LIMIT` в ветки объединения, поэтому каждая ветка материализовала все попадания окна, и лишь потом шла общая сортировка. Теперь фильтр, курсор, порядок и `LIMIT` находятся внутри каждой ветки, а снаружи остаётся слияние (`Merge Append`) с тем же `LIMIT`. Корректность: глобальные top-N по `(event_timestamp, event_id)` — подмножество объединения per-branch top-N. Запрос с большим числом совпадений: **12484 буфера и 204 мс → 208 буферов и 0.18 мс**.
- **Фильтр применялся после JOIN с `events_ts`.** Из-за этого индекс по текстовой колонке и индекс по времени оказывались на разных отношениях, и планировщику было нечем воспользоваться. Ветка теперь работает только по typed-таблице (в ней есть `source_id`, `event_timestamp`, `event_id`), а `events_ts` присоединяется после слияния ради `legacy_row_id` — для ≤`limit` строк по первичному ключу. Оба индекса оказываются на одной таблице, и планировщик выбирает между «битмап по trgm + сортировка» и «скан по времени с ранним выходом». Отличие в поведении: `LIMIT` считает строки до соединения, поэтому при наличии строк typed-таблицы без пары в `events_ts` вернётся меньше `limit` — цикл дозаполнения `read_events` это переносит.
- **`uapg_read_events_v2` сортировал через `ORDER BY CASE WHEN p_order = 'DESC' THEN ... END`.** Планировщик не сопоставляет такое выражение с индексом `idx_events_ts_source_ts`, и предикат курсора по той же причине не был индексным: каждая следующая страница перечитывала окно с начала. Миграция `006_events_v2_read.sql` разбивает функцию на четыре `RETURN QUERY` (порядок × наличие курсора) с обычным `ORDER BY`; для варианта с курсором граница окна сужается самим курсором, а тай-брейк записан строковым сравнением `(e.event_timestamp, e.event_id) < (...)`, которое PostgreSQL берёт как индексный предикат. Вызов с курсором: **108 мс → 0.75 мс**. Сигнатура и `RETURNS TABLE` не изменились.
- **Метаданные читались построчно на каждое чтение.** `_common_pushdown_fields` и `_event_type_ids_with_fields` делали по запросу схемы на каждый тип, причём дважды одни и те же, а `_read_typed_multi_rows` — по запросу `physical_table` на тип: 3×K обращений для K типов. Добавлены `get_schema_fields_for_event_types` и `get_storage_tables`, читающие набор одним запросом; на чтение стало два обращения вместо 3×K.
- **DDL-проход выполнялся на каждую запись события.** `ensure_columns_from_typed_values` собирает непустой список полей практически всегда, поэтому `_ensure_physical_table` (advisory lock + `CREATE TABLE IF NOT EXISTS` + запрос к `information_schema.columns`) вызывался на каждый `insert_typed_row`. Registry запоминает уже известные колонки typed-таблиц и выходит сразу, если все запрошенные колонки на месте.

### Добавлено

- **Миграция `006_events_v2_read.sql`** — sargable `uapg_read_events_v2`. Отдельным файлом потому, что `003` на существующих БД уже отмечен применённым в `uapg_schema_migrations`.
- **Параметры `events_trgm_index_enabled` (по умолчанию `True`) и `events_trgm_index_timeout_sec` (300 с)** конструктора `HistoryTimescaleV2`. Индексы создаются на старте по образцу `_ensure_index`: ошибка или таймаут не валят процесс, попытка повторяется на следующем старте. `CREATE EXTENSION pg_trgm` тоже best-effort — без прав суперпользователя в лог идёт WARNING с готовым DDL, и старт продолжается. Отдельный бюджет нужен потому, что сборка GIN на большой typed-таблице не укладывается в `db_query_timeout_sec` (30 с), а обычный `_execute` на таймауте пересоздаёт пул.
- **Метрики `events_v2.trgm_*`** в `get_performance_metrics()` (и узлы OPC UA `HistoryMetrics`): `trgm_index_enabled`, `trgm_extension_available`, `trgm_indexes_created_total`, `trgm_indexes_missing`, `trgm_index_failures_total`. `trgm_indexes_missing > 0` или `trgm_extension_available = false` — прямое указание, что подстрочный поиск по-прежнему идёт просмотром окна.
- **Параметр `ensure_columns` у `EventSchemaRegistry.insert_typed_row`** уже был добавлен в 0.2.17; здесь к нему добавлено запоминание колонок, которое снимает повторные DDL-проходы и в пути записи.

### Примечания по эксплуатации

- На горячем стенде обычный `CREATE INDEX` держит ShareLock и на время сборки блокирует запись событий. Чтобы собрать индекс заранее и без блокировки:

  ```sql
  CREATE INDEX CONCURRENTLY idx_<table>_<column>_trgm
      ON "<schema>"."<table>" USING gin ("<column>" gin_trgm_ops);
  ```

  Имя должно совпадать с тем, которое строит UAPG (`idx_<table>_<column>_trgm`, а при превышении 63 байт — `idx_<md5(table:column)[:16]>_trgm`), иначе догоняющий проход создаст второй индекс.
- Btree по тем же колонкам сохранён: он нужен для `eq` и `IN`. Если по полю ищут только подстроку, `idx_<table>_<column>` можно удалить вручную и сэкономить на записи.
- Без trgm-индекса новая форма запроса сама по себе не ускоряет случай «совпадений нет» (фильтр всё равно неиндексируем) — обе правки нужны вместе.
- У typed-таблиц `evt_*` по-прежнему нет retention-политики: у `events_history` она есть через `_setup_retention_policy`, у `events_ts` — через миграцию 004, а `evt_*` растут неограниченно вместе с GIN-индексом. Это отдельная работа.

### Тесты

- **Прогон доведён до зелёного: было 24 падения из 153, стало 162 проходящих теста.** Все падения были дефектами самих тестов — legacy-набор отстал от кода; в продукте ничего менять не потребовалось. Каждая правка проверена мутацией: сломанный `stop()`, потеря `VariantType` в кодеке payload и пропущенный `pool.release` действительно роняют соответствующие тесты.
- **`pytest.ini` использовал секцию `[tool:pytest]`**, которая валидна только в `setup.cfg`. Из-за этого не применялись ни `addopts` (включая `--asyncio-mode=auto` и `--strict-markers`), ни объявленные маркеры: pytest молча работал с дефолтами, и каждому async-тесту требовался явный `@pytest.mark.asyncio`.
- **`tests/test_connection_pool.py`** (7 падений): `patch('asyncpg.create_pool')` подставляет `MagicMock`, и `await` его результата падает с `TypeError` — нужен `AsyncMock`. Двойник пула не поддерживал `pool._closed` (его смотрит `_ensure_pool`), `await pool.close()` и вход в `pool.acquire()` как в асинхронный контекст. Ожидаемые параметры пула (`command_timeout=60`, `statement_cache_size=0`) и дефолты `min_size`/`max_size` (5/20) не соответствовали `_build_pool_params` и конструктору (1/10).
- **`tests/test_history_pgsql.py`** (8 падений): тесты работали с `history._db` — одним соединением из архитектуры до пула. Переведены на `_pool`; `test_init` больше не ходит на реальный 127.0.0.1:5432. Порядок аргументов `ua.NodeId(1, "TestVariable")` был перепутан (identifier и namespace), `ua.DataValue(StatusCode_=...)` — устаревшее имя поля asyncua. `_list_to_sql_str` в коде нет — тест заменён проверкой `_format_node_id`. Тесты `Buffer` лезли в `buffer.data`/`buffer.pos` (теперь `_data`/`_pos`) — переписаны на поведение `read`/`skip`/`copy`.
- **`tests/test_sql_syntax.py`** (5 падений) проверял `HistoryPgSQL._format_event` — метод, собиравший колонку на каждое поле события. Такой схемы давно нет: payload целиком уходит в JSONB-колонку `_eventdata` как карта `field -> "base64:<variant>"`. Файл заменён на `tests/test_event_payload_codec.py`, который держит под тестом round-trip `_event_to_binary_map` ↔ `_binary_map_to_event_values` (именно на этом пути в 0.2.15 ломались значения полей при HistoryRead), обработку `None` и неподходящих значений, `_extract_variant_values` и фактическую форму INSERT события.
- **`tests/test_db_manager.py`** (3 падения): `get_database_info` без конфигурации возвращает причину отказа, а не пустой словарь; тест патчил `uapg.db_manager.psycopg2`, хотя модуль импортирует `psycopg`; `DatabaseManager.migrate_schema` не существует — тест переписан на `migrate_to_timescale` (нет конфигурации, повторная миграция, успешный путь, ошибка подключения).
- **`tests/test_flush_event_batch_v2_mode.py`**: двойник `_run_db_operation` не принимал `timeout`/`layer`, добавленные в 0.2.14, а двойник соединения реализовывал асинхронный контекст, хотя `_flush_on_connection` берёт соединение через `await pool.acquire(timeout=...)` и управляет транзакцией явно (`start`/`commit`/`rollback`).
- **Прогон тестов больше не меняет файлы в рабочем дереве.** `DatabaseManager` с параметрами по умолчанию пишет `db_config.enc` и `.db_key` в текущий каталог, и один из тестов создавал менеджер без явных путей — после каждого прогона `db_config.enc` в корне репозитория оказывался изменённым.

### Известное, не исправлено

- **`DatabaseManager.create_database` несовместим с настоящим psycopg3.** Модуль импортирует `psycopg` (v3), но использует API psycopg2: `psycopg.ISOLATION_LEVEL_AUTOCOMMIT` в psycopg3 отсутствует, поэтому на живой установке вызов упадёт в собственный `except` и вернёт `False`. В тестах это не видно, потому что `conftest.py` подменяет модуль `psycopg` на `Mock`. Вдобавок `requirements-db-manager.txt` требует `psycopg2-binary`, которого код не импортирует. Нужна отдельная правка: `conn.autocommit = True` вместо `set_isolation_level` и согласование зависимости.

## [0.2.18] - 2026-10-02

### Исправлено

- **Чтение истории событий оставалось на закрытом пуле после обрыва PostgreSQL.** Неудачный `_force_reconnect` закрывал старый пул и оставлял `self._pool = None`, а `_rebind_v2_pool()` вызывался только после успешного пересоздания. Следующий `_ensure_pool()` поднимал новый пул для записи, но `EventStoreV2`, `ProcedureGateway` и `EventsBackfillWorker` продолжали держать закрытый объект: `HistoryRead` отвечал `pool is closed`, пока flush переменных и событий уже шёл в живой пул. Теперь ссылки V2 сбрасываются и при неудачном реконнекте, обновляются при каждом `_ensure_pool()` (в том числе если пул уже открыт), а чтение событий само вызывает `_ensure_pool()` и не ждёт следующего flush.


## [0.2.17] - 2026-09-30

### Исправлено

- **Typed-бэкфил не переносил исторические события вообще.** `EventsBackfillWorker._backfill_typed_rows` выбирал строки `ORDER BY et.legacy_row_id DESC LIMIT n` без курсора: каждый вызов брал одни и те же самые свежие N строк и до старых не доходил никогда, сколько бы раз ни вызывался `run_events_backfill()`. Добавлен восходящий курсор — отдельная строка `domain = 'events_typed'` в уже существующей `uapg_backfill_state` (схема таблицы не меняется). Дойдя до хвоста, курсор сбрасывается в 0: следующий круг подхватывает строки тех типов, у которых typed-таблица появилась позже. Повторная вставка безвредна — `insert_typed_row` делает `ON CONFLICT (event_id, event_timestamp) DO NOTHING`.
- **Сортировка всей гипертаблицы `events_ts` на каждый батч.** Восходящий keyset (`legacy_row_id > $1 ORDER BY et.legacy_row_id LIMIT n`) обслуживается частичным индексом `idx_events_ts_legacy_row`; в плане `Index Scan` вместо `Sort` по всем чанкам. Проверка существования typed-таблицы переписана с `NOT EXISTS (SELECT 1 FROM information_schema.tables ...) IS FALSE` (двойное отрицание плюс коррелированный подзапрос к системному представлению на каждую строку) на `to_regclass(format('%I.%I', ...))`, а `physical_table` берётся из JOIN с `event_type_storage` — построчный `registry.get_storage_table()` больше не нужен.
- **Батч бэкфила делал 1+2N обращений к БД.** `ProcedureGateway.backfill_events_batch` переписывал на Python логику, которая уже была в SQL, и на каждую строку выполнял `SELECT 1 FROM events_ts WHERE legacy_row_id = $1` и отдельный INSERT: при `batch_size=500` до 1001 обращения. Теперь один вызов SQL-функции.
- **`uapg_backfill_events_batch` был мёртвым кодом с построчным циклом.** Процедура из `003_events_v2_functions.sql` не вызывалась нигде и сама шла `FOR ... LOOP`. Новая миграция `005_events_v2_backfill.sql` заменяет её функцией того же имени (`RETURNS TABLE (last_legacy_id, rows_processed, rows_inserted)`) с одним set-based `INSERT ... SELECT` — по образцу `uapg_save_event_v2`, которую гейтвей вызывает так же. Правка вынесена в отдельный файл потому, что `003` на существующих БД уже отмечен применённым в `uapg_schema_migrations` и повторно не выполняется. Семантика watermark сохранена: `last_legacy_id` двигается до максимального `id` батча (включая строки, уже записанные dual-write), `rows_processed` растёт только на фактически вставленные.
- **Построчные обращения в typed-бэкфиле.** `event_data` тянется на весь батч одним `WHERE id = ANY($1::bigint[])` (индекс `idx_events_history_id`) вместо N запросов `WHERE id = $1`; избыточная построчная проверка существования в typed-таблице удалена; `ensure_columns_from_typed_values` вызывается один раз на таблицу по объединению ключей батча, а не на каждую строку — иначе advisory lock, `CREATE TABLE IF NOT EXISTS` и запрос к `information_schema.columns` выполнялись N раз. Итого на батч ~N+3 обращения вместо ~3N+2.
- **Одна битая строка роняла весь батч.** Ошибка декодирования `event_data` или вставки одной строки теперь пишется в лог на WARNING и считается в `typed_rows_failed`; остальные строки батча обрабатываются. Циклический проход вернётся к строке на следующем круге.

### Добавлено

- **Миграция `005_events_v2_backfill.sql`** — функция `uapg_backfill_events_batch` вместо процедуры и seed-строка курсора `events_typed` в `uapg_backfill_state`.
- **Ключи `typed_rows_inserted` и `typed_rows_failed`** в результате `run_events_backfill()`. Прежние четыре ключа (`last_legacy_id`, `rows_processed`, `backfill_lag_rows`, `v2_coverage_pct`) не изменились.
- **Параметр `ensure_columns` у `EventSchemaRegistry.insert_typed_row`** (по умолчанию `True`, поведение записи не меняется) — для батчевых вызовов, которые досоздают колонки один раз на батч.
- **Параметр `timeout` у `ProcedureGateway.backfill_events_batch`** — воркер передаёт свой `db_query_timeout_sec`, чтобы батч не висел бесконечно на сыром пуле.

## [0.2.16] - 2026-09-30

### Исправлено

- **Старт сервера больше не рвёт пул 30-секундным таймаутом.** В логе 30.09 старт писал `PostgreSQL fetchval timed out after 30.0 seconds (layer=query)` и следом `Fetchval timed out, will reconnect without retrying`. Причина — `HistoryTimescaleV2.refresh_history_settings_nodes` считал полный anti-join `events_history` × `events_ts` (`SELECT count(*) ... WHERE NOT EXISTS (...)`), который на проде разворачивался в Parallel Hash Anti Join по всем 34 чанкам гипертаблицы (~1.2 ГБ, ~514k строк). Запрос шёл через `_fetchval`, не укладывался в `db_query_timeout_sec` и по ветке «таймаут» вызывал `_force_reconnect`: пул уничтожался и пересоздавался, а вся остальная историзация вставала на `_reconnect_lock` — и всё это ради косметического OPC UA узла `EventsBackfillComplete`.
- **`expose_history_settings_nodes` выполнял refresh дважды.** Базовый `expose` сам зовёт `refresh_history_settings_nodes`, и виртуальная диспетчеризация уводила вызов в V2-override; следом `_expose_events_v2_capability_nodes` звал refresh ещё раз. За один старт anti-join исполнялся два раза. Теперь refresh откладывается до конца `expose` и выполняется один раз.
- **Оценка прогресса бэкфила считается по watermark, а не полным сканом.** Алгоритм бэкфила и так watermark-овый (`uapg_backfill_state.last_legacy_id`, `WHERE id > last_legacy_id ORDER BY id LIMIT n`), поэтому полный anti-join для ответа «готово / сколько осталось» не нужен. Готовность определяется ограниченной пробой: есть ли среди первых `events_backfill_probe_rows` строк выше watermark хоть одна без пары в `events_ts` — ровно то, что нашёл бы следующий батч `run_events_backfill()`. Стоимость не зависит от размера `events_history`. Голое «есть строки с `id > watermark`» здесь не подошло бы: dual-write кладёт новое событие сразу в обе таблицы, и флаг сбрасывался бы после каждой записи. Оборотная сторона: пропуск дальше `events_backfill_probe_rows` строк от watermark (например, после сбоя dual-write) пробой не виден — узел информационный.
- **Тот же anti-join исполнялся на каждом чтении событий.** `read_event_history` считал неперенесённые строки по `source_id` без ограничения по времени — только чтобы выставить `partial`, который дальше попадает лишь в DEBUG-лог. Теперь используется общая кэшированная оценка (TTL `events_backfill_status_ttl_sec`, по умолчанию 30 с): в типовом случае чтение не делает ни одного дополнительного запроса. Оценка стала глобальной, а не по источнику, то есть консервативнее — `partial=true`, если отстал любой источник.
- **Служебная проба физически не может уронить пул.** Она идёт по отдельному пути в обход `_run_db_operation`/`_fetchval`, с собственным коротким бюджетом (минимум из 5 с и `db_query_timeout_sec`), и любая её ошибка только пишется в DEBUG и считается в счётчике. Неудачная проба тоже попадает под TTL, иначе на медленной БД каждое чтение ждало бы свой таймаут заново.
- **Два полных скана на каждый батч бэкфила.** `EventsBackfillWorker.run_batch` считал тот же anti-join плюс `count(*)` всей `events_history`, причём на сыром пуле — мимо `_fetchval`, то есть вообще без таймаута и без обработки обрыва. Оба запроса заменены одним, целиком обслуживаемым индексом по `id`. Все запросы воркера получили явный `timeout`, так что батч больше не может висеть бесконечно.
- **Смысл метрик бэкфила уточнён.** `backfill_lag_rows` — число строк, которых sweep ещё не касался (`id > last_legacy_id`); это надмножество прежнего точного anti-join, так как новые строки dual-write тоже попадают в счёт, но батч, дошедший до хвоста, оставляет значение около нуля. `v2_coverage_pct` — позиция watermark в диапазоне `id` (`min(id)`/`max(id)`), где `min(id)` учитывает уже вычищенный retention-политикой хвост. Набор ключей, возвращаемых `run_events_backfill()`, не изменился.

### Добавлено

- **Индекс `idx_events_history_id` по `events_history(id)`**, создаётся на старте в V2-режимах через существующий `_ensure_index` (проверка по `pg_indexes`, таймаут или lock не валят процесс, повтор на следующем старте). `id` — `BIGSERIAL` без PRIMARY KEY, и без индекса полным сканом по всем чанкам шли не только проба готовности, но и keyset-батч бэкфила (`WHERE id > $1 ORDER BY id LIMIT n`), и гидрация каждого чтения событий (`WHERE id = ANY($1::bigint[])` в `EventStoreV2._hydrate_events`). На горячем стенде индекс можно создать заранее вручную, чтобы не держать ShareLock на всей гипертаблице во время сборки:

  ```sql
  CREATE INDEX idx_events_history_id ON "<schema>".events_history (id)
      WITH (timescaledb.transaction_per_chunk);
  ```

- **Параметры `events_backfill_probe_rows` (1000) и `events_backfill_status_ttl_sec` (30 с)** конструктора `HistoryTimescaleV2` — глубина пробы готовности бэкфила и время жизни её результата.
- **Секция `events_v2` в `get_performance_metrics()`** (а значит и узлы OPC UA `HistoryMetrics`: `EventsV2StorageMode`, `EventsV2StorageReady`, `EventsV2BackfillProbeFailuresTotal`, `EventsV2BackfillProbeRows`, `EventsV2BackfillStatusTtlSec`). Счётчик `backfill_probe_failures_total` показывает, сколько раз проба готовности не получила ответа (таймаут или обрыв); ненулевое значение при отсутствующем `idx_events_history_id` — прямое указание создать индекс вручную.


## [0.2.15] - 2026-09-14

Чат `98383d56-2906-440e-b09f-925f492a4033`, план `flush_retry_and_init_41edcfaa.plan.md`.

### Исправлено

- **Таймаут флаша больше не выбрасывает батч без повтора.** В логе 14.09 за две ночи 79 циклов подряд писали `will reconnect without retrying` и теряли пачку: цикл из двух попыток при `TimeoutError` сразу выходил. Таймаут обрабатывается как обычная ошибка SQL — реконнект и одна повторная запись. Второй провал отбрасывает батч и считает его в `flush_dropped_items_total`.
- **Реконнект, который ничего не делал, стал видимым.** Если пул уже заменён другой задачей, skip пишется на WARNING (`pool was already replaced`) и считается в `db_reconnect_skipped_total`; повтор идёт на текущий пул, а не молча завершается.
- **В логе больше не маскируется внутренний таймаут ярлыком «120s».** Сообщение слоя (`query` / `flush` / `command` / `buffer`) и фактическая длительность сохраняются.
- **Бюджет флаша не режется 30-секундным query timeout и 60-секундным command_timeout.** Statement и операция флаша используют `history_flush_timeout_sec` (120 с); `command_timeout` пула берётся как максимум из своего значения и бюджета флаша. Внешний потолок буфера — две попытки плюс ожидание замка (~300 с).
- **Отмена флаша не зависает на `Transaction.__aexit__` закрытого соединения.** При таймауте или cancel соединение `terminate()`, `ROLLBACK` в мёртвый сокет не отправляется.
- **Старт не блокирует живую историю `CREATE INDEX IF NOT EXISTS`.** Индекс, который уже есть в `pg_indexes`, не создаётся повторно. Недостающий индекс при таймауте или lock не валит процесс. Перед DDL снимаются чужие сессии с тем же `application_name` (хвосты после SIGKILL контейнера).

### Добавлено

- **`application_name` соединений с БД** (параметр `db_application_name`, по умолчанию `uapg-history`) — чтобы отличать и завершать только свои зависшие backend'ы.

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