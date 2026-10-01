# uapg — хранение истории OPC UA в PostgreSQL/TimescaleDB

`uapg` — бэкенд историзации для сервера [asyncua](https://github.com/FreeOpcUa/opcua-asyncio):
реализует `HistoryStorageInterface`, пишет значения переменных и события в
TimescaleDB и отдаёт их клиентам по `HistoryRead`.

```bash
pip install uapg              # сам бэкенд
pip install "uapg[crypto]"    # плюс чтение зашифрованной конфигурации
```

Требования: Python 3.12, asyncua 1.1.8 или 2.x, PostgreSQL с расширением TimescaleDB 2.x.

## Быстрый старт

```python
from datetime import timedelta
from asyncua import Server
from uapg import HistoryTimescale

server = Server()
await server.init()
idx = await server.register_namespace("http://example.com")

storage = HistoryTimescale(
    user="opcua", password="...", database="opcua", host="db",
    global_retention_period=timedelta(days=365),   # политика хранения TimescaleDB
)
server.iserver.history_manager.set_storage(storage)
await storage.init()          # схема, пул, буферы записи, надзор за соединением
await server.start()

temperature = await server.nodes.objects.add_variable(idx, "Temperature", 0.0)
await server.historize_node_data_change(temperature, period=None)
```

Схема создаётся сама при первом `init()` и приводится к нужному виду при каждом
старте; повторный запуск ничего не ломает. Полный пример с событиями, узлами
метрик и корректной остановкой — в [`examples/server_with_history.py`](examples/server_with_history.py).

## Два класса

| Класс | События |
|---|---|
| `HistoryTimescale` | хранятся целиком в `events_history`; фильтр `HistoryRead` применяется в памяти |
| `HistoryTimescaleV2` | дополнительно раскладываются по полям в таблицы поиска, фильтр выполняется в SQL |

Это один и тот же бэкенд. `HistoryTimescaleV2` отличается только тем, что
включает поиск событий: режим берётся из аргумента `events_storage_mode` или из
переменной окружения `UAPG_EVENTS_STORAGE_MODE`.

| Режим | Запись | Чтение |
|---|---|---|
| `legacy` | только `events_history` | из `events_history`, фильтр в памяти |
| `dual` (по умолчанию) | `events_history` + слой поиска | через слой поиска, фильтр в SQL |
| `v2` | то же, что `dual` | то же, что `dual` |

`events_history` при этом не устаревшая таблица: из неё восстанавливается полный
состав полей события, слой поиска хранит только то, по чему ищут.

Зачем фильтровать в SQL. Выборка ограничена числом значений, которое просит
клиент. Если фильтр применяется после неё, клиент получает не «первые N
подходящих», а «подходящие среди первых N» — и редкое событие в длинной истории
не находится вовсе.

Какие поля индексировать и по каким разрешать поиск, решает установка:

```python
from uapg import HistoryTimescaleV2
from uapg.v2.events_config import EventsV2Config
from uapg.v2.storage_mode import StorageMode

storage = HistoryTimescaleV2(
    ...,
    events_storage_mode=StorageMode.DUAL,
    events_v2_config=EventsV2Config.from_csv(
        indexed="dev_eui,serial",        # btree-индексы по колонкам
        filterable="dev_eui,serial",     # что разрешено фильтровать в SQL; пусто — всё
        aliases="devEui:dev_eui",        # имя в фильтре → имя колонки
    ),
)
```

События, записанные до включения слоя поиска, переносятся порциями:
`await storage.run_events_backfill(batch_size=500)` — вызывать, пока
`backfill_lag_rows` не станет нулём. Прогресс отслеживается по отметке в
`uapg_backfill_state`, а не полным сопоставлением таблиц, поэтому не зависит от
размера истории; глубину пробы и время жизни её результата задают
`events_backfill_probe_rows` и `events_backfill_status_ttl_sec`.

## Что поддерживается из OPC UA Part 11

`HistoryRead` для сырых значений (`ReadRawModifiedDetails`) и событий
(`ReadEventDetails`), включая точки продолжения и чтение «от свежих к старым»,
когда время начала не задано или диапазон перевёрнут. Фильтр событий понимает
`Equals`, `Like`, `InList`, `IsNull`, `And`, `Or` в SQL; остальные операторы
`ContentFilter` вычисляются в памяти.

`ReadProcessedDetails` (агрегаты), `ReadAtTimeDetails` и `HistoryUpdate` asyncua
отвергает в своём диспетчере раньше, чем обращается к бэкенду, поэтому они не
поддерживаются ни одним бэкендом asyncua. Агрегаты — в планах (ADR-003).

## Надёжность

Каждое правило ниже появилось после разбора реальной остановки историзации.

- **Все ожидания ограничены**: запрос, флаш, создание пула, ожидание замка
  реконнекта. Молчаливый обрыв сокета ловится на уровне ОС через TCP keepalive и
  `TCP_USER_TIMEOUT` — прикладная отмена уходит в тот же мёртвый сокет.
- **Батч не теряется на первом сбое**: флаш после обрыва или таймаута
  повторяется один раз на новом соединении. Одиночный запрос по таймауту не
  повторяется — это только удвоило бы ожидание.
- **Ошибка SQL не рвёт пул**: если ответил сам сервер (синтаксис, ограничение),
  соединение исправно, и пересоздавать его незачем.
- **Отменённый флаш освобождает сервер**: соединение обрывается, а его backend
  в PostgreSQL завершается сразу, а не держит блокировки до конца запроса.
- **Очередь записи ограничена**: при недоступности БД лишние значения
  отбрасываются и считаются в метрике; сообщение об этом пишется не чаще раза в
  минуту. Воркер, умерший или залипший во флаше, поднимается заново.
- **Один плохой отсчёт не уносит пачку**: значения с кодом качества Bad и без
  меток времени записываются корректно или отвергаются поштучно.
- **Старт не мешает записи**: недостающий индекс создаётся по возможности, а
  зависшие сессии того же `db_application_name` после аварийного перезапуска
  снимаются.

## Настройки

Все параметры — именованные аргументы конструктора; значения по умолчанию
подобраны под прод.

| Группа | Параметры |
|---|---|
| Подключение | `user`, `password`, `database`, `host`, `port`, `schema`, `sslmode`, `min_size`, `max_size`, `db_application_name` |
| Запись | `history_write_batch_enabled`, `history_write_max_batch_size`, `history_write_max_batch_interval_sec`, `history_write_queue_max_size`, `history_write_durability_mode` (`async`/`sync`), `history_write_read_consistency_mode` (`local`/`global`) |
| Таймауты | `db_query_timeout_sec`, `db_command_timeout_sec`, `db_pool_create_timeout_sec`, `db_pool_close_timeout_sec`, `db_lock_wait_timeout_sec`, `history_flush_timeout_sec`, `history_worker_stall_timeout_sec` |
| Keepalive | `db_tcp_keepalive_idle_sec`, `db_tcp_keepalive_interval_sec`, `db_tcp_keepalive_count`, `db_tcp_user_timeout_sec` |
| Кэши | `history_last_values_cache_enabled`, `history_last_values_cache_max_size_mb`, `history_last_values_init_batch_size`, `history_metadata_cache_enabled`, `history_metadata_cache_init_max_rows` |
| Хранение | `global_retention_period` — политика TimescaleDB; меняется без перезапуска через `reapply_global_retention_policy()` |

Таймаут `0` или `None` означает «без ограничения».

## Метрики и узлы OPC UA

`get_performance_metrics()` отдаёт снимок без обращения к БД: состояние очередей,
длительности флашей, таймауты и реконнекты, текущие настройки.
Ключевые показатели для диагностики: `queue_fill_ratio` и `dropped_total` —
backpressure и потери; `seconds_in_current_flush` — отличает залипший флаш от
простоя; `timeouts_total`, `reconnects_total` — доступность PostgreSQL.

Те же метрики и настройки можно опубликовать в адресном пространстве:

```python
await storage.expose_history_settings_nodes(server, idx)   # Server/History/HistorySettings
await storage.expose_history_metrics_nodes(server, idx)    # Server/History/HistoryMetrics
await storage.refresh_history_metrics_nodes()               # обновлять периодически
```

Имена и типы узлов стабильны между версиями и проверяются тестами.

## Зашифрованная конфигурация

`HistoryTimescale.from_config_file(path, master_password)` читает параметры
подключения из файла, зашифрованного Fernet (нужен `uapg[crypto]`). Ключ лежит в
отдельном файле `.db_key`, и **при его наличии мастер-пароль в расшифровке не
участвует**: файл ключа рядом с файлом конфигурации равносилен открытым учётным
данным. Храните их раздельно и не коммитьте.

## Хранение

Таблицы ядра: `variables_history` и `events_history` (гипертаблицы TimescaleDB),
`variable_metadata`, `event_sources`, `event_types`, `variables_last_value`. Слой
поиска событий: `events_ts` и по таблице `evt_<тип>` на каждый тип события.

## Переход с 0.2.x

Схема БД не меняется: 3.0 работает с существующей базой без миграции. Публичный
API `HistoryTimescale` и `HistoryTimescaleV2` сохранён.

Из пакета удалены `HistoryPgSQL` (бэкенд без TimescaleDB), `DatabaseManager`,
`create_database_standalone`, `backup_database_standalone` и CLI
`python -m uapg.cli`. Проекты, которые ими пользовались (`opc-vibroiot-db`),
должны взять этот код из версии 0.2.15 (`main`, коммит `18df1cf`) к себе.
Подробности — в [CHANGELOG](CHANGELOG.md) и [ADR-005](doc/adr/ADR-005-single-timescale-backend.md).

## Разработка

```bash
uv venv && uv pip install -e ".[dev]" pytest-asyncio
docker compose -f docker-compose.test.yml up -d    # одноразовая TimescaleDB на 55432
pytest                                             # юнит, контракт и интеграция
mypy src/
```

Без поднятой БД интеграционные тесты пропускаются. Контрактные тесты сверяют
публичный API, схему и формат записи с замороженным снимком 0.2.15 в
`tests/contract/baseline/` — любое отступление от него должно быть явно
перечислено в `tests/contract/divergences.py` с причиной.

Устройство пакета:

```
src/uapg/
  history_timescale.py     фасад: разбор вызовов OPC UA, публичный API
  history_timescale_v2.py  тот же бэкенд с поиском событий по умолчанию
  core/      настройки, соединение и реконнект, метрики, буфер записи, SQL
  codec/     формат записи значений и событий, ключи NodeId
  storage/   схема, переменные, события, слой поиска, кэши
  opcua/     границы чтения, фильтр событий, публикация узлов
  sql/       весь SQL: схема, миграции, запросы
```

## Лицензия

MIT.
