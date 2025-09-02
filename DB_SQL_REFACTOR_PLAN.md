### Цели
- **Отделить SQL (DDL/DML) от Python-кода**
- **Ввести версионирование схемы** и предсказуемые деплои
- **Стандартизировать доступ к данным** и повысить тестируемость

### Предлагаемая структура проекта
```
db/
  migrations/                 # Только DDL (версионируемая схема)
    0001_init_schema.sql
    0002_add_indexes.sql
    0003_timescale_hypertables.sql
  seeds/                      # Начальные/референсные данные (детерминированные)
    0001_seed_reference_data.sql
  queries/                    # DML/SELECT (используется приложением)
    telemetry/
      insert_measurements.sql
      select_measurements_by_time.sql
      delete_measurements_by_retention.sql
      upsert_device_state.sql
    admin/
      vacuum_analyze.sql
      reindex_concurrently.sql
    reports/
      select_agg_daily.sql
      select_topn_devices.sql
  views/                      # VIEW/MATERIALIZED VIEW + refresh-скрипты
    v_device_last_state.sql
    mv_daily_agg.sql
  maintenance/                # Плановые задачи БД (политики, права)
    retention_policies.sql
    compression_policies.sql
    permissions.sql
  README.md                   # Правила, стиль, порядок запуска
python/
  db/
    repositories/             # Python-репозитории (тонкие обертки над SQL)
      telemetry_repository.py
      admin_repository.py
    sql_loader.py             # Загрузка/кеширование SQL-файлов
    tx.py                     # Транзакции/подключение
    types.py                  # Типы/DTO
tests/
  sql/fixtures/               # Фикстуры SQL для интегра-тестов
  integration/                # Тесты репозиториев
```

### Миграции и схема
- Инструмент миграций:
  - Если используется SQLAlchemy → Alembic (тело миграций хранить в .sql и исполнять через Alembic)
  - Если ORM не критичен → чистые SQL миграции + раннер (`yoyo-migrations` или свой минималистичный с таблицей `schema_migrations`)
- Принципы:
  - Только DDL в `db/migrations`. Один файл — один атомарный шаг
  - Индексы в проде: `CREATE INDEX CONCURRENTLY`, перестройка: `REINDEX CONCURRENTLY`
  - Для TimescaleDB: `create_hypertable`, политики ретенции и компрессии — отдельными файлами
  - Разрешения и роли — `maintenance/permissions.sql`, исполняется после базовой схемы

### Рабочие запросы (DML/SELECT)
- Хранить в `db/queries/<домен>/*.sql`. Один файл — один use case
- Строгая параметризация плейсхолдеров:
  - psycopg: `%(param)s`
  - asyncpg: `$1, $2, ...` (по согласованию)
- В Python-репозитории — только загрузка SQL и подстановка параметров. Никаких SQL-строк в коде
- Для сложных запросов — допускается Jinja2-шаблонизация (`.sql.j2`) с whitelisting-подходом

### Слой доступа к данным
- Репозитории по доменам: `telemetry_repository.py`, `admin_repository.py`
- Конвенции:
  - Имена методов — по действию: `insert_measurements`, `get_measurements_by_time`, `refresh_daily_mv`
  - Возвращаемые типы — DTO/TypedDict/`pydantic`-модели
  - Транзакции: явные контекст-менеджеры; по возможности разделение чтения/записи (CQRS)
  - Подключение: единый пул (sync/async) + retry с backoff

### Индексы и производительность
- Индексы — в DDL миграциях; тяжелые операции — отдельными миграциями
- TimescaleDB:
  - Создание hypertable, нужных time/space-partitions
  - Политики `add_retention_policy`, `add_compression_policy`
  - Материализованные VIEW для отчетов + `REFRESH`-скрипты
- Хранить эталоны EXPLAIN для ключевых запросов (в комментариях .sql или рядом `.md`)

### Тестирование
- Интеграционные тесты репозиториев:
  - Поднять тестовую БД (Docker), применить миграции, загрузить фикстуры `tests/sql/fixtures`
  - Для Timescale — проверять политики и агрегаты
- Контроль регрессий SELECT:
  - Снепшоты результатов критичных запросов (обрезать нестабильные поля)

### Качество и стиль
- Линтер SQL: sqlfluff (диалект `postgres`), правила на кавычки/алиасы/формат
- Запрет конкатенации SQL в репозиториях — только параметризация
- Нейминг:
  - Таблицы: `snake_case`
  - Индексы: `ix_<table>__<col1>[_<colN>]`
  - Ограничения: `fk_...`, `pk_...`, `chk_...`
  - VIEW: `v_...`, материализованные: `mv_...`

### CI/CD
- Джоб: поднять Postgres/Timescale контейнер → прогнать миграции → интеграционные тесты
- Отдельный джоб «сухой прогон миграций» на пустой БД и на БД со «старым снапшотом»
- sqlfluff + статический линт Python

### Конфигурация
- `.env`/`config.yml`: `APP_DB_URL`, `MIGRATIONS_DB_URL`, роли/схемы
- Роли и гранты — `maintenance/permissions.sql`
- Фича-флаги, влияющие на SQL (например, компрессия) — через env с дефолтами

### Пошаговое внедрение (итерациями)
1. Создать `db/migrations`, `db/queries` и перенести 2–3 критичных запроса как пилот
2. Добавить `python/db/sql_loader.py` с кешем и параметризацией
3. Ввести инструмент миграций и перенести DDL (без тяжелых индексов сначала)
4. Разделить Python-логику: `repositories` (тонкий слой) и `services` (бизнес-правила)
5. Подключить sqlfluff и интеграционные тесты репозиториев
6. Переносить остальные запросы домен за доменом

### Дополнительно (по запросу)
- Шаблоны файлов: `sql_loader.py`, пример репозитория и 2–3 `.sql`
- CI-джоб для миграций и «сухого прогона»
