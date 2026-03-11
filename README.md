# UAPG - OPC UA PostgreSQL History Storage Backend

[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Version](https://img.shields.io/badge/version-0.1.15-orange.svg)](https://github.com/CyberNet-Git/uapg)

UAPG - это модуль для хранения исторических данных OPC UA в PostgreSQL с поддержкой TimescaleDB для эффективной работы с временными рядами. 

### Основные компоненты

- **HistoryTimescale** ⭐ **Рекомендуется** - Продвинутая версия с едиными таблицами, батчевой записью и многоуровневым кэшированием для TimescaleDB. Основное направление развития проекта.
- **HistoryPgSQL** - Базовый модуль для работы с историческими данными OPC UA (поддерживается для обратной совместимости)
- **DatabaseManager** - Утилита для управления базой данных с зашифрованной конфигурацией
- **CLI интерфейс** - Командная строка для административных операций

## Направление развития проекта

**Основное направление развития проекта - использование TimescaleDB и компонента HistoryTimescale.**

### Почему HistoryTimescale?

HistoryTimescale является приоритетным компонентом проекта и рекомендуется для всех новых проектов по следующим причинам:

- 🚀 **Высокая производительность** - Батчевая запись и многоуровневое кэширование обеспечивают оптимальную производительность
- 📊 **Масштабируемость** - Единые таблицы и TimescaleDB hypertables позволяют эффективно работать с большими объемами данных
- 🔧 **Гибкость** - Настраиваемые параметры батчевой записи, кэширования и консистентности
- 🔄 **Надежность** - Автоматическое переподключение и мониторинг состояния БД
- 📈 **Активная разработка** - Все новые функции и оптимизации добавляются в первую очередь в HistoryTimescale

### ИсторияPgSQL

HistoryPgSQL поддерживается для обратной совместимости с существующими проектами, но новые функции и оптимизации в первую очередь добавляются в HistoryTimescale. Для новых проектов рекомендуется использовать HistoryTimescale.

### Выбор компонента

- **Используйте HistoryTimescale**, если:
  - Вы начинаете новый проект
  - Вам нужна максимальная производительность
  - Вы работаете с большими объемами данных
  - Вы используете или планируете использовать TimescaleDB
  
- **Используйте HistoryPgSQL**, если:
  - У вас уже есть работающий проект на HistoryPgSQL
  - Вам не требуется TimescaleDB
  - Вы хотите минимальную конфигурацию без дополнительных оптимизаций

## Возможности

### Основной функционал
- 📊 Хранение исторических данных OPC UA в PostgreSQL
- ⚡ Поддержка TimescaleDB для оптимизации временных рядов
- 🔄 Автоматическое управление жизненным циклом данных
- 📈 Индексация для быстрых запросов
- 🎯 Поддержка событий и изменений данных
- 🛡️ Валидация имен таблиц для безопасности
- 🚀 Пул подключений PostgreSQL для высокой производительности
- 🔒 Решение проблемы "another operation is in progress"

### HistoryTimescale - Продвинутые возможности
- 🗂️ **Единые таблицы** - Все переменные и события хранятся в общих таблицах вместо отдельных таблиц для каждой переменной
- ⚡ **Батчевая запись** - Оптимизированная запись данных пакетами с настраиваемым размером и интервалом
- 💾 **Многоуровневое кэширование** - In-memory кэш последних значений, кэш метаданных, кэш источников событий
- 🔄 **Автоматическое переподключение** - Мониторинг состояния БД и автоматическое восстановление соединения
- 📊 **TimescaleDB hypertables** - Поддержка временного и пространственного партиционирования (TimescaleDB 2+)
- 🎯 **Настраиваемая схема** - Размещение таблиц в указанной схеме (по умолчанию 'public')
- 📈 **Статистика кэшей** - Мониторинг эффективности кэширования с детальной статистикой
- 🔧 **Гибкая конфигурация** - Настройка режимов durability (async/sync) и консистентности (local/global)

### DatabaseManager - Управление базой данных
- 🗄️ **Автоматическое создание базы данных и пользователей**
- 🔐 **Зашифрованное хранение конфигурации с главным паролем**
- 📦 **Резервное копирование и восстановление**
- 🔄 **Миграция схемы с версионированием**
- 🧹 **Очистка старых данных и управление жизненным циклом**
- 📊 **Мониторинг и диагностика базы данных**

### CLI интерфейс
- 💻 **Командная строка для всех административных операций**
- 🚀 **Быстрое создание и настройка базы данных**
- 🔧 **Управление конфигурацией и паролями**
- 📋 **Автоматизация резервного копирования**

### Безопасность
- 🔐 **PBKDF2 + Fernet шифрование конфигурации**
- 🛡️ **Безопасное хранение учетных данных**
- 🔑 **Управление главными паролями**

## Установка

```bash
pip install uapg
```

## Быстрый старт

> **💡 Рекомендация**: Для новых проектов используйте **HistoryTimescale** - это основное направление развития проекта с лучшей производительностью и функциональностью.

### Использование HistoryTimescale (рекомендуется)

```python
import asyncio
from datetime import datetime, timedelta, timezone
from uapg.history_timescale import HistoryTimescale
from asyncua import ua

async def main():
    # Создание экземпляра HistoryTimescale с оптимизациями
    history = HistoryTimescale(
        user='postgres',
        password='your_password',
        database='opcua',
        host='localhost',
        port=5432,
        min_size=5,
        max_size=20,
        schema='public',  # Настраиваемая схема
        # Глобальная глубина хранения (TimescaleDB retention policy) для таблиц *history
        global_retention_period=timedelta(days=365),
        # Параметры батчевой записи
        history_write_batch_enabled=True,
        history_write_max_batch_size=500,
        history_write_max_batch_interval_sec=1.0,
        history_write_durability_mode="async",  # async или sync
        history_write_read_consistency_mode="local",  # local или global
        # Параметры кэширования
        history_cache_enabled=True,
        history_last_values_cache_enabled=True,
        history_last_values_cache_max_size_mb=100,
        history_metadata_cache_enabled=True,
        history_metadata_cache_init_max_rows=500000
    )
    
    # Инициализация (создает таблицы и загружает кэши)
    await history.init()

    # Если используете HistoryTimescale внутри asyncua Server — можно экспортировать настройки хранения
    # в дерево OPC UA (read-only переменные под 0:Server/<idx:History>/<idx:HistorySettings>):
    #
    # from asyncua import Server
    # server = Server()
    # await server.init()
    # idx = await server.register_namespace("http://example.com")
    # server.iserver.history_manager.set_storage(history)
    # await history.expose_history_settings_nodes(server, idx)
    
    # Настройка историзации узла
    node_id = ua.NodeId(1, "MyVariable")
    await history.new_historized_node(
        node_id=node_id,
        period=timedelta(days=30),
        count=10000
    )
    
    # Сохранение значения (автоматически батчируется)
    datavalue = ua.DataValue(
        Value=ua.Variant(42.0, ua.VariantType.Double),
        SourceTimestamp=datetime.now(timezone.utc),
        ServerTimestamp=datetime.now(timezone.utc)
    )
    await history.save_node_value(node_id, datavalue)
    
    # Чтение истории (использует кэш для последних значений)
    start_time = datetime.now(timezone.utc) - timedelta(hours=1)
    end_time = datetime.now(timezone.utc)
    results, continuation = await history.read_node_history(
        node_id, start_time, end_time, nb_values=100
    )
    
    # Получение статистики кэшей
    cache_stats = history.get_cache_stats()
    print(f"Cache hits: {cache_stats}")
    
    # Закрытие
    await history.stop()

asyncio.run(main())
```

### Базовое использование HistoryPgSQL (для обратной совместимости)

> **⚠️ Примечание**: HistoryPgSQL поддерживается для обратной совместимости. Для новых проектов рекомендуется использовать HistoryTimescale.

```python
import asyncio
from datetime import datetime, timedelta, timezone
from uapg import HistoryPgSQL
from asyncua import ua

async def main():
    # Создание экземпляра истории с пулом подключений
    history = HistoryPgSQL(
        user='postgres',
        password='your_password',
        database='opcua',
        host='localhost',
        port=5432,
        min_size=5,      # Минимальное количество соединений в пуле
        max_size=20      # Максимальное количество соединений в пуле
    )
    
    # Инициализация пула подключений
    await history.init()
    
    # Настройка историзации узла
    node_id = ua.NodeId(1, "MyVariable")
    await history.new_historized_node(
        node_id=node_id,
        period=timedelta(days=30),  # Хранить данные 30 дней
        count=10000  # Максимум 10000 записей
    )
    
    # Сохранение значения
    datavalue = ua.DataValue(
        Value=ua.Variant(42.0, ua.VariantType.Double),
        SourceTimestamp=datetime.now(timezone.utc),
        ServerTimestamp=datetime.now(timezone.utc)
    )
    await history.save_node_value(node_id, datavalue)
    
    # Чтение истории
    start_time = datetime.now(timezone.utc) - timedelta(hours=1)
    end_time = datetime.now(timezone.utc)
    results, continuation = await history.read_node_history(
        node_id, start_time, end_time, nb_values=100
    )
    
    # Закрытие пула подключений
    await history.stop()

# Запуск
asyncio.run(main())
```

### Конфигурация пула подключений

UAPG поддерживает настройку пула подключений для различных сценариев использования:

#### Для высоконагруженных систем
```python
history = HistoryTimescale(
    user='postgres',
    password='your_password',
    database='opcua',
    host='localhost',
    min_size=10,     # Больше минимальных соединений для быстрого отклика
    max_size=50,     # Больше максимальных соединений для пиковых нагрузок
    history_write_max_batch_size=1000,  # Большие батчи для высокой нагрузки
    history_write_max_batch_interval_sec=0.5  # Частые флаши
)
```

#### Для ресурсоэффективных систем
```python
history = HistoryTimescale(
    user='postgres',
    password='your_password',
    database='opcua',
    host='localhost',
    min_size=2,      # Минимальное количество соединений
    max_size=10,     # Ограниченное количество максимальных соединений
    history_last_values_cache_max_size_mb=50,  # Меньший кэш
    history_metadata_cache_init_max_rows=100000  # Меньший кэш метаданных
)
```

#### Для сбалансированных систем (по умолчанию)
```python
history = HistoryTimescale(
    user='postgres',
    password='your_password',
    database='opcua',
    host='localhost',
    min_size=5,      # Умеренное количество минимальных соединений
    max_size=20      # Умеренное количество максимальных соединений
)
```

## HistoryTimescale - Подробное руководство

### Архитектура единых таблиц

HistoryTimescale использует новую архитектуру с едиными таблицами вместо создания отдельных таблиц для каждой переменной:

- **`variables_history`** - Единая таблица для всех переменных с полем `variable_id`
- **`events_history`** - Единая таблица для всех событий с полями `source_id` и `event_type_id`
- **`variable_metadata`** - Метаданные переменных (node_id, data_type, retention_period, max_records)
- **`event_sources`** - Источники событий с периодом хранения
- **`event_types`** - Типы событий
- **`variables_last_value`** - Кэш последних значений переменных

### Батчевая запись

HistoryTimescale поддерживает оптимизированную батчевую запись данных:

```python
history = HistoryTimescale(
    # ... параметры подключения ...
    history_write_batch_enabled=True,           # Включить батчевую запись
    history_write_max_batch_size=500,            # Максимальный размер батча
    history_write_max_batch_interval_sec=1.0,     # Интервал флаша (секунды)
    history_write_queue_max_size=10000,          # Максимальный размер очереди
    history_write_durability_mode="async",       # async (быстро) или sync (надежно)
    history_write_read_consistency_mode="local"   # local (быстро) или global (консистентно)
)
```

**Режимы durability:**
- `async` - Запись асинхронная, не блокирует вызывающий код (рекомендуется для высокой производительности)
- `sync` - Запись синхронная, ожидает завершения флаша (гарантирует запись перед возвратом)

**Режимы консистентности:**
- `local` - Чтение может не видеть только что записанные данные (быстрее)
- `global` - Чтение всегда видит записанные данные (консистентнее, но медленнее)

### Многоуровневое кэширование

HistoryTimescale использует несколько уровней кэширования для оптимизации производительности:

#### 1. In-memory кэш последних значений
```python
history = HistoryTimescale(
    history_last_values_cache_enabled=True,
    history_last_values_cache_max_size_mb=100,  # Лимит памяти для кэша
    history_last_values_init_batch_size=1000     # Размер батча при инициализации
)
```

#### 2. Кэш метаданных переменных
```python
history = HistoryTimescale(
    history_metadata_cache_enabled=True,
    history_metadata_cache_init_max_rows=500000  # Максимальное количество записей в кэше
)
```

#### 3. Статистика кэшей
```python
# Получение статистики эффективности кэшей
cache_stats = history.get_cache_stats()
print(f"Last values memory hits: {cache_stats['last_values_memory_hits']}")
print(f"Last values memory misses: {cache_stats['last_values_memory_misses']}")
print(f"Variable metadata hits: {cache_stats['variable_metadata_hits']}")
print(f"Variable metadata misses: {cache_stats['variable_metadata_misses']}")

# Сброс статистики
history.reset_cache_stats()
```

### TimescaleDB hypertables

HistoryTimescale автоматически создает TimescaleDB hypertables для оптимизации временных рядов:

- **Временное партиционирование** - По полю timestamp (sourcetimestamp для переменных, event_timestamp для событий)
- **Пространственное партиционирование** - По variable_id или source_id (TimescaleDB 2+)
- **Автоматическое определение версии** - Поддержка TimescaleDB 1.x и 2.x
- **Глобальная ретенция** - Опциональная политика хранения через TimescaleDB `add_retention_policy` (параметр `global_retention_period`).
  При изменении глобального периода в рантайме можно вызвать `reapply_global_retention_policy(...)`.

### Настраиваемая схема

Таблицы можно размещать в указанной схеме:

```python
history = HistoryTimescale(
    # ... параметры подключения ...
    schema='opcua_history'  # Все таблицы будут созданы в этой схеме
)
```

### Автоматическое переподключение

HistoryTimescale автоматически отслеживает состояние подключения к БД и восстанавливает соединение при сбоях:

- Фоновый монитор состояния подключения
- Автоматические попытки переподключения с экспоненциальной задержкой
- Антиспам логирование при длительной недоступности БД
- Уведомления о восстановлении соединения

### Зашифрованная конфигурация

HistoryTimescale поддерживает зашифрованную конфигурацию так же, как HistoryPgSQL:

```python
# Создание из файла конфигурации
history = HistoryTimescale.from_config_file(
    config_file="db_config.enc",
    master_password="my_secure_password",
    min_size=5,
    max_size=20
)

# Создание из зашифрованной строки
history = HistoryTimescale.from_encrypted_config(
    encrypted_config=encrypted_config_string,
    master_password="my_secure_password",
    min_size=5,
    max_size=20
)

# Обновление конфигурации
history.update_config(
    config_file="db_config.enc",
    master_password="my_secure_password"
)
```

## DatabaseManager - Управление базой данных

`DatabaseManager` - это дополнительный модуль для управления базой данных PostgreSQL, используемой в `HistoryPgSQL`. Предоставляет инструменты для административных задач с поддержкой зашифрованной конфигурации.

### Быстрое создание базы данных

```python
import asyncio
from uapg.db_manager import DatabaseManager

async def main():
    # Создание менеджера с главным паролем
    db_manager = DatabaseManager("my_secure_master_password")
    
    # Создание базы данных
    success = await db_manager.create_database(
        user="opcua_user",
        password="opcua_password",
        database="opcua_history",
        host="localhost",
        port=5432
    )
    
    if success:
        print("База данных создана успешно!")

asyncio.run(main())
```

### CLI команды

```bash
# Создание базы данных
python -m uapg.cli create-db \
    --master-password "my_secure_password" \
    --user "opcua_user" \
    --password "opcua_password" \
    --database "opcua_history"

# Создание резервной копии
python -m uapg.cli backup \
    --master-password "my_secure_password" \
    --output "backup.backup"

# Восстановление из резервной копии
python -m uapg.cli restore \
    --master-password "my_secure_password" \
    --backup-file "backup.backup"

# Очистка старых данных
python -m uapg.cli cleanup \
    --master-password "my_secure_password" \
    --retention-days 90

# Миграция схемы
python -m uapg.cli migrate \
    --master-password "my_secure_password" \
    --target-version "1.1" \
    --migration-file "migrations.json"

# Получение информации о БД
python -m uapg.cli info \
    --master-password "my_secure_password"

# Управление конфигурацией
python -m uapg.cli config \
    --master-password "my_secure_password" \
    --export "config.json"
```

### Зашифрованная конфигурация

UAPG поддерживает безопасное хранение конфигурации базы данных с использованием PBKDF2 + Fernet шифрования. Поддерживается как в HistoryPgSQL, так и в HistoryTimescale.

#### Создание из файла конфигурации
```python
from uapg.history_timescale import HistoryTimescale

# Создание из файла зашифрованной конфигурации
history = HistoryTimescale.from_config_file(
    config_file="db_config.enc",
    master_password="my_secure_password",
    min_size=5,
    max_size=20
)

await history.init()
```

#### Создание из зашифрованной строки
```python
from uapg.history_timescale import HistoryTimescale

# Создание из зашифрованной строки
history = HistoryTimescale.from_encrypted_config(
    encrypted_config=encrypted_config_string,
    master_password="my_secure_password",
    min_size=5,
    max_size=20
)

await history.init()
```

#### Смешанное использование с fallback
```python
from uapg.history_timescale import HistoryTimescale

# Создание с приоритетом зашифрованной конфигурации
history = HistoryTimescale(
    user="fallback_user",           # Fallback параметры
    password="fallback_password",
    database="fallback_db",
    config_file="db_config.enc",    # Приоритетная конфигурация
    master_password="my_secure_password"
)

await history.init()
```

#### Обновление конфигурации на лету
```python
# Создание с базовыми параметрами
history = HistoryTimescale(
    user="postgres",
    password="postmaster",
    database="opcua"
)

# Обновление конфигурации
success = history.update_config(
    config_file="db_config.enc",
    master_password="my_secure_password"
)

if success:
    await history.init()  # Инициализация с новой конфигурацией
```

## Решение проблем

### Ошибка "another operation is in progress"

Эта ошибка возникает при использовании одного соединения для нескольких одновременных операций. UAPG решает эту проблему с помощью пула подключений:

- **До**: Одно соединение `asyncpg.Connection` для всех операций
- **После**: Пул соединений `asyncpg.Pool` с автоматическим управлением

```python
# Старый способ (может вызывать ошибки)
self._db = await asyncpg.connect(**self._conn_params)
await self._db.execute(query)

# Новый способ (решает проблему)
self._pool = await asyncpg.create_pool(**self._conn_params, min_size=5, max_size=20)
async with self._pool.acquire() as conn:
    await conn.execute(query)
```

### Мониторинг пула подключений

```python
# Получение статуса пула
pool_status = await history._pool.get_status()
print(f"Active connections: {pool_status['active_connections']}")
print(f"Free connections: {pool_status['free_size']}")
```

## Требования

- Python 3.12+
- PostgreSQL 12+
- **TimescaleDB** (требуется для HistoryTimescale, рекомендуется для всех проектов)

## Зависимости

### Основные зависимости
- `asyncua>=1.0.0` - OPC UA клиент/сервер
- `asyncpg>=0.29.0` - Асинхронный драйвер PostgreSQL

### Дополнительные зависимости для DatabaseManager
- `psycopg2-binary` - Синхронный драйвер PostgreSQL (для CLI операций)
- `cryptography` - Шифрование конфигурации

### Установка всех зависимостей

```bash
# Основной пакет
pip install uapg

# Для DatabaseManager и CLI
pip install -r requirements-db-manager.txt

# Для разработки
pip install -e ".[dev]"
```

## Примеры

### Базовые примеры
```bash
cd examples
python basic_usage.py                    # Базовое использование HistoryPgSQL
python advanced_pool_config.py          # Продвинутая конфигурация пула
python history_timescale_usage.py       # Использование HistoryTimescale
python batch_write_config.py            # Конфигурация батчевой записи
python cache_optimization.py            # Оптимизация кэширования
```

### DatabaseManager примеры
```bash
cd examples
python db_manager_usage.py              # Использование DatabaseManager
python history_with_encrypted_config.py # Работа с зашифрованной конфигурацией
```

### CLI примеры
```bash
# Создание базы данных
python -m uapg.cli create-db --user opcua_user --password secret --database opcua_history

# Создание резервной копии
python -m uapg.cli backup --output backup.backup

# Получение информации о БД
python -m uapg.cli info
```

## Разработка

### Установка для разработки

```bash
git clone https://github.com/CyberNet-Git/uapg.git
cd uapg
pip install -e ".[dev]"
```

### Запуск тестов

```bash
pytest
```

### Форматирование кода

```bash
black src/
isort src/
```

### Проверка типов

```bash
mypy src/
```

## Лицензия

MIT License - см. файл [LICENSE](LICENSE) для подробностей.

## Дополнительная документация

- [**DatabaseManager**](README_DB_MANAGER.md) - Подробное руководство по DatabaseManager
- [**Быстрый старт с DatabaseManager**](QUICK_START_DB_MANAGER.md) - Быстрое создание и настройка БД
- [**История с зашифрованной конфигурацией**](QUICK_START_HISTORY_WITH_CONFIG.md) - Работа с зашифрованными настройками
- [**План рефакторинга БД**](DB_SQL_REFACTOR_PLAN.md) - Техническая документация
- [**Миграция пула подключений**](POOL_MIGRATION_SUMMARY.md) - Информация о миграции
- [**Установка**](INSTALLATION.md) - Подробные инструкции по установке
- [**Миграция**](MIGRATION.md) - Руководство по миграции
- [**Changelog**](CHANGELOG.md) - История изменений

## Поддержка

Если у вас есть вопросы или проблемы, создайте issue в репозитории проекта.

### Полезные ссылки

- [Репозиторий проекта](https://github.com/CyberNet-Git/uapg)
- [Документация](https://github.com/CyberNet-Git/uapg#readme)
- [Issues](https://github.com/CyberNet-Git/uapg/issues)
