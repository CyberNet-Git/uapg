# DatabaseManager - Утилита управления базой данных OPC UA History

## Описание

`DatabaseManager` - это дополнительный модуль-утилита для управления базой данных PostgreSQL, используемой в `HistoryPgSQL`. Этот модуль работает независимо от основного функционала и предоставляет инструменты для административных задач.

## Основные возможности

### 1. Создание базы данных
- Автоматическое создание пользователя и базы данных
- Настройка схемы с базовыми таблицами метаданных
- Поддержка TimescaleDB для оптимизации временных рядов
- Создание необходимых индексов

### 2. Управление учетными данными
- Безопасное хранение конфигурации в зашифрованном виде
- Использование главного пароля для шифрования/дешифрования
- Возможность изменения главного пароля
- Экспорт/импорт конфигурации

### 3. Миграция схемы
- Версионирование схемы базы данных
- Применение скриптов миграции
- Отслеживание примененных изменений
- Поддержка инкрементальных обновлений

### 4. Резервное копирование
- Создание резервных копий в различных форматах
- Поддержка сжатия
- Восстановление из резервных копий
- Автоматическое именование файлов

### 5. Очистка данных
- Удаление старых данных по времени
- Выборочная очистка по узлам или типам событий
- Удаление таблиц узлов
- Полная очистка всех данных

## Установка

### Зависимости

```bash
pip install -r requirements-db-manager.txt
```

Или установка отдельных пакетов:

```bash
pip install asyncpg psycopg2-binary cryptography
```

### Требования к системе

- PostgreSQL 12+ 
- Python 3.8+
- TimescaleDB (опционально, для оптимизации)

## Использование

### Базовое использование

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

### Создание резервной копии

```python
# Создание бэкапа
backup_path = await db_manager.backup_database(
    backup_path="my_backup.backup",
    backup_format="custom",
    compression=True
)

# Восстановление из бэкапа
success = await db_manager.restore_database(backup_path)
```

### Миграция схемы

```python
migration_scripts = [
    {
        'version': '1.1',
        'description': 'Добавление нового поля',
        'sql': 'ALTER TABLE variable_metadata ADD COLUMN description TEXT;'
    }
]

success = await db_manager.migrate_schema('1.1', migration_scripts)
```

### Очистка данных

```python
# Очистка данных старше 90 дней
success = await db_manager.cleanup_old_data(retention_days=90)

# Удаление таблиц конкретных узлов
success = await db_manager.remove_node_tables(['ns=2;s=Node1', 'ns=2;s=Node2'])

# Полная очистка всех данных
success = await db_manager.clear_all_data()
```

### Standalone функции

```python
from uapg.db_manager import create_database_standalone, backup_database_standalone

# Создание БД без экземпляра класса
success = await create_database_standalone(
    user="opcua_user",
    password="opcua_password",
    database="opcua_history"
)

# Создание бэкапа без экземпляра класса
backup_path = await backup_database_standalone(
    user="opcua_user",
    password="opcua_password",
    database="opcua_history"
)
```

## Конфигурация

### Файлы конфигурации

- `db_config.enc` - зашифрованная конфигурация базы данных
- `.db_key` - ключ шифрования (содержит соль и ключ)

### Структура конфигурации

```json
{
  "user": "opcua_user",
  "password": "opcua_password",
  "database": "opcua_history",
  "host": "localhost",
  "port": 5432,
  "created_at": "2024-01-01T00:00:00",
  "version": "1.0"
}
```

## Безопасность

### Шифрование

- Использование PBKDF2 для генерации ключей
- Fernet для шифрования конфигурации
- Соль для каждого ключа
- 100,000 итераций для защиты от брутфорса

### Рекомендации

- Используйте сложные главные пароли
- Храните `.db_key` в безопасном месте
- Регулярно меняйте главный пароль
- Не передавайте ключи по незащищенным каналам

## Мониторинг и диагностика

### Получение информации о БД

```python
db_info = await db_manager.get_database_info()
print(f"Размер БД: {db_info['database_size']}")
print(f"Версия схемы: {db_info['schema_version']}")
print(f"Количество таблиц: {db_info['variable_tables']}")
```

### Логирование

Модуль использует стандартный Python logging. Настройте уровень логирования:

```python
import logging
logging.getLogger('uapg.db_manager').setLevel(logging.DEBUG)
```

## Примеры миграций

### Добавление нового поля

```sql
ALTER TABLE variable_metadata 
ADD COLUMN IF NOT EXISTS description TEXT;
```

### Создание нового индекса

```sql
CREATE INDEX IF NOT EXISTS idx_variable_metadata_description 
ON variable_metadata(description);
```

### Изменение типа данных

```sql
ALTER TABLE variable_metadata 
ALTER COLUMN data_type TYPE VARCHAR(100);
```

## Обработка ошибок

Все методы модуля возвращают `bool` или `None` для индикации успеха/неудачи. Подробная информация об ошибках записывается в лог.

```python
try:
    success = await db_manager.create_database(...)
    if not success:
        print("Ошибка создания БД - проверьте логи")
except Exception as e:
    print(f"Критическая ошибка: {e}")
```

## Интеграция с HistoryPgSQL

`DatabaseManager` создает схему, совместимую с `HistoryPgSQL`. После создания базы данных с помощью `DatabaseManager`, вы можете использовать `HistoryPgSQL` для работы с данными:

### Базовое использование

```python
from uapg.history_pgsql import HistoryPgSQL

# Использование созданной БД
history = HistoryPgSQL(
    user="opcua_user",
    password="opcua_password",
    database="opcua_history"
)

await history.init()
```

### Использование зашифрованной конфигурации

#### 1. Из файла конфигурации

```python
from uapg.history_pgsql import HistoryPgSQL

# Создание из файла зашифрованной конфигурации
history = HistoryPgSQL.from_config_file(
    config_file="db_config.enc",
    master_password="my_secure_password"
)

await history.init()
```

#### 2. Из зашифрованной строки

```python
from uapg.history_pgsql import HistoryPgSQL

# Создание из зашифрованной строки
history = HistoryPgSQL.from_encrypted_config(
    encrypted_config=encrypted_config_string,
    master_password="my_secure_password"
)

await history.init()
```

#### 3. Смешанное использование

```python
from uapg.history_pgsql import HistoryPgSQL

# Создание с приоритетом зашифрованной конфигурации
history = HistoryPgSQL(
    user="fallback_user",           # Fallback параметры
    password="fallback_password",
    database="fallback_db",
    config_file="db_config.enc",    # Приоритетная конфигурация
    master_password="my_secure_password"
)

await history.init()
```

#### 4. Обновление конфигурации на лету

```python
# Создание с базовыми параметрами
history = HistoryPgSQL(
    user="postgres",
    password="postmaster",
    database="opcua"
)

# Обновление конфигурации из файла
success = history.update_config(
    config_file="db_config.enc",
    master_password="my_secure_password"
)

if success:
    await history.init()  # Инициализация с новой конфигурацией
```

### Приоритет конфигурации

При создании `HistoryPgSQL` используется следующий приоритет:

1. **Зашифрованная строка** (`encrypted_config` + `master_password`)
2. **Файл конфигурации** (`config_file` + `master_password`)
3. **Прямые параметры** (fallback)

### Получение информации о подключении

```python
# Получение текущих параметров подключения
conn_info = history.get_connection_info()
print(f"Подключение к: {conn_info['host']}:{conn_info['port']}")
print(f"База данных: {conn_info['database']}")
print(f"Пользователь: {conn_info['user']}")
print(f"Инициализирован: {conn_info['initialized']}")
```

### Полный пример интеграции

```python
import asyncio
from uapg.db_manager import DatabaseManager
from uapg.history_pgsql import HistoryPgSQL

async def main():
    # 1. Создание базы данных с помощью DatabaseManager
    db_manager = DatabaseManager("my_secure_password")
    
    success = await db_manager.create_database(
        user="opcua_user",
        password="opcua_password",
        database="opcua_history",
        host="localhost",
        port=5432
    )
    
    if success:
        # 2. Использование созданной БД с HistoryPgSQL
        history = HistoryPgSQL.from_config_file(
            config_file="db_config.enc",
            master_password="my_secure_password"
        )
        
        # 3. Инициализация и работа
        await history.init()
        
        # 4. Ваша логика работы с историей
        # ...
        
        # 5. Остановка
        await history.stop()

asyncio.run(main())
```

## Поддержка и развитие

### Отчеты об ошибках

При обнаружении проблем создавайте issue с:
- Описанием проблемы
- Версией Python и зависимостей
- Логами ошибок
- Примером кода для воспроизведения

### Вклад в развитие

Приветствуются:
- Исправления ошибок
- Улучшения документации
- Новые функции
- Тесты

### Лицензия

См. файл `LICENSE` в корне проекта.
