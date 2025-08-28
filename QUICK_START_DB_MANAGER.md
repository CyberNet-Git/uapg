# Быстрый старт с DatabaseManager

## Установка

```bash
# Установка зависимостей
pip install asyncpg psycopg2-binary cryptography

# Или из файла requirements
pip install -r requirements-db-manager.txt
```

## Быстрое создание базы данных

### Python код

```python
import asyncio
from uapg.db_manager import DatabaseManager

async def main():
    # Создание менеджера
    db_manager = DatabaseManager("my_secure_password")
    
    # Создание БД
    success = await db_manager.create_database(
        user="opcua_user",
        password="opcua_password",
        database="opcua_history"
    )
    
    if success:
        print("База данных создана!")

asyncio.run(main())
```

### CLI команда

```bash
python -m uapg.cli create-db \
    --master-password "my_secure_password" \
    --user "opcua_user" \
    --password "opcua_password" \
    --database "opcua_history"
```

## Основные операции

### Резервное копирование

```bash
# Создание бэкапа
python -m uapg.cli backup \
    --master-password "my_secure_password" \
    --output "backup.backup"

# Восстановление из бэкапа
python -m uapg.cli restore \
    --master-password "my_secure_password" \
    --backup-file "backup.backup"
```

### Очистка данных

```bash
# Очистка данных старше 90 дней
python -m uapg.cli cleanup \
    --master-password "my_secure_password" \
    --retention-days 90

# Полная очистка
python -m uapg.cli cleanup \
    --master-password "my_secure_password" \
    --clear-all
```

### Информация о БД

```bash
python -m uapg.cli info \
    --master-password "my_secure_password"
```

### Управление конфигурацией

```bash
# Экспорт конфигурации
python -m uapg.cli config \
    --master-password "my_secure_password" \
    --export "config.json"

# Изменение главного пароля
python -m uapg.cli config \
    --master-password "old_password" \
    --change-password "new_password"
```

## Миграция схемы

1. Создайте файл миграции `migrations.json`:

```json
[
  {
    "version": "1.1",
    "description": "Добавление нового поля",
    "sql": "ALTER TABLE variable_metadata ADD COLUMN description TEXT;"
  }
]
```

2. Выполните миграцию:

```bash
python -m uapg.cli migrate \
    --master-password "my_secure_password" \
    --target-version "1.1" \
    --migration-file "migrations.json"
```

## Интеграция с HistoryPgSQL

После создания БД с помощью DatabaseManager:

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

## Безопасность

- **Главный пароль**: Используйте сложный пароль для шифрования
- **Файлы ключей**: Храните `.db_key` в безопасном месте
- **Права доступа**: Ограничьте доступ к файлам конфигурации

## Получение справки

```bash
# Общая справка
python -m uapg.cli --help

# Справка по команде
python -m uapg.cli create-db --help
python -m uapg.cli backup --help
```
