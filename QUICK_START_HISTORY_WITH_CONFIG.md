# Быстрый старт с HistoryPgSQL и зашифрованной конфигурацией

## 🚀 Создание экземпляра с зашифрованной конфигурацией

### 1. Из файла конфигурации

```python
from uapg.history_pgsql import HistoryPgSQL

# Создание из файла db_config.enc
history = HistoryPgSQL.from_config_file(
    config_file="db_config.enc",
    master_password="my_secure_password"
)

await history.init()
```

### 2. Из зашифрованной строки

```python
from uapg.history_pgsql import HistoryPgSQL
from uapg.db_manager import DatabaseManager

# Получение зашифрованной строки
db_manager = DatabaseManager("my_secure_password")
encrypted_string = db_manager._encrypt_config(db_manager.config).decode()

# Создание из строки
history = HistoryPgSQL.from_encrypted_config(
    encrypted_config=encrypted_string,
    master_password="my_secure_password"
)

await history.init()
```

### 3. Смешанное использование (с fallback)

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

## 🔧 Обновление конфигурации на лету

```python
# Создание с базовыми параметрами
history = HistoryPgSQL(
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

## 📊 Получение информации о подключении

```python
# Получение текущих параметров
conn_info = history.get_connection_info()
print(f"Подключение к: {conn_info['host']}:{conn_info['port']}")
print(f"База данных: {conn_info['database']}")
print(f"Пользователь: {conn_info['user']}")
print(f"Инициализирован: {conn_info['initialized']}")
```

## 🎯 Полный рабочий пример

```python
import asyncio
from uapg.history_pgsql import HistoryPgSQL

async def main():
    # Создание из файла конфигурации
    history = HistoryPgSQL.from_config_file(
        config_file="db_config.enc",
        master_password="my_secure_password"
    )
    
    # Проверка параметров
    print("Параметры подключения:", history.get_connection_info())
    
    # Инициализация
    await history.init()
    
    # Ваша логика работы с историей
    # ...
    
    # Остановка
    await history.stop()

# Запуск
asyncio.run(main())
```

## 🔐 Приоритет конфигурации

1. **Зашифрованная строка** (`encrypted_config` + `master_password`)
2. **Файл конфигурации** (`config_file` + `master_password`)  
3. **Прямые параметры** (fallback)

## ⚠️ Важные моменты

- **Главный пароль** должен совпадать с тем, что использовался в `DatabaseManager`
- **Файл конфигурации** должен быть создан с помощью `DatabaseManager`
- **Обновление конфигурации** возможно только до вызова `init()`
- **Fallback параметры** используются, если зашифрованная конфигурация недоступна

## 🧪 Тестирование

```bash
# Проверка импорта
uv run python -c "from uapg.history_pgsql import HistoryPgSQL; print('✅ OK')"

# Создание из файла конфигурации
uv run python -c "from uapg.history_pgsql import HistoryPgSQL; h = HistoryPgSQL.from_config_file('db_config.enc', 'aaa'); print('✅ OK')"

# Запуск примера
uv run python examples/history_with_encrypted_config.py
```
