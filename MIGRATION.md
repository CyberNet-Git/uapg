# Миграция на UAPG с пулом подключений

Этот документ описывает процесс миграции с предыдущих версий UAPG на новую версию с пулом подключений PostgreSQL.

## Что изменилось

### Основные изменения

1. **Замена одиночного подключения на пул подключений**
   - `self._db: asyncpg.Connection` → `self._pool: asyncpg.Pool`
   - Решение проблемы "another operation is in progress"

2. **Новые параметры конструктора**
   - `port: int = 5432` - порт PostgreSQL
   - `min_size: int = 5` - минимальное количество соединений в пуле
   - `max_size: int = 20` - максимальное количество соединений в пуле

3. **Новые методы для работы с пулом**
   - `_execute()` - выполнение SQL запросов
   - `_fetch()` - выборка данных
   - `_fetchval()` - получение одного значения

## Пошаговая миграция

### Шаг 1: Обновление импортов

```python
# Старый код
from uapg import HistoryPgSQL

# Новый код (без изменений)
from uapg import HistoryPgSQL
```

### Шаг 2: Обновление конструктора

```python
# Старый код
history = HistoryPgSQL(
    user='postgres',
    password='postmaster',
    database='opcua',
    host='127.0.0.1'
)

# Новый код
history = HistoryPgSQL(
    user='postgres',
    password='postmaster',
    database='opcua',
    host='127.0.0.1',
    port=5432,           # Новый параметр
    min_size=5,          # Новый параметр
    max_size=20          # Новый параметр
)
```

### Шаг 3: Обновление инициализации

```python
# Старый код
await history.init()  # Создает одно соединение

# Новый код (без изменений в вызове)
await history.init()  # Создает пул соединений
```

### Шаг 4: Обновление закрытия

```python
# Старый код
await history.stop()  # Закрывает одно соединение

# Новый код (без изменений в вызове)
await history.stop()  # Закрывает пул соединений
```

## Примеры миграции

### Простой пример

```python
# Старый код
import asyncio
from uapg import HistoryPgSQL

async def main():
    history = HistoryPgSQL(
        user='postgres',
        password='postmaster',
        database='opcua',
        host='127.0.0.1'
    )
    
    await history.init()
    
    # Ваш код...
    
    await history.stop()

# Новый код
import asyncio
from uapg import HistoryPgSQL

async def main():
    history = HistoryPgSQL(
        user='postgres',
        password='postmaster',
        database='opcua',
        host='127.0.0.1',
        port=5432,
        min_size=5,
        max_size=20
    )
    
    await history.init()
    
    # Ваш код...
    
    await history.stop()
```

### OPC UA сервер

```python
# Старый код
server = Server()
await server.init()

history = HistoryPgSQL(
    user='postgres',
    password='postmaster',
    database='opcua',
    host='127.0.0.1'
)
await history.init()
server.iserver.history_manager.set_storage(history)

# Новый код
server = Server()
await server.init()

history = HistoryPgSQL(
    user='postgres',
    password='postmaster',
    database='opcua',
    host='127.0.0.1',
    port=5432,
    min_size=5,
    max_size=20
)
await history.init()
server.iserver.history_manager.set_storage(history)
```

## Рекомендации по настройке пула

### Для разработки и тестирования
```python
history = HistoryPgSQL(
    user='postgres',
    password='postmaster',
    database='opcua',
    host='127.0.0.1',
    min_size=2,
    max_size=10
)
```

### Для продакшена со средней нагрузкой
```python
history = HistoryPgSQL(
    user='postgres',
    password='postmaster',
    database='opcua',
    host='127.0.0.1',
    min_size=5,
    max_size=20
)
```

### Для высоконагруженных систем
```python
history = HistoryPgSQL(
    user='postgres',
    password='postmaster',
    database='opcua',
    host='127.0.0.1',
    min_size=10,
    max_size=50
)
```

## Обратная совместимость

Новая версия UAPG **полностью обратно совместима** с предыдущими версиями:

- Все существующие методы работают без изменений
- Старые параметры конструктора поддерживаются
- Новые параметры имеют разумные значения по умолчанию

## Проверка миграции

После миграции проверьте:

1. **Логи инициализации**
   ```
   INFO:uapg.history_pgsql:Historizing PgSQL pool initialized with 5-20 connections
   ```

2. **Отсутствие ошибок "another operation is in progress"**

3. **Улучшенная производительность** при одновременных операциях

## Устранение неполадок

### Ошибка "Pool is closed"
```python
# Убедитесь, что пул инициализирован
if not history._pool:
    await history.init()
```

### Медленная работа
```python
# Увеличьте размер пула
history = HistoryPgSQL(
    # ... другие параметры ...
    min_size=10,
    max_size=30
)
```

### Высокое потребление памяти
```python
# Уменьшите размер пула
history = HistoryPgSQL(
    # ... другие параметры ...
    min_size=2,
    max_size=10
)
```

## Поддержка

Если у вас возникли проблемы с миграцией:

1. Проверьте логи на наличие ошибок
2. Убедитесь, что PostgreSQL доступен
3. Проверьте параметры подключения
4. Создайте issue в репозитории проекта
