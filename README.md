# UAPG - OPC UA PostgreSQL History Storage Backend

[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

UAPG - это модуль для хранения исторических данных OPC UA в PostgreSQL с поддержкой TimescaleDB для эффективной работы с временными рядами.

## Возможности

- 📊 Хранение исторических данных OPC UA в PostgreSQL
- ⚡ Поддержка TimescaleDB для оптимизации временных рядов
- 🔄 Автоматическое управление жизненным циклом данных
- 📈 Индексация для быстрых запросов
- 🎯 Поддержка событий и изменений данных
- 🛡️ Валидация имен таблиц для безопасности
- 🚀 **Пул подключений PostgreSQL для высокой производительности**
- 🔒 **Решение проблемы "another operation is in progress"**

## Установка

```bash
pip install uapg
```

## Быстрый старт

### Базовое использование

```python
import asyncio
from uapg import HistoryPgSQL

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
    await history.new_historized_node(
        node_id=ua.NodeId(1, "MyVariable"),
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
history = HistoryPgSQL(
    user='postgres',
    password='your_password',
    database='opcua',
    host='localhost',
    min_size=10,     # Больше минимальных соединений для быстрого отклика
    max_size=50      # Больше максимальных соединений для пиковых нагрузок
)
```

#### Для ресурсоэффективных систем
```python
history = HistoryPgSQL(
    user='postgres',
    password='your_password',
    database='opcua',
    host='localhost',
    min_size=2,      # Минимальное количество соединений
    max_size=10      # Ограниченное количество максимальных соединений
)
```

#### Для сбалансированных систем (по умолчанию)
```python
history = HistoryPgSQL(
    user='postgres',
    password='your_password',
    database='opcua',
    host='localhost',
    min_size=5,      # Умеренное количество минимальных соединений
    max_size=20      # Умеренное количество максимальных соединений
)
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
- TimescaleDB (рекомендуется для больших объемов данных)

## Зависимости

- `asyncua>=1.0.0` - OPC UA клиент/сервер
- `asyncpg>=0.29.0` - Асинхронный драйвер PostgreSQL

## Примеры

### Базовый пример
```bash
cd examples
python basic_usage.py
```

### Продвинутая конфигурация пула
```bash
cd examples
python advanced_pool_config.py
```

## Разработка

### Установка для разработки

```bash
git clone https://github.com/rts-iot/uapg.git
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

## Поддержка

Если у вас есть вопросы или проблемы, создайте issue в репозитории проекта.
