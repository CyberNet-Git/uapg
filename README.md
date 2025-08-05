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

## Установка

```bash
pip install uapg
```

## Быстрый старт

```python
import asyncio
from uapg import HistoryPgSQL

async def main():
    # Создание экземпляра истории
    history = HistoryPgSQL(
        user='postgres',
        password='your_password',
        database='opcua',
        host='localhost'
    )
    
    # Инициализация соединения
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
    
    # Закрытие соединения
    await history.stop()

# Запуск
asyncio.run(main())
```

## Требования

- Python 3.12+
- PostgreSQL 12+
- TimescaleDB (рекомендуется для больших объемов данных)

## Зависимости

- `asyncua>=1.0.0` - OPC UA клиент/сервер
- `asyncpg>=0.29.0` - Асинхронный драйвер PostgreSQL

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
