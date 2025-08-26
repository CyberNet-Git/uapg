# Резюме миграции на пул подключений

## Проблема

Ошибка `"cannot perform operation: another operation is in progress"` возникала из-за использования одного соединения PostgreSQL для всех операций.

## Решение

Замена одиночного подключения на пул подключений с автоматическим управлением.

## Ключевые изменения

### 1. Конструктор
```python
# Было
HistoryPgSQL(user='postgres', password='pass', database='db', host='localhost')

# Стало
HistoryPgSQL(
    user='postgres', 
    password='pass', 
    database='db', 
    host='localhost',
    port=5432,        # Новый
    min_size=5,       # Новый
    max_size=20       # Новый
)
```

### 2. Внутренняя структура
```python
# Было
self._db: asyncpg.Connection

# Стало  
self._pool: asyncpg.Pool
```

### 3. SQL операции
```python
# Было
await self._db.execute(query, *args)

# Стало
async with self._pool.acquire() as conn:
    await conn.execute(query, *args)
```

## Преимущества

✅ **Решает ошибку** "another operation is in progress"  
✅ **Улучшает производительность** при параллельных запросах  
✅ **Автоматическое управление** соединениями  
✅ **Масштабируемость** для высоконагруженных систем  
✅ **Обратная совместимость** с существующим кодом  

## Быстрая миграция

```python
# Добавьте новые параметры в конструктор
history = HistoryPgSQL(
    user='postgres',
    password='postmaster', 
    database='opcua',
    host='127.0.0.1',
    port=5432,        # Добавить
    min_size=5,       # Добавить  
    max_size=20       # Добавить
)

# Остальной код остается без изменений
await history.init()
# ... ваш код ...
await history.stop()
```

## Рекомендуемые настройки

| Сценарий | min_size | max_size |
|----------|----------|----------|
| Разработка | 2 | 10 |
| Продакшен | 5 | 20 |
| Высокая нагрузка | 10 | 50 |

## Важно: Обновление модуля

После внесения изменений в код модуля **ОБЯЗАТЕЛЬНО** пересоберите его:

```bash
cd uapg
rm -rf build/ dist/ *.egg-info/
uv pip install -e . --force-reinstall
```

### Проверка обновления

```bash
cd uapg/examples
uv run python3 -c "from uapg import HistoryPgSQL; print('Constructor parameters:', HistoryPgSQL.__init__.__code__.co_varnames)"
```

Должно показать: `('self', 'user', 'password', 'database', 'host', 'port', 'min_size', 'max_size', 'max_history_data_response_size')`

### Решение проблем с обновлением

Если модуль не обновляется, замените в `pyproject.toml`:

```toml
# Было (может вызывать проблемы)
[build-system]
requires = ["uv_build>=0.8.3,<0.9.0"]
build-backend = "uv_build"

# Стало (более надежно)
[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.build_meta"

[tool.setuptools.packages.find]
where = ["src"]
```

## Файлы изменений

- `uapg/src/uapg/history_pgsql.py` - основная логика
- `uapg/examples/basic_usage.py` - обновленный пример
- `uapg/examples/advanced_pool_config.py` - новый пример
- `uapg/README.md` - обновленная документация
- `uapg/MIGRATION.md` - подробная инструкция по миграции
- `uapg/INSTALLATION.md` - инструкция по установке и обновлению
- `uapg/tests/test_connection_pool.py` - тесты пула
- `uapg/CHANGELOG.md` - история изменений
- `uapg/pyproject.toml` - обновленные настройки сборки
