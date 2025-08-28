# Исправление SQL синтаксиса в UAPG

## Проблема

Ошибка `"syntax error at or near "$"` возникала в методе `save_event` при сохранении событий OPC UA в PostgreSQL.

## Причина

В методе `_format_event` использовался неправильный синтаксис для генерации плейсхолдеров PostgreSQL:

```python
# Было (неправильно)
placeholders.append(f"${{len(placeholders)+3}}")  # Создавало $3, $4, $5...

# Стало (правильно)
placeholder_start = 3
for i, name in enumerate(names):
    placeholders.append(f"${placeholder_start + i}")  # Создает $3, $4, $5...
```

## Что было исправлено

### 1. Метод `_format_event`

```python
def _format_event(self, event: Any) -> Tuple[str, str, Tuple[Any, ...]]:
    """
    Форматирование события для вставки в базу данных.
    """
    placeholders = []
    ev_variant_binaries = []
    ev_variant_dict = event.get_event_props_as_fields_dict()
    names = list(ev_variant_dict.keys())
    names.sort()
    
    # Начинаем с $3, так как $1 и $2 уже используются для времени и типа события
    placeholder_start = 3
    for i, name in enumerate(names):
        placeholders.append(f"${placeholder_start + i}")
        ev_variant_binaries.append(variant_to_binary(ev_variant_dict[name]))
    
    return self._list_to_sql_str(names), ", ".join(placeholders), tuple(ev_variant_binaries)
```

### 2. Правильная последовательность плейсхолдеров

Теперь SQL запросы генерируются корректно:

```sql
-- Было (неправильно)
INSERT INTO "evt_2_1" (_Timestamp, _EventTypeName, "Field1", "Field2") 
VALUES ($1, $2, $3, $4, $5, $6)

-- Стало (правильно)
INSERT INTO "evt_2_1" (_Timestamp, _EventTypeName, "Field1", "Field2") 
VALUES ($1, $2, $3, $4)
```

## Результат исправления

✅ **Устранена ошибка** "syntax error at or near "$""  
✅ **Правильная генерация** плейсхолдеров PostgreSQL  
✅ **Корректные SQL запросы** для вставки событий  
✅ **Последовательная нумерация** плейсхолдеров ($1, $2, $3, $4...)  

## Тестирование

Созданы тесты для проверки правильности SQL синтаксиса:

```bash
cd uapg
uv run python3 -m pytest tests/test_sql_syntax.py -v
```

Все тесты проходят успешно.

## Применение исправления

После внесения изменений **ОБЯЗАТЕЛЬНО** пересоберите модуль:

```bash
cd uapg
rm -rf build/ dist/ *.egg-info/
uv pip install -e . --force-reinstall
```

## Проверка исправления

После обновления проверьте, что ошибка SQL синтаксиса больше не возникает при сохранении событий.

## Файлы изменений

- `uapg/src/uapg/history_pgsql.py` - исправлен метод `_format_event`
- `uapg/tests/test_sql_syntax.py` - добавлены тесты для проверки SQL синтаксиса
- `uapg/SQL_SYNTAX_FIX.md` - данная документация

## Примеры SQL запросов

### Вставка событий
```sql
INSERT INTO "evt_2_1" (_timestamp, _eventtypename, "Field1", "Field2")
VALUES ($1, $2, $3, $4);
```

### Вставка переменных
```sql
INSERT INTO "evt_2_1" (_timestamp, _eventtypename, "Field1", "Field2")
VALUES ($1, $2, $3, $4);
```
