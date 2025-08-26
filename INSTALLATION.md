# Инструкция по установке и обновлению UAPG

## Проблема с обновлением модуля

При использовании `uv sync` в папке `examples` модуль `uapg` не обновляется автоматически, потому что он установлен из внешнего источника, а не из локального кода.

## Решение 1: Использование локального пути (рекомендуется)

В папке `examples` уже настроен локальный путь к модулю в `pyproject.toml`:

```toml
[tool.uv.sources]
uapg = { path = ".." }
```

Это означает, что модуль берется из родительской папки. Для применения изменений:

```bash
cd uapg/examples
uv sync --reinstall
```

## Решение 2: Установка в режиме разработки

```bash
# В корневой папке uapg
uv pip install -e . --force-reinstall

# Или в папке examples
cd examples
uv pip install -e .. --force-reinstall
```

## Решение 3: Принудительное обновление зависимостей

```bash
cd uapg/examples
uv pip uninstall uapg
uv pip install -e .. --force-reinstall
```

## Решение 4: Исправление проблемы с uv_build

Если модуль не обновляется, проблема может быть в `uv_build`. Замените в `pyproject.toml`:

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

Затем пересоберите модуль:

```bash
cd uapg
rm -rf build/ dist/ *.egg-info/
uv pip install -e . --force-reinstall
```

## Проверка установленной версии

```bash
cd uapg/examples
uv run python3 -c "import uapg; print(uapg.__file__)"
```

Если путь указывает на локальную папку `src/uapg`, то модуль установлен правильно.

## Проверка новых параметров

После обновления проверьте, что новые параметры доступны:

```bash
cd uapg/examples
uv run python3 -c "from uapg import HistoryPgSQL; print('Constructor parameters:', HistoryPgSQL.__init__.__code__.co_varnames)"
```

Должно показать: `('self', 'user', 'password', 'database', 'host', 'port', 'min_size', 'max_size', 'max_history_data_response_size')`

## Структура проекта

```
uapg/
├── src/uapg/           # Исходный код модуля
├── examples/           # Примеры использования
│   ├── pyproject.toml  # Настройки для examples
│   └── basic_usage.py  # Базовый пример
├── pyproject.toml      # Основные настройки проекта
└── uv.lock            # Зафиксированные версии
```

## Пошаговая инструкция по обновлению

### Шаг 1: Проверка текущей версии
```bash
cd uapg/examples
uv run python3 -c "from uapg import HistoryPgSQL; print('Constructor parameters:', HistoryPgSQL.__init__.__code__.co_varnames)"
```

### Шаг 2: Обновление модуля
```bash
cd uapg
rm -rf build/ dist/ *.egg-info/
uv pip install -e . --force-reinstall
```

### Шаг 3: Проверка обновления
```bash
cd examples
uv run python3 -c "from uapg import HistoryPgSQL; print('Constructor parameters:', HistoryPgSQL.__init__.__code__.co_varnames)"
```

## Устранение неполадок

### Ошибка "Module not found"
```bash
cd uapg/examples
uv pip install -e ..
```

### Модуль не обновляется
```bash
cd uapg
rm -rf build/ dist/ *.egg-info/
uv pip install -e . --force-reinstall
```

### Проблемы с зависимостями
```bash
cd uapg/examples
rm -rf .venv
uv sync
```

### Проблемы с uv_build
Замените `uv_build` на `setuptools` в `pyproject.toml` и пересоберите модуль.

## Альтернативный способ запуска

Вместо `uv run` можно использовать прямое выполнение Python:

```bash
cd uapg/examples
python3 basic_usage.py
```

## Проверка функциональности

После обновления проверьте, что новые параметры доступны:

```python
from uapg import HistoryPgSQL

# Должно работать без ошибок
history = HistoryPgSQL(
    user='postgres',
    password='postmaster',
    database='opcua',
    host='127.0.0.1',
    port=5432,        # Новый параметр
    min_size=5,       # Новый параметр
    max_size=20       # Новый параметр
)
```

## Рекомендации

1. **Всегда используйте `uv pip install -e . --force-reinstall`** после изменения исходного кода
2. **Проверяйте путь к модулю** через `uv run python3 -c "import uapg; print(uapg.__file__)"`
3. **Используйте режим разработки** (`-e`) для активной разработки
4. **Очищайте виртуальное окружение** при проблемах с зависимостями
5. **Используйте `setuptools` вместо `uv_build`** для более надежной сборки
