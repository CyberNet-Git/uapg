"""Загрузка SQL из файлов пакета.

SQL живёт в ``.sql``, а не в f-строках посреди Python: так его видно целиком,
можно прочитать глазами, прогнать линтером и сравнить с тем, что реально
оказалось в базе.

Имя схемы подставляется текстом — иначе его не написать, PostgreSQL не
принимает идентификатор параметром. Поэтому оно обязательно проверяется:
подстановка чужой строки в DDL была бы прямой инъекцией.
"""

from __future__ import annotations

import re
from functools import lru_cache
from importlib import resources
from typing import List, Optional

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_INDEX_NAME = re.compile(
    r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?"
    r'"?(?P<name>[A-Za-z_][A-Za-z0-9_]*)"?',
    re.IGNORECASE,
)


def validate_identifier(name: str) -> str:
    """Проверить, что имя годится для подстановки в SQL."""
    if not _IDENTIFIER.match(name or ""):
        raise ValueError(
            f"недопустимый идентификатор {name!r}: ожидались буквы, цифры и подчёркивание"
        )
    return name


@lru_cache(maxsize=None)
def _read(package: str, filename: str) -> str:
    return resources.files(package).joinpath(filename).read_text(encoding="utf-8")


def load_sql(filename: str, schema: str, *, package: str = "uapg.sql.core") -> str:
    """Прочитать SQL-файл пакета и подставить имя схемы."""
    validate_identifier(schema)
    return _read(package, filename).replace("{schema}", schema)


def split_statements(sql: str) -> List[str]:
    """Разбить файл на отдельные инструкции.

    Разбор намеренно простой: в этих файлах нет ни функций с телом в ``$$``, ни
    точек с запятой внутри литералов. Файлы, где такое есть (миграции с
    процедурами), выполняются целиком и сюда не попадают.
    """
    statements = []
    for raw in sql.split(";"):
        statement = "\n".join(
            line for line in raw.splitlines() if not line.strip().startswith("--")
        ).strip()
        if statement:
            statements.append(statement)
    return statements


def index_name(statement: str) -> Optional[str]:
    """Имя индекса из инструкции CREATE INDEX."""
    match = _INDEX_NAME.search(statement)
    return match.group("name") if match else None
