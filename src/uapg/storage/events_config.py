"""Настройка поиска по событиям для конкретной установки.

Какие поля событий индексировать и по каким разрешать фильтрацию в SQL —
свойство развёртывания, а не библиотеки: у разных продуктов разные события.
Поэтому список задаётся снаружи, обычно переменными окружения вида
``UAPG_EVENTS_INDEXED_FIELDS``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, FrozenSet, Mapping, Optional, Set


def parse_csv_set(raw: Optional[str]) -> FrozenSet[str]:
    """Разобрать список имён через запятую."""
    if not raw or not str(raw).strip():
        return frozenset()
    return frozenset(part.strip() for part in str(raw).split(",") if part.strip())


def parse_field_aliases(raw: Optional[str]) -> Dict[str, str]:
    """Разобрать пары ``имя_в_api:имя_колонки`` через запятую.

    Псевдонимы нужны, когда поле события названо неудобно для колонки или когда
    клиент ищет по одному имени, а хранится другое.
    """
    if not raw or not str(raw).strip():
        return {}
    aliases: Dict[str, str] = {}
    for part in str(raw).split(","):
        piece = part.strip()
        if not piece or ":" not in piece:
            continue
        source, target = (side.strip() for side in piece.split(":", 1))
        if source and target:
            aliases[source] = target
    return aliases


@dataclass(frozen=True)
class EventsV2Config:
    """Какие поля событий индексируются и участвуют в фильтрации на стороне БД."""

    indexed_fields: FrozenSet[str] = field(default_factory=frozenset)
    sql_filter_fields: FrozenSet[str] = field(default_factory=frozenset)
    field_aliases: Mapping[str, str] = field(default_factory=dict)

    @classmethod
    def from_csv(
        cls,
        *,
        indexed: Optional[str] = None,
        filterable: Optional[str] = None,
        aliases: Optional[str] = None,
    ) -> "EventsV2Config":
        return cls(
            indexed_fields=parse_csv_set(indexed),
            sql_filter_fields=parse_csv_set(filterable),
            field_aliases=parse_field_aliases(aliases),
        )

    def column_name(self, field_name: str) -> str:
        return str(self.field_aliases.get(field_name, field_name))

    def sql_filter_fields_csv(self) -> str:
        return ",".join(sorted(self.sql_filter_fields))


def expand_sql_filter_fields(
    fields: FrozenSet[str] | Set[str],
    aliases: Optional[Mapping[str, str]] = None,
) -> Set[str]:
    """Дополнить список полей их псевдонимами в обе стороны.

    Клиент может прислать как исходное имя, так и имя колонки; разрешение
    фильтрации не должно зависеть от того, какое из них он выбрал.
    """
    expanded = set(fields)
    if aliases:
        for source, target in aliases.items():
            if source in expanded:
                expanded.add(target)
            if target in expanded:
                expanded.add(source)
    return expanded


def typed_fields_supported(
    typed_fields: Set[str],
    configured: FrozenSet[str] | Set[str],
    aliases: Optional[Mapping[str, str]] = None,
) -> bool:
    """Можно ли опустить фильтр по этим полям в SQL.

    Пустой список разрешённых полей означает «ограничений нет»: установка не
    настраивала политику, и поиск работает по всем полям, заведённым колонками.
    """
    if not configured:
        return True
    return typed_fields.issubset(expand_sql_filter_fields(configured, aliases))
