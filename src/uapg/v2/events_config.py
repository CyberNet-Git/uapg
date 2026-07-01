"""Runtime configuration for events V2 (deployment-specific, not hardcoded in core)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, FrozenSet, Mapping, Optional


def parse_csv_set(raw: Optional[str]) -> FrozenSet[str]:
    if not raw or not str(raw).strip():
        return frozenset()
    return frozenset(part.strip() for part in str(raw).split(",") if part.strip())


def parse_field_aliases(raw: Optional[str]) -> Dict[str, str]:
    """Parse ``api_name:column_name`` pairs separated by commas."""
    if not raw or not str(raw).strip():
        return {}
    result: Dict[str, str] = {}
    for part in str(raw).split(","):
        piece = part.strip()
        if not piece:
            continue
        if ":" not in piece:
            continue
        src, dst = piece.split(":", 1)
        src = src.strip()
        dst = dst.strip()
        if src and dst:
            result[src] = dst
    return result


@dataclass(frozen=True)
class EventsV2Config:
    """Per-deployment events search/index policy."""

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
    ) -> EventsV2Config:
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
    fields: FrozenSet[str] | set[str],
    aliases: Optional[Mapping[str, str]] = None,
) -> set[str]:
    """Расширяет whitelist полей парой alias src↔dst (конфигурация деплоя)."""
    expanded = set(fields)
    if aliases:
        for src, dst in aliases.items():
            if src in expanded:
                expanded.add(dst)
            if dst in expanded:
                expanded.add(src)
    return expanded


def typed_fields_supported(
    typed_fields: set[str],
    configured: FrozenSet[str] | set[str],
    aliases: Optional[Mapping[str, str]] = None,
) -> bool:
    if not configured:
        return True
    return typed_fields.issubset(expand_sql_filter_fields(configured, aliases))
