"""Tests for events V2 runtime config."""

from uapg.v2.events_config import EventsV2Config, expand_sql_filter_fields, typed_fields_supported


def test_column_name_uses_alias() -> None:
    cfg = EventsV2Config.from_csv(aliases="api_name:db_column")
    assert cfg.column_name("api_name") == "db_column"
    assert cfg.column_name("serial") == "serial"


def test_sql_filter_fields_csv_sorted() -> None:
    cfg = EventsV2Config.from_csv(filterable="serial,EventType,dev_eui")
    assert cfg.sql_filter_fields_csv() == "EventType,dev_eui,serial"


def test_expand_sql_filter_fields_with_aliases() -> None:
    aliases = {"legacy_name": "canonical_name"}
    expanded = expand_sql_filter_fields({"legacy_name"}, aliases)
    assert expanded == {"legacy_name", "canonical_name"}


def test_typed_fields_supported_with_aliases() -> None:
    configured = frozenset({"EventType", "legacy_name"})
    aliases = {"legacy_name": "canonical_name"}
    assert typed_fields_supported({"canonical_name"}, configured, aliases) is True
    assert typed_fields_supported({"unknown"}, configured, aliases) is False
