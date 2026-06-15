"""Tests for events V2 runtime config."""

from uapg.v2.events_config import EventsV2Config


def test_column_name_uses_alias() -> None:
    cfg = EventsV2Config.from_csv(aliases="api_mp:mountpoint_tag")
    assert cfg.column_name("api_mp") == "mountpoint_tag"
    assert cfg.column_name("serial") == "serial"


def test_sql_filter_fields_csv_sorted() -> None:
    cfg = EventsV2Config.from_csv(filterable="serial,EventType,dev_eui")
    assert cfg.sql_filter_fields_csv() == "EventType,dev_eui,serial"
