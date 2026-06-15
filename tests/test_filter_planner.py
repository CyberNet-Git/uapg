"""Tests for EventFilterPlanner."""

from uapg.v2.events_config import EventsV2Config, parse_csv_set, parse_field_aliases
from uapg.v2.filter_planner import EventFilterPlanner, normalize_field_name, sql_where_from_plan


def test_events_v2_config_from_csv() -> None:
    cfg = EventsV2Config.from_csv(
        indexed="serial,dev_eui",
        filterable="EventType,serial",
        aliases="mountpoint:mountpoint_tag",
    )
    assert cfg.indexed_fields == frozenset({"serial", "dev_eui"})
    assert cfg.sql_filter_fields == frozenset({"EventType", "serial"})
    assert cfg.column_name("mountpoint") == "mountpoint_tag"


def test_normalize_field_name_with_aliases() -> None:
    aliases = {"mountpoint": "mountpoint_tag"}
    assert normalize_field_name("mountpoint", aliases) == "mountpoint_tag"
    assert normalize_field_name("serial", aliases) == "serial"


def test_sql_where_ilike() -> None:
    plan = {"field": "serial", "op": "ilike", "value": "%2311%"}
    where, params, _ = sql_where_from_plan(plan, param_offset=4)
    assert '"serial"' in where
    assert "ILIKE" in where
    assert params == ["%2311%"]


def test_sql_where_and() -> None:
    plan = {
        "and": [
            {"field": "serial", "op": "eq", "value": "abc"},
            {"field": "dev_eui", "op": "is_null", "value": True},
        ]
    }
    where, params, _ = sql_where_from_plan(plan)
    assert "AND" in where
    assert params == ["abc"]


def test_strip_event_type() -> None:
    planner = EventFilterPlanner()
    plan = {
        "and": [
            {"field": "EventType", "op": "in", "value": [1, 2]},
            {"field": "serial", "op": "ilike", "value": "%x%"},
        ]
    }
    stripped = planner.strip_event_type(plan)
    assert stripped == {"and": [{"field": "serial", "op": "ilike", "value": "%x%"}]}


def test_parse_csv_set_empty() -> None:
    assert parse_csv_set(None) == frozenset()
    assert parse_field_aliases("") == {}
