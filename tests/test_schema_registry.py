"""Tests for schema registry helpers."""

from asyncua import ua

from uapg.v2.schema_registry import physical_table_name, slug_from_node_id


def test_slug_from_node_id() -> None:
    node_id = ua.NodeId(1001, 2)
    slug = slug_from_node_id(node_id)
    assert "1001" in slug
    assert physical_table_name(slug).startswith("evt_")
