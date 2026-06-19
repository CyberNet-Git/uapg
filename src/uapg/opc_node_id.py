"""Helpers for normalizing OPC UA Node / NodeId values."""

from __future__ import annotations

from typing import Any

from asyncua import ua


def coerce_node_id(node_or_id: Any) -> ua.NodeId:
    """Приводит asyncua Node / NodeId / Variant к ua.NodeId."""
    if isinstance(node_or_id, ua.NodeId):
        return node_or_id
    if hasattr(node_or_id, "nodeid"):
        nid = node_or_id.nodeid
        if isinstance(nid, ua.NodeId):
            return nid
    if hasattr(node_or_id, "Value") and isinstance(node_or_id.Value, ua.NodeId):
        return node_or_id.Value
    raise TypeError(f"Expected NodeId or Node, got {type(node_or_id)!r}")
