#!/usr/bin/env python3

import sys
from pathlib import Path

from asyncua import ua

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from uapg.opc_node_id import coerce_node_id  # noqa: E402
from uapg.v2.schema_registry import slug_from_node_id  # noqa: E402


class _FakeNode:
    def __init__(self, nodeid: ua.NodeId) -> None:
        self.nodeid = nodeid


def test_coerce_node_id_from_node():
    nid = ua.NodeId("Events.SensorMountedEvent", 2)
    assert coerce_node_id(_FakeNode(nid)) == nid


def test_slug_from_node_id_accepts_node():
    nid = ua.NodeId("Events.SensorMountedEvent", 2)
    slug = slug_from_node_id(_FakeNode(nid))
    assert slug == "t_2_events_sensormountedevent"


if __name__ == "__main__":
    test_coerce_node_id_from_node()
    test_slug_from_node_id_accepts_node()
    print("OK")
