"""Формат записи данных обязан совпадать с эталоном 0.2.15.

Расхождение здесь не роняет ничего сразу: сервер продолжает писать, а уже
накопленная история становится нечитаемой. Поэтому проверка побайтовая.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from tests.contract.wire_introspect import wire_snapshot

BASELINE = Path(__file__).parent / "baseline" / "wire.json"


def _current_snapshot() -> Dict[str, Any]:
    from uapg.codec import decode_event_data, encode_event_fields

    return wire_snapshot(encode_event_fields, decode_event_data)


def _diff(baseline: Any, current: Any, path: str = "") -> List[str]:
    if isinstance(baseline, dict) and isinstance(current, dict):
        diffs: List[str] = []
        for key in sorted(set(baseline) - set(current)):
            diffs.append(f"{path}.{key}: пропало")
        for key in sorted(set(current) - set(baseline)):
            diffs.append(f"{path}.{key}: появилось")
        for key in sorted(set(baseline) & set(current)):
            diffs += _diff(baseline[key], current[key], f"{path}.{key}")
        return diffs
    if baseline != current:
        return [f"{path}: {baseline!r} -> {current!r}"]
    return []


def test_wire_format_matches_baseline() -> None:
    baseline = json.loads(BASELINE.read_text())
    diffs = _diff(baseline, _current_snapshot())
    assert not diffs, "Формат записи разошёлся с эталоном 0.2.15:\n" + "\n".join(diffs)
