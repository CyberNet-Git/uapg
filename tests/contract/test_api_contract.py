"""Публичный API обязан совпадать с эталоном 0.2.15.

opc-vibro-iot-server импортирует оба класса, передаёт конструктору около тридцати
именованных аргументов и строит узлы OPC UA из ключей метрик. Любое расхождение
здесь ломает сервер тихо — либо на старте, либо в адресном пространстве.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from tests.contract.api_introspect import api_snapshot
from tests.contract.divergences import API as ALLOWED

BASELINE = Path(__file__).parent / "baseline" / "api.json"


def _diff_mapping(kind: str, baseline: Dict[str, Any], current: Dict[str, Any]) -> List[str]:
    diffs = []
    for key in sorted(set(baseline) - set(current)):
        diffs.append(f"{kind} removed: {key}")
    for key in sorted(set(current) - set(baseline)):
        diffs.append(f"{kind} added: {key}")
    for key in sorted(set(baseline) & set(current)):
        if baseline[key] != current[key]:
            diffs.append(f"{kind} changed: {key}: {baseline[key]} -> {current[key]}")
    return diffs


def _diff_sequence(kind: str, baseline: List[str], current: List[str]) -> List[str]:
    diffs = []
    for key in sorted(set(baseline) - set(current)):
        diffs.append(f"{kind} removed: {key}")
    for key in sorted(set(current) - set(baseline)):
        diffs.append(f"{kind} added: {key}")
    return diffs


def diff_api(baseline: Dict[str, Any], current: Dict[str, Any]) -> List[str]:
    diffs: List[str] = []
    diffs += _diff_sequence("module_all", baseline["module_all"], current["module_all"])
    diffs += _diff_mapping("constructor", baseline["constructors"], current["constructors"])

    for cls in sorted(set(baseline["classes"]) | set(current["classes"])):
        diffs += _diff_mapping(
            cls,
            baseline["classes"].get(cls, {}),
            current["classes"].get(cls, {}),
        )

    diffs += _diff_mapping(
        "instance_attribute",
        baseline["instance_attributes"],
        current["instance_attributes"],
    )
    for key in ("metric_paths", "metric_paths_v2", "cache_stat_keys", "connection_info_keys"):
        diffs += _diff_sequence(key, baseline[key], current[key])
    return diffs


def test_public_api_matches_baseline() -> None:
    baseline = json.loads(BASELINE.read_text())
    unexpected = [d for d in diff_api(baseline, api_snapshot()) if d not in ALLOWED]
    assert not unexpected, "Публичный API разошёлся с эталоном 0.2.15:\n" + "\n".join(unexpected)
