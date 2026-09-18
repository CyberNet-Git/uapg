"""Снимок публичной поверхности uapg.

Общий код для снятия эталона со старой реализации и для контрактного теста новой.
"""

from __future__ import annotations

import inspect
from typing import Any, Dict, List


def plain_signature(func: Any) -> str:
    """Сигнатура без аннотаций типов.

    Контракт — имена, порядок, вид и значения по умолчанию параметров. Текст
    аннотаций зависит от того, вычислены они или отложены (``from __future__
    import annotations``), и к поведению отношения не имеет.
    """
    signature = inspect.signature(func)
    parameters = [p.replace(annotation=inspect.Parameter.empty) for p in signature.parameters.values()]
    return str(signature.replace(parameters=parameters, return_annotation=inspect.Signature.empty))


def public_members(cls: type) -> Dict[str, str]:
    """Сигнатуры публичных методов и свойств класса."""
    members: Dict[str, str] = {}
    for name, member in inspect.getmembers(cls):
        if name.startswith("_"):
            continue
        if isinstance(member, property):
            members[name] = "property"
            continue
        if not callable(member):
            continue
        try:
            members[name] = f"{name}{plain_signature(member)}"
        except (TypeError, ValueError):
            members[name] = f"{name}(?)"
    return members


def metric_paths(value: Any, prefix: str = "") -> List[str]:
    """Плоские пути метрик: из них строятся имена узлов OPC UA."""
    if not isinstance(value, dict):
        return [prefix]
    paths: List[str] = []
    for key in sorted(value):
        child = f"{prefix}.{key}" if prefix else str(key)
        paths.extend(metric_paths(value[key], child))
    return paths


def api_snapshot() -> Dict[str, Any]:
    """Собрать снимок публичного API из установленного пакета uapg."""
    import uapg
    from uapg.history_timescale import HistoryTimescale
    from uapg.history_timescale_v2 import HistoryTimescaleV2

    storage = HistoryTimescale()
    storage_v2 = HistoryTimescaleV2()

    return {
        "module_all": sorted(uapg.__all__),
        # Конструктор в getmembers отфильтровывается как приватный, а именно его
        # opc-vibro-iot-server заполняет тремя десятками именованных аргументов.
        "constructors": {
            "HistoryTimescale": plain_signature(HistoryTimescale.__init__),
            "HistoryTimescaleV2": plain_signature(HistoryTimescaleV2.__init__),
        },
        "classes": {
            "HistoryTimescale": public_members(HistoryTimescale),
            "HistoryTimescaleV2": public_members(HistoryTimescaleV2),
        },
        "instance_attributes": {
            "max_history_data_response_size": storage.max_history_data_response_size,
            "suppress_initial_datachange": storage.suppress_initial_datachange,
        },
        "metric_paths": metric_paths(storage.get_performance_metrics()),
        "metric_paths_v2": metric_paths(storage_v2.get_performance_metrics()),
        "cache_stat_keys": sorted(storage.get_cache_stats()),
        "connection_info_keys": sorted(storage.get_connection_info()),
    }
