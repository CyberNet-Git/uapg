"""Публикация настроек и метрик историзации в адресном пространстве OPC UA.

Узлы — часть контракта с сервером: их имена строятся из ключей метрик, а тип
данных узла — из типа значения. Узел создаётся один раз, и значение другого
типа в него потом не запишется, поэтому и имена, и типы сверяются с эталоном.

Ошибки публикации никогда не ломают историзацию: витрина вторична.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from asyncua import ua


def metric_node_name(metric_path: str) -> str:
    """``write.variables.queue_size`` → ``WriteVariablesQueueSize``."""
    parts: List[str] = []
    for path_part in metric_path.split("."):
        for word in path_part.split("_"):
            if word:
                parts.append(word[:1].upper() + word[1:])
    return "".join(parts)


def flatten_metrics(metrics: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    flattened: Dict[str, Any] = {}
    for key, value in metrics.items():
        path = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, dict):
            flattened.update(flatten_metrics(value, path))
        else:
            flattened[path] = value
    return flattened


def metric_variant(value: Any) -> ua.Variant:
    """Значение метрики в Variant; тип определяет тип данных узла."""
    if isinstance(value, bool):
        return ua.Variant(value, ua.VariantType.Boolean)
    if isinstance(value, int):
        return ua.Variant(value, ua.VariantType.Int64)
    if isinstance(value, float):
        return ua.Variant(value, ua.VariantType.Double)
    if value is None:
        return ua.Variant("", ua.VariantType.String)
    return ua.Variant(str(value), ua.VariantType.String)


async def server_parent(server: Any, parent: Any = None) -> Any:
    """Узел, под которым создаётся History: явный или 0:Server."""
    if parent is not None:
        return parent
    try:
        node = getattr(getattr(server, "nodes", None), "server", None)
        if node is not None:
            return node
    except Exception:
        pass
    return await server.nodes.objects.get_child(["0:Server"])


async def get_or_add_object(parent: Any, namespace_index: int, name: str) -> Any:
    try:
        return await parent.get_child([f"{namespace_index}:{name}"])
    except Exception:
        return await parent.add_object(namespace_index, name)


async def get_or_add_variable(
    parent: Any, namespace_index: int, name: str, initial: ua.Variant
) -> Any:
    """Переменная только для чтения: в asyncua переменные по умолчанию такие."""
    try:
        return await parent.get_child([f"{namespace_index}:{name}"])
    except Exception:
        return await parent.add_variable(namespace_index, name, initial)


async def write_values(nodes: Dict[str, Any], values: Dict[str, ua.Variant]) -> None:
    """Записать значения в узлы, пропуская удалённые и недоступные."""
    for key, variant in values.items():
        node: Optional[Any] = nodes.get(key)
        if node is None:
            continue
        try:
            await node.write_value(variant)
        except Exception:
            continue
