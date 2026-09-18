"""NodeId в строку-ключ и обратно к ua.NodeId.

Строка ``ns=X;t=Y`` — ключ, по которому переменные, источники и типы событий
лежат в ``variable_metadata``, ``event_sources`` и ``event_types``. Формат
обязан совпадать с 0.2.15 до символа: иначе после обновления сервер не узнает
ни одной уже зарегистрированной переменной и начнёт писать историю под новыми
идентификаторами.
"""

from __future__ import annotations

from typing import Any

from asyncua import ua

_TYPE_CHARS = {
    ua.NodeIdType.TwoByte: "i",
    ua.NodeIdType.FourByte: "i",
    ua.NodeIdType.Numeric: "i",
    ua.NodeIdType.String: "s",
    ua.NodeIdType.Guid: "g",
    ua.NodeIdType.ByteString: "b",
}


def coerce_node_id(node_or_id: Any) -> ua.NodeId:
    """Привести asyncua Node, NodeId или Variant с NodeId к ua.NodeId."""
    if isinstance(node_or_id, ua.NodeId):
        return node_or_id
    nodeid = getattr(node_or_id, "nodeid", None)
    if isinstance(nodeid, ua.NodeId):
        return nodeid
    value = getattr(node_or_id, "Value", None)
    if isinstance(value, ua.NodeId):
        return value
    raise TypeError(f"ожидался NodeId или Node, получено {type(node_or_id)!r}")


def format_node_id(node_id: Any) -> str:
    """Строковый ключ узла в формате ``ns=X;t=Y``."""
    try:
        node_id = coerce_node_id(node_id)
    except TypeError:
        pass

    if isinstance(node_id, str) and node_id.startswith("ns=") and ";" in node_id:
        return node_id

    node_type = getattr(node_id, "NodeIdType", None)
    namespace = getattr(node_id, "NamespaceIndex", None)
    identifier = getattr(node_id, "Identifier", None)
    if node_type is not None and namespace is not None and identifier is not None:
        return f"ns={namespace};{_TYPE_CHARS.get(node_type, 'x')}={identifier}"
    return str(node_id)


def group_key(node_id_str: str) -> str:
    """Префикс ключа до последней точки: группа «соседних» переменных."""
    if "." in node_id_str:
        return node_id_str.rsplit(".", 1)[0]
    return node_id_str or "default"


_VARIANT_TYPE_NAMES = {
    ua.VariantType.Boolean: "Boolean",
    ua.VariantType.SByte: "SByte",
    ua.VariantType.Byte: "Byte",
    ua.VariantType.Int16: "Int16",
    ua.VariantType.UInt16: "UInt16",
    ua.VariantType.Int32: "Int32",
    ua.VariantType.UInt32: "UInt32",
    ua.VariantType.Int64: "Int64",
    ua.VariantType.UInt64: "UInt64",
    ua.VariantType.Float: "Float",
    ua.VariantType.Double: "Double",
    ua.VariantType.String: "String",
    ua.VariantType.DateTime: "DateTime",
    ua.VariantType.Guid: "Guid",
    ua.VariantType.ByteString: "ByteString",
    ua.VariantType.XmlElement: "XmlElement",
    ua.VariantType.NodeId: "NodeId",
    ua.VariantType.ExpandedNodeId: "ExpandedNodeId",
    ua.VariantType.StatusCode: "StatusCode",
    ua.VariantType.QualifiedName: "QualifiedName",
    ua.VariantType.LocalizedText: "LocalizedText",
    ua.VariantType.ExtensionObject: "ExtensionObject",
    ua.VariantType.DataValue: "DataValue",
    ua.VariantType.Variant: "Variant",
    ua.VariantType.DiagnosticInfo: "DiagnosticInfo",
}


def data_type_name(datavalue: Any) -> str:
    """Имя типа значения для колонки variable_metadata.data_type."""
    variant = getattr(datavalue, "Value", None)
    variant_type = getattr(variant, "VariantType", None)
    if variant is None or not variant_type:
        return "Unknown"
    return _VARIANT_TYPE_NAMES.get(variant_type, str(variant_type))
