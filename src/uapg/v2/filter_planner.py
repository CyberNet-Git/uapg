"""EventFilter → FilterPlan JSON for SQL push-down."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping, Optional, Set

from asyncua import ua

FilterPlan = Dict[str, Any]
FilterExpr = Dict[str, Any]


def normalize_field_name(name: str, aliases: Optional[Mapping[str, str]] = None) -> str:
    if aliases and name in aliases:
        return str(aliases[name])
    return name


def _like_to_ilike(pattern: str) -> str:
    if pattern is None:
        return "%"
    return str(pattern).replace("*", "%").replace("?", "_")


class EventFilterPlanner:
    """Builds a FilterPlan JSON document from ua.EventFilter."""

    def __init__(
        self,
        allowed_fields: Optional[Set[str]] = None,
        field_aliases: Optional[Mapping[str, str]] = None,
    ) -> None:
        self._allowed_fields = allowed_fields
        self._field_aliases = field_aliases or {}

    def build(self, evfilter: Optional[ua.EventFilter]) -> FilterPlan:
        if evfilter is None or not evfilter.WhereClause or not evfilter.WhereClause.Elements:
            return {}
        root_index = len(evfilter.WhereClause.Elements) - 1
        expr = self._build_element(evfilter.WhereClause, root_index)
        return expr or {}

    def has_typed_pushdown(self, plan: FilterPlan) -> bool:
        return bool(self._collect_typed_fields(plan))

    def extract_event_type_ids(self, plan: FilterPlan) -> Optional[List[int]]:
        ids: List[int] = []

        def walk(node: FilterPlan) -> None:
            if not node:
                return
            if "field" in node and node.get("field") == "EventType" and node.get("op") == "in":
                for v in node.get("value") or []:
                    if isinstance(v, int):
                        ids.append(v)
                    elif hasattr(v, "Identifier"):
                        ids.append(int(v.Identifier))
            for key in ("and", "or"):
                for child in node.get(key) or []:
                    walk(child)

        walk(plan)
        return ids or None

    def strip_event_type(self, plan: FilterPlan) -> FilterPlan:
        if not plan:
            return {}
        if "and" in plan:
            children = [self.strip_event_type(c) for c in plan["and"]]
            children = [c for c in children if c]
            return {"and": children} if children else {}
        if "or" in plan:
            children = [self.strip_event_type(c) for c in plan["or"]]
            children = [c for c in children if c]
            return {"or": children} if children else {}
        if plan.get("field") == "EventType":
            return {}
        return plan

    def _collect_typed_fields(self, plan: FilterPlan) -> Set[str]:
        found: Set[str] = set()

        def walk(node: FilterPlan) -> None:
            if not node:
                return
            field = node.get("field")
            if field and field not in ("EventType", "Time", "SourceNode"):
                found.add(str(field))
            for key in ("and", "or"):
                for child in node.get(key) or []:
                    walk(child)

        walk(plan)
        return found

    def _build_element(self, content_filter: ua.ContentFilter, index: int) -> Optional[FilterExpr]:
        elements = content_filter.Elements or []
        if index < 0 or index >= len(elements):
            return None
        element = elements[index]
        operator = element.FilterOperator
        operands = []
        for operand_ext in element.FilterOperands or []:
            if isinstance(operand_ext, ua.ExtensionObject):
                operands.append(operand_ext.Body)
            else:
                operands.append(operand_ext)

        if operator == ua.FilterOperator.And:
            parts = [self._operand_to_expr(content_filter, op) for op in operands]
            parts = [p for p in parts if p]
            return {"and": parts} if parts else None
        if operator == ua.FilterOperator.Or:
            parts = [self._operand_to_expr(content_filter, op) for op in operands]
            parts = [p for p in parts if p]
            return {"or": parts} if parts else None
        if operator in (ua.FilterOperator.Equals, ua.FilterOperator.Like, ua.FilterOperator.InList):
            return self._simple_compare(operator, operands)
        if operator == ua.FilterOperator.IsNull:
            field = self._operand_field_name(operands[0]) if operands else None
            return {"field": field, "op": "is_null", "value": True} if field else None
        return None

    def _operand_to_expr(
        self, content_filter: ua.ContentFilter, operand: Any
    ) -> Optional[FilterExpr]:
        if isinstance(operand, ua.ElementOperand):
            return self._build_element(content_filter, operand.Index)
        return None

    def _simple_compare(self, operator: ua.FilterOperator, operands: List[Any]) -> Optional[FilterExpr]:
        if len(operands) < 2:
            return None
        field = self._operand_field_name(operands[0])
        if not field:
            return None
        if self._allowed_fields is not None and field not in self._allowed_fields and field != "EventType":
            return None
        value_operand = operands[1]
        if operator == ua.FilterOperator.Equals:
            return {"field": field, "op": "eq", "value": self._literal_value(value_operand)}
        if operator == ua.FilterOperator.Like:
            raw = self._literal_value(value_operand)
            return {"field": field, "op": "ilike", "value": _like_to_ilike(str(raw))}
        if operator == ua.FilterOperator.InList:
            return {"field": field, "op": "in", "value": self._literal_list(value_operand)}
        return None

    def _operand_field_name(self, operand: Any) -> Optional[str]:
        if isinstance(operand, ua.SimpleAttributeOperand):
            browse_path = operand.BrowsePath or []
            if browse_path:
                parts = [str(getattr(el, "Name", "")) for el in browse_path if getattr(el, "Name", None)]
                if parts:
                    return normalize_field_name(parts[-1], self._field_aliases)
            if operand.TypeDefinitionId:
                return "EventType"
        return None

    def _literal_value(self, operand: Any) -> Any:
        if isinstance(operand, ua.LiteralOperand):
            variant = operand.Value
            if variant is None:
                return None
            return variant.Value if hasattr(variant, "Value") else variant
        return operand

    def _literal_list(self, operand: Any) -> List[Any]:
        value = self._literal_value(operand)
        if isinstance(value, (list, tuple)):
            return list(value)
        return [value]


def sql_where_from_plan(
    plan: FilterPlan,
    *,
    table_alias: str = "t",
    param_offset: int = 1,
) -> tuple[str, List[Any], int]:
    """Render SQL WHERE fragment for typed table columns."""
    if not plan:
        return "", [], param_offset

    params: List[Any] = []

    def render(node: FilterPlan) -> Optional[str]:
        nonlocal param_offset
        if not node:
            return None
        if "and" in node:
            parts = [render(child) for child in node["and"]]
            parts = [p for p in parts if p]
            return f"({' AND '.join(parts)})" if parts else None
        if "or" in node:
            parts = [render(child) for child in node["or"]]
            parts = [p for p in parts if p]
            return f"({' OR '.join(parts)})" if parts else None
        field = node.get("field")
        op = node.get("op")
        if not field or field in ("EventType", "Time", "SourceNode"):
            return None
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", str(field)):
            return None
        col = f'{table_alias}."{field}"'
        if op == "eq":
            clause = f"{col} = ${param_offset}"
            params.append(node.get("value"))
            param_offset += 1
            return clause
        if op == "ilike":
            clause = f"{col} ILIKE ${param_offset}"
            params.append(node.get("value"))
            param_offset += 1
            return clause
        if op == "in":
            values = node.get("value") or []
            placeholders = []
            for v in values:
                placeholders.append(f"${param_offset}")
                params.append(v)
                param_offset += 1
            return f"{col} IN ({', '.join(placeholders)})" if placeholders else None
        if op == "is_null":
            return f"{col} IS NULL"
        return None

    top = render(plan)
    return top or "", params, param_offset
