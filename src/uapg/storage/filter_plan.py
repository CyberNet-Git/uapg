"""Перевод EventFilter в условие SQL.

Фильтр событий OPC UA приходит деревом ContentFilter. Его можно применить двумя
способами: выбрать строки и отсеять их в памяти или отдать условие базе. Второе
принципиально лучше не из-за скорости: выборка ограничена LIMIT, и фильтрация
после неё возвращает не «первые N подходящих», а «подходящие среди первых N».
Клиент, ищущий событие недельной давности, получает пустой ответ.

Поддерживается не весь ContentFilter: сравнения, вхождение в список, шаблон и
проверка на null. Остальное остаётся на долю фильтрации в памяти — она никуда
не делась и применяется к тому, что вернула БД.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set

from asyncua import ua

FilterPlan = Dict[str, Any]

# Эти поля живут в events_ts, а не в колонках типизированной таблицы.
NON_COLUMN_FIELDS = frozenset({"EventType", "Time", "SourceNode"})

_COLUMN_NAME = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def like_to_ilike(pattern: Optional[str]) -> str:
    """Шаблон OPC UA в шаблон SQL: * и ? против % и _."""
    if pattern is None:
        return "%"
    return str(pattern).replace("*", "%").replace("?", "_")


def event_type_name_from_literal(value: Any) -> Optional[str]:
    """Короткое имя типа события из литерала фильтра."""
    if value is None or isinstance(value, int):
        return None
    identifier = value.Identifier if hasattr(value, "Identifier") else value
    if isinstance(identifier, int):
        return None
    name = str(identifier).strip()
    if name.startswith("ns=") and ";s=" in name:
        name = name.split(";s=", 1)[1]
    if name.startswith("Events."):
        name = name[len("Events.") :]
    return name or None


class EventFilterPlanner:
    """Строит план фильтрации из ua.EventFilter."""

    def __init__(
        self,
        allowed_fields: Optional[Set[str]] = None,
        field_aliases: Optional[Mapping[str, str]] = None,
    ) -> None:
        self._allowed_fields = allowed_fields
        self._aliases = field_aliases or {}

    def build(self, evfilter: Optional[ua.EventFilter]) -> FilterPlan:
        where = getattr(evfilter, "WhereClause", None) if evfilter is not None else None
        if not where or not where.Elements:
            return {}
        return self._build_element(where, len(where.Elements) - 1) or {}

    # ------------------------------------------------------------------ разбор

    def _build_element(self, content_filter: ua.ContentFilter, index: int) -> Optional[FilterPlan]:
        elements = content_filter.Elements or []
        if not 0 <= index < len(elements):
            return None

        element = elements[index]
        operands = [
            operand.Body if isinstance(operand, ua.ExtensionObject) else operand
            for operand in (element.FilterOperands or [])
        ]
        operator = element.FilterOperator

        if operator in (ua.FilterOperator.And, ua.FilterOperator.Or):
            key = "and" if operator == ua.FilterOperator.And else "or"
            parts = [
                self._build_element(content_filter, operand.Index)
                for operand in operands
                if isinstance(operand, ua.ElementOperand)
            ]
            present = [part for part in parts if part]
            return {key: present} if present else None

        if operator in (
            ua.FilterOperator.Equals,
            ua.FilterOperator.Like,
            ua.FilterOperator.InList,
        ):
            return self._compare(operator, operands)

        if operator == ua.FilterOperator.IsNull and operands:
            field = self._field_name(operands[0])
            return {"field": field, "op": "is_null", "value": True} if field else None

        return None

    def _compare(self, operator: ua.FilterOperator, operands: List[Any]) -> Optional[FilterPlan]:
        if len(operands) < 2:
            return None
        field = self._field_name(operands[0])
        if not field:
            return None
        # Установка может ограничить список полей, по которым разрешён поиск в SQL.
        if (
            self._allowed_fields is not None
            and field not in self._allowed_fields
            and field != "EventType"
        ):
            return None

        if operator == ua.FilterOperator.Equals:
            return {"field": field, "op": "eq", "value": self._literal(operands[1])}
        if operator == ua.FilterOperator.Like:
            return {"field": field, "op": "ilike", "value": like_to_ilike(str(self._literal(operands[1])))}

        values: List[Any] = []
        for operand in operands[1:]:
            literal = self._literal(operand)
            values.extend(literal if isinstance(literal, (list, tuple)) else [literal])
        return {"field": field, "op": "in", "value": values}

    def _field_name(self, operand: Any) -> Optional[str]:
        if not isinstance(operand, ua.SimpleAttributeOperand):
            return None
        names = [
            str(element.Name)
            for element in (operand.BrowsePath or [])
            if getattr(element, "Name", None)
        ]
        if names:
            return str(self._aliases.get(names[-1], names[-1]))
        return "EventType" if operand.TypeDefinitionId else None

    @staticmethod
    def _literal(operand: Any) -> Any:
        if isinstance(operand, ua.LiteralOperand):
            variant = operand.Value
            if variant is None:
                return None
            return variant.Value if hasattr(variant, "Value") else variant
        return operand

    # ------------------------------------------------------------------ разбор плана

    def typed_fields(self, plan: FilterPlan) -> Set[str]:
        """Поля плана, которые ищутся по колонкам типизированных таблиц."""
        found: Set[str] = set()

        def walk(node: FilterPlan) -> None:
            if not node:
                return
            field = node.get("field")
            if field and field not in NON_COLUMN_FIELDS:
                found.add(str(field))
            for key in ("and", "or"):
                for child in node.get(key) or []:
                    walk(child)

        walk(plan)
        return found

    def event_type_literals(self, plan: FilterPlan) -> List[Any]:
        literals: List[Any] = []

        def walk(node: FilterPlan) -> None:
            if not node:
                return
            if node.get("field") == "EventType" and node.get("op") == "in":
                literals.extend(node.get("value") or [])
            for key in ("and", "or"):
                for child in node.get(key) or []:
                    walk(child)

        walk(plan)
        return literals

    def event_type_names(self, plan: FilterPlan) -> List[str]:
        names: List[str] = []
        for value in self.event_type_literals(plan):
            name = event_type_name_from_literal(value)
            if name and name not in names:
                names.append(name)
        return names

    def event_type_ids(self, plan: FilterPlan) -> List[int]:
        """Идентификаторы типов, названные числом напрямую."""
        ids: List[int] = []
        for value in self.event_type_literals(plan):
            if isinstance(value, int):
                ids.append(value)
            elif hasattr(value, "Identifier") and isinstance(value.Identifier, int):
                ids.append(int(value.Identifier))
        return ids

    def without_event_type(self, plan: FilterPlan) -> FilterPlan:
        """Убрать из плана условия по типу события: он задаётся отдельным столбцом."""
        if not plan:
            return {}
        for key in ("and", "or"):
            if key in plan:
                children = [self.without_event_type(child) for child in plan[key]]
                present = [child for child in children if child]
                return {key: present} if present else {}
        return {} if plan.get("field") == "EventType" else plan


@dataclass
class RenderedFilter:
    """Условие SQL и его параметры."""

    sql: str
    params: List[Any]
    next_param: int


def render_filter(
    plan: FilterPlan,
    *,
    available_columns: Optional[Set[str]] = None,
    table_alias: str = "t",
    param_offset: int = 1,
) -> RenderedFilter:
    """Построить условие WHERE по плану.

    ``available_columns`` — колонки, которые есть у конкретного типа события.
    Поле, которого у типа нет, не выбрасывается из условия, а превращается в
    константу: сравнение с отсутствующим полем не выполняется никогда, а
    проверка на null — всегда. Если такое поле просто убрать, условие
    ослабнет, и запрос вернёт события, которые клиент не запрашивал.
    """
    params: List[Any] = []
    next_param = param_offset

    def render(node: FilterPlan) -> Optional[str]:
        nonlocal next_param
        if not node:
            return None

        for key, joiner in (("and", " AND "), ("or", " OR ")):
            if key in node:
                parts = [render(child) for child in node[key]]
                present = [part for part in parts if part]
                return f"({joiner.join(present)})" if present else None

        field = node.get("field")
        operation = node.get("op")
        if not field or field in NON_COLUMN_FIELDS:
            return None
        if not _COLUMN_NAME.match(str(field)):
            return None

        if available_columns is not None and field not in available_columns:
            return "TRUE" if operation == "is_null" else "FALSE"

        column = f'{table_alias}."{field}"'
        if operation == "eq":
            params.append(node.get("value"))
            next_param += 1
            return f"{column} = ${next_param - 1}"
        if operation == "ilike":
            params.append(node.get("value"))
            next_param += 1
            return f"{column} ILIKE ${next_param - 1}"
        if operation == "in":
            values = node.get("value") or []
            if not values:
                return None
            placeholders = []
            for value in values:
                params.append(value)
                placeholders.append(f"${next_param}")
                next_param += 1
            return f"{column} IN ({', '.join(placeholders)})"
        if operation == "is_null":
            return f"{column} IS NULL"
        return None

    sql = render(plan) or ""
    return RenderedFilter(sql=sql, params=params, next_param=next_param)


def plan_is_pushable(plan: FilterPlan, fields: Sequence[str]) -> bool:
    """Все ли поля плана могут быть проверены в SQL."""
    return all(_COLUMN_NAME.match(str(field)) for field in fields) and bool(plan)
