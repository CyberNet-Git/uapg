"""План фильтрации событий и его перевод в SQL."""

from __future__ import annotations

from typing import Any, List

from asyncua import ua

from uapg.storage.filter_plan import (
    EventFilterPlanner,
    like_to_ilike,
    render_filter,
)


def _operand(field: str, type_name: str = "Events.Test") -> ua.SimpleAttributeOperand:
    return ua.SimpleAttributeOperand(
        TypeDefinitionId=ua.NodeId(type_name, 2),
        BrowsePath=[ua.QualifiedName(field)],
        AttributeId=ua.AttributeIds.Value,
    )


def _element(operator: ua.FilterOperator, operands: List[Any]) -> ua.ContentFilterElement:
    element = ua.ContentFilterElement()
    element.FilterOperator = operator
    element.FilterOperands = operands
    return element


def _filter(*elements: ua.ContentFilterElement) -> ua.EventFilter:
    content = ua.ContentFilter()
    content.Elements = list(elements)
    event_filter = ua.EventFilter()
    event_filter.WhereClause = content
    return event_filter


class TestPlanBuilding:
    def test_empty_filter(self) -> None:
        assert EventFilterPlanner().build(None) == {}
        assert EventFilterPlanner().build(_filter()) == {}

    def test_equals(self) -> None:
        plan = EventFilterPlanner().build(
            _filter(
                _element(
                    ua.FilterOperator.Equals,
                    [_operand("dev_eui"), ua.LiteralOperand(ua.Variant("abc"))],
                )
            )
        )
        assert plan == {"field": "dev_eui", "op": "eq", "value": "abc"}

    def test_like_pattern_is_translated(self) -> None:
        """Шаблоны OPC UA используют * и ?, SQL — % и _."""
        plan = EventFilterPlanner().build(
            _filter(
                _element(
                    ua.FilterOperator.Like,
                    [_operand("serial"), ua.LiteralOperand(ua.Variant("A*B?C"))],
                )
            )
        )
        assert plan["value"] == "A%B_C"

    def test_alias_is_applied(self) -> None:
        planner = EventFilterPlanner(field_aliases={"devEui": "dev_eui"})
        plan = planner.build(
            _filter(
                _element(
                    ua.FilterOperator.Equals,
                    [_operand("devEui"), ua.LiteralOperand(ua.Variant("x"))],
                )
            )
        )
        assert plan["field"] == "dev_eui"

    def test_disallowed_field_is_dropped(self) -> None:
        planner = EventFilterPlanner(allowed_fields={"serial"})
        plan = planner.build(
            _filter(
                _element(
                    ua.FilterOperator.Equals,
                    [_operand("secret"), ua.LiteralOperand(ua.Variant("x"))],
                )
            )
        )
        assert plan == {}

    def test_and_of_two_conditions(self) -> None:
        planner = EventFilterPlanner()
        plan = planner.build(
            _filter(
                _element(
                    ua.FilterOperator.Equals,
                    [_operand("serial"), ua.LiteralOperand(ua.Variant("S1"))],
                ),
                _element(
                    ua.FilterOperator.Like,
                    [_operand("dev_eui"), ua.LiteralOperand(ua.Variant("%ab%"))],
                ),
                _element(
                    ua.FilterOperator.And,
                    [ua.ElementOperand(Index=0), ua.ElementOperand(Index=1)],
                ),
            )
        )
        assert set(planner.typed_fields(plan)) == {"serial", "dev_eui"}


class TestEventTypeExtraction:
    def _type_filter(self) -> ua.EventFilter:
        return _filter(
            _element(
                ua.FilterOperator.InList,
                [
                    _operand("EventType"),
                    ua.LiteralOperand(ua.Variant(ua.NodeId("Events.SensorInactive", 2))),
                ],
            )
        )

    def test_names_are_extracted(self) -> None:
        planner = EventFilterPlanner()
        plan = planner.build(self._type_filter())
        assert planner.event_type_names(plan) == ["SensorInactive"]

    def test_event_type_is_removed_from_typed_plan(self) -> None:
        """Тип события ищется отдельным столбцом, а не колонкой типизированной таблицы."""
        planner = EventFilterPlanner()
        plan = planner.build(self._type_filter())
        assert planner.without_event_type(plan) == {}

    def test_typed_fields_ignore_event_type(self) -> None:
        planner = EventFilterPlanner()
        plan = planner.build(self._type_filter())
        assert planner.typed_fields(plan) == set()


class TestRendering:
    def test_equals(self) -> None:
        rendered = render_filter({"field": "serial", "op": "eq", "value": "S1"})
        assert rendered.sql == 't."serial" = $1'
        assert rendered.params == ["S1"]

    def test_parameters_are_numbered_from_offset(self) -> None:
        rendered = render_filter(
            {"field": "serial", "op": "eq", "value": "S1"}, param_offset=5
        )
        assert rendered.sql == 't."serial" = $5'
        assert rendered.next_param == 6

    def test_and_or_composition(self) -> None:
        plan = {
            "and": [
                {"field": "a", "op": "eq", "value": 1},
                {"or": [{"field": "b", "op": "ilike", "value": "%x%"},
                        {"field": "c", "op": "is_null", "value": True}]},
            ]
        }
        rendered = render_filter(plan)
        assert rendered.sql == '(t."a" = $1 AND (t."b" ILIKE $2 OR t."c" IS NULL))'
        assert rendered.params == [1, "%x%"]

    def test_in_list(self) -> None:
        rendered = render_filter({"field": "a", "op": "in", "value": [1, 2, 3]})
        assert rendered.sql == 't."a" IN ($1, $2, $3)'
        assert rendered.params == [1, 2, 3]

    def test_injection_attempt_in_field_name_is_refused(self) -> None:
        rendered = render_filter({"field": 'a"; DROP TABLE x --', "op": "eq", "value": 1})
        assert rendered.sql == ""


class TestMissingColumns:
    """Поле, которого у типа события нет, обязано стать константой, а не исчезнуть."""

    def test_comparison_with_missing_column_never_matches(self) -> None:
        rendered = render_filter(
            {"field": "dev_eui", "op": "eq", "value": "x"},
            available_columns={"serial"},
        )
        assert rendered.sql == "FALSE"
        assert rendered.params == []

    def test_is_null_on_missing_column_always_matches(self) -> None:
        rendered = render_filter(
            {"field": "dev_eui", "op": "is_null", "value": True},
            available_columns={"serial"},
        )
        assert rendered.sql == "TRUE"

    def test_condition_does_not_weaken_when_field_is_missing(self) -> None:
        """Если просто убрать условие, запрос вернёт события, которых не просили."""
        plan = {
            "and": [
                {"field": "serial", "op": "eq", "value": "S1"},
                {"field": "dev_eui", "op": "eq", "value": "D1"},
            ]
        }
        rendered = render_filter(plan, available_columns={"serial"})
        assert rendered.sql == '(t."serial" = $1 AND FALSE)'

    def test_or_branch_survives_missing_field(self) -> None:
        plan = {
            "or": [
                {"field": "serial", "op": "eq", "value": "S1"},
                {"field": "dev_eui", "op": "eq", "value": "D1"},
            ]
        }
        rendered = render_filter(plan, available_columns={"serial"})
        assert rendered.sql == '(t."serial" = $1 OR FALSE)'


def test_like_translation() -> None:
    assert like_to_ilike("a*b?c") == "a%b_c"
    assert like_to_ilike(None) == "%"
