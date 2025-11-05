"""
Тесты для фильтрации событий OPC UA.
"""

import unittest
from asyncua import ua
from asyncua.common.events import Event
from asyncua.common.event_objects import BaseEvent
from uapg.event_filter import (
    EventFilterEvaluator,
    apply_event_filter,
    create_source_node_filter,
    create_event_type_filter,
    create_property_filter,
)


class TestEventFilterBasic(unittest.TestCase):
    """Базовые тесты для фильтрации событий."""
    
    def setUp(self):
        """Подготовка тестовых данных."""
        # Создаем тестовые события
        self.event1 = BaseEvent()
        self.event1.Severity = 300
        self.event1.Message = ua.LocalizedText("Normal operation")
        self.event1.SourceNode = ua.NodeId(1234, 2)
        self.event1.EventType = ua.NodeId(ua.ObjectIds.BaseEventType)
        
        self.event2 = BaseEvent()
        self.event2.Severity = 700
        self.event2.Message = ua.LocalizedText("High severity alarm")
        self.event2.SourceNode = ua.NodeId(1234, 2)
        self.event2.EventType = ua.NodeId(ua.ObjectIds.AlarmConditionType)
        
        self.event3 = BaseEvent()
        self.event3.Severity = 900
        self.event3.Message = ua.LocalizedText("Critical error")
        self.event3.SourceNode = ua.NodeId(5678, 2)
        self.event3.EventType = ua.NodeId(ua.ObjectIds.BaseEventType)
        
        self.events = [self.event1, self.event2, self.event3]
    
    def test_no_filter(self):
        """Тест без фильтра - должны вернуться все события."""
        result = apply_event_filter(self.events, None)
        self.assertEqual(len(result), 3)
    
    def test_empty_filter(self):
        """Тест с пустым фильтром - должны вернуться все события."""
        evfilter = ua.EventFilter()
        result = apply_event_filter(self.events, evfilter)
        self.assertEqual(len(result), 3)
    
    def test_severity_filter_greater_than(self):
        """Тест фильтрации по Severity > 500."""
        evfilter = create_property_filter(
            "Severity",
            ua.FilterOperator.GreaterThan,
            500
        )
        result = apply_event_filter(self.events, evfilter)
        self.assertEqual(len(result), 2)  # event2 и event3
        self.assertIn(self.event2, result)
        self.assertIn(self.event3, result)
    
    def test_severity_filter_greater_than_or_equal(self):
        """Тест фильтрации по Severity >= 700."""
        evfilter = create_property_filter(
            "Severity",
            ua.FilterOperator.GreaterThanOrEqual,
            700
        )
        result = apply_event_filter(self.events, evfilter)
        self.assertEqual(len(result), 2)  # event2 и event3
    
    def test_severity_filter_less_than(self):
        """Тест фильтрации по Severity < 500."""
        evfilter = create_property_filter(
            "Severity",
            ua.FilterOperator.LessThan,
            500
        )
        result = apply_event_filter(self.events, evfilter)
        self.assertEqual(len(result), 1)  # только event1
        self.assertIn(self.event1, result)
    
    def test_severity_filter_equals(self):
        """Тест фильтрации по Severity == 700."""
        evfilter = create_property_filter(
            "Severity",
            ua.FilterOperator.Equals,
            700
        )
        result = apply_event_filter(self.events, evfilter)
        self.assertEqual(len(result), 1)  # только event2
        self.assertIn(self.event2, result)
    
    def test_source_node_filter(self):
        """Тест фильтрации по SourceNode."""
        evfilter = create_source_node_filter(ua.NodeId(1234, 2))
        result = apply_event_filter(self.events, evfilter)
        self.assertEqual(len(result), 2)  # event1 и event2
        self.assertIn(self.event1, result)
        self.assertIn(self.event2, result)
    
    def test_event_type_filter(self):
        """Тест фильтрации по EventType."""
        evfilter = create_event_type_filter([
            ua.NodeId(ua.ObjectIds.AlarmConditionType)
        ])
        result = apply_event_filter(self.events, evfilter)
        self.assertEqual(len(result), 1)  # только event2
        self.assertIn(self.event2, result)
    
    def test_event_type_filter_multiple(self):
        """Тест фильтрации по нескольким типам событий."""
        evfilter = create_event_type_filter([
            ua.NodeId(ua.ObjectIds.BaseEventType),
            ua.NodeId(ua.ObjectIds.AlarmConditionType)
        ])
        result = apply_event_filter(self.events, evfilter)
        # Все события должны пройти фильтр
        self.assertEqual(len(result), 3)


class TestEventFilterEvaluator(unittest.TestCase):
    """Тесты для класса EventFilterEvaluator."""
    
    def setUp(self):
        """Подготовка тестовых данных."""
        self.event = BaseEvent()
        self.event.Severity = 500
        self.event.Message = ua.LocalizedText("Test message")
        self.event.SourceNode = ua.NodeId(1234, 2)
    
    def test_evaluator_matches_true(self):
        """Тест совпадения события с фильтром."""
        evfilter = create_property_filter(
            "Severity",
            ua.FilterOperator.Equals,
            500
        )
        evaluator = EventFilterEvaluator(evfilter)
        self.assertTrue(evaluator.matches(self.event))
    
    def test_evaluator_matches_false(self):
        """Тест несовпадения события с фильтром."""
        evfilter = create_property_filter(
            "Severity",
            ua.FilterOperator.Equals,
            700
        )
        evaluator = EventFilterEvaluator(evfilter)
        self.assertFalse(evaluator.matches(self.event))
    
    def test_evaluator_greater_than(self):
        """Тест оператора GreaterThan."""
        evfilter = create_property_filter(
            "Severity",
            ua.FilterOperator.GreaterThan,
            400
        )
        evaluator = EventFilterEvaluator(evfilter)
        self.assertTrue(evaluator.matches(self.event))
    
    def test_evaluator_less_than(self):
        """Тест оператора LessThan."""
        evfilter = create_property_filter(
            "Severity",
            ua.FilterOperator.LessThan,
            600
        )
        evaluator = EventFilterEvaluator(evfilter)
        self.assertTrue(evaluator.matches(self.event))


class TestEventFilterOperators(unittest.TestCase):
    """Тесты для различных операторов фильтрации."""
    
    def setUp(self):
        """Подготовка тестовых данных."""
        self.event = BaseEvent()
        self.event.Severity = 500
        self.event.Message = ua.LocalizedText("Error in system")
        self.event.SourceNode = ua.NodeId(1234, 2)
    
    def test_in_list_operator(self):
        """Тест оператора InList."""
        evfilter = ua.EventFilter()
        
        # SelectClauses
        op = ua.SimpleAttributeOperand()
        op.AttributeId = ua.AttributeIds.Value
        op.BrowsePath = [ua.QualifiedName("Severity", 0)]
        op.TypeDefinitionId = ua.NodeId(ua.ObjectIds.BaseEventType)
        evfilter.SelectClauses.append(op)
        
        # WhereClause - Severity InList [300, 500, 700]
        cf = ua.ContentFilter()
        el = ua.ContentFilterElement()
        
        # Первый операнд - Severity
        severity_operand = ua.SimpleAttributeOperand()
        severity_operand.BrowsePath = [ua.QualifiedName("Severity", 0)]
        severity_operand.AttributeId = ua.AttributeIds.Value
        severity_operand.TypeDefinitionId = ua.NodeId(ua.ObjectIds.BaseEventType)
        el.FilterOperands.append(ua.ExtensionObject(Body=severity_operand))
        
        # Список значений
        for value in [300, 500, 700]:
            literal = ua.LiteralOperand(Value=ua.Variant(value, ua.VariantType.UInt16))
            el.FilterOperands.append(ua.ExtensionObject(Body=literal))
        
        el.FilterOperator = ua.FilterOperator.InList
        cf.Elements.append(el)
        evfilter.WhereClause = cf
        
        evaluator = EventFilterEvaluator(evfilter)
        self.assertTrue(evaluator.matches(self.event))
    
    def test_between_operator(self):
        """Тест оператора Between."""
        evfilter = ua.EventFilter()
        
        # SelectClauses
        op = ua.SimpleAttributeOperand()
        op.AttributeId = ua.AttributeIds.Value
        op.BrowsePath = [ua.QualifiedName("Severity", 0)]
        op.TypeDefinitionId = ua.NodeId(ua.ObjectIds.BaseEventType)
        evfilter.SelectClauses.append(op)
        
        # WhereClause - Severity Between 400 and 600
        cf = ua.ContentFilter()
        el = ua.ContentFilterElement()
        
        # Первый операнд - Severity
        severity_operand = ua.SimpleAttributeOperand()
        severity_operand.BrowsePath = [ua.QualifiedName("Severity", 0)]
        severity_operand.AttributeId = ua.AttributeIds.Value
        severity_operand.TypeDefinitionId = ua.NodeId(ua.ObjectIds.BaseEventType)
        el.FilterOperands.append(ua.ExtensionObject(Body=severity_operand))
        
        # Нижняя граница
        lower = ua.LiteralOperand(Value=ua.Variant(400, ua.VariantType.UInt16))
        el.FilterOperands.append(ua.ExtensionObject(Body=lower))
        
        # Верхняя граница
        upper = ua.LiteralOperand(Value=ua.Variant(600, ua.VariantType.UInt16))
        el.FilterOperands.append(ua.ExtensionObject(Body=upper))
        
        el.FilterOperator = ua.FilterOperator.Between
        cf.Elements.append(el)
        evfilter.WhereClause = cf
        
        evaluator = EventFilterEvaluator(evfilter)
        self.assertTrue(evaluator.matches(self.event))


class TestEventFilterComplex(unittest.TestCase):
    """Тесты для сложных фильтров."""
    
    def setUp(self):
        """Подготовка тестовых данных."""
        self.event = BaseEvent()
        self.event.Severity = 700
        self.event.Message = ua.LocalizedText("Alarm event")
        self.event.SourceNode = ua.NodeId(1234, 2)
        self.event.EventType = ua.NodeId(ua.ObjectIds.AlarmConditionType)


def run_tests():
    """Запуск всех тестов."""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    suite.addTests(loader.loadTestsFromTestCase(TestEventFilterBasic))
    suite.addTests(loader.loadTestsFromTestCase(TestEventFilterEvaluator))
    suite.addTests(loader.loadTestsFromTestCase(TestEventFilterOperators))
    suite.addTests(loader.loadTestsFromTestCase(TestEventFilterComplex))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result


if __name__ == "__main__":
    run_tests()

