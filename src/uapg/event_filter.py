"""
Модуль для фильтрации событий OPC UA по EventFilter.

Поддерживает три основных режима фильтрации:
1. По узлу источника событий (SourceNode)
2. По типу события (EventType)  
3. По значениям свойств события
"""

from typing import Any, List, Optional
import logging
import re
from asyncua import ua
from asyncua.common.events import Event

_logger = logging.getLogger(__name__)


class EventFilterEvaluator:
    """
    Класс для оценки EventFilter и фильтрации событий.
    """
    
    def __init__(self, evfilter: ua.EventFilter):
        """
        Инициализация оценщика фильтров событий.
        
        Args:
            evfilter: EventFilter для применения к событиям
        """
        self.evfilter = evfilter
        self.select_clauses = evfilter.SelectClauses
        self.where_clause = evfilter.WhereClause
        
    def matches(self, event: Event) -> bool:
        """
        Проверяет, соответствует ли событие фильтру WhereClause.
        
        Args:
            event: Событие для проверки
            
        Returns:
            True если событие соответствует фильтру, False иначе
        """
        # Если нет WhereClause, то все события проходят фильтр
        if not self.where_clause or not self.where_clause.Elements:
            return True
            
        try:
            # Применяем ContentFilter к событию
            return self._evaluate_content_filter(event, self.where_clause)
        except Exception as e:
            _logger.warning(f"Ошибка при оценке фильтра события: {e}")
            return False
    
    def _evaluate_content_filter(self, event: Event, content_filter: ua.ContentFilter) -> bool:
        """
        Оценивает ContentFilter для события.
        
        Args:
            event: Событие для оценки
            content_filter: ContentFilter для применения
            
        Returns:
            Результат оценки фильтра
        """
        if not content_filter.Elements:
            return True
            
        # Оцениваем первый элемент фильтра (обычно это корневой элемент)
        return self._evaluate_filter_element(event, content_filter.Elements[0], content_filter)
    
    def _evaluate_filter_element(
        self, 
        event: Event, 
        element: ua.ContentFilterElement,
        content_filter: ua.ContentFilter
    ) -> bool:
        """
        Оценивает один элемент ContentFilterElement.
        
        Args:
            event: Событие для оценки
            element: Элемент фильтра
            content_filter: Полный ContentFilter (для доступа к другим элементам)
            
        Returns:
            Результат оценки элемента
        """
        operator = element.FilterOperator
        operands = element.FilterOperands
        
        # Распаковываем операнды из ExtensionObject
        unpacked_operands = []
        for operand_ext in operands:
            if isinstance(operand_ext, ua.ExtensionObject):
                unpacked_operands.append(operand_ext.Body)
            else:
                unpacked_operands.append(operand_ext)
        
        # Обрабатываем различные операторы
        if operator == ua.FilterOperator.Equals:
            return self._evaluate_equals(event, unpacked_operands)
        elif operator == ua.FilterOperator.GreaterThan:
            return self._evaluate_greater_than(event, unpacked_operands)
        elif operator == ua.FilterOperator.LessThan:
            return self._evaluate_less_than(event, unpacked_operands)
        elif operator == ua.FilterOperator.GreaterThanOrEqual:
            return self._evaluate_greater_than_or_equal(event, unpacked_operands)
        elif operator == ua.FilterOperator.LessThanOrEqual:
            return self._evaluate_less_than_or_equal(event, unpacked_operands)
        elif operator == ua.FilterOperator.InList:
            return self._evaluate_in_list(event, unpacked_operands)
        elif operator == ua.FilterOperator.And:
            return self._evaluate_and(event, unpacked_operands, content_filter)
        elif operator == ua.FilterOperator.Or:
            return self._evaluate_or(event, unpacked_operands, content_filter)
        elif operator == ua.FilterOperator.Not:
            return self._evaluate_not(event, unpacked_operands, content_filter)
        elif operator == ua.FilterOperator.IsNull:
            return self._evaluate_is_null(event, unpacked_operands)
        elif operator == ua.FilterOperator.Like:
            return self._evaluate_like(event, unpacked_operands)
        elif operator == ua.FilterOperator.Between:
            return self._evaluate_between(event, unpacked_operands)
        else:
            _logger.warning(f"Неподдерживаемый оператор фильтра: {operator}")
            return True
    
    def _get_operand_value(self, event: Event, operand: Any) -> Any:
        """
        Получает значение операнда.
        
        Args:
            event: Событие для получения значений атрибутов
            operand: Операнд (SimpleAttributeOperand, LiteralOperand и т.д.)
            
        Returns:
            Значение операнда
        """
        if isinstance(operand, ua.SimpleAttributeOperand):
            # Получаем значение атрибута события
            return self._get_event_attribute_value(event, operand)
        elif isinstance(operand, ua.LiteralOperand):
            # Возвращаем литеральное значение
            return operand.Value.Value if operand.Value else None
        elif isinstance(operand, ua.AttributeOperand):
            # Для AttributeOperand нужна более сложная логика
            _logger.warning("AttributeOperand не полностью поддерживается")
            return None
        else:
            _logger.warning(f"Неизвестный тип операнда: {type(operand)}")
            return None
    
    def _get_event_attribute_value(self, event: Event, operand: ua.SimpleAttributeOperand) -> Any:
        """
        Получает значение атрибута события по SimpleAttributeOperand.
        
        Args:
            event: Событие
            operand: SimpleAttributeOperand, описывающий атрибут
            
        Returns:
            Значение атрибута события
        """
        # Если BrowsePath пустой, то это стандартный атрибут
        if not operand.BrowsePath:
            attr_id = operand.AttributeId
            if attr_id == ua.AttributeIds.NodeId:
                return getattr(event, 'emitting_node', None)
            elif attr_id == ua.AttributeIds.Value:
                # Для Value нужно знать имя атрибута
                return None
            else:
                return None
        
        # Строим имя атрибута из BrowsePath
        attr_name = "/".join([qn.Name for qn in operand.BrowsePath])
        
        # Пытаемся получить значение атрибута из события
        try:
            # Сначала пытаемся напрямую
            if hasattr(event, attr_name):
                return getattr(event, attr_name)
            
            # Затем пытаемся с одним уровнем вложенности (без '/')
            simple_name = operand.BrowsePath[0].Name if operand.BrowsePath else None
            if simple_name and hasattr(event, simple_name):
                return getattr(event, simple_name)
            
            return None
        except Exception as e:
            _logger.debug(f"Не удалось получить атрибут {attr_name}: {e}")
            return None
    
    # Операторы сравнения
    
    def _evaluate_equals(self, event: Event, operands: List[Any]) -> bool:
        """Оценивает оператор Equals."""
        if len(operands) < 2:
            return False
        
        val1 = self._get_operand_value(event, operands[0])
        val2 = self._get_operand_value(event, operands[1])
        
        return self._compare_values(val1, val2, lambda a, b: a == b)
    
    def _evaluate_greater_than(self, event: Event, operands: List[Any]) -> bool:
        """Оценивает оператор GreaterThan."""
        if len(operands) < 2:
            return False
        
        val1 = self._get_operand_value(event, operands[0])
        val2 = self._get_operand_value(event, operands[1])
        
        return self._compare_values(val1, val2, lambda a, b: a > b)
    
    def _evaluate_less_than(self, event: Event, operands: List[Any]) -> bool:
        """Оценивает оператор LessThan."""
        if len(operands) < 2:
            return False
        
        val1 = self._get_operand_value(event, operands[0])
        val2 = self._get_operand_value(event, operands[1])
        
        return self._compare_values(val1, val2, lambda a, b: a < b)
    
    def _evaluate_greater_than_or_equal(self, event: Event, operands: List[Any]) -> bool:
        """Оценивает оператор GreaterThanOrEqual."""
        if len(operands) < 2:
            return False
        
        val1 = self._get_operand_value(event, operands[0])
        val2 = self._get_operand_value(event, operands[1])
        
        return self._compare_values(val1, val2, lambda a, b: a >= b)
    
    def _evaluate_less_than_or_equal(self, event: Event, operands: List[Any]) -> bool:
        """Оценивает оператор LessThanOrEqual."""
        if len(operands) < 2:
            return False
        
        val1 = self._get_operand_value(event, operands[0])
        val2 = self._get_operand_value(event, operands[1])
        
        return self._compare_values(val1, val2, lambda a, b: a <= b)
    
    def _compare_values(self, val1: Any, val2: Any, comparator) -> bool:
        """
        Сравнивает два значения с учетом их типов.
        
        Args:
            val1: Первое значение
            val2: Второе значение
            comparator: Функция сравнения
            
        Returns:
            Результат сравнения
        """
        if val1 is None or val2 is None:
            return False
        
        # Специальная обработка для NodeId
        if isinstance(val1, ua.NodeId) and isinstance(val2, ua.NodeId):
            return comparator(val1, val2)
        
        # Преобразуем значения к сравнимым типам
        try:
            if type(val1) != type(val2):
                # Пытаемся привести к общему типу
                if isinstance(val1, (int, float)) and isinstance(val2, (int, float)):
                    return comparator(float(val1), float(val2))
                elif isinstance(val1, str) and isinstance(val2, str):
                    return comparator(val1, val2)
                else:
                    # Преобразуем оба к строке для сравнения
                    return comparator(str(val1), str(val2))
            else:
                return comparator(val1, val2)
        except Exception as e:
            _logger.debug(f"Ошибка при сравнении значений: {e}")
            return False
    
    def _evaluate_in_list(self, event: Event, operands: List[Any]) -> bool:
        """
        Оценивает оператор InList.
        Первый операнд - проверяемое значение, остальные - список значений.
        """
        if len(operands) < 2:
            return False
        
        val = self._get_operand_value(event, operands[0])
        
        # Проверяем, есть ли val в списке остальных операндов
        for i in range(1, len(operands)):
            list_val = self._get_operand_value(event, operands[i])
            if self._compare_values(val, list_val, lambda a, b: a == b):
                return True
        
        return False
    
    # Логические операторы
    
    def _evaluate_and(self, event: Event, operands: List[Any], content_filter: ua.ContentFilter) -> bool:
        """Оценивает оператор And."""
        # Все операнды должны быть ElementOperand
        for operand in operands:
            if isinstance(operand, ua.ElementOperand):
                element_index = operand.Index
                if element_index < len(content_filter.Elements):
                    if not self._evaluate_filter_element(event, content_filter.Elements[element_index], content_filter):
                        return False
            else:
                _logger.warning(f"And оператор ожидает ElementOperand, получен {type(operand)}")
                return False
        return True
    
    def _evaluate_or(self, event: Event, operands: List[Any], content_filter: ua.ContentFilter) -> bool:
        """Оценивает оператор Or."""
        # Хотя бы один операнд должен быть истинным
        for operand in operands:
            if isinstance(operand, ua.ElementOperand):
                element_index = operand.Index
                if element_index < len(content_filter.Elements):
                    if self._evaluate_filter_element(event, content_filter.Elements[element_index], content_filter):
                        return True
            else:
                _logger.warning(f"Or оператор ожидает ElementOperand, получен {type(operand)}")
        return False
    
    def _evaluate_not(self, event: Event, operands: List[Any], content_filter: ua.ContentFilter) -> bool:
        """Оценивает оператор Not."""
        if len(operands) != 1:
            return False
        
        operand = operands[0]
        if isinstance(operand, ua.ElementOperand):
            element_index = operand.Index
            if element_index < len(content_filter.Elements):
                return not self._evaluate_filter_element(event, content_filter.Elements[element_index], content_filter)
        
        return False
    
    def _evaluate_is_null(self, event: Event, operands: List[Any]) -> bool:
        """Оценивает оператор IsNull."""
        if len(operands) < 1:
            return False
        
        val = self._get_operand_value(event, operands[0])
        return val is None
    
    def _evaluate_like(self, event: Event, operands: List[Any]) -> bool:
        """
        Оценивает оператор Like (паттерн-матчинг).
        Первый операнд - строка, второй - паттерн.
        """
        if len(operands) < 2:
            return False
        
        val = self._get_operand_value(event, operands[0])
        pattern = self._get_operand_value(event, operands[1])
        
        if val is None or pattern is None:
            return False
        
        # Преобразуем OPC UA паттерн в regex
        # В OPC UA: % - любое количество символов, _ - один символ
        regex_pattern = pattern.replace('%', '.*').replace('_', '.')
        
        try:
            return bool(re.match(f'^{regex_pattern}$', str(val)))
        except Exception as e:
            _logger.debug(f"Ошибка в Like паттерне: {e}")
            return False
    
    def _evaluate_between(self, event: Event, operands: List[Any]) -> bool:
        """
        Оценивает оператор Between.
        Первый операнд - проверяемое значение, второй - нижняя граница, третий - верхняя граница.
        """
        if len(operands) < 3:
            return False
        
        val = self._get_operand_value(event, operands[0])
        lower = self._get_operand_value(event, operands[1])
        upper = self._get_operand_value(event, operands[2])
        
        if val is None or lower is None or upper is None:
            return False
        
        try:
            return lower <= val <= upper
        except Exception as e:
            _logger.debug(f"Ошибка в Between операторе: {e}")
            return False


def apply_event_filter(events: List[Event], evfilter: Optional[ua.EventFilter]) -> List[Event]:
    """
    Применяет EventFilter к списку событий.
    
    Поддерживает три режима фильтрации:
    1. По узлу источника событий (SourceNode)
    2. По типу события (EventType)
    3. По значениям свойств события
    
    Args:
        events: Список событий для фильтрации
        evfilter: EventFilter для применения (если None, возвращаются все события)
        
    Returns:
        Отфильтрованный список событий
    """
    if not evfilter or not evfilter.WhereClause or not evfilter.WhereClause.Elements:
        return events
    
    evaluator = EventFilterEvaluator(evfilter)
    filtered_events = []
    
    for event in events:
        try:
            if evaluator.matches(event):
                filtered_events.append(event)
        except Exception as e:
            _logger.warning(f"Ошибка при фильтрации события: {e}")
            # В случае ошибки можно либо пропустить событие, либо включить его
            # Здесь мы пропускаем событие с ошибкой
            continue
    
    return filtered_events


# Вспомогательные функции для создания фильтров

def create_source_node_filter(source_node_id: ua.NodeId) -> ua.EventFilter:
    """
    Создает EventFilter для фильтрации по узлу источника событий.
    
    Args:
        source_node_id: NodeId источника событий
        
    Returns:
        EventFilter, фильтрующий по SourceNode
    """
    evfilter = ua.EventFilter()
    
    # SelectClauses - выбираем стандартные поля события
    op = ua.SimpleAttributeOperand()
    op.AttributeId = ua.AttributeIds.Value
    op.BrowsePath = [ua.QualifiedName("SourceNode", 0)]
    op.TypeDefinitionId = ua.NodeId(ua.ObjectIds.BaseEventType)
    evfilter.SelectClauses.append(op)
    
    # WhereClause - фильтр по SourceNode
    cf = ua.ContentFilter()
    el = ua.ContentFilterElement()
    
    # Первый операнд - атрибут SourceNode
    source_operand = ua.SimpleAttributeOperand()
    source_operand.BrowsePath = [ua.QualifiedName("SourceNode", 0)]
    source_operand.AttributeId = ua.AttributeIds.Value
    source_operand.TypeDefinitionId = ua.NodeId(ua.ObjectIds.BaseEventType)
    el.FilterOperands.append(ua.ExtensionObject(Body=source_operand))
    
    # Второй операнд - литерал с NodeId источника
    literal_operand = ua.LiteralOperand(Value=ua.Variant(source_node_id, ua.VariantType.NodeId))
    el.FilterOperands.append(ua.ExtensionObject(Body=literal_operand))
    
    el.FilterOperator = ua.FilterOperator.Equals
    cf.Elements.append(el)
    evfilter.WhereClause = cf
    
    return evfilter


def create_event_type_filter(event_type_ids: List[ua.NodeId]) -> ua.EventFilter:
    """
    Создает EventFilter для фильтрации по типу события.
    
    Args:
        event_type_ids: Список NodeId типов событий для фильтрации
        
    Returns:
        EventFilter, фильтрующий по EventType
    """
    evfilter = ua.EventFilter()
    
    # SelectClauses - выбираем стандартные поля события
    op = ua.SimpleAttributeOperand()
    op.AttributeId = ua.AttributeIds.Value
    op.BrowsePath = [ua.QualifiedName("EventType", 0)]
    op.TypeDefinitionId = ua.NodeId(ua.ObjectIds.BaseEventType)
    evfilter.SelectClauses.append(op)
    
    # WhereClause - фильтр по EventType (InList)
    cf = ua.ContentFilter()
    el = ua.ContentFilterElement()
    
    # Первый операнд - атрибут EventType
    event_type_operand = ua.SimpleAttributeOperand()
    event_type_operand.BrowsePath = [ua.QualifiedName("EventType", 0)]
    event_type_operand.AttributeId = ua.AttributeIds.Value
    event_type_operand.TypeDefinitionId = ua.NodeId(ua.ObjectIds.BaseEventType)
    el.FilterOperands.append(ua.ExtensionObject(Body=event_type_operand))
    
    # Остальные операнды - литералы с типами событий
    for event_type_id in event_type_ids:
        literal_operand = ua.LiteralOperand(Value=ua.Variant(event_type_id, ua.VariantType.NodeId))
        el.FilterOperands.append(ua.ExtensionObject(Body=literal_operand))
    
    el.FilterOperator = ua.FilterOperator.InList
    cf.Elements.append(el)
    evfilter.WhereClause = cf
    
    return evfilter


def create_property_filter(
    property_name: str, 
    operator: ua.FilterOperator, 
    *values
) -> ua.EventFilter:
    """
    Создает EventFilter для фильтрации по значению свойства события.
    
    Args:
        property_name: Имя свойства события
        operator: Оператор фильтрации
        *values: Значения для сравнения
        
    Returns:
        EventFilter, фильтрующий по свойству события
    """
    evfilter = ua.EventFilter()
    
    # SelectClauses - выбираем нужное свойство
    op = ua.SimpleAttributeOperand()
    op.AttributeId = ua.AttributeIds.Value
    op.BrowsePath = [ua.QualifiedName(property_name, 0)]
    op.TypeDefinitionId = ua.NodeId(ua.ObjectIds.BaseEventType)
    evfilter.SelectClauses.append(op)
    
    # WhereClause - фильтр по свойству
    cf = ua.ContentFilter()
    el = ua.ContentFilterElement()
    
    # Первый операнд - атрибут свойства
    property_operand = ua.SimpleAttributeOperand()
    property_operand.BrowsePath = [ua.QualifiedName(property_name, 0)]
    property_operand.AttributeId = ua.AttributeIds.Value
    property_operand.TypeDefinitionId = ua.NodeId(ua.ObjectIds.BaseEventType)
    el.FilterOperands.append(ua.ExtensionObject(Body=property_operand))
    
    # Добавляем значения для сравнения
    for value in values:
        # Определяем тип варианта автоматически
        if isinstance(value, bool):
            variant_type = ua.VariantType.Boolean
        elif isinstance(value, int):
            variant_type = ua.VariantType.Int32
        elif isinstance(value, float):
            variant_type = ua.VariantType.Double
        elif isinstance(value, str):
            variant_type = ua.VariantType.String
        elif isinstance(value, ua.NodeId):
            variant_type = ua.VariantType.NodeId
        else:
            variant_type = ua.VariantType.Variant
        
        literal_operand = ua.LiteralOperand(Value=ua.Variant(value, variant_type))
        el.FilterOperands.append(ua.ExtensionObject(Body=literal_operand))
    
    el.FilterOperator = operator
    cf.Elements.append(el)
    evfilter.WhereClause = cf
    
    return evfilter

