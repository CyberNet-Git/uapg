"""
Модуль для фильтрации событий OPC UA по EventFilter.

Поддерживает три основных режима фильтрации:
1. По узлу источника событий (SourceNode)
2. По типу события (EventType)  
3. По значениям свойств события
"""

from typing import Any, Callable, List, Optional, Set
import logging
import re
from asyncua import ua
from asyncua.common.events import Event

_logger = logging.getLogger(__name__)
# Отдельный логгер для трассировки фильтрации событий истории
_trace_logger = logging.getLogger("history_timescale.events")


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
            if _trace_logger.isEnabledFor(logging.DEBUG):
                _trace_logger.debug("EventFilterEvaluator.matches: no WhereClause, event passes by default")
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
        elements = content_filter.Elements or []
        if not elements:
            if _trace_logger.isEnabledFor(logging.DEBUG):
                _trace_logger.debug(
                    "EventFilterEvaluator._evaluate_content_filter: empty ContentFilter, event passes"
                )
            return True

        # Согласно типичному паттерну OPC UA корневым элементом считаем последний
        root_index = len(elements) - 1
        visited: Set[int] = set()
        if _trace_logger.isEnabledFor(logging.DEBUG):
            _trace_logger.debug(
                "EventFilterEvaluator._evaluate_content_filter: using root_index=%d of %d elements",
                root_index,
                len(elements),
            )
        result = self._evaluate_filter_element(
            event,
            elements[root_index],
            content_filter,
            visited_indices=visited,
            element_index=root_index,
        )
        if _trace_logger.isEnabledFor(logging.DEBUG):
            _trace_logger.debug(
                "EventFilterEvaluator._evaluate_content_filter: root element index=%d result=%s",
                root_index,
                result,
            )
        return result
    
    def _evaluate_filter_element(
        self,
        event: Event,
        element: ua.ContentFilterElement,
        content_filter: ua.ContentFilter,
        visited_indices: Optional[Set[int]] = None,
        element_index: Optional[int] = None,
    ) -> bool:
        """
        Оценивает один элемент ContentFilterElement.
        
        Args:
            event: Событие для оценки
            element: Элемент фильтра
            content_filter: Полный ContentFilter (для доступа к другим элементам)
            visited_indices: Набор уже посещённых индексов элементов (для защиты от циклов)
            element_index: Индекс текущего элемента в content_filter.Elements (если известен)
            
        Returns:
            Результат оценки элемента
        """
        if visited_indices is not None and element_index is not None:
            if element_index in visited_indices:
                _logger.warning(
                    "Обнаружен потенциальный цикл в ContentFilter: повторная оценка элемента с index=%d",
                    element_index,
                )
                return False
            visited_indices.add(element_index)
        operator = element.FilterOperator
        operands = element.FilterOperands
        
        # Распаковываем операнды из ExtensionObject
        unpacked_operands = []
        for operand_ext in operands:
            if isinstance(operand_ext, ua.ExtensionObject):
                unpacked_operands.append(operand_ext.Body)
            else:
                unpacked_operands.append(operand_ext)
        
        if _trace_logger.isEnabledFor(logging.DEBUG):
            _trace_logger.debug(
                "EventFilterEvaluator._evaluate_filter_element: index=%s operator=%s operands=%d",
                element_index,
                operator,
                len(unpacked_operands),
            )
        
        # Обрабатываем различные операторы
        result: bool
        if operator == ua.FilterOperator.Equals:
            result = self._evaluate_equals(event, unpacked_operands)
        elif operator == ua.FilterOperator.GreaterThan:
            result = self._evaluate_greater_than(event, unpacked_operands)
        elif operator == ua.FilterOperator.LessThan:
            result = self._evaluate_less_than(event, unpacked_operands)
        elif operator == ua.FilterOperator.GreaterThanOrEqual:
            result = self._evaluate_greater_than_or_equal(event, unpacked_operands)
        elif operator == ua.FilterOperator.LessThanOrEqual:
            result = self._evaluate_less_than_or_equal(event, unpacked_operands)
        elif operator == ua.FilterOperator.InList:
            result = self._evaluate_in_list(event, unpacked_operands)
        elif operator == ua.FilterOperator.And:
            result = self._evaluate_and(event, unpacked_operands, content_filter, visited_indices)
        elif operator == ua.FilterOperator.Or:
            result = self._evaluate_or(event, unpacked_operands, content_filter, visited_indices)
        elif operator == ua.FilterOperator.Not:
            result = self._evaluate_not(event, unpacked_operands, content_filter, visited_indices)
        elif operator == ua.FilterOperator.IsNull:
            result = self._evaluate_is_null(event, unpacked_operands)
        elif operator == ua.FilterOperator.Like:
            result = self._evaluate_like(event, unpacked_operands)
        elif operator == ua.FilterOperator.Between:
            result = self._evaluate_between(event, unpacked_operands)
        else:
            _logger.warning(f"Неподдерживаемый оператор фильтра: {operator}")
            result = True

        if _trace_logger.isEnabledFor(logging.DEBUG):
            _trace_logger.debug(
                "EventFilterEvaluator._evaluate_filter_element: index=%s operator=%s result=%s",
                element_index,
                operator,
                result,
            )
        return result
    
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
    
    def _compare_values(
        self, val1: Any, val2: Any, comparator: Callable[[Any, Any], Any]
    ) -> bool:
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
            return bool(comparator(val1, val2))
        
        # Преобразуем значения к сравнимым типам
        try:
            if type(val1) != type(val2):
                # Пытаемся привести к общему типу
                if isinstance(val1, (int, float)) and isinstance(val2, (int, float)):
                    return bool(comparator(float(val1), float(val2)))
                elif isinstance(val1, str) and isinstance(val2, str):
                    return bool(comparator(val1, val2))
                else:
                    # Преобразуем оба к строке для сравнения
                    return bool(comparator(str(val1), str(val2)))
            else:
                return bool(comparator(val1, val2))
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
    
    def _evaluate_and(
        self,
        event: Event,
        operands: List[Any],
        content_filter: ua.ContentFilter,
        visited_indices: Optional[Set[int]] = None,
    ) -> bool:
        """Оценивает оператор And."""
        # Все операнды должны быть ElementOperand
        for operand in operands:
            if isinstance(operand, ua.ElementOperand):
                element_index = operand.Index
                if element_index < 0 or element_index >= len(content_filter.Elements):
                    _logger.warning(
                        "And оператор: некорректный индекс элемента ContentFilter (index=%d, size=%d)",
                        element_index,
                        len(content_filter.Elements),
                    )
                    return False
                if not self._evaluate_filter_element(
                    event,
                    content_filter.Elements[element_index],
                    content_filter,
                    visited_indices=visited_indices,
                    element_index=element_index,
                ):
                    return False
            else:
                _logger.warning(f"And оператор ожидает ElementOperand, получен {type(operand)}")
                return False
        return True
    
    def _evaluate_or(
        self,
        event: Event,
        operands: List[Any],
        content_filter: ua.ContentFilter,
        visited_indices: Optional[Set[int]] = None,
    ) -> bool:
        """Оценивает оператор Or."""
        # Хотя бы один операнд должен быть истинным
        for operand in operands:
            if isinstance(operand, ua.ElementOperand):
                element_index = operand.Index
                if element_index < 0 or element_index >= len(content_filter.Elements):
                    _logger.warning(
                        "Or оператор: некорректный индекс элемента ContentFilter (index=%d, size=%d)",
                        element_index,
                        len(content_filter.Elements),
                    )
                    continue
                if self._evaluate_filter_element(
                    event,
                    content_filter.Elements[element_index],
                    content_filter,
                    visited_indices=visited_indices,
                    element_index=element_index,
                ):
                    return True
            else:
                _logger.warning(f"Or оператор ожидает ElementOperand, получен {type(operand)}")
        return False
    
    def _evaluate_not(
        self,
        event: Event,
        operands: List[Any],
        content_filter: ua.ContentFilter,
        visited_indices: Optional[Set[int]] = None,
    ) -> bool:
        """Оценивает оператор Not."""
        if len(operands) != 1:
            return False
        
        operand = operands[0]
        if isinstance(operand, ua.ElementOperand):
            element_index = operand.Index
            if element_index < 0 or element_index >= len(content_filter.Elements):
                _logger.warning(
                    "Not оператор: некорректный индекс элемента ContentFilter (index=%d, size=%d)",
                    element_index,
                    len(content_filter.Elements),
                )
                return False
            return not self._evaluate_filter_element(
                event,
                content_filter.Elements[element_index],
                content_filter,
                visited_indices=visited_indices,
                element_index=element_index,
            )
        
        _logger.warning(f"Not оператор ожидает ElementOperand, получен {type(operand)}")
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
            return bool(lower <= val <= upper)
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
        if _trace_logger.isEnabledFor(logging.DEBUG):
            _trace_logger.debug(
                "apply_event_filter: no WhereClause, returning all events (count=%d)",
                len(events),
            )
        return events
    
    input_count = len(events)
    if _trace_logger.isEnabledFor(logging.DEBUG):
        # Краткое описание структуры фильтра
        where = evfilter.WhereClause
        select_count = len(evfilter.SelectClauses or [])
        elements_count = len(where.Elements or []) if where else 0
        _trace_logger.debug(
            "apply_event_filter: input_count=%d, select_clauses=%d, where_elements=%d",
            input_count,
            select_count,
            elements_count,
        )
        if where and where.Elements:
            for idx, el in enumerate(where.Elements):
                operand_kinds = []
                for op_ext in el.FilterOperands:
                    body = op_ext.Body if isinstance(op_ext, ua.ExtensionObject) else op_ext
                    if isinstance(body, ua.SimpleAttributeOperand):
                        path = "/".join(qn.Name for qn in (body.BrowsePath or []))
                        operand_kinds.append(f"Attr({path})")
                    elif isinstance(body, ua.LiteralOperand):
                        v = body.Value.Value if body.Value else None
                        v_str = str(v)
                        if len(v_str) > 64:
                            v_str = v_str[:61] + "..."
                        operand_kinds.append(f"Literal({v_str})")
                    elif isinstance(body, ua.ElementOperand):
                        operand_kinds.append(f"Element(idx={body.Index})")
                    else:
                        operand_kinds.append(type(body).__name__)
                _trace_logger.debug(
                    "apply_event_filter: element[%d] operator=%s operands=%s",
                    idx,
                    el.FilterOperator,
                    ", ".join(operand_kinds),
                )
    
    evaluator = EventFilterEvaluator(evfilter)
    filtered_events: List[Event] = []
    
    for event in events:
        try:
            match = evaluator.matches(event)
            if match:
                filtered_events.append(event)
            if _trace_logger.isEnabledFor(logging.DEBUG):
                # Пытаемся вывести краткое описание события
                time_val = getattr(event, "Time", None) or getattr(event, "EventTime", None)
                etype = getattr(event, "EventType", None)
                _trace_logger.debug(
                    "apply_event_filter: event time=%s type=%s matched=%s",
                    time_val,
                    etype,
                    match,
                )
        except Exception as e:
            _logger.warning(f"Ошибка при фильтрации события: {e}")
            # В случае ошибки можно либо пропустить событие, либо включить его
            # Здесь мы пропускаем событие с ошибкой
            continue
    
    if _trace_logger.isEnabledFor(logging.DEBUG):
        _trace_logger.debug(
            "apply_event_filter: finished, input_count=%d, passed=%d",
            input_count,
            len(filtered_events),
        )
    return filtered_events


# Вспомогательные функции для создания фильтров
