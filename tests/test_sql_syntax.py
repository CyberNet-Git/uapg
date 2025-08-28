"""
Тесты для проверки правильности SQL синтаксиса в UAPG.
"""

import pytest
from unittest.mock import Mock
from uapg import HistoryPgSQL
from asyncua import ua


class TestSQLSyntax:
    """Тесты SQL синтаксиса."""
    
    def test_format_event_placeholders(self):
        """Тест правильности генерации плейсхолдеров для событий."""
        history = HistoryPgSQL()
        
        # Мокаем событие с правильными OPC UA типами
        mock_event = Mock()
        mock_event.get_event_props_as_fields_dict.return_value = {
            'Field1': ua.Variant('value1', ua.VariantType.String),
            'Field2': ua.Variant(42, ua.VariantType.Int32),
            'Field3': ua.Variant(3.14, ua.VariantType.Double)
        }
        
        # Получаем отформатированные данные
        columns, placeholders, values = history._format_event(mock_event)
        
        # Проверяем, что плейсхолдеры правильные (placeholders - это строка)
        expected_placeholders = '$3, $4, $5'  # Начинаем с $3, так как $1 и $2 для времени и типа
        assert placeholders == expected_placeholders
        
        # Проверяем, что колонки правильные
        expected_columns = '"Field1", "Field2", "Field3"'
        assert columns == expected_columns
        
        # Проверяем количество значений
        assert len(values) == 3
    
    def test_format_event_empty_fields(self):
        """Тест обработки событий без полей."""
        history = HistoryPgSQL()
        
        # Мокаем событие без полей
        mock_event = Mock()
        mock_event.get_event_props_as_fields_dict.return_value = {}
        
        # Получаем отформатированные данные
        columns, placeholders, values = history._format_event(mock_event)
        
        # Проверяем, что все пусто
        assert columns == ""
        assert placeholders == ""
        assert values == ()
    
    def test_format_event_single_field(self):
        """Тест обработки событий с одним полем."""
        history = HistoryPgSQL()
        
        # Мокаем событие с одним полем
        mock_event = Mock()
        mock_event.get_event_props_as_fields_dict.return_value = {
            'SingleField': ua.Variant('single_value', ua.VariantType.String)
        }
        
        # Получаем отформатированные данные
        columns, placeholders, values = history._format_event(mock_event)
        
        # Проверяем, что плейсхолдеры правильные
        assert placeholders == '$3'
        assert columns == '"SingleField"'
        assert len(values) == 1
    
    def test_sql_query_construction(self):
        """Тест построения SQL запроса для вставки событий."""
        history = HistoryPgSQL()
        
        # Мокаем событие
        mock_event = Mock()
        mock_event.get_event_props_as_fields_dict.return_value = {
            'Field1': ua.Variant('value1', ua.VariantType.String),
            'Field2': ua.Variant(42, ua.VariantType.Int32)
        }
        
        # Получаем отформатированные данные
        columns, placeholders, values = history._format_event(mock_event)
        
        # Строим SQL запрос
        sql_query = f'INSERT INTO "test_table" (_timestamp, _eventtypename, {columns}) VALUES ($1, $2, {placeholders})'
        
        # Проверяем, что SQL запрос корректен
        expected_sql = 'INSERT INTO "test_table" (_timestamp, _eventtypename, "Field1", "Field2") VALUES ($1, $2, $3, $4)'
        assert sql_query == expected_sql
        
        # Проверяем, что плейсхолдеры последовательные
        assert '$1' in sql_query
        assert '$2' in sql_query
        assert '$3' in sql_query
        assert '$4' in sql_query
    
    def test_placeholder_sequence(self):
        """Тест последовательности плейсхолдеров."""
        history = HistoryPgSQL()
        
        # Мокаем событие с несколькими полями
        mock_event = Mock()
        mock_event.get_event_props_as_fields_dict.return_value = {
            'A': ua.Variant('a_value', ua.VariantType.String),
            'B': ua.Variant('b_value', ua.VariantType.String),
            'C': ua.Variant('c_value', ua.VariantType.String),
            'D': ua.Variant('d_value', ua.VariantType.String)
        }
        
        # Получаем отформатированные данные
        columns, placeholders, values = history._format_event(mock_event)
        
        # Проверяем последовательность плейсхолдеров (placeholders - это строка)
        expected_placeholders = '$3, $4, $5, $6'
        assert placeholders == expected_placeholders
        
        # Проверяем, что нет дублирующихся плейсхолдеров
        placeholder_list = [p.strip() for p in placeholders.split(',')]
        assert len(set(placeholder_list)) == len(placeholder_list)
        
        # Проверяем, что плейсхолдеры идут по порядку
        for i, placeholder in enumerate(placeholder_list):
            expected_number = 3 + i  # Начинаем с 3
            assert placeholder == f'${expected_number}'


if __name__ == "__main__":
    pytest.main([__file__])
