"""
Тесты для модуля UAPG - OPC UA PostgreSQL History Storage Backend
"""

import pytest
import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from asyncua import ua
from uapg import HistoryPgSQL


class TestHistoryPgSQL:
    """Тесты для класса HistoryPgSQL."""
    
    @pytest.fixture
    def history(self):
        """Фикстура для создания экземпляра HistoryPgSQL."""
        return HistoryPgSQL(
            user='test_user',
            password='test_password',
            database='test_db',
            host='localhost'
        )
    
    @pytest.fixture
    def mock_connection(self):
        """Фикстура для мок-соединения с базой данных."""
        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.fetch = AsyncMock()
        mock_conn.close = AsyncMock()
        return mock_conn
    
    @pytest.mark.asyncio
    async def test_init(self, history, mock_connection):
        """Тест инициализации соединения."""
        with patch('asyncpg.connect', return_value=mock_connection):
            await history.init()
            assert history._db == mock_connection
    
    @pytest.mark.asyncio
    async def test_stop(self, history, mock_connection):
        """Тест закрытия соединения."""
        history._db = mock_connection
        await history.stop()
        mock_connection.close.assert_called_once()
    
    def test_get_table_name(self, history):
        """Тест генерации имени таблицы."""
        node_id = ua.NodeId("TestVariable", 1)
        # Тест для переменных (по умолчанию)
        table_name = history._get_table_name(node_id)
        assert table_name == "var_1_TestVariable"
        # Тест для событий
        event_table_name = history._get_table_name(node_id, "evt")
        assert event_table_name == "evt_1_TestVariable"
    
    def test_validate_table_name_valid(self):
        """Тест валидации корректного имени таблицы."""
        from uapg.history_pgsql import validate_table_name
        # Должно пройти без ошибок
        validate_table_name("valid_table_name")
        validate_table_name("table123")
        validate_table_name("table-name")
    
    def test_validate_table_name_invalid(self):
        """Тест валидации некорректного имени таблицы."""
        from uapg.history_pgsql import validate_table_name
        with pytest.raises(ValueError):
            validate_table_name("invalid table name")
        with pytest.raises(ValueError):
            validate_table_name("table;name")
        with pytest.raises(ValueError):
            validate_table_name("table'name")
    
    def test_get_bounds(self, history):
        """Тест определения границ запроса."""
        start = datetime.now(timezone.utc) - timedelta(hours=1)
        end = datetime.now(timezone.utc)
        
        start_time, end_time, order, limit = history._get_bounds(start, end, 100)
        
        assert start_time == start
        assert end_time == end
        assert order == "ASC"
        assert limit == 100
    
    def test_get_bounds_none_values(self, history):
        """Тест определения границ с None значениями."""
        start_time, end_time, order, limit = history._get_bounds(None, None, None)
        
        assert order == "DESC"
        assert limit == 10000
    
    def test_list_to_sql_str(self, history):
        """Тест преобразования списка в SQL строку."""
        test_list = ["column1", "column2", "column3"]
        result = history._list_to_sql_str(test_list)
        expected = '"column1", "column2", "column3"'
        assert result == expected
    
    @pytest.mark.asyncio
    async def test_new_historized_node(self, history, mock_connection):
        """Тест создания таблицы для историзации узла."""
        history._db = mock_connection
        node_id = ua.NodeId(1, "TestVariable")
        
        await history.new_historized_node(node_id, timedelta(days=1), 1000)
        
        # Проверяем, что были вызваны SQL команды
        assert mock_connection.execute.call_count >= 3
    
    @pytest.mark.asyncio
    async def test_save_node_value(self, history, mock_connection):
        """Тест сохранения значения узла."""
        history._db = mock_connection
        node_id = ua.NodeId(1, "TestVariable")
        history._datachanges_period[node_id] = (timedelta(days=1), 1000)
        
        datavalue = ua.DataValue(
            Value=ua.Variant(42.0, ua.VariantType.Double),
            SourceTimestamp=datetime.now(timezone.utc),
            ServerTimestamp=datetime.now(timezone.utc),
            StatusCode_=ua.StatusCode(ua.StatusCodes.Good)
        )
        
        await history.save_node_value(node_id, datavalue)
        
        # Проверяем, что был вызван INSERT
        mock_connection.execute.assert_called()
    
    @pytest.mark.asyncio
    async def test_read_node_history(self, history, mock_connection):
        """Тест чтения истории узла."""
        history._db = mock_connection
        
        # Мокаем результат запроса
        mock_row = {
            'servertimestamp': datetime.now(timezone.utc),
            'sourcetimestamp': datetime.now(timezone.utc),
            'statuscode': 0,
            'variantbinary': b'test_binary_data'
        }
        mock_connection.fetch.return_value = [mock_row]
        
        node_id = ua.NodeId(1, "TestVariable")
        start_time = datetime.now(timezone.utc) - timedelta(hours=1)
        end_time = datetime.now(timezone.utc)
        
        results, continuation = await history.read_node_history(
            node_id, start_time, end_time, 100
        )
        
        assert isinstance(results, list)
        assert continuation is None or isinstance(continuation, datetime)


class TestBuffer:
    """Тесты для класса Buffer."""
    
    def test_buffer_init(self):
        """Тест инициализации буфера."""
        from uapg.history_pgsql import Buffer
        data = b"test_data"
        buffer = Buffer(data)
        assert buffer.data == data
        assert buffer.pos == 0
    
    def test_buffer_read(self):
        """Тест чтения из буфера."""
        from uapg.history_pgsql import Buffer
        data = b"test_data"
        buffer = Buffer(data)
        
        result = buffer.read(4)
        assert result == b"test"
        assert buffer.pos == 4
        
        result = buffer.read(4)
        assert result == b"_dat"
        assert buffer.pos == 8


if __name__ == "__main__":
    pytest.main([__file__]) 