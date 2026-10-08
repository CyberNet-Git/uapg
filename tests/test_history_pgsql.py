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
    def mock_pool(self):
        """Мок пула: _ensure_pool смотрит на _closed, stop() ждёт await close()."""
        pool = MagicMock()
        pool._closed = False
        pool.close = AsyncMock()
        return pool

    @pytest.fixture
    def mock_connection(self, mock_pool):
        """Соединение, которое отдаёт pool.acquire() как асинхронный контекст."""
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
        return mock_conn

    @pytest.fixture
    def connected(self, history, mock_pool, mock_connection):
        """История с подставленным пулом — без обращения к настоящей БД."""
        history._pool = mock_pool
        history._initialized = True
        return history

    async def test_init(self, history, mock_pool):
        """Тест инициализации: создаётся пул соединений."""
        with patch('asyncpg.create_pool', new=AsyncMock(return_value=mock_pool)), patch.object(
            HistoryPgSQL, '_create_metadata_tables', AsyncMock()
        ), patch.object(HistoryPgSQL, '_reconnect_monitor', AsyncMock()):
            await history.init()

        assert history._pool is mock_pool

    async def test_stop(self, history, mock_pool):
        """Тест закрытия: закрывается пул, ссылка сбрасывается."""
        history._pool = mock_pool

        await history.stop()

        mock_pool.close.assert_awaited_once()
        assert history._pool is None
    
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
    
    def test_format_node_id(self, history):
        """Строковое представление NodeId — то, по чему узлы ищутся в метаданных."""
        assert history._format_node_id(ua.NodeId("TestVariable", 1)) == "ns=1;s=TestVariable"
        assert history._format_node_id(ua.NodeId(42, 2)) == "ns=2;i=42"
    
    async def test_new_historized_node(self, connected, mock_connection):
        """Тест создания таблицы для историзации узла."""
        node_id = ua.NodeId("TestVariable", 1)

        await connected.new_historized_node(node_id, timedelta(days=1), 1000)

        # Проверяем, что были вызваны SQL команды
        assert mock_connection.execute.await_count >= 3
        assert connected._datachanges_period[node_id] == (timedelta(days=1), 1000)
    
    async def test_save_node_value(self, connected, mock_connection):
        """Тест сохранения значения узла."""
        node_id = ua.NodeId("TestVariable", 1)
        connected._datachanges_period[node_id] = (timedelta(days=1), 1000)
        
        datavalue = ua.DataValue(
            Value=ua.Variant(42.0, ua.VariantType.Double),
            SourceTimestamp=datetime.now(timezone.utc),
            ServerTimestamp=datetime.now(timezone.utc),
            # На целевой asyncua 1.x поле называется StatusCode_; StatusCode — только property.
            StatusCode_=ua.StatusCode(ua.StatusCodes.Good),
        )
        
        await connected.save_node_value(node_id, datavalue)

        # Проверяем, что был вызван INSERT в таблицу переменных
        mock_connection.execute.assert_awaited()
        assert any(
            'INSERT INTO "var_1_TestVariable"' in call.args[0]
            for call in mock_connection.execute.await_args_list
        )
    
    async def test_read_node_history(self, connected, mock_connection):
        """Тест чтения истории узла."""
        # Мокаем результат запроса
        mock_row = {
            'servertimestamp': datetime.now(timezone.utc),
            'sourcetimestamp': datetime.now(timezone.utc),
            'statuscode': 0,
            'variantbinary': b'test_binary_data'
        }
        mock_connection.fetch.return_value = [mock_row]
        
        node_id = ua.NodeId("TestVariable", 1)
        start_time = datetime.now(timezone.utc) - timedelta(hours=1)
        end_time = datetime.now(timezone.utc)

        results, continuation = await connected.read_node_history(
            node_id, start_time, end_time, 100
        )
        
        assert isinstance(results, list)
        assert continuation is None or isinstance(continuation, datetime)


class TestBuffer:
    """Тесты для класса Buffer."""
    
    def test_buffer_read_consumes_sequentially(self):
        """Чтение отдаёт куски подряд и двигает позицию."""
        from uapg.history_pgsql import Buffer
        buffer = Buffer(b"test_data")

        assert buffer.read(4) == b"test"
        assert buffer.read(4) == b"_dat"
        assert buffer.read(4) == b"a"
        assert buffer.read(4) == b""

    def test_buffer_skip_and_copy(self):
        """skip двигает позицию, copy отдаёт остаток как новый буфер."""
        from uapg.history_pgsql import Buffer
        buffer = Buffer(b"test_data")

        buffer.skip(5)
        assert buffer.read(4) == b"data"

        buffer = Buffer(b"test_data")
        buffer.skip(5)
        tail = buffer.copy()
        assert tail.read(9) == b"data"
        # Исходный буфер остался на своей позиции.
        assert buffer.read(4) == b"data"


if __name__ == "__main__":
    pytest.main([__file__]) 