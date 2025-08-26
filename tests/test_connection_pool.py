"""
Тесты для пула подключений PostgreSQL в UAPG.
"""

import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock
from uapg.history_pgsql import HistoryPgSQL
from asyncua import ua


class TestConnectionPool:
    """Тесты для пула подключений."""
    
    @pytest.fixture
    def history_storage(self):
        """Создание экземпляра HistoryPgSQL для тестирования."""
        return HistoryPgSQL(
            user='test_user',
            password='test_password',
            database='test_db',
            host='test_host',
            port=5432,
            min_size=2,
            max_size=5
        )
    
    @pytest.fixture
    def mock_pool(self):
        """Мок пула подключений."""
        pool = Mock()
        pool.get_min_size.return_value = 2
        pool.get_max_size.return_value = 5
        pool.get_size.return_value = 3
        pool.get_free_size.return_value = 1
        return pool
    
    def test_constructor_parameters(self, history_storage):
        """Тест параметров конструктора."""
        assert history_storage._min_size == 2
        assert history_storage._max_size == 5
        assert history_storage._conn_params['port'] == 5432
        assert history_storage._pool is None
    
    @pytest.mark.asyncio
    async def test_init_creates_pool(self, history_storage):
        """Тест создания пула при инициализации."""
        with patch('asyncpg.create_pool') as mock_create_pool:
            mock_pool = AsyncMock()
            mock_create_pool.return_value = mock_pool
            
            await history_storage.init()
            
            mock_create_pool.assert_called_once_with(
                user='test_user',
                password='test_password',
                database='test_db',
                host='test_host',
                port=5432,
                min_size=2,
                max_size=5,
                command_timeout=60,
                statement_cache_size=0
            )
            assert history_storage._pool == mock_pool
    
    @pytest.mark.asyncio
    async def test_init_handles_errors(self, history_storage):
        """Тест обработки ошибок при инициализации."""
        with patch('asyncpg.create_pool') as mock_create_pool:
            mock_create_pool.side_effect = Exception("Connection failed")
            
            with pytest.raises(Exception, match="Connection failed"):
                await history_storage.init()
            
            assert history_storage._pool is None
    
    @pytest.mark.asyncio
    async def test_stop_closes_pool(self, history_storage, mock_pool):
        """Тест закрытия пула."""
        history_storage._pool = mock_pool
        
        await history_storage.stop()
        
        mock_pool.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_stop_handles_none_pool(self, history_storage):
        """Тест закрытия при отсутствии пула."""
        history_storage._pool = None
        
        # Не должно вызывать ошибку
        await history_storage.stop()
    
    @pytest.mark.asyncio
    async def test_execute_uses_pool(self, history_storage, mock_pool):
        """Тест использования пула для выполнения запросов."""
        history_storage._pool = mock_pool
        
        # Мокаем контекстный менеджер
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value.__aexit__.return_value = None
        
        await history_storage._execute("SELECT 1", "param1")
        
        mock_pool.acquire.assert_called_once()
        mock_conn.execute.assert_called_once_with("SELECT 1", "param1")
    
    @pytest.mark.asyncio
    async def test_fetch_uses_pool(self, history_storage, mock_pool):
        """Тест использования пула для выборки данных."""
        history_storage._pool = mock_pool
        
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value.__aexit__.return_value = None
        
        await history_storage._fetch("SELECT * FROM table", "param1")
        
        mock_pool.acquire.assert_called_once()
        mock_conn.fetch.assert_called_once_with("SELECT * FROM table", "param1")
    
    @pytest.mark.asyncio
    async def test_fetchval_uses_pool(self, history_storage, mock_pool):
        """Тест использования пула для получения одного значения."""
        history_storage._pool = mock_pool
        
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value.__aexit__.return_value = None
        
        await history_storage._fetchval("SELECT COUNT(*) FROM table")
        
        mock_pool.acquire.assert_called_once()
        mock_conn.fetchval.assert_called_once_with("SELECT COUNT(*) FROM table")
    
    def test_default_parameters(self):
        """Тест параметров по умолчанию."""
        history = HistoryPgSQL()
        
        assert history._min_size == 5
        assert history._max_size == 20
        assert history._conn_params['port'] == 5432
    
    def test_custom_parameters(self):
        """Тест пользовательских параметров."""
        history = HistoryPgSQL(
            min_size=10,
            max_size=100,
            port=5433
        )
        
        assert history._min_size == 10
        assert history._max_size == 100
        assert history._conn_params['port'] == 5433


class TestPoolIntegration:
    """Тесты интеграции с реальным пулом подключений."""
    
    @pytest.mark.asyncio
    async def test_pool_lifecycle(self):
        """Тест жизненного цикла пула."""
        history = HistoryPgSQL(
            user='test_user',
            password='test_password',
            database='test_db',
            host='test_host',
            min_size=1,
            max_size=3
        )
        
        # Проверяем, что пул не создан до инициализации
        assert history._pool is None
        
        # Мокаем создание пула
        with patch('asyncpg.create_pool') as mock_create_pool:
            mock_pool = AsyncMock()
            mock_create_pool.return_value = mock_pool
            
            await history.init()
            
            # Проверяем, что пул создан
            assert history._pool == mock_pool
            
            # Проверяем, что пул закрывается при остановке
            await history.stop()
            mock_pool.close.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__])
