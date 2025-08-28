"""
Тесты для модуля DatabaseManager.
"""

import pytest
import tempfile
import json
from pathlib import Path
from unittest.mock import Mock, patch, AsyncMock

from uapg.db_manager import DatabaseManager


class TestDatabaseManager:
    """Тесты для класса DatabaseManager."""
    
    def setup_method(self):
        """Настройка перед каждым тестом."""
        self.master_password = "test_master_password_123"
        self.temp_dir = tempfile.mkdtemp()
        self.config_file = Path(self.temp_dir) / "db_config.enc"
        self.key_file = Path(self.temp_dir) / ".db_key"
        
        # Создание временного менеджера
        self.db_manager = DatabaseManager(
            self.master_password,
            str(self.config_file),
            str(self.key_file)
        )
    
    def teardown_method(self):
        """Очистка после каждого теста."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_init_encryption(self):
        """Тест инициализации шифрования."""
        assert self.db_manager.key_file.exists()
        assert self.db_manager.cipher is not None
        assert self.db_manager.master_password == self.master_password
    
    def test_encrypt_decrypt_config(self):
        """Тест шифрования и дешифрования конфигурации."""
        test_config = {
            'user': 'test_user',
            'password': 'test_password',
            'database': 'test_db'
        }
        
        # Шифрование
        encrypted = self.db_manager._encrypt_config(test_config)
        assert isinstance(encrypted, bytes)
        assert encrypted != test_config
        
        # Дешифрование
        decrypted = self.db_manager._decrypt_config(encrypted)
        assert decrypted == test_config
    
    def test_save_load_config(self):
        """Тест сохранения и загрузки конфигурации."""
        test_config = {
            'user': 'test_user',
            'password': 'test_password',
            'database': 'test_db',
            'host': 'localhost',
            'port': 5432
        }
        
        # Сохранение
        self.db_manager._save_config(test_config)
        assert self.config_file.exists()
        
        # Загрузка
        loaded_config = self.db_manager._load_config()
        assert loaded_config == test_config
    
    def test_change_master_password(self):
        """Тест изменения главного пароля."""
        # Сначала сохраняем конфигурацию
        test_config = {'test': 'data'}
        self.db_manager._save_config(test_config)
        
        # Меняем пароль
        new_password = "new_master_password_456"
        success = self.db_manager.change_master_password(new_password)
        
        assert success
        assert self.db_manager.master_password == new_password
        
        # Проверяем, что конфигурация все еще доступна
        loaded_config = self.db_manager._load_config()
        assert loaded_config == test_config
    
    def test_export_import_config(self):
        """Тест экспорта и импорта конфигурации."""
        test_config = {
            'user': 'test_user',
            'password': 'test_password',
            'database': 'test_db'
        }
        
        # Сохраняем конфигурацию
        self.db_manager._save_config(test_config)
        
        # Экспорт
        export_path = Path(self.temp_dir) / "exported_config.json"
        success = self.db_manager.export_config(str(export_path))
        assert success
        assert export_path.exists()
        
        # Проверяем содержимое экспортированного файла
        with open(export_path, 'r', encoding='utf-8') as f:
            exported_data = json.load(f)
        assert exported_data == test_config
        
        # Импорт в новый менеджер
        new_manager = DatabaseManager("new_password")
        success = new_manager.import_config(str(export_path))
        assert success
        assert new_manager.config == test_config
    
    @pytest.mark.asyncio
    async def test_get_database_info_no_config(self):
        """Тест получения информации о БД без конфигурации."""
        info = await self.db_manager.get_database_info()
        assert info == {}
    
    @pytest.mark.asyncio
    async def test_create_database_mock(self):
        """Тест создания базы данных с моком."""
        with patch('uapg.db_manager.psycopg2.connect') as mock_connect, \
             patch('uapg.db_manager.asyncpg.connect') as mock_async_connect:
            
            # Мок для psycopg2
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_conn.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_conn
            
            # Мок для asyncpg
            mock_async_conn = AsyncMock()
            mock_async_connect.return_value = mock_async_conn
            
            success = await self.db_manager.create_database(
                user="test_user",
                password="test_password",
                database="test_db"
            )
            
            assert success
            mock_connect.assert_called_once()
            mock_async_connect.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_backup_database_mock(self):
        """Тест создания резервной копии с моком."""
        # Устанавливаем конфигурацию
        self.db_manager.config = {
            'user': 'test_user',
            'password': 'test_password',
            'database': 'test_db',
            'host': 'localhost',
            'port': 5432
        }
        
        with patch('subprocess.run') as mock_run:
            mock_result = Mock()
            mock_result.returncode = 0
            mock_run.return_value = mock_result
            
            backup_path = await self.db_manager.backup_database()
            
            assert backup_path is not None
            assert "backup_test_db_" in backup_path
            mock_run.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_cleanup_old_data_mock(self):
        """Тест очистки старых данных с моком."""
        # Устанавливаем конфигурацию
        self.db_manager.config = {
            'user': 'test_user',
            'password': 'test_password',
            'database': 'test_db',
            'host': 'localhost',
            'port': 5432
        }
        
        with patch('uapg.db_manager.asyncpg.connect') as mock_connect:
            mock_conn = AsyncMock()
            mock_connect.return_value = mock_conn
            
            # Мок для запросов
            mock_conn.fetch.return_value = []
            mock_conn.fetchrow.return_value = None
            
            success = await self.db_manager.cleanup_old_data(retention_days=30)
            
            assert success
            mock_connect.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_migrate_schema_mock(self):
        """Тест миграции схемы с моком."""
        # Устанавливаем конфигурацию
        self.db_manager.config = {
            'user': 'test_user',
            'password': 'test_password',
            'database': 'test_db',
            'host': 'localhost',
            'port': 5432
        }
        
        with patch('uapg.db_manager.asyncpg.connect') as mock_connect:
            mock_conn = AsyncMock()
            mock_connect.return_value = mock_conn
            
            # Мок для запросов
            mock_conn.fetchval.return_value = "1.0"
            
            migration_scripts = [
                {
                    'version': '1.1',
                    'description': 'Test migration',
                    'sql': 'CREATE INDEX test_idx ON test_table;'
                }
            ]
            
            success = await self.db_manager.migrate_schema('1.1', migration_scripts)
            
            assert success
            mock_connect.assert_called_once()


class TestStandaloneFunctions:
    """Тесты для standalone функций."""
    
    @pytest.mark.asyncio
    async def test_create_database_standalone(self):
        """Тест standalone функции создания БД."""
        from uapg.db_manager import create_database_standalone
        
        with patch('uapg.db_manager.DatabaseManager') as mock_manager_class:
            mock_manager = Mock()
            mock_manager_class.return_value = mock_manager
            mock_manager.create_database = AsyncMock(return_value=True)
            
            success = await create_database_standalone(
                user="test_user",
                password="test_password",
                database="test_db"
            )
            
            assert success
            mock_manager.create_database.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_backup_database_standalone(self):
        """Тест standalone функции создания бэкапа."""
        from uapg.db_manager import backup_database_standalone
        
        with patch('uapg.db_manager.DatabaseManager') as mock_manager_class:
            mock_manager = Mock()
            mock_manager_class.return_value = mock_manager
            mock_manager.backup_database = AsyncMock(return_value="backup.backup")
            
            backup_path = await backup_database_standalone(
                user="test_user",
                password="test_password",
                database="test_db"
            )
            
            assert backup_path == "backup.backup"
            mock_manager.backup_database.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
