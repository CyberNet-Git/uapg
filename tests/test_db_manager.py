"""
Тесты для модуля DatabaseManager.
"""

import pytest
import tempfile
import json
from pathlib import Path
from unittest.mock import Mock, patch, AsyncMock

import psycopg

from uapg.db_manager import DatabaseManager

# conftest подменяет psycopg заглушкой, когда пакета нет (это опциональная
# зависимость DatabaseManager). Тесты create_database опираются на настоящую
# композицию psycopg.sql и классы ошибок, поэтому на заглушке их пропускаем.
_REAL_PSYCOPG = type(psycopg).__name__ == "module"
requires_psycopg = pytest.mark.skipif(
    not _REAL_PSYCOPG, reason="нужен настоящий psycopg, а не заглушка из conftest"
)


class _FakeCursor:
    """Курсор psycopg3: контекстный менеджер, пишет выполненные запросы."""

    def __init__(self, errors=None):
        self.statements = []
        self.errors = list(errors or [])
        self.closed_by_context = False

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.closed_by_context = True
        return False

    def execute(self, statement, *args):
        self.statements.append(statement)
        if self.errors:
            error = self.errors.pop(0)
            if error is not None:
                raise error("already exists")


class _FakeConnection:
    """Соединение psycopg3. set_isolation_level — API psycopg2, его быть не должно."""

    def __init__(self, cursor):
        self._cursor = cursor
        self.entered = False
        self.exited = False

    def __enter__(self):
        self.entered = True
        return self

    def __exit__(self, *exc_info):
        self.exited = True
        return False

    def cursor(self):
        return self._cursor

    def set_isolation_level(self, *args, **kwargs):
        raise AssertionError("set_isolation_level — API psycopg2, в psycopg3 его нет")

_CONN_CONFIG = {
    'user': 'test_user',
    'password': 'test_password',
    'database': 'test_db',
    'host': 'localhost',
    'port': 5432,
}


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
        
        # Импорт в новый менеджер. Пути задаём явно: с значениями по умолчанию
        # DatabaseManager пишет db_config.enc и .db_key в текущий каталог, то есть
        # прогон тестов менял файлы в корне репозитория.
        new_manager = DatabaseManager(
            "new_password",
            str(Path(self.temp_dir) / "imported_config.enc"),
            str(Path(self.temp_dir) / ".imported_key"),
        )
        success = new_manager.import_config(str(export_path))
        assert success
        assert new_manager.config == test_config
    
    async def test_get_database_info_no_config(self):
        """Без конфигурации возвращается причина, а не пустой словарь."""
        info = await self.db_manager.get_database_info()
        assert info == {"error": "No database configuration found"}
    
    @requires_psycopg
    async def test_create_database_mock(self):
        """Создание пользователя и БД идёт по API psycopg3.

        Раньше здесь вызывался set_isolation_level(psycopg.ISOLATION_LEVEL_AUTOCOMMIT) —
        API psycopg2, которого в psycopg3 нет: на живой установке вызов падал с
        AttributeError в собственный except и возвращал False. В тестах это было не
        видно, потому что conftest подменял модуль psycopg заглушкой.
        """
        cursor = _FakeCursor()
        conn = _FakeConnection(cursor)
        mock_async_connect = AsyncMock(return_value=AsyncMock())

        with patch('uapg.db_manager.psycopg.connect', return_value=conn) as mock_connect, \
             patch('uapg.db_manager.asyncpg.connect', new=mock_async_connect):
            success = await self.db_manager.create_database(
                user="test_user",
                password="test_password",
                database="test_db"
            )

        assert success
        # autocommit задаётся при подключении: CREATE DATABASE нельзя выполнить
        # внутри транзакционного блока.
        kwargs = mock_connect.call_args.kwargs
        assert kwargs['autocommit'] is True
        # Имя базы в psycopg3 — dbname; алиас database остался только в psycopg2
        # и даёт `invalid connection option "database"`.
        assert kwargs['dbname'] == 'postgres'
        assert 'database' not in kwargs
        assert conn.entered and conn.exited, "соединение должно браться контекстным менеджером"
        assert cursor.closed_by_context
        mock_async_connect.assert_awaited()

        statements = [stmt.as_string() for stmt in cursor.statements]
        assert 'CREATE USER "test_user" WITH PASSWORD' in statements[0]
        assert statements[1] == 'CREATE DATABASE "test_db" OWNER "test_user"'

    @requires_psycopg
    async def test_create_database_quotes_identifiers_and_password(self):
        """Пароль с кавычкой раньше ломал запрос: подстановка шла f-строкой."""
        cursor = _FakeCursor()
        conn = _FakeConnection(cursor)

        with patch('uapg.db_manager.psycopg.connect', return_value=conn), \
             patch('uapg.db_manager.asyncpg.connect', new=AsyncMock(return_value=AsyncMock())):
            success = await self.db_manager.create_database(
                user="o'brien",
                password="pa'ss",
                database="my-db"
            )

        assert success
        create_user = cursor.statements[0].as_string()
        assert create_user == """CREATE USER "o\'brien" WITH PASSWORD \'pa\'\'ss\'"""
        assert cursor.statements[1].as_string() == """CREATE DATABASE "my-db" OWNER "o\'brien\""""

    @requires_psycopg
    async def test_create_database_tolerates_existing_user_and_database(self):
        """Повторный запуск не считается ошибкой."""
        cursor = _FakeCursor(
            errors=[psycopg.errors.DuplicateObject, psycopg.errors.DuplicateDatabase]
        )
        conn = _FakeConnection(cursor)

        with patch('uapg.db_manager.psycopg.connect', return_value=conn), \
             patch('uapg.db_manager.asyncpg.connect', new=AsyncMock(return_value=AsyncMock())):
            success = await self.db_manager.create_database(
                user="test_user",
                password="test_password",
                database="test_db"
            )

        assert success
        assert len(cursor.statements) == 2

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
    
    async def test_migrate_to_timescale_requires_config(self):
        """Без конфигурации миграция не выполняется и возвращает False."""
        self.db_manager.config = {}

        assert await self.db_manager.migrate_to_timescale() is False

    async def test_migrate_to_timescale_is_noop_when_already_migrated(self):
        """Повторная миграция не трогает схему, если архитектура уже целевая."""
        self.db_manager.config = dict(_CONN_CONFIG)
        mock_conn = AsyncMock()
        mock_conn.fetchval.return_value = "timescale_hypertables"

        with patch('uapg.db_manager.asyncpg.connect', new=AsyncMock(return_value=mock_conn)):
            assert await self.db_manager.migrate_to_timescale() is True

        mock_conn.execute.assert_not_awaited()
        mock_conn.close.assert_awaited_once()

    async def test_migrate_to_timescale_mock(self):
        """Миграция со старой архитектуры: таблицы, данные, версия схемы."""
        self.db_manager.config = dict(_CONN_CONFIG)
        mock_conn = AsyncMock()
        mock_conn.fetchval.return_value = "1.0"

        with patch('uapg.db_manager.asyncpg.connect', new=AsyncMock(return_value=mock_conn)), \
             patch.object(DatabaseManager, '_create_timescale_tables', AsyncMock()) as tables, \
             patch.object(DatabaseManager, '_create_timescale_metadata_tables', AsyncMock()) as meta, \
             patch.object(DatabaseManager, '_migrate_data_to_timescale', AsyncMock()) as data:
            success = await self.db_manager.migrate_to_timescale()

        assert success
        tables.assert_awaited_once()
        meta.assert_awaited_once()
        data.assert_awaited_once()
        # Версия схемы помечена как перенесённая на hypertables.
        inserts = " ".join(call.args[0] for call in mock_conn.execute.await_args_list)
        assert "INSERT INTO schema_version" in inserts
        assert "timescale_hypertables" in inserts
        assert self.db_manager.config['architecture'] == 'timescale_hypertables'
        assert self.db_manager.config['version'] == '2.0'

    async def test_migrate_to_timescale_returns_false_on_error(self):
        """Ошибка подключения не выбрасывается наружу, а возвращается как False."""
        self.db_manager.config = dict(_CONN_CONFIG)

        with patch(
            'uapg.db_manager.asyncpg.connect', new=AsyncMock(side_effect=OSError("down"))
        ):
            assert await self.db_manager.migrate_to_timescale() is False


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
