"""
Утилита для управления базой данных PostgreSQL для OPC UA History.

Этот модуль предоставляет функциональность для:
1. Первоначального создания базы данных
2. Управления учетными данными (с шифрованием)
3. Миграции схемы базы данных
4. Резервного копирования
5. Очистки данных и таблиц
"""

import asyncio
import json
import logging
import os
import subprocess
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import base64
import asyncpg
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


class DatabaseManager:
    """
    Менеджер базы данных для OPC UA History.
    
    Этот класс предоставляет утилиты для управления базой данных PostgreSQL
    вне основного функционала HistoryPgSQL.
    """
    
    def __init__(
        self,
        master_password: str,
        config_file: str = "db_config.enc",
        key_file: str = ".db_key"
    ):
        """
        Инициализация менеджера базы данных.
        
        Args:
            master_password: Главный пароль для шифрования/дешифрования
            config_file: Файл для хранения зашифрованной конфигурации
            key_file: Файл для хранения ключа шифрования
        """
        self.master_password = master_password
        self.config_file = Path(config_file)
        self.key_file = Path(key_file)
        self.logger = logging.getLogger(__name__)
        
        # Инициализация шифрования
        self._init_encryption()
        
        # Загрузка конфигурации
        self.config = self._load_config()
    
    def _init_encryption(self) -> None:
        """Инициализация системы шифрования."""
        if self.key_file.exists():
            # Загружаем существующий ключ
            with open(self.key_file, 'rb') as f:
                self.key = f.read()
        else:
            # Создаем новый ключ на основе master_password
            salt = os.urandom(16)
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=salt,
                iterations=100000,
            )
            key = base64.urlsafe_b64encode(kdf.derive(self.master_password.encode()))
            
            # Сохраняем ключ и соль
            with open(self.key_file, 'wb') as f:
                f.write(salt + key)
            
            self.key = salt + key
        
        self.cipher = Fernet(self.key[16:])  # Используем только ключ, без соли
    
    def _encrypt_config(self, config: Dict[str, Any]) -> bytes:
        """Шифрование конфигурации."""
        config_str = json.dumps(config, ensure_ascii=False)
        return self.cipher.encrypt(config_str.encode())
    
    def _decrypt_config(self, encrypted_data: bytes) -> Dict[str, Any]:
        """Дешифрование конфигурации."""
        decrypted_data = self.cipher.decrypt(encrypted_data)
        return json.loads(decrypted_data.decode())
    
    def _save_config(self, config: Dict[str, Any]) -> None:
        """Сохранение зашифрованной конфигурации."""
        encrypted_data = self._encrypt_config(config)
        with open(self.config_file, 'wb') as f:
            f.write(encrypted_data)
        self.config = config
    
    def _load_config(self) -> Dict[str, Any]:
        """Загрузка конфигурации из файла."""
        if not self.config_file.exists():
            return {}
        
        try:
            with open(self.config_file, 'rb') as f:
                encrypted_data = f.read()
            return self._decrypt_config(encrypted_data)
        except Exception as e:
            self.logger.warning(f"Failed to load config: {e}")
            return {}
    
    def _get_connection_params(self) -> Dict[str, Any]:
        """Получение параметров подключения без служебных полей."""
        if not self.config:
            return {}
        
        return {
            'host': self.config['host'],
            'port': self.config['port'],
            'user': self.config['user'],
            'password': self.config['password'],
            'database': self.config['database']
        }

    async def create_database(
        self,
        user: str,
        password: str,
        database: str,
        host: str = "localhost",
        port: int = 5432,
        superuser: str = "postgres",
        superuser_password: str = None,
        enable_timescaledb: bool = True
    ) -> bool:
        """
        Создание базы данных и пользователя.
        
        Args:
            user: Имя пользователя для создания
            password: Пароль пользователя
            database: Имя базы данных
            host: Хост PostgreSQL
            port: Порт PostgreSQL
            superuser: Суперпользователь для создания БД
            superuser_password: Пароль суперпользователя
            enable_timescaledb: Включить поддержку TimescaleDB
            
        Returns:
            True если успешно создано
        """
        try:
            # Подключение к PostgreSQL как суперпользователь
            if superuser_password:
                conn_params = {
                    'host': host,
                    'port': port,
                    'user': superuser,
                    'password': superuser_password,
                    'database': 'postgres'
                }
            else:
                # Попытка подключения без пароля (для локальной установки)
                conn_params = {
                    'host': host,
                    'port': port,
                    'user': superuser,
                    'database': 'postgres'
                }
            
            # Создание пользователя и базы данных
            conn = psycopg2.connect(**conn_params)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            
            # Создание пользователя
            try:
                cursor.execute(f"CREATE USER {user} WITH PASSWORD '{password}'")
                self.logger.info(f"User {user} created successfully")
            except psycopg2.errors.DuplicateObject:
                self.logger.info(f"User {user} already exists")
            
            # Создание базы данных
            try:
                cursor.execute(f"CREATE DATABASE {database} OWNER {user}")
                self.logger.info(f"Database {database} created successfully")
            except psycopg2.errors.DuplicateDatabase:
                self.logger.info(f"Database {database} already exists")
            
            cursor.close()
            conn.close()
            
            # Подключение к новой базе данных для настройки
            new_conn_params = {
                'host': host,
                'port': port,
                'user': user,
                'password': password,
                'database': database
            }
            
            # Создание схемы и расширений
            await self._setup_database_schema(new_conn_params, enable_timescaledb)
            
            # Сохранение конфигурации
            config = {
                'user': user,
                'password': password,
                'database': database,
                'host': host,
                'port': port,
                'created_at': datetime.now().isoformat(),
                'version': '1.0'
            }
            self._save_config(config)
            
            self.logger.info(f"Database {database} setup completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create database: {e}")
            return False
    
    async def _setup_database_schema(
        self,
        conn_params: Dict[str, Any],
        enable_timescaledb: bool
    ) -> None:
        """Настройка схемы базы данных."""
        try:
            conn = await asyncpg.connect(**conn_params)
            
            # Создание расширений
            if enable_timescaledb:
                try:
                    await conn.execute('CREATE EXTENSION IF NOT EXISTS timescaledb')
                    self.logger.info("TimescaleDB extension enabled")
                except Exception as e:
                    self.logger.warning(f"TimescaleDB extension not available: {e}")
            
            await conn.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
            
            # Создание базовых таблиц метаданных
            await self._create_base_tables(conn)
            
            await conn.close()
            
        except Exception as e:
            self.logger.error(f"Failed to setup database schema: {e}")
            raise
    
    async def _create_base_tables(self, conn: asyncpg.Connection) -> None:
        """Создание базовых таблиц метаданных."""
        # Таблица метаданных переменных
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS variable_metadata (
                id SERIAL PRIMARY KEY,
                node_id TEXT NOT NULL UNIQUE,
                node_name TEXT,
                data_type TEXT,
                table_name TEXT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                retention_period INTERVAL,
                max_records INTEGER
            )
        ''')
        
        # Таблица метаданных типов событий
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS event_type_metadata (
                id SERIAL PRIMARY KEY,
                event_type_id TEXT NOT NULL,
                event_type_name TEXT,
                source_node_id TEXT NOT NULL,
                table_name TEXT NOT NULL,
                fields JSONB NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                retention_period INTERVAL,
                max_records INTEGER,
                UNIQUE(event_type_id, source_node_id)
            )
        ''')
        
        # Таблица версий схемы
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS schema_version (
                id SERIAL PRIMARY KEY,
                version TEXT NOT NULL UNIQUE,
                applied_at TIMESTAMPTZ DEFAULT NOW(),
                description TEXT,
                migration_script TEXT
            )
        ''')
        
        # Создание индексов
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_variable_metadata_node_id ON variable_metadata(node_id)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_event_type_metadata_source ON event_type_metadata(source_node_id)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_event_type_metadata_type ON event_type_metadata(event_type_id)')
        
        # Вставка начальной версии схемы
        await conn.execute('''
            INSERT INTO schema_version (version, description) 
            VALUES ('1.0', 'Initial schema') 
            ON CONFLICT (version) DO NOTHING
        ''')
        
        self.logger.info("Base tables created successfully")
    
    async def migrate_schema(
        self,
        target_version: str,
        migration_scripts: List[Dict[str, str]]
    ) -> bool:
        """
        Миграция схемы базы данных на новую версию.
        
        Args:
            target_version: Целевая версия схемы
            migration_scripts: Список скриптов миграции
            
        Returns:
            True если миграция успешна
        """
        if not self.config:
            self.logger.error("No database configuration found")
            return False
        
        try:
            conn_params = self._get_connection_params()
            conn = await asyncpg.connect(**conn_params)
            
            # Проверка текущей версии
            current_version = await conn.fetchval(
                'SELECT version FROM schema_version ORDER BY applied_at DESC LIMIT 1'
            )
            
            if current_version == target_version:
                self.logger.info(f"Schema already at version {target_version}")
                return True
            
            self.logger.info(f"Migrating schema from {current_version} to {target_version}")
            
            # Применение миграций
            for migration in migration_scripts:
                if migration['version'] > current_version:
                    try:
                        # Выполнение SQL скрипта
                        await conn.execute(migration['sql'])
                        
                        # Запись о миграции
                        await conn.execute('''
                            INSERT INTO schema_version (version, description, migration_script)
                            VALUES ($1, $2, $3)
                        ''', migration['version'], migration['description'], migration['sql'])
                        
                        self.logger.info(f"Applied migration to version {migration['version']}")
                        
                    except Exception as e:
                        self.logger.error(f"Migration {migration['version']} failed: {e}")
                        await conn.close()
                        return False
            
            await conn.close()
            
            # Обновление версии в конфигурации
            self.config['version'] = target_version
            self._save_config(self.config)
            
            self.logger.info(f"Schema migration to {target_version} completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Schema migration failed: {e}")
            return False
    
    async def backup_database(
        self,
        backup_path: str = None,
        backup_format: str = "custom",
        compression: bool = True
    ) -> Optional[str]:
        """
        Создание резервной копии базы данных.
        
        Args:
            backup_path: Путь для сохранения бэкапа
            backup_format: Формат бэкапа (custom, plain, directory)
            compression: Использовать сжатие
            
        Returns:
            Путь к созданному бэкапу или None при ошибке
        """
        if not self.config:
            self.logger.error("No database configuration found")
            return None
        
        try:
            if not backup_path:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_path = f"backup_{self.config['database']}_{timestamp}.backup"
            
            # Формирование команды pg_dump
            cmd = [
                'pg_dump',
                f'--host={self.config["host"]}',
                f'--port={self.config["port"]}',
                f'--username={self.config["user"]}',
                f'--dbname={self.config["database"]}',
                f'--format={backup_format}',
                f'--file={backup_path}'
            ]
            
            if compression:
                cmd.append('--compress=9')
            
            # Установка переменной окружения для пароля
            env = os.environ.copy()
            env['PGPASSWORD'] = self.config['password']
            
            # Выполнение команды
            result = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                self.logger.info(f"Database backup created: {backup_path}")
                return backup_path
            else:
                self.logger.error(f"Backup failed: {result.stderr}")
                return None
                
        except Exception as e:
            self.logger.error(f"Backup creation failed: {e}")
            return None
    
    async def restore_database(
        self,
        backup_path: str,
        drop_existing: bool = False
    ) -> bool:
        """
        Восстановление базы данных из резервной копии.
        
        Args:
            backup_path: Путь к файлу бэкапа
            drop_existing: Удалить существующую БД перед восстановлением
            
        Returns:
            True если восстановление успешно
        """
        if not self.config:
            self.logger.error("No database configuration found")
            return False
        
        try:
            # Определение формата бэкапа
            if backup_path.endswith('.backup') or backup_path.endswith('.dump'):
                format_flag = '--format=custom'
            elif backup_path.endswith('.sql'):
                format_flag = '--format=plain'
            else:
                format_flag = '--format=custom'
            
            # Формирование команды pg_restore
            cmd = [
                'pg_restore',
                f'--host={self.config["host"]}',
                f'--port={self.config["port"]}',
                f'--username={self.config["user"]}',
                f'--dbname={self.config["database"]}',
                format_flag,
                '--clean',  # Очистка существующих объектов
                '--if-exists',  # Продолжать если объект не существует
                backup_path
            ]
            
            # Установка переменной окружения для пароля
            env = os.environ.copy()
            env['PGPASSWORD'] = self.config['password']
            
            # Выполнение команды
            result = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                self.logger.info(f"Database restored from: {backup_path}")
                return True
            else:
                self.logger.error(f"Restore failed: {result.stderr}")
                return False
                
        except Exception as e:
            self.logger.error(f"Database restore failed: {e}")
            return False
    
    async def cleanup_old_data(
        self,
        retention_days: int = 365,
        node_ids: List[str] = None,
        event_types: List[str] = None
    ) -> bool:
        """
        Очистка старых данных по времени.
        
        Args:
            retention_days: Количество дней для хранения данных
            node_ids: Список ID узлов для очистки (None - все узлы)
            event_types: Список типов событий для очистки (None - все типы)
            
        Returns:
            True если очистка успешна
        """
        if not self.config:
            self.logger.error("No database configuration found")
            return False
        
        try:
            conn_params = self._get_connection_params()
            conn = await asyncpg.connect(**conn_params)
            
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            
            # Очистка данных переменных
            if node_ids:
                for node_id in node_ids:
                    metadata = await conn.fetchrow(
                        'SELECT table_name FROM variable_metadata WHERE node_id = $1',
                        node_id
                    )
                    if metadata:
                        deleted = await conn.execute('''
                            DELETE FROM "{}" WHERE sourcetimestamp < $1
                        '''.format(metadata['table_name']), cutoff_date)
                        self.logger.info(f"Cleaned {deleted.split()[-1]} old records from {metadata['table_name']}")
            else:
                # Очистка всех таблиц переменных
                tables = await conn.fetch('SELECT table_name FROM variable_metadata')
                for table in tables:
                    deleted = await conn.execute('''
                        DELETE FROM "{}" WHERE sourcetimestamp < $1
                    '''.format(table['table_name']), cutoff_date)
                    self.logger.info(f"Cleaned {deleted.split()[-1]} old records from {table['table_name']}")
            
            # Очистка данных событий
            if event_types:
                for event_type in event_types:
                    metadata = await conn.fetch(
                        'SELECT table_name FROM event_type_metadata WHERE event_type_id = $1',
                        event_type
                    )
                    for table in metadata:
                        deleted = await conn.execute('''
                            DELETE FROM "{}" WHERE _timestamp < $1
                        '''.format(table['table_name']), cutoff_date)
                        self.logger.info(f"Cleaned {deleted.split()[-1]} old records from {table['table_name']}")
            else:
                # Очистка всех таблиц событий
                tables = await conn.fetch('SELECT table_name FROM event_type_metadata')
                for table in tables:
                    deleted = await conn.execute('''
                        DELETE FROM "{}" WHERE _timestamp < $1
                    '''.format(table['table_name']), cutoff_date)
                    self.logger.info(f"Cleaned {deleted.split()[-1]} old records from {table['table_name']}")
            
            await conn.close()
            
            self.logger.info(f"Data cleanup completed for records older than {retention_days} days")
            return True
            
        except Exception as e:
            self.logger.error(f"Data cleanup failed: {e}")
            return False
    
    async def remove_node_tables(self, node_ids: List[str]) -> bool:
        """
        Удаление таблиц узлов и связанных метаданных.
        
        Args:
            node_ids: Список ID узлов для удаления
            
        Returns:
            True если удаление успешно
        """
        if not self.config:
            self.logger.error("No database configuration found")
            return False
        
        try:
            conn_params = self._get_connection_params()
            conn = await asyncpg.connect(**conn_params)
            
            for node_id in node_ids:
                # Получение информации о таблице
                metadata = await conn.fetchrow(
                    'SELECT table_name FROM variable_metadata WHERE node_id = $1',
                    node_id
                )
                
                if metadata:
                    # Удаление таблицы данных
                    await conn.execute(f'DROP TABLE IF EXISTS "{metadata["table_name"]}"')
                    
                    # Удаление метаданных
                    await conn.execute(
                        'DELETE FROM variable_metadata WHERE node_id = $1',
                        node_id
                    )
                    
                    self.logger.info(f"Removed table and metadata for node {node_id}")
            
            await conn.close()
            
            self.logger.info(f"Removed {len(node_ids)} node tables successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to remove node tables: {e}")
            return False
    
    async def clear_all_data(self) -> bool:
        """
        Полная очистка базы данных от всех данных.
        
        Returns:
            True если очистка успешна
        """
        if not self.config:
            self.logger.error("No database configuration found")
            return False
        
        try:
            conn_params = self._get_connection_params()
            conn = await asyncpg.connect(**conn_params)
            
            # Получение всех таблиц данных
            variable_tables = await conn.fetch('SELECT table_name FROM variable_metadata')
            event_tables = await conn.fetch('SELECT table_name FROM event_type_metadata')
            
            # Очистка таблиц данных
            for table in variable_tables:
                await conn.execute(f'TRUNCATE TABLE "{table["table_name"]}"')
                self.logger.info(f"Cleared table {table['table_name']}")
            
            for table in event_tables:
                await conn.execute(f'TRUNCATE TABLE "{table["table_name"]}"')
                self.logger.info(f"Cleared table {table['table_name']}")
            
            # Очистка метаданных (но не удаляем структуру)
            await conn.execute('DELETE FROM variable_metadata')
            await conn.execute('DELETE FROM event_type_metadata')
            
            await conn.close()
            
            self.logger.info("All data cleared from database")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to clear all data: {e}")
            return False
    
    async def get_database_info(self) -> Dict[str, Any]:
        """
        Получение информации о базе данных.
        
        Returns:
            Словарь с информацией о БД
        """
        if not self.config:
            return {}
        
        try:
            conn_params = self._get_connection_params()
            conn = await asyncpg.connect(**conn_params)
            
            # Размер базы данных
            db_size = await conn.fetchval('''
                SELECT pg_size_pretty(pg_database_size($1))
            ''', self.config['database'])
            
            # Количество таблиц
            table_count = await conn.fetchval('''
                SELECT COUNT(*) FROM variable_metadata
            ''')
            
            event_table_count = await conn.fetchval('''
                SELECT COUNT(*) FROM event_type_metadata
            ''')
            
            # Текущая версия схемы
            schema_version = await conn.fetchval('''
                SELECT version FROM schema_version ORDER BY applied_at DESC LIMIT 1
            ''')
            
            # Общий размер данных
            total_data_size = await conn.fetchval('''
                SELECT COALESCE(SUM(pg_total_relation_size(table_name::regclass)), 0)
                FROM variable_metadata
            ''')
            
            await conn.close()
            
            return {
                'database_name': self.config['database'],
                'host': self.config['host'],
                'port': self.config['port'],
                'user': self.config['user'],
                'schema_version': schema_version,
                'database_size': db_size,
                'variable_tables': table_count,
                'event_tables': event_table_count,
                'total_data_size_bytes': total_data_size,
                'created_at': self.config.get('created_at'),
                'version': self.config.get('version')
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get database info: {e}")
            return {}
    
    def change_master_password(self, new_master_password: str) -> bool:
        """
        Изменение главного пароля.
        
        Args:
            new_master_password: Новый главный пароль
            
        Returns:
            True если изменение успешно
        """
        try:
            # Создание нового ключа
            salt = os.urandom(16)
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=salt,
                iterations=100000,
            )
            new_key = base64.urlsafe_b64encode(kdf.derive(new_master_password.encode()))
            
            # Перешифрование конфигурации
            if self.config:
                new_cipher = Fernet(new_key)
                encrypted_data = new_cipher.encrypt(json.dumps(self.config, ensure_ascii=False).encode())
                
                # Сохранение нового ключа
                with open(self.key_file, 'wb') as f:
                    f.write(salt + new_key)
                
                # Сохранение перешифрованной конфигурации
                with open(self.config_file, 'wb') as f:
                    f.write(encrypted_data)
                
                self.key = salt + new_key
                self.cipher = new_cipher
                self.master_password = new_master_password
                
                self.logger.info("Master password changed successfully")
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to change master password: {e}")
            return False
    
    def export_config(self, export_path: str) -> bool:
        """
        Экспорт конфигурации в открытом виде.
        
        Args:
            export_path: Путь для сохранения конфигурации
            
        Returns:
            True если экспорт успешен
        """
        try:
            if self.config:
                with open(export_path, 'w', encoding='utf-8') as f:
                    json.dump(self.config, f, indent=2, ensure_ascii=False)
                
                self.logger.info(f"Configuration exported to {export_path}")
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to export configuration: {e}")
            return False
    
    def import_config(self, import_path: str) -> bool:
        """
        Импорт конфигурации из файла.
        
        Args:
            import_path: Путь к файлу конфигурации
            
        Returns:
            True если импорт успешен
        """
        try:
            with open(import_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Шифрование и сохранение
            self._save_config(config)
            
            self.logger.info(f"Configuration imported from {import_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to import configuration: {e}")
            return False


# Утилитарные функции для работы без создания экземпляра класса
async def create_database_standalone(
    user: str,
    password: str,
    database: str,
    host: str = "localhost",
    port: int = 5432,
    superuser: str = "postgres",
    superuser_password: str = None,
    master_password: str = "default_master_password"
) -> bool:
    """
    Создание базы данных без создания экземпляра DatabaseManager.
    
    Args:
        user: Имя пользователя для создания
        password: Пароль пользователя
        database: Имя базы данных
        host: Хост PostgreSQL
        port: Порт PostgreSQL
        superuser: Суперпользователь для создания БД
        superuser_password: Пароль суперпользователя
        master_password: Главный пароль для шифрования
        
    Returns:
        True если успешно создано
    """
    manager = DatabaseManager(master_password)
    return await manager.create_database(
        user, password, database, host, port, superuser, superuser_password
    )


async def backup_database_standalone(
    user: str,
    password: str,
    database: str,
    host: str = "localhost",
    port: int = 5432,
    backup_path: str = None,
    master_password: str = "default_master_password"
) -> Optional[str]:
    """
    Создание резервной копии без создания экземпляра DatabaseManager.
    
    Args:
        user: Имя пользователя
        password: Пароль пользователя
        database: Имя базы данных
        host: Хост PostgreSQL
        port: Порт PostgreSQL
        backup_path: Путь для сохранения бэкапа
        master_password: Главный пароль для шифрования
        
    Returns:
        Путь к созданному бэкапу или None при ошибке
    """
    manager = DatabaseManager(master_password)
    # Установка конфигурации
    manager.config = {
        'user': user,
        'password': password,
        'database': database,
        'host': host,
        'port': port
    }
    return await manager.backup_database(backup_path)
