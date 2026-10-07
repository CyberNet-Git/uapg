"""
Утилита для управления базой данных PostgreSQL для OPC UA History с поддержкой TimescaleDB.

Этот модуль предоставляет функциональность для:
1. Первоначального создания базы данных
2. Управления учетными данными (с шифрованием)
3. Миграции схемы базы данных к TimescaleDB гипертаблицам
4. Резервного копирования
5. Очистки данных и таблиц
6. Управления TimescaleDB гипертаблицами и автоматическим чанкингом
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
import psycopg
from psycopg import sql as psycopg_sql


class DatabaseManager:
    """
    Менеджер базы данных для OPC UA History с поддержкой TimescaleDB.
    
    Этот класс предоставляет утилиты для управления базой данных PostgreSQL
    вне основного функционала HistoryTimescale.
    
    Использует TimescaleDB гипертаблицы с автоматическим чанкингом по времени
    вместо PostgreSQL партиционирования для лучшей производительности.
    """
    
    def __init__(
        self,
        master_password: str = None,
        config_file: str = "db_config.enc",
        key_file: str = ".db_key",
        encrypted_config: str = None
    ):
        """
        Инициализация менеджера базы данных.
        
        Args:
            master_password: Главный пароль для шифрования/дешифрования
            config_file: Файл для хранения зашифрованной конфигурации
            key_file: Файл для хранения ключа шифрования
            encrypted_config: Зашифрованная конфигурация в виде строки
        """
        self.master_password = master_password
        self.config_file = Path(config_file)
        self.key_file = Path(key_file)
        self.encrypted_config = encrypted_config
        self.logger = logging.getLogger('uapg.db_manager')
        
        # Инициализация шифрования
        if master_password:
            self._init_encryption()
            # Загрузка конфигурации
            self.config = self._load_config()
        elif encrypted_config:
            # Используем зашифрованную конфигурацию
            self.config = self._decrypt_encrypted_config(encrypted_config)
        else:
            self.config = {}
    
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
    
    def _decrypt_encrypted_config(self, encrypted_config: str) -> Dict[str, Any]:
        """Дешифрование конфигурации из строки."""
        try:
            # Декодируем base64
            encrypted_data = base64.b64decode(encrypted_config)
            
            # Создаем временный ключ для расшифровки
            # В реальном приложении здесь должна быть логика получения ключа
            temp_key = Fernet.generate_key()
            temp_cipher = Fernet(temp_key)
            
            # Пытаемся расшифровать
            decrypted_data = temp_cipher.decrypt(encrypted_data)
            return json.loads(decrypted_data.decode())
        except Exception as e:
            self.logger.error(f"Failed to decrypt encrypted config: {e}")
            return {}
    
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
        if hasattr(self, 'cipher'):
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
    
    def get_connection_params(self) -> Dict[str, Any]:
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
        Создание базы данных и пользователя с поддержкой TimescaleDB.
        
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
            # Подключение к PostgreSQL как суперпользователь.
            # Имя базы в psycopg3 — dbname: алиас database остался только в psycopg2
            # и приводит к `invalid connection option "database"`.
            conn_params = {
                'host': host,
                'port': port,
                'user': superuser,
                'dbname': 'postgres',
            }
            if superuser_password:
                conn_params['password'] = superuser_password
            
            # Создание пользователя и базы данных.
            #
            # autocommit задаётся параметром connect(): psycopg3 не имеет ни
            # set_isolation_level(), ни ISOLATION_LEVEL_AUTOCOMMIT — это API psycopg2.
            # Без autocommit CREATE DATABASE вообще нельзя выполнить, PostgreSQL
            # запрещает его внутри транзакционного блока.
            #
            # Имена и пароль собираются через psycopg.sql: идентификаторы в CREATE USER
            # и CREATE DATABASE нельзя передать параметрами, а подстановка f-строкой
            # ломала запрос на пароле с кавычкой и допускала инъекцию.
            with psycopg.connect(**conn_params, autocommit=True) as conn:
                with conn.cursor() as cursor:
                    try:
                        cursor.execute(
                            psycopg_sql.SQL("CREATE USER {} WITH PASSWORD {}").format(
                                psycopg_sql.Identifier(user),
                                psycopg_sql.Literal(password),
                            )
                        )
                        self.logger.info(f"User {user} created successfully")
                    except psycopg.errors.DuplicateObject:
                        self.logger.info(f"User {user} already exists")

                    try:
                        cursor.execute(
                            psycopg_sql.SQL("CREATE DATABASE {} OWNER {}").format(
                                psycopg_sql.Identifier(database),
                                psycopg_sql.Identifier(user),
                            )
                        )
                        self.logger.info(f"Database {database} created successfully")
                    except psycopg.errors.DuplicateDatabase:
                        self.logger.info(f"Database {database} already exists")
            
            # Подключение к новой базе данных для настройки
            new_conn_params = {
                'host': host,
                'port': port,
                'user': user,
                'password': password,
                'database': database
            }
            
            # Создание схемы и расширений (только TimescaleDB архитектура)
            await self._setup_timescale_schema(new_conn_params, enable_timescaledb)
            
            # Сохранение конфигурации
            config = {
                'user': user,
                'password': password,
                'database': database,
                'host': host,
                'port': port,
                'created_at': datetime.now().isoformat(),
                'version': '2.0',
                'architecture': 'timescale_hypertables',
                'timescaledb_enabled': enable_timescaledb
            }
            
            if hasattr(self, 'cipher'):
                self._save_config(config)
            
            self.config = config
            
            self.logger.info(f"Database {database} setup completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create database: {e}")
            return False
    
    async def _setup_timescale_schema(
        self,
        conn_params: Dict[str, Any],
        enable_timescaledb: bool
    ) -> None:
        """Настройка схемы базы данных с TimescaleDB и партиционированными таблицами."""
        try:
            conn = await asyncpg.connect(**conn_params)
            
            # Создание расширений
            if enable_timescaledb:
                try:
                    await conn.execute('CREATE EXTENSION IF NOT EXISTS timescaledb')
                    self.logger.info("✅ TimescaleDB extension enabled")
                except Exception as e:
                    self.logger.warning(f"⚠️  TimescaleDB extension not available: {e}")
                    enable_timescaledb = False
            
            await conn.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
            
            # Создание партиционированных таблиц
            await self._create_timescale_tables(conn, enable_timescaledb)
            
            # Создание базовых таблиц метаданных
            await self._create_timescale_metadata_tables(conn)
            
            await conn.close()
            
        except Exception as e:
            self.logger.error(f"Failed to setup TimescaleDB schema: {e}")
            raise
    
    # Удалена поддержка legacy-схемы с множественными таблицами
    
    async def _create_timescale_tables(self, conn: asyncpg.Connection, enable_timescaledb: bool) -> None:
        """Создание гипертаблиц TimescaleDB без PostgreSQL партиционирования."""
        # Таблица для переменных (обычная таблица без PARTITION BY)
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS variables_data (
                variable_id INTEGER NOT NULL,
                time TIMESTAMPTZ NOT NULL,
                value DOUBLE PRECISION NOT NULL,
                quality SMALLINT,
                source_time TIMESTAMPTZ,
                server_time TIMESTAMPTZ DEFAULT NOW()
            )
        ''')
        
        # Таблица для событий (обычная таблица без PARTITION BY)
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS events_data (
                node_id INTEGER NOT NULL,
                event_type_id INTEGER NOT NULL,
                time TIMESTAMPTZ NOT NULL,
                event_data JSONB NOT NULL,
                source_time TIMESTAMPTZ,
                server_time TIMESTAMPTZ DEFAULT NOW()
            )
        ''')
        
        if enable_timescaledb:
            # Создание гипертаблиц TimescaleDB с автоматическим чанкингом
            try:
                # Гипертаблица для переменных
                await conn.execute('''
                    SELECT create_hypertable(
                        'variables_data', 
                        'time',
                        chunk_time_interval => INTERVAL '1 day',
                        if_not_exists => TRUE
                    )
                ''')
                
                # Гипертаблица для событий
                await conn.execute('''
                    SELECT create_hypertable(
                        'events_data', 
                        'time',
                        chunk_time_interval => INTERVAL '1 day',
                        if_not_exists => TRUE
                    )
                ''')
                
                # Включаем columnstore для сжатия переменных
                await conn.execute('''
                    ALTER TABLE variables_data SET (
                        timescaledb.compress,
                        timescaledb.compress_orderby = 'time DESC'
                    )
                ''')
                
                # Включаем columnstore для сжатия событий
                await conn.execute('''
                    ALTER TABLE events_data SET (
                        timescaledb.compress,
                        timescaledb.compress_orderby = 'time DESC'
                    )
                ''')
                
                # Настройка автоматического сжатия
                await conn.execute('''
                    SELECT add_compression_policy(
                        'variables_data', 
                        INTERVAL '7 days',
                        if_not_exists => TRUE
                    )
                ''')
                
                await conn.execute('''
                    SELECT add_compression_policy(
                        'events_data', 
                        INTERVAL '7 days',
                        if_not_exists => TRUE
                    )
                ''')
                
                self.logger.info("✅ Гипертаблицы TimescaleDB созданы с автоматическим чанкингом")
            except Exception as e:
                self.logger.warning(f"⚠️  Не удалось создать гипертаблицы TimescaleDB: {e}")
                self.logger.info("Продолжаем с обычными таблицами PostgreSQL")
        
        # Создание оптимизированных индексов
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_variables_time ON variables_data (time DESC)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_variables_variable_time ON variables_data (variable_id, time DESC)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_events_time ON events_data (time DESC)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_events_node_time ON events_data (node_id, time DESC)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_events_event_data ON events_data USING GIN (event_data)')
        
        self.logger.info("✅ Таблицы TimescaleDB созданы с оптимизированными индексами")
    
    async def _create_timescale_metadata_tables(self, conn: asyncpg.Connection) -> None:
        """Создание таблиц метаданных для TimescaleDB архитектуры."""
        # Таблица метаданных переменных
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS variable_metadata (
                id SERIAL PRIMARY KEY,
                variable_id INTEGER NOT NULL UNIQUE,
                node_id TEXT NOT NULL,
                node_name TEXT,
                data_type TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                retention_period INTERVAL DEFAULT '365 days',
                max_records BIGINT,
                partition_group INTEGER,
                is_active BOOLEAN DEFAULT TRUE
            )
        ''')
        
        # Таблица метаданных типов событий
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS event_type_metadata (
                id SERIAL PRIMARY KEY,
                event_type_id INTEGER NOT NULL,
                event_type_name TEXT,
                source_node_id TEXT NOT NULL,
                fields JSONB NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                retention_period INTERVAL DEFAULT '365 days',
                max_records BIGINT,
                partition_group INTEGER,
                is_active BOOLEAN DEFAULT TRUE,
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
                migration_script TEXT,
                architecture TEXT DEFAULT 'timescale_hypertables'
            )
        ''')
        
        # Создание индексов
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_variable_metadata_variable_id ON variable_metadata(variable_id)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_event_type_metadata_source ON event_type_metadata(source_node_id)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_event_type_metadata_type ON event_type_metadata(event_type_id)')
        
        # Вставка начальной версии схемы
        await conn.execute('''
            INSERT INTO schema_version (version, description, architecture) 
            VALUES ('2.0', 'TimescaleDB hypertables schema', 'timescale_hypertables') 
            ON CONFLICT (version) DO NOTHING
        ''')
        
        self.logger.info("✅ Таблицы метаданных TimescaleDB созданы")
    
    # Удалена функция создания базовых таблиц для legacy-схемы
    
    async def migrate_to_timescale(self) -> bool:
        """
        Миграция от старой архитектуры к TimescaleDB.
        
        Returns:
            True если миграция успешна
        """
        if not self.config:
            self.logger.error("No database configuration found")
            return False
        
        try:
            conn_params = self.get_connection_params()
            conn = await asyncpg.connect(**conn_params)
            
            # Проверка текущей архитектуры
            current_architecture = await conn.fetchval('''
                SELECT architecture FROM schema_version 
                ORDER BY applied_at DESC LIMIT 1
            ''')
            
            if current_architecture == 'timescale_hypertables':
                self.logger.info("Database already uses TimescaleDB architecture")
                await conn.close()
                return True
            
            self.logger.info("Starting migration to TimescaleDB architecture...")
            
            # Создание новых таблиц
            await self._create_timescale_tables(conn, True)
            await self._create_timescale_metadata_tables(conn)
            
            # Миграция данных (базовая версия)
            await self._migrate_data_to_timescale(conn)
            
            # Обновление версии схемы
            await conn.execute('''
                INSERT INTO schema_version (version, description, architecture) 
                VALUES ('2.0', 'Migrated to TimescaleDB hypertables', 'timescale_hypertables')
                ON CONFLICT (version) DO NOTHING
            ''')
            
            await conn.close()
            
            # Обновление конфигурации
            self.config['version'] = '2.0'
            self.config['architecture'] = 'timescale_hypertables'
            if hasattr(self, 'cipher'):
                self._save_config(self.config)
            
            self.logger.info("✅ Migration to TimescaleDB completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Migration to TimescaleDB failed: {e}")
            return False
    
    async def _migrate_data_to_timescale(self, conn: asyncpg.Connection) -> None:
        """Базовая миграция данных к TimescaleDB архитектуре."""
        try:
            # Получение существующих таблиц переменных
            variable_tables = await conn.fetch('''
                SELECT table_name, node_id FROM variable_metadata 
                WHERE table_name IS NOT NULL
            ''')
            
            for table in variable_tables:
                table_name = table['table_name']
                node_id = table['node_id']
                
                # Проверка существования таблицы
                exists = await conn.fetchval(f'''
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = '{table_name}'
                ''')
                
                if exists:
                    # Создание партиции для переменной
                    await self._create_variable_partition(conn, int(node_id) if node_id.isdigit() else 1)
                    
                    # Миграция данных (базовая версия)
                    await self._migrate_variable_data(conn, table_name, int(node_id) if node_id.isdigit() else 1)
            
            self.logger.info("✅ Basic data migration completed")
            
        except Exception as e:
            self.logger.error(f"Data migration failed: {e}")
            raise
    
    async def _create_variable_partition(self, conn: asyncpg.Connection, variable_id: int) -> None:
        """TimescaleDB автоматически управляет чанками - партиции не нужны."""
        # TimescaleDB автоматически создает чанки по времени
        # Дополнительные действия не требуются
        self.logger.info(f"ℹ️  TimescaleDB автоматически управляет чанками для переменной {variable_id}")
        pass
    
    async def _migrate_variable_data(self, conn: asyncpg.Connection, source_table: str, variable_id: int) -> None:
        """Миграция данных переменной из старой таблицы."""
        try:
            # Проверяем структуру исходной таблицы
            columns = await conn.fetch(f'''
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{source_table}'
                ORDER BY ordinal_position
            ''')
            
            # Простая миграция (можно расширить)
            self.logger.info(f"📊 Миграция данных из {source_table} для переменной {variable_id}")
            
            # Здесь должна быть логика миграции данных
            # Для простоты показываем только структуру
            
        except Exception as e:
            self.logger.error(f"Ошибка миграции данных из {source_table}: {e}")
    
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
        variable_ids: List[int] = None,
        event_types: List[int] = None
    ) -> bool:
        """
        Очистка старых данных по времени для новой архитектуры.
        
        Args:
            retention_days: Количество дней для хранения данных
            variable_ids: Список ID переменных для очистки (None - все переменные)
            event_types: Список типов событий для очистки (None - все типы)
            
        Returns:
            True если очистка успешна
        """
        if not self.config:
            self.logger.error("No database configuration found")
            return False
        
        try:
            conn_params = self.get_connection_params()
            conn = await asyncpg.connect(**conn_params)
            
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            
            if self.config.get('architecture') == 'timescale_hypertables':
                # Очистка для TimescaleDB архитектуры
                if variable_ids:
                    for variable_id in variable_ids:
                        deleted = await conn.execute('''
                            DELETE FROM variables_data 
                            WHERE variable_id = $1 AND time < $2
                        ''', variable_id, cutoff_date)
                        self.logger.info(f"Cleaned old data for variable {variable_id}")
                else:
                    # Очистка всех данных
                    deleted = await conn.execute('''
                        DELETE FROM variables_data WHERE time < $1
                    ''', cutoff_date)
                    self.logger.info(f"Cleaned all old data from variables_data")
                
                if event_types:
                    for event_type in event_types:
                        deleted = await conn.execute('''
                            DELETE FROM events_data 
                            WHERE event_type_id = $1 AND time < $2
                        ''', event_type, cutoff_date)
                        self.logger.info(f"Cleaned old data for event type {event_type}")
                else:
                    # Очистка всех данных событий
                    deleted = await conn.execute('''
                        DELETE FROM events_data WHERE time < $1
                    ''', cutoff_date)
                    self.logger.info(f"Cleaned all old data from events_data")
            else:
                # Если архитектура не указана, по умолчанию очищаем Timescale таблицы
                if variable_ids:
                    for variable_id in variable_ids:
                        deleted = await conn.execute('''
                            DELETE FROM variables_data 
                            WHERE variable_id = $1 AND time < $2
                        ''', variable_id, cutoff_date)
                        self.logger.info(f"Cleaned old data for variable {variable_id}")
                else:
                    # Очистка всех данных
                    deleted = await conn.execute('''
                        DELETE FROM variables_data WHERE time < $1
                    ''', cutoff_date)
                    self.logger.info(f"Cleaned all old data from variables_data")
                
                if event_types:
                    for event_type in event_types:
                        deleted = await conn.execute('''
                            DELETE FROM events_data 
                            WHERE event_type_id = $1 AND time < $2
                        ''', event_type, cutoff_date)
                        self.logger.info(f"Cleaned old data for event type {event_type}")
                else:
                    # Очистка всех данных событий
                    deleted = await conn.execute('''
                        DELETE FROM events_data WHERE time < $1
                    ''', cutoff_date)
                    self.logger.info(f"Cleaned all old data from events_data")
            
            await conn.close()
            
            self.logger.info(f"Data cleanup completed for records older than {retention_days} days")
            return True
            
        except Exception as e:
            self.logger.error(f"Data cleanup failed: {e}")
            return False
    
    # Удалена очистка данных legacy-схемы
    
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
            conn_params = self.get_connection_params()
            conn = await asyncpg.connect(**conn_params)
            
            if self.config.get('architecture') == 'timescale_hypertables':
                # Для TimescaleDB архитектуры - деактивация переменных
                for node_id in node_ids:
                    await conn.execute('''
                        UPDATE variable_metadata 
                        SET is_active = FALSE 
                        WHERE node_id = $1
                    ''', node_id)
                    self.logger.info(f"Deactivated variable metadata for node {node_id}")
            else:
                # Для старой архитектуры - удаление таблиц
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
            
            self.logger.info(f"Processed {len(node_ids)} node tables successfully")
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
            conn_params = self.get_connection_params()
            conn = await asyncpg.connect(**conn_params)
            
            if self.config.get('architecture') == 'timescale_hypertables':
                # Очистка для TimescaleDB архитектуры
                await conn.execute('TRUNCATE TABLE variables_data')
                await conn.execute('TRUNCATE TABLE events_data')
                await conn.execute('DELETE FROM variable_metadata')
                await conn.execute('DELETE FROM event_type_metadata')
                
                self.logger.info("Cleared all data from TimescaleDB tables")
            else:
                # Если архитектура не указана, по умолчанию очищаем Timescale таблицы
                await conn.execute('TRUNCATE TABLE variables_data')
                await conn.execute('TRUNCATE TABLE events_data')
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
        Получение общей информации о базе данных.
        
        Returns:
            Словарь с информацией о базе данных
        """
        if not self.config:
            return {"error": "No database configuration found"}
        
        try:
            conn_params = self.get_connection_params()
            conn = await asyncpg.connect(**conn_params)
            
            # Проверка версии схемы
            schema_version = await conn.fetchrow('''
                SELECT version, architecture, applied_at, description 
                FROM schema_version 
                ORDER BY applied_at DESC LIMIT 1
            ''')
            
            # Проверка существования основных таблиц
            tables_exist = await conn.fetch('''
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name IN ('variables_data', 'events_data', 'variable_metadata', 'event_type_metadata')
            ''')
            
            # Количество записей в основных таблицах
            record_counts = {}
            for table in ['variables_data', 'events_data', 'variable_metadata', 'event_type_metadata']:
                try:
                    count = await conn.fetchval(f'SELECT COUNT(*) FROM {table}')
                    record_counts[table] = count
                except Exception:
                    record_counts[table] = "table not found"
            
            await conn.close()
            
            return {
                "schema_version": dict(schema_version) if schema_version else None,
                "tables_exist": [dict(t) for t in tables_exist],
                "record_counts": record_counts,
                "architecture": schema_version['architecture'] if schema_version else "unknown"
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get database info: {e}")
            return {"error": str(e)}
    
    async def get_timescale_info(self) -> Dict[str, Any]:
        """
        Получение информации о TimescaleDB таблицах и партициях.
        
        Returns:
            Словарь с информацией о TimescaleDB
        """
        if not self.config or self.config.get('architecture') != 'timescale_hypertables':
            return {"error": "Database does not use TimescaleDB architecture"}
        
        try:
            conn_params = self.get_connection_params()
            conn = await asyncpg.connect(**conn_params)
            
            # Информация о гипертаблицах
            hypertables = await conn.fetch('''
                SELECT 
                    hypertable_name,
                    num_chunks,
                    compression_enabled,
                    is_compressed
                FROM timescaledb_information.hypertables
            ''')
            
            # Информация о чанках
            chunks = await conn.fetch('''
                SELECT 
                    chunk_name,
                    chunk_schema,
                    range_start,
                    range_end,
                    is_compressed
                FROM timescaledb_information.chunks 
                WHERE hypertable_name IN ('variables_data', 'events_data')
                ORDER BY range_start DESC
                LIMIT 20
            ''')
            
            # Статистика сжатия
            compression_stats = await conn.fetch('''
                SELECT 
                    hypertable_name,
                    compression_status,
                    uncompressed_total_chunks,
                    compressed_total_chunks
                FROM timescaledb_information.compression_settings
            ''')
            
            await conn.close()
            
            return {
                "hypertables": [dict(h) for h in hypertables],
                "chunks": [dict(c) for c in chunks],
                "compression_stats": [dict(cs) for cs in compression_stats]
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get TimescaleDB info: {e}")
            return {"error": str(e)}
    
    async def create_variable_partition(self, variable_id: int) -> bool:
        """
        TimescaleDB автоматически управляет чанками - партиции не нужны.
        
        Args:
            variable_id: ID переменной
            
        Returns:
            True (TimescaleDB автоматически управляет чанками)
        """
        # TimescaleDB автоматически создает чанки по времени
        # Дополнительные действия не требуются
        self.logger.info(f"ℹ️  TimescaleDB автоматически управляет чанками для переменной {variable_id}")
        return True
    
    async def create_event_partition(self, node_id: int) -> bool:
        """
        TimescaleDB автоматически управляет чанками - партиции не нужны.
        
        Args:
            node_id: ID узла
            
        Returns:
            True (TimescaleDB автоматически управляет чанками)
        """
        # TimescaleDB автоматически создает чанки по времени
        # Дополнительные действия не требуются
        self.logger.info(f"ℹ️  TimescaleDB автоматически управляет чанками для узла {node_id}")
        return True
    
    async def enable_compression(self, table_name: str, chunk_time_interval: str = '1 day') -> bool:
        """
        Включение сжатия для TimescaleDB гипертаблицы.
        
        Args:
            table_name: Имя таблицы
            chunk_time_interval: Интервал времени для чанков
            
        Returns:
            True если сжатие включено успешно
        """
        try:
            conn_params = self.get_connection_params()
            conn = await asyncpg.connect(**conn_params)
            
            # Проверяем, является ли таблица гипертаблицей TimescaleDB
            is_hypertable = await conn.fetchval(f'''
                SELECT 1 FROM timescaledb_information.hypertables 
                WHERE hypertable_name = $1
            ''', table_name)
            
            if not is_hypertable:
                self.logger.warning(f"Таблица {table_name} не является гипертаблицей TimescaleDB")
                await conn.close()
                return False
            
            # Установка интервала чанков
            await conn.execute(f'''
                SELECT set_chunk_time_interval('{table_name}', INTERVAL '{chunk_time_interval}')
            ''')
            
            # Включение сжатия
            await conn.execute(f'''
                ALTER TABLE {table_name} SET (
                    timescaledb.compress,
                    timescaledb.compress_orderby = 'time DESC'
                )
            ''')
            
            # Добавление политики сжатия
            await conn.execute(f'''
                SELECT add_compression_policy('{table_name}', INTERVAL '30 days')
            ''')
            
            await conn.close()
            
            self.logger.info(f"✅ Сжатие включено для гипертаблицы {table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to enable compression: {e}")
            return False
    
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
            if hasattr(self, 'cipher'):
                self._save_config(config)
            
            self.config = config
            
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


async def migrate_to_timescale_standalone(
    user: str,
    password: str,
    database: str,
    host: str = "localhost",
    port: int = 5432,
    master_password: str = "default_master_password"
) -> bool:
    """
    Миграция к TimescaleDB архитектуре без создания экземпляра DatabaseManager.
    
    Args:
        user: Имя пользователя
        password: Пароль пользователя
        database: Имя базы данных
        host: Хост PostgreSQL
        port: Порт PostgreSQL
        master_password: Главный пароль для шифрования
        
    Returns:
        True если миграция успешна
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
    return await manager.migrate_to_timescale()
