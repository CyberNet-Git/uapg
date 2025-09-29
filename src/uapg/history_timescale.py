"""
Модуль историзации OPC UA с использованием TimescaleDB

Новая архитектура: единые таблицы для всех переменных и событий вместо
создания отдельных таблиц для каждой переменной. Данные размещаются в
настраиваемой схеме с поддержкой TimescaleDB для временных рядов.
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, List, Optional, Tuple, Union

import asyncpg
from asyncua import ua
from asyncua.server.history import HistoryStorageInterface
from asyncua.ua.ua_binary import variant_from_binary, variant_to_binary

# Импорт для работы с зашифрованной конфигурацией
from .db_manager import DatabaseManager

# Правильный буфер для побайтного чтения в variant_from_binary
class Buffer:
    def __init__(self, data: bytes) -> None:
        self._data = data
        self._pos = 0
    
    def read(self, n: int) -> bytes:
        chunk = self._data[self._pos:self._pos + n]
        self._pos += n
        return chunk
    
    def copy(self, *args, **kwargs) -> 'Buffer':
        return Buffer(self._data[self._pos:])
    
    def skip(self, n: int) -> None:
        self._pos += n

# Импорт для работы с событиями
from asyncua.common.events import Event

try:
    from asyncua.server.history import get_event_properties_from_type_node
except ImportError:
    # Fallback для старых версий asyncua
    async def get_event_properties_from_type_node(event_type):
        """Получение свойств события из типа узла"""
        return []

def validate_table_name(name: str) -> None:
    """
    Валидация имени таблицы для предотвращения SQL инъекций.
    
    Args:
        name: Имя таблицы для проверки
        
    Raises:
        ValueError: Если имя таблицы содержит недопустимые символы
    """
    import re
    if not re.match(r'^[\w\-]+$', name):
        raise ValueError(f"Invalid table name: {name}")

class HistoryTimescale(HistoryStorageInterface):
    """
    Backend для хранения исторических данных OPC UA в PostgreSQL с TimescaleDB.
    
    Новая архитектура использует единые таблицы:
    - variables_history: для всех переменных
    - events_history: для всех событий
    - variable_metadata: метаданные переменных
    - event_sources: источники событий (с периодом хранения)
    - event_types: типы событий (с расширенными полями)
    
    Особенности:
    - Единая таблица для всех переменных с полем variable_id
    - Единая таблица для всех событий с полями source_id и event_type_id
    - Настраиваемая схема (по умолчанию 'public')
    - TimescaleDB hypertables для оптимизации временных рядов
    - Дополнительное партиционирование по source_id (TimescaleDB 2+)
    - Период хранения и max_records устанавливается для источника событий
    
    Attributes:
        max_history_data_response_size (int): Максимальный размер ответа с историческими данными
        logger (logging.Logger): Логгер для записи событий
        _datachanges_period (dict): Словарь периодов хранения данных по узлам
        _conn_params (dict): Параметры подключения к базе данных
        _event_fields (dict): Словарь полей событий по источникам
        _pool (asyncpg.Pool): Пул соединений с базой данных
        _min_size (int): Минимальное количество соединений в пуле
        _max_size (int): Максимальное количество соединений в пуле
        _initialized (bool): Флаг инициализации таблиц
        _schema (str): Имя схемы для размещения таблиц истории
    """

    def _format_node_id(self, node_id: ua.NodeId) -> str:
        """
        Формирует стандартное имя узла OPC UA в формате ns=X;t=Y.
        
        Args:
            node_id: OPC UA NodeId или совместимый объект (Node, Variant)
            
        Returns:
            str: Строка в формате "ns=X;t=Y"
        """
        # Приведение к ua.NodeId при необходимости
        try:
            # Случай: asyncua Node
            if hasattr(node_id, 'nodeid'):
                node_id = node_id.nodeid
            # Случай: Variant с Value=NodeId
            if hasattr(node_id, 'Value') and isinstance(node_id.Value, ua.NodeId):
                node_id = node_id.Value
        except Exception:
            pass
        
        # Если после приведения это строка вида ns=..;t=.. — вернуть как есть
        if isinstance(node_id, str) and node_id.startswith('ns=') and ';' in node_id:
            return node_id
        
        # Если это уже ua.NodeId — собрать каноническую строку
        try:
            node_id_type_map = {
                ua.NodeIdType.TwoByte: 'i',
                ua.NodeIdType.FourByte: 'i', 
                ua.NodeIdType.Numeric: 'i',
                ua.NodeIdType.String: 's',
                ua.NodeIdType.Guid: 'g',
                ua.NodeIdType.ByteString: 'b'
            }
            type_key = getattr(node_id, 'NodeIdType', None)
            ns = getattr(node_id, 'NamespaceIndex', None)
            ident = getattr(node_id, 'Identifier', None)
            if type_key is not None and ns is not None and ident is not None:
                tchar = node_id_type_map.get(type_key, 'x')
                return f"ns={ns};{tchar}={ident}"
        except Exception:
            pass
        
        # Фоллбэк: строковое представление
        return str(node_id)

    def _normalize_event_type_name(self, name: str) -> str:
        """
        Нормализует имя типа события: убирает префикс вида 'ns=..;s=' если он присутствует.
        """
        if name.startswith('ns=') and ';s=' in name:
            try:
                return name.split(';s=', 1)[1]
            except Exception:
                return name
        return name

    def _get_node_data_type(self, node_id: ua.NodeId, datavalue: Optional[ua.DataValue] = None) -> str:
        """
        Определяет тип данных переменной на основе DataValue или контекста.
        
        Args:
            node_id: OPC UA NodeId переменной
            datavalue: DataValue переменной (опционально)
            
        Returns:
            str: Строковое представление типа данных переменной
        """
        # Если передан DataValue, определяем тип по нему
        if datavalue and hasattr(datavalue, 'Value') and datavalue.Value is not None:
            variant_type = datavalue.Value.VariantType
            if variant_type:
                # Маппинг типов OPC UA VariantType на читаемые названия
                variant_type_map = {
                    ua.VariantType.Boolean: 'Boolean',
                    ua.VariantType.SByte: 'SByte',
                    ua.VariantType.Byte: 'Byte',
                    ua.VariantType.Int16: 'Int16',
                    ua.VariantType.UInt16: 'UInt16',
                    ua.VariantType.Int32: 'Int32',
                    ua.VariantType.UInt32: 'UInt32',
                    ua.VariantType.Int64: 'Int64',
                    ua.VariantType.UInt64: 'UInt64',
                    ua.VariantType.Float: 'Float',
                    ua.VariantType.Double: 'Double',
                    ua.VariantType.String: 'String',
                    ua.VariantType.DateTime: 'DateTime',
                    ua.VariantType.Guid: 'Guid',
                    ua.VariantType.ByteString: 'ByteString',
                    ua.VariantType.XmlElement: 'XmlElement',
                    ua.VariantType.NodeId: 'NodeId',
                    ua.VariantType.ExpandedNodeId: 'ExpandedNodeId',
                    ua.VariantType.StatusCode: 'StatusCode',
                    ua.VariantType.QualifiedName: 'QualifiedName',
                    ua.VariantType.LocalizedText: 'LocalizedText',
                    ua.VariantType.ExtensionObject: 'ExtensionObject',
                    ua.VariantType.DataValue: 'DataValue',
                    ua.VariantType.Variant: 'Variant',
                    ua.VariantType.DiagnosticInfo: 'DiagnosticInfo',
                }
                return variant_type_map.get(variant_type, str(variant_type))
        
        # Если DataValue не передан, пытаемся определить по контексту NodeId
        # Это может быть полезно для предварительной регистрации переменных
        # Например, если знаем, что переменная с определенным NodeId всегда Double
        
        # Маппинг известных переменных по их NodeId
        known_variables = {
            # Пример: если знаем, что переменная с NodeId i=2 всегда Double
            # Можно расширить этот маппинг на основе специфики приложения
        }
        
        # Формируем ключ для поиска
        node_key = f"ns={node_id.NamespaceIndex};i={node_id.Identifier}"
        
        # Возвращаем известный тип или "Unknown" если не определен
        return known_variables.get(node_key, "Unknown")

    def __init__(
        self, 
        user: str = 'postgres', 
        password: str = 'postmaster', 
        database: str = 'opcua', 
        host: str = 'localhost', 
        port: int = 5432,
        min_size: int = 1,
        max_size: int = 10,
        schema: str = 'public',
        sslmode: Optional[str] = None,
        config_file: Optional[str] = None,
        encrypted_config: Optional[str] = None,
        master_password: Optional[str] = None,
        **kwargs
    ) -> None:
        """
        Инициализация HistoryTimescale.
        
        Args:
            user: Имя пользователя базы данных
            password: Пароль пользователя
            database: Имя базы данных
            host: Хост базы данных
            port: Порт базы данных
            min_size: Минимальное количество соединений в пуле
            max_size: Максимальное количество соединений в пуле
            schema: Имя схемы для размещения таблиц истории (по умолчанию 'public')
            sslmode: Режим SSL подключения ('disable', 'require', 'verify-ca', 'verify-full')
            config_file: Путь к файлу зашифрованной конфигурации
            encrypted_config: Зашифрованная конфигурация в виде строки
            master_password: Главный пароль для расшифровки конфигурации
        """
        self.max_history_data_response_size = 1000
        self.logger = logging.getLogger(__name__)
        self._datachanges_period = {}
        self._event_fields = {}
        self._pool = None
        self._min_size = min_size
        self._max_size = max_size
        self._initialized = False
        self._schema = schema
        
        # Инициализация параметров подключения
        self._conn_params = self._init_connection_params(
            user, password, database, host, port,
            sslmode, config_file, encrypted_config, master_password, **kwargs
        )
    
    def _init_connection_params(
        self,
        user: str,
        password: str,
        database: str,
        host: str,
        port: int,
        sslmode: Optional[str] = None,
        config_file: Optional[str] = None,
        encrypted_config: Optional[str] = None,
        master_password: Optional[str] = None,
        **kwargs
    ) -> dict:
        """
        Инициализация параметров подключения с поддержкой зашифрованной конфигурации.
        
        Args:
            user: Имя пользователя базы данных
            password: Пароль пользователя
            database: Имя базы данных
            host: Хост базы данных
            port: Порт базы данных
            sslmode: Режим SSL подключения ('disable', 'require', 'verify-ca', 'verify-full')
            config_file: Путь к файлу зашифрованной конфигурации
            encrypted_config: Зашифрованная конфигурация в виде строки
            master_password: Главный пароль для расшифровки конфигурации
            
        Returns:
            Словарь с параметрами подключения
        """
        # Приоритет: зашифрованная конфигурация > файл конфигурации > прямые параметры
        if encrypted_config and master_password:
            try:
                # Создаем временный DatabaseManager для расшифровки
                temp_manager = DatabaseManager(master_password)
                # Расшифровываем конфигурацию из строки
                decrypted_config = temp_manager._decrypt_config(encrypted_config.encode())
                self.logger.info("Using encrypted configuration from string")
                config_params = {
                    'user': decrypted_config.get('user', user),
                    'password': decrypted_config.get('password', password),
                    'database': decrypted_config.get('database', database),
                    'host': decrypted_config.get('host', host),
                    'port': decrypted_config.get('port', port),
                    'schema': decrypted_config.get('schema', self._schema),
                    'sslmode': decrypted_config.get('sslmode', sslmode)
                }
                # Обновляем схему если она указана в конфигурации
                if 'schema' in decrypted_config:
                    self._schema = decrypted_config['schema']
                # Добавляем дополнительные параметры из kwargs
                config_params.update(kwargs)
                return config_params
            except Exception as e:
                self.logger.warning(f"Failed to decrypt configuration string: {e}, using direct parameters")
        
        elif config_file and master_password:
            try:
                # Создаем DatabaseManager для загрузки конфигурации из файла
                temp_manager = DatabaseManager(master_password, config_file)
                if temp_manager.config:
                    self.logger.info(f"Using configuration from file: {config_file}")
                    config_params = {
                        'user': temp_manager.config.get('user', user),
                        'password': temp_manager.config.get('password', password),
                        'database': temp_manager.config.get('database', database),
                        'host': temp_manager.config.get('host', host),
                        'port': temp_manager.config.get('port', port),
                        'schema': temp_manager.config.get('schema', self._schema),
                        'sslmode': temp_manager.config.get('sslmode', sslmode)
                    }
                    # Обновляем схему если она указана в конфигурации
                    if 'schema' in temp_manager.config:
                        self._schema = temp_manager.config['schema']
                    # Добавляем дополнительные параметры из kwargs
                    config_params.update(kwargs)
                    return config_params
                else:
                    self.logger.warning(f"Configuration file {config_file} is empty or invalid, using direct parameters")
            except Exception as e:
                self.logger.warning(f"Failed to load configuration from file {config_file}: {e}, using direct parameters")
        
        # Используем прямые параметры как fallback
        self.logger.info("Using direct connection parameters")
        base_params = {
            'user': user,
            'password': password,
            'database': database,
            'host': host,
            'port': port,
            'schema': self._schema
        }
        # Добавляем sslmode если он указан
        if sslmode is not None:
            base_params['sslmode'] = sslmode
        # Добавляем дополнительные параметры из kwargs
        base_params.update(kwargs)
        return base_params

    def get_connection_info(self) -> dict:
        """
        Получение информации о текущих параметрах подключения.
        
        Returns:
            Словарь с информацией о подключении
        """
        return {
            'user': self._conn_params['user'],
            'host': self._conn_params['host'],
            'port': self._conn_params['port'],
            'database': self._conn_params['database'],
            'schema': self._schema,
            'min_size': self._min_size,
            'max_size': self._max_size,
            'initialized': self._initialized
        }

    @classmethod
    def from_config_file(
        cls,
        config_file: str,
        master_password: str,
        min_size: int = 1,
        max_size: int = 10
    ) -> 'HistoryTimescale':
        """
        Создание экземпляра из файла зашифрованной конфигурации.
        
        Args:
            config_file: Путь к файлу зашифрованной конфигурации
            master_password: Главный пароль для расшифровки
            min_size: Минимальное количество соединений в пуле
            max_size: Максимальное количество соединений в пуле
            
        Returns:
            Экземпляр HistoryTimescale с загруженной конфигурацией
        """
        return cls(
            config_file=config_file,
            master_password=master_password,
            min_size=min_size,
            max_size=max_size
        )
    
    @classmethod
    def from_encrypted_config(
        cls,
        encrypted_config: str,
        master_password: str,
        min_size: int = 1,
        max_size: int = 10
    ) -> 'HistoryTimescale':
        """
        Создание экземпляра из зашифрованной конфигурации в виде строки.
        
        Args:
            encrypted_config: Зашифрованная конфигурация в виде строки
            master_password: Главный пароль для расшифровки
            min_size: Минимальное количество соединений в пуле
            max_size: Максимальное количество соединений в пуле
            
        Returns:
            Экземпляр HistoryTimescale с расшифрованной конфигурацией
        """
        return cls(
            encrypted_config=encrypted_config,
            master_password=master_password,
            min_size=min_size,
            max_size=max_size
        )

    def update_config(
        self,
        config_file: Optional[str] = None,
        encrypted_config: Optional[str] = None,
        master_password: Optional[str] = None
    ) -> bool:
        """
        Обновление конфигурации подключения.
        
        Args:
            config_file: Путь к файлу зашифрованной конфигурации
            encrypted_config: Зашифрованная конфигурация в виде строки
            master_password: Главный пароль для расшифровки конфигурации
            
        Returns:
            True если конфигурация обновлена успешно
        """
        if self._pool:
            self.logger.warning("Cannot update config while pool is active. Call stop() first.")
            return False
        
        try:
            # Сбрасываем флаг инициализации
            self._initialized = False
            
            # Обновляем параметры подключения
            self._conn_params = self._init_connection_params(
                self._conn_params.get('user', 'postgres'),
                self._conn_params.get('password', 'postmaster'),
                self._conn_params.get('database', 'opcua'),
                self._conn_params.get('host', 'localhost'),
                self._conn_params.get('port', 5432),
                config_file, encrypted_config, master_password
            )
            
            self.logger.info("Configuration updated successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to update configuration: {e}")
            return False

    async def init(self) -> None:
        """Инициализация подключения к базе данных и создание таблиц метаданных."""
        try:
            # Создаем пул соединений, отделяя основные параметры от дополнительных
            pool_params = {
                'user': self._conn_params['user'],
                'password': self._conn_params['password'],
                'database': self._conn_params['database'],
                'host': self._conn_params['host'],
                'port': self._conn_params['port'],
                'min_size': self._min_size,
                'max_size': self._max_size
            }
            
            # Добавляем дополнительные параметры, исключая те, что не поддерживаются asyncpg
            exclude_params = {'user', 'password', 'database', 'host', 'port', 'min_size', 'max_size', 'sslmode', 'schema'}
            for key, value in self._conn_params.items():
                if key not in exclude_params:
                    pool_params[key] = value
            
            # Обрабатываем SSL параметры отдельно
            if self._conn_params.get('sslmode') == 'disable':
                pool_params['ssl'] = False
            elif self._conn_params.get('sslmode') in ('require', 'verify-ca', 'verify-full'):
                pool_params['ssl'] = True
            
            self._pool = await asyncpg.create_pool(**pool_params)
            
            # Создаем таблицы метаданных только один раз
            if not self._initialized:
                await self._create_metadata_tables()
                self._initialized = True
                
            self.logger.info("HistoryTimescale initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize HistoryTimescale: {e}")
            raise

    async def stop(self) -> None:
        """Остановка и закрытие пула соединений."""
        if self._pool:
            await self._pool.close()
            self._pool = None
            self.logger.info("HistoryTimescale stopped")

    async def _execute(self, query: str, *args) -> Any:
        """
        Выполнение SQL запроса.
        
        Args:
            query: SQL запрос
            *args: Аргументы для запроса
            
        Returns:
            Результат выполнения запроса
        """
        async with self._pool.acquire() as conn:
            return await conn.execute(query, *args)

    async def _fetch(self, query: str, *args) -> List[asyncpg.Record]:
        """
        Выполнение SQL запроса с возвратом результатов.
        
        Args:
            query: SQL запрос
            *args: Аргументы для запроса
            
        Returns:
            Список записей
        """
        async with self._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async def _fetchval(self, query: str, *args) -> Any:
        """
        Выполнение SQL запроса с возвратом одного значения.
        
        Args:
            query: SQL запрос
            *args: Аргументы для запроса
            
        Returns:
            Значение
        """
        async with self._pool.acquire() as conn:
            return await conn.fetchval(query, *args)

    async def _create_metadata_tables(self) -> None:
        """Создание единых таблиц для историзации в указанной схеме."""
        try:
            # Создаем схему если она не существует
            await self._execute(f'CREATE SCHEMA IF NOT EXISTS "{self._schema}"')
            
                        # Единая таблица для всех переменных
            await self._execute(f'''
                CREATE TABLE IF NOT EXISTS "{self._schema}".variables_history (
                    id BIGSERIAL,
                    variable_id BIGINT NOT NULL,
                    servertimestamp TIMESTAMPTZ NOT NULL,
                    sourcetimestamp TIMESTAMPTZ NOT NULL,
                    statuscode INTEGER,
                    value TEXT,
                    varianttype INTEGER,
                    variantbinary BYTEA,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            ''')

            # Единая таблица для всех событий
            await self._execute(f'''
                CREATE TABLE IF NOT EXISTS "{self._schema}".events_history (
                    id BIGSERIAL,
                    source_id BIGINT NOT NULL,
                    event_type_id BIGINT NOT NULL,
                    event_timestamp TIMESTAMPTZ NOT NULL,
                    event_data JSONB,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            ''')

            # Таблица метаданных переменных
            await self._execute(f'''
                CREATE TABLE IF NOT EXISTS "{self._schema}".variable_metadata (
                    id BIGSERIAL PRIMARY KEY,
                    variable_id BIGINT GENERATED ALWAYS AS (id) STORED,
                    node_id TEXT NOT NULL,
                    data_type TEXT,
                    retention_period INTERVAL,
                    max_records INTEGER,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(variable_id)
                )
            ''')

            # Таблица источников событий
            await self._execute(f'''
                CREATE TABLE IF NOT EXISTS "{self._schema}".event_sources (
                    id BIGSERIAL PRIMARY KEY,
                    source_id BIGINT GENERATED ALWAYS AS (id) STORED,
                    source_node_id TEXT NOT NULL,
                    retention_period INTERVAL,
                    max_records INTEGER,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(source_id)
                )
            ''')

            # Таблица типов событий
            await self._execute(f'''
                CREATE TABLE IF NOT EXISTS "{self._schema}".event_types (
                    id BIGSERIAL PRIMARY KEY,
                    event_type_id BIGINT GENERATED ALWAYS AS (id) STORED,
                    event_type_name TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(event_type_id)
                )
            ''')
            
            # Создаем индексы для производительности и связей
            # Индексы для таблиц истории (bigint поля для оптимизации)
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_variables_variable_id ON "{self._schema}".variables_history(variable_id)')
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_variables_timestamp ON "{self._schema}".variables_history(sourcetimestamp)')
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_variables_server_timestamp ON "{self._schema}".variables_history(servertimestamp)')
            # Уникальный индекс должен включать столбцы партиционирования TimescaleDB
            await self._execute(f'CREATE UNIQUE INDEX IF NOT EXISTS idx_variables_varid_sourcets ON "{self._schema}".variables_history(variable_id, sourcetimestamp)')

            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_events_source_id ON "{self._schema}".events_history(source_id)')
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_events_event_type_id ON "{self._schema}".events_history(event_type_id)')
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_events_timestamp ON "{self._schema}".events_history(event_timestamp)')
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_events_data_gin ON "{self._schema}".events_history USING GIN (event_data)')
            # Уникальный индекс должен включать столбцы партиционирования TimescaleDB
            await self._execute(f'CREATE UNIQUE INDEX IF NOT EXISTS idx_events_sourceid_eventts ON "{self._schema}".events_history(source_id, event_timestamp)')

            # Индексы для таблиц метаданных (bigint поля)
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_variable_metadata_variable_id ON "{self._schema}".variable_metadata(variable_id)')
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_event_sources_source_id ON "{self._schema}".event_sources(source_id)')
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_event_types_event_type_id ON "{self._schema}".event_types(event_type_id)')
            
            # Уникальные индексы для event_sources
            await self._execute(f'CREATE UNIQUE INDEX IF NOT EXISTS idx_event_sources_node_id ON "{self._schema}".event_sources(source_node_id)')
            
            # Уникальный индекс для event_types по имени типа события
            await self._execute(f'CREATE UNIQUE INDEX IF NOT EXISTS idx_event_types_name ON "{self._schema}".event_types(event_type_name)')
            
            # Уникальный индекс для variable_metadata по node_id
            await self._execute(f'CREATE UNIQUE INDEX IF NOT EXISTS idx_variable_metadata_node_id ON "{self._schema}".variable_metadata(node_id)')

            # Дополнительные индексы для оптимизации связей (bigint поля)
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_variables_history_variable_id_timestamp ON "{self._schema}".variables_history(variable_id, sourcetimestamp)')
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_events_history_source_timestamp ON "{self._schema}".events_history(source_id, event_timestamp)')
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_events_history_type_source ON "{self._schema}".events_history(event_type_id, source_id)')

            # Составной индекс для оптимизации поиска по типу события и источнику
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_events_history_event_type_source ON "{self._schema}".events_history(event_type_id, source_id)')

            # Индексы для каскадных операций удаления
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_variable_metadata_created ON "{self._schema}".variable_metadata(created_at)')
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_event_sources_created ON "{self._schema}".event_sources(created_at)')
            await self._execute(f'CREATE INDEX IF NOT EXISTS idx_event_types_created ON "{self._schema}".event_types(created_at)')
            
            self.logger.info(f"Unified history tables created successfully in schema '{self._schema}'")
            
            # Настраиваем TimescaleDB hypertables после создания всех индексов
            await self._setup_timescale_hypertable(f'{self._schema}.variables_history', 'sourcetimestamp', 'variable_id', 128)
            await self._setup_timescale_hypertable(f'{self._schema}.events_history', 'event_timestamp', 'source_id', 64)
        except Exception as e:
            self.logger.error(f"Failed to create unified history tables: {e}")
            raise

    async def _setup_timescale_hypertable(self, table: str, partition_column: str, space_partition_column: Optional[str] = None, space_partitions: Optional[int] = None) -> None:
        """
        Настройка TimescaleDB hypertable с возможностью дополнительного партиционирования.
        
        Args:
            table: Имя таблицы
            partition_column: Колонка для временного партиционирования
            space_partition_column: Дополнительная колонка для пространственного партиционирования (TimescaleDB 2+)
            space_partitions: Количество партиций для space-измерения (1..32767)
        """
        try:
            # Проверяем, доступно ли расширение TimescaleDB
            extension_check = await self._fetchval("SELECT COUNT(*) FROM pg_extension WHERE extname = 'timescaledb'")
            if extension_check == 0:
                self.logger.warning("TimescaleDB extension not found. Creating regular table without hypertable.")
                return
            
            # Проверяем версию TimescaleDB
            timescale_version = await self._fetchval("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")
            if timescale_version:
                major_version = int(timescale_version.split('.')[0])
                if major_version >= 2 and space_partition_column:
                    # Устанавливаем дефолт для количества партиций, если не задано
                    partitions = space_partitions if (space_partitions and 1 <= space_partitions <= 32767) else 32
                    await self._execute(
                        f"SELECT create_hypertable('{table}', '{partition_column}', partitioning_column => '{space_partition_column}', number_partitions => {partitions}, if_not_exists => TRUE)"
                    )
                    self.logger.info(f"TimescaleDB hypertable created for table {table} with space partitioning on {space_partition_column} (number_partitions={partitions})")
                else:
                    # Стандартное партиционирование только по времени
                    await self._execute(
                        f"SELECT create_hypertable('{table}', '{partition_column}', if_not_exists => TRUE)"
                    )
                    self.logger.info(f"TimescaleDB hypertable created for table {table}")
            else:
                # Fallback для старых версий
                await self._execute(
                    f"SELECT create_hypertable('{table}', '{partition_column}', if_not_exists => TRUE)"
                )
                self.logger.info(f"TimescaleDB hypertable created for table {table}")
        except Exception as e:
            self.logger.warning(f"Failed to create TimescaleDB hypertable for table {table}: {e}")
            self.logger.info("Continuing with regular table (without TimescaleDB optimization)")

    async def _save_variable_metadata(self, node_id: ua.NodeId, period: Optional[timedelta], count: int) -> int:
        """
        Сохранение метаданных переменной.

        Args:
            node_id: Идентификатор узла
            period: Период хранения
            count: Максимальное количество записей

        Returns:
            int: variable_id для использования в таблице истории
        """
        # Сохраняем метаданные переменной (используем INSERT ... RETURNING для получения ID)
        # Создаем полное имя узла для уникальной идентификации
        node_id_str = self._format_node_id(node_id)
        
        # При регистрации переменной тип данных пока неизвестен
        # Будет обновлен при первом сохранении значения
        data_type = "Unknown"
        
        result = await self._fetchval(f'''
            INSERT INTO "{self._schema}".variable_metadata (node_id, data_type, retention_period, max_records)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (node_id) DO UPDATE SET
                data_type = EXCLUDED.data_type,
                retention_period = EXCLUDED.retention_period,
                max_records = EXCLUDED.max_records,
                updated_at = NOW()
            RETURNING variable_id
        ''', node_id_str, data_type, period, count)

        if result is None:
            # Если не удалось вставить, получаем существующий ID
            result = await self._fetchval(f'''
                SELECT variable_id FROM "{self._schema}".variable_metadata
                WHERE node_id = $1
                LIMIT 1
            ''', node_id_str)

        return result

    async def _save_event_source(self, source_id: ua.NodeId, period: Optional[timedelta], count: int) -> int:
        """
        Сохранение источника событий.

        Args:
            source_id: Идентификатор источника событий
            period: Период хранения
            count: Максимальное количество записей

        Returns:
            int: source_id для использования в таблице истории
        """
        source_node_id_str = self._format_node_id(source_id)
        
        result = await self._fetchval(f'''
            INSERT INTO "{self._schema}".event_sources (source_node_id, retention_period, max_records)
            VALUES ($1, $2, $3)
            ON CONFLICT (source_node_id) DO UPDATE SET
                retention_period = EXCLUDED.retention_period,
                max_records = EXCLUDED.max_records,
                updated_at = NOW()
            RETURNING source_id
        ''', source_node_id_str, period, count)

        if result is None:
            # Если не удалось вставить, получаем существующий ID
            result = await self._fetchval(f'''
                SELECT source_id FROM "{self._schema}".event_sources
                WHERE source_node_id = $1
                LIMIT 1
            ''', source_node_id_str)

        return result

    async def _save_event_metadata(self, event_type: ua.NodeId, source_id: ua.NodeId, fields: List[str], period: Optional[timedelta], count: int) -> Tuple[int, int]:
        """
        Сохранение метаданных события.

        Args:
            event_type: Тип события
            source_id: Идентификатор источника
            fields: Список расширенных полей события
            period: Период хранения
            count: Максимальное количество записей

        Returns:
            Tuple[int, int]: (source_id, event_type_id) для использования в таблице истории
        """
        # Сначала создаем или получаем источник событий
        source_db_id = await self._save_event_source(source_id, period, count)
        
        # Теперь создаем запись для типа события
        event_type_name = self._format_node_id(event_type)
        
        event_db_id = await self._fetchval(f'''
            INSERT INTO "{self._schema}".event_types (event_type_name)
            VALUES ($1)
            ON CONFLICT (event_type_name) DO UPDATE SET
                updated_at = NOW()
            RETURNING event_type_id
        ''', event_type_name)

        if event_db_id is None:
            # Если не удалось вставить, получаем существующий ID
            event_db_id = await self._fetchval(f'''
                SELECT event_type_id FROM "{self._schema}".event_types
                WHERE event_type_name = $1
                LIMIT 1
            ''', event_type_name)

        return source_db_id, event_db_id

    def _extract_variant_values(self, event_data: dict) -> dict:
        """
        Извлекает значения из Variant объектов для JSON сериализации.
        Преобразует все несериализуемые типы в сериализуемые.
        
        Args:
            event_data: Словарь с данными события, содержащий Variant объекты
            
        Returns:
            Словарь с извлеченными значениями, готовыми для JSON сериализации
        """
        extracted = {}
        for key, value in event_data.items():
            if hasattr(value, 'Value'):
                # Если это Variant, извлекаем значение и рекурсивно обрабатываем
                extracted[key] = self._make_json_serializable(value.Value)
            else:
                # Если не Variant, обрабатываем значение
                extracted[key] = self._make_json_serializable(value)
        return extracted

    def _make_json_serializable(self, value: Any) -> Any:
        """
        Преобразует значение в JSON-сериализуемый тип.
        
        Args:
            value: Значение для преобразования
            
        Returns:
            JSON-сериализуемое значение
        """
        if value is None:
            return None
        elif isinstance(value, (str, int, float, bool)):
            # Базовые типы уже сериализуемы
            return value
        elif isinstance(value, bytes):
            # Байты преобразуем в base64 строку
            import base64
            return base64.b64encode(value).decode('utf-8')
        elif isinstance(value, (list, tuple)):
            # Списки и кортежи обрабатываем рекурсивно
            return [self._make_json_serializable(item) for item in value]
        elif isinstance(value, dict):
            # Словари обрабатываем рекурсивно
            return {k: self._make_json_serializable(v) for k, v in value.items()}
        elif hasattr(value, '__dict__'):
            # Для объектов с атрибутами пытаемся извлечь основные поля
            try:
                # Пытаемся получить строковое представление
                return str(value)
            except:
                # Если не получается, возвращаем имя типа
                return f"{type(value).__name__}"
        else:
            # Для остальных типов используем строковое представление
            try:
                return str(value)
            except:
                return f"{type(value).__name__}"

    def _event_to_binary_map(self, ev_dict: dict) -> dict:
        import base64
        result = {}
        for key, variant in ev_dict.items():
            try:
                # Диагностика для ExtensionObject
                if hasattr(variant, 'VariantType') and variant.VariantType == ua.VariantType.ExtensionObject:
                    #self.logger.debug(f"_event_to_binary_map: Processing ExtensionObject for key '{key}': {variant.Value}")
                    try:
                        binary_data = variant_to_binary(variant)
                        #self.logger.debug(f"_event_to_binary_map: variant_to_binary success for '{key}', binary length: {len(binary_data)}")
                        result[key] = f"base64:{base64.b64encode(binary_data).decode('utf-8')}"
                    except Exception as e:
                        self.logger.error(f"_event_to_binary_map: variant_to_binary failed for '{key}': {e}")
                        result[key] = None
                else:
                    # Обычная обработка для не-ExtensionObject
                    binary_data = variant_to_binary(variant)
                    result[key] = f"base64:{base64.b64encode(binary_data).decode('utf-8')}"
            except Exception as e:
                self.logger.error(f"_event_to_binary_map: Failed to process key '{key}' with value {variant}: {e}")
                # На всякий случай, если вдруг попадётся не Variant
                try:
                    binary_data = variant_to_binary(ua.Variant(variant))
                    result[key] = f"base64:{base64.b64encode(binary_data).decode('utf-8')}"
                except Exception as e2:
                    self.logger.error(f"_event_to_binary_map: Fallback also failed for '{key}': {e2}")
                    result[key] = None
        return result

    def _binary_map_to_event_values(self, data: dict) -> dict:
        import base64
        result = {}
        for key, b64s in data.items():
            try:
                if b64s is None:
                    self.logger.debug(f"_binary_map_to_event_values: Skipping None value for key '{key}'")
                    result[key] = None
                    continue
                    
                if not isinstance(b64s, str) or not b64s.startswith('base64:'):
                    self.logger.debug(f"_binary_map_to_event_values: Non-base64 value for key '{key}': {type(b64s)} - {b64s}")
                    result[key] = b64s
                    continue
                    
                raw = base64.b64decode(b64s[7:])
                self.logger.debug(f"_binary_map_to_event_values: Decoded binary for key '{key}', length: {len(raw)}")
                
                v = variant_from_binary(Buffer(raw))
                self.logger.debug(f"_binary_map_to_event_values: variant_from_binary success for '{key}': {v}")
                
                # Диагностика для ExtensionObject
                if hasattr(v, 'VariantType') and v.VariantType == ua.VariantType.ExtensionObject:
                    self.logger.debug(f"_binary_map_to_event_values: Recovered ExtensionObject for key '{key}': {v.Value}")
                
                result[key] = v
            except Exception as e:
                self.logger.error(f"_binary_map_to_event_values: Failed to process key '{key}' with value {b64s}: {e}")
                # Фоллбэк: вернуть None
                result[key] = None
        return result

    async def _get_event_fields(self, evtypes: List[ua.NodeId]) -> List[str]:
        """
        Получение полей событий из типов узлов.
        
        Args:
            evtypes: Список типов событий
            
        Returns:
            Список имен полей событий
        """
        ev_aggregate_fields = []
        for event_type in evtypes:
            ev_aggregate_fields.extend(await get_event_properties_from_type_node(event_type))
        ev_fields = []
        for field in set(ev_aggregate_fields):
            ev_fields.append((await field.read_display_name()).Text)
        return ev_fields
    
    async def new_historized_node(
        self,
        node_id: ua.NodeId,
        period: Optional[timedelta],
        count: int = 0
    ) -> None:
        """
        Регистрация нового узла для историзации в единой таблице.
        Таблица уже создана при инициализации.

        Args:
            node_id: Идентификатор узла OPC UA
            period: Период хранения данных (None для бесконечного хранения)
            count: Максимальное количество записей (0 для неограниченного)
        """
        #self.logger.debug("new_historized_node: node_id=%s period=%s count=%s",node_id, period, count,)

        try:
            # Сохраняем метаданные переменной и получаем variable_id
            variable_id = await self._save_variable_metadata(node_id, period, count)

            # Сохраняем mapping node_id -> variable_id для быстрого доступа
            self._datachanges_period[node_id] = (period, count, variable_id)

            #self.logger.info(f"Variable node {node_id} registered for historization in unified table (variable_id: {variable_id})")
        except Exception as e:
            self.logger.error(f"Failed to register variable node {node_id}: {e}")
            raise
    
    async def new_historized_event(
        self,
        source_id: ua.NodeId,
        evtypes: List[ua.NodeId],
        period: Optional[timedelta],
        count: int = 0
    ) -> None:
        """
        Регистрация нового источника событий для историзации в единой таблице.
        Таблица уже создана при инициализации.

        Args:
            source_id: Идентификатор источника событий
            evtypes: Список типов событий
            period: Период хранения данных (None для бесконечного хранения)
            count: Максимальное количество записей (0 для неограниченного)
        """
        self.logger.debug(
            "new_historized_event: source_id=%s evtypes=%s period=%s count=%s",
            source_id, evtypes, period, count,
        )

        try:
            # Получаем поля событий
            ev_fields = await self._get_event_fields(evtypes)
            self._event_fields[source_id] = ev_fields

            # Сохраняем метаданные для каждого типа события и получаем IDs
            event_ids = {}
            for event_type in evtypes:
                source_db_id, event_db_id = await self._save_event_metadata(event_type, source_id, ev_fields, period, count)
                event_ids[event_type] = (source_db_id, event_db_id)

            # Сохраняем mapping source_id -> (period, count, source_db_id, event_ids)
            self._datachanges_period[source_id] = (period, count, source_db_id, event_ids)

            self.logger.info(f"Event source {source_id} registered for historization in unified table (source_id: {source_db_id})")
        except Exception as e:
            self.logger.error(f"Failed to register event source {source_id}: {e}")
            raise
    
    async def save_node_value(self, node_id: ua.NodeId, datavalue: ua.DataValue) -> None:
        """
        Сохранение значения узла в единую таблицу истории переменных.

        Args:
            node_id: Идентификатор узла OPC UA
            datavalue: Значение данных для сохранения
        """
        #self.logger.debug(
        #    "save_node_value: node_id=%s source_ts=%s server_ts=%s status=%s",
        #    node_id, getattr(datavalue, 'SourceTimestamp', None), getattr(datavalue, 'ServerTimestamp', None), getattr(datavalue, 'StatusCode', None),
        #)

        try:
            # Получаем variable_id из mapping
            node_data = self._datachanges_period.get(node_id)
            if node_data is None:
                variable_id = None
            else:
                # Проверяем формат данных
                if len(node_data) == 3:
                    period, count, variable_id = node_data
                elif len(node_data) == 4:
                    # Формат для событий: (period, count, source_db_id, event_ids)
                    self.logger.warning(f"Node {node_id} is registered as event source, not variable")
                    return
                else:
                    self.logger.warning(f"Unexpected data format for node {node_id}: {node_data}")
                    return
                    
            if variable_id is None:
                # Если mapping не найден, получаем variable_id из базы данных
                node_id_str = self._format_node_id(node_id)
                variable_id = await self._fetchval(f'''
                    SELECT variable_id FROM "{self._schema}".variable_metadata
                    WHERE node_id = $1
                    LIMIT 1
                ''', node_id_str)

                if variable_id is None:
                    # Если метаданные не найдены, создаем их
                    variable_id = await self._save_variable_metadata(node_id, None, 0)
                    self._datachanges_period[node_id] = (None, 0, variable_id)

            # Вставка в единую таблицу переменных
            await self._execute(
                f'INSERT INTO "{self._schema}".variables_history (variable_id, servertimestamp, sourcetimestamp, statuscode, value, varianttype, variantbinary) VALUES ($1, $2, $3, $4, $5, $6, $7)',
                variable_id,
                datavalue.ServerTimestamp,
                datavalue.SourceTimestamp,
                datavalue.StatusCode.value,
                str(datavalue.Value.Value),
                int(datavalue.Value.VariantType),
                variant_to_binary(datavalue.Value)
            )

            # Обновляем тип данных в метаданных на основе реального DataValue только при изменении
            if datavalue and hasattr(datavalue, 'Value') and datavalue.Value is not None:
                actual_data_type = self._get_node_data_type(node_id, datavalue)
                if actual_data_type != "Unknown":
                    # Проверяем, изменился ли тип данных
                    current_data_type = await self._fetchval(f'''
                        SELECT data_type FROM "{self._schema}".variable_metadata 
                        WHERE variable_id = $1
                    ''', variable_id)
                    
                    # Обновляем только если тип изменился
                    if current_data_type != actual_data_type:
                        await self._execute(f'''
                            UPDATE "{self._schema}".variable_metadata 
                            SET data_type = $1, updated_at = NOW() 
                            WHERE variable_id = $2
                        ''', actual_data_type, variable_id)

            # Очистка старых данных по периоду хранения
            if period:
                date_limit = datetime.now(timezone.utc) - period
                await self._execute(f'DELETE FROM "{self._schema}".variables_history WHERE variable_id = $1 AND sourcetimestamp < $2', variable_id, date_limit)
            elif count > 0:
                # Удаляем лишние записи по количеству для конкретного узла
                await self._execute(f'''
                    DELETE FROM "{self._schema}".variables_history
                    WHERE variable_id = $1 AND id NOT IN (
                        SELECT id FROM "{self._schema}".variables_history
                        WHERE variable_id = $1
                        ORDER BY sourcetimestamp DESC LIMIT $2
                    )
                ''', variable_id, count)

        except Exception as e:
            self.logger.error(f"Failed to save node value for {node_id}: {e} \n {datavalue}")
    
    async def save_event(self, event: Any) -> None:
        """
        Сохранение события в единую таблицу истории событий.

        Args:
            event: Событие OPC UA для сохранения
        """
        #self.logger.debug(f"save_event: {type(event)}")
        #self.logger.debug(f"save_event: {dir(event)}")
        #self.logger.debug(f"save_event: {event.get_event_props_as_fields_dict()}")

        if event is None or not hasattr(event, 'SourceNode') or event.SourceNode is None:
            self.logger.error("save_event: invalid event")
            return

        event_type = getattr(event, 'EventType', None)

        if event_type is None:
            self.logger.error("save_event: event.EventType is None")
            return

        try:
            # Получаем source_id и event_type_id из mapping
            source_data = self._datachanges_period.get(event.SourceNode)
            if source_data is None:
                source_db_id = None
                event_db_id = None
            else:
                # Проверяем формат данных
                if len(source_data) == 4:
                    period, count, source_db_id, event_ids = source_data
                    event_db_id = event_ids.get(event_type, (None, None))[1]
                elif len(source_data) == 3:
                    # Старый формат для переменных: (period, count, variable_id)
                    self.logger.warning(f"Source {event.SourceNode} is registered as variable, not event source")
                    return
                else:
                    self.logger.warning(f"Unexpected data format for source {event.SourceNode}: {source_data}")
                    return

            if source_db_id is None or event_db_id is None:
                # Если mapping не найден, получаем IDs из базы данных
                # Сначала получаем source_id из event_sources
                source_node_id_str = self._format_node_id(event.SourceNode)
                source_db_id = await self._fetchval(f'''
                    SELECT source_id FROM "{self._schema}".event_sources 
                    WHERE source_node_id = $1
                    LIMIT 1
                ''', source_node_id_str)
                
                if source_db_id is None:
                    # Если источник не найден, создаем его
                    source_db_id = await self._save_event_source(event.SourceNode, None, 0)

                # Теперь получаем event_type_id из event_types
                event_type_name = self._format_node_id(event_type)
                event_db_id = await self._fetchval(f'''
                    SELECT event_type_id FROM "{self._schema}".event_types 
                    WHERE event_type_name = $1
                    LIMIT 1
                ''', event_type_name)
                
                if event_db_id is None:
                    # Если тип события не найден, создаем его
                    ev_fields = self._event_fields.get(event.SourceNode, [])
                    source_db_id, event_db_id = await self._save_event_metadata(event_type, event.SourceNode, ev_fields, None, 0)
                    if event.SourceNode not in self._datachanges_period:
                        self._datachanges_period[event.SourceNode] = (None, 0, source_db_id, {event_type: (source_db_id, event_db_id)})

            # Получаем время события
            event_time = getattr(event, 'Time', None) or getattr(event, 'time', None) or datetime.now(timezone.utc)

            # Получаем все поля события (Variant) и сериализуем в бинарь (base64)
            raw_event_data = event.get_event_props_as_fields_dict() if hasattr(event, 'get_event_props_as_fields_dict') else {}
            bin_event_data = self._event_to_binary_map(raw_event_data)

            # Вставка в единую таблицу событий
            await self._execute(
                f'INSERT INTO "{self._schema}".events_history (source_id, event_type_id, event_timestamp, event_data) VALUES ($1, $2, $3, $4)',
                source_db_id,
                event_db_id,
                event_time,
                json.dumps(bin_event_data)  # asyncpg требует сериализованную строку для JSONB
            )

            # Очистка старых данных по периоду хранения
            # Получаем параметры хранения из event_sources
            retention_rows = await self._fetch(f'''
                SELECT retention_period, max_records FROM "{self._schema}".event_sources 
                WHERE source_id = $1
                LIMIT 1
            ''', source_db_id)
            
            if retention_rows:
                retention_period = retention_rows[0]['retention_period']
                max_records = retention_rows[0]['max_records']
                if retention_period:
                    date_limit = datetime.now(timezone.utc) - retention_period
                    await self._execute(f'DELETE FROM "{self._schema}".events_history WHERE source_id = $1 AND event_timestamp < $2', source_db_id, date_limit)
                elif max_records and max_records > 0:
                    # Удаляем лишние записи по количеству для конкретного источника
                    await self._execute(f'''
                        DELETE FROM "{self._schema}".events_history
                        WHERE source_id = $1 AND id NOT IN (
                            SELECT id FROM "{self._schema}".events_history
                            WHERE source_id = $1
                            ORDER BY event_timestamp DESC LIMIT $2
                        )
                    ''', source_db_id, max_records)

        except Exception as e:
            self.logger.error(f"Failed to save event for {event.SourceNode}: {e}")

    async def read_node_history(
        self,
        node_id: ua.NodeId,
        start: Optional[datetime],
        end: Optional[datetime],
        nb_values: Optional[int],
        return_bounds: bool = False
    ) -> Tuple[List[ua.DataValue], Optional[datetime]]:
        """
        Чтение истории узла из единой таблицы переменных.

        Args:
            node_id: Идентификатор узла
            start: Начальное время
            end: Конечное время
            nb_values: Количество значений
            return_bounds: Возвращать ли границы

        Returns:
            Кортеж (список значений, время продолжения)
        """
        #self.logger.debug(f"read_node_history: {node_id} {start} {end} {nb_values} {return_bounds}")
        start_time, end_time, order, limit = self._get_bounds(start, end, nb_values)

        try:
            # Получаем variable_id
            node_data = self._datachanges_period.get(node_id)
            if node_data is None:
                variable_id = None
            else:
                # Проверяем формат данных
                if len(node_data) == 3:
                    period, count, variable_id = node_data
                elif len(node_data) == 4:
                    # Формат для событий: (period, count, source_db_id, event_ids)
                    self.logger.warning(f"Node {node_id} is registered as event source, not variable")
                    return [], None
                else:
                    self.logger.warning(f"Unexpected data format for node {node_id}: {node_data}")
                    return [], None
                    
            if variable_id is None:
                # Если mapping не найден, получаем variable_id из базы данных
                node_id_str = self._format_node_id(node_id)
                variable_id = await self._fetchval(f'''
                    SELECT variable_id FROM "{self._schema}".variable_metadata
                    WHERE node_id = $1
                    LIMIT 1
                ''', node_id_str)

            if variable_id is None:
                self.logger.warning(f"No metadata found for node {node_id}")
                return [], None

            # Запрос к единой таблице переменных
            select_sql = f'''
                SELECT servertimestamp, sourcetimestamp, statuscode, value, varianttype, variantbinary
                FROM "{self._schema}".variables_history
                WHERE variable_id = $1 AND sourcetimestamp BETWEEN $2 AND $3
                ORDER BY sourcetimestamp {order}
                LIMIT $4
            '''
            #self.logger.debug(f"read_node_history: {select_sql}")
            rows = await self._fetch(select_sql, variable_id, start_time, end_time, limit)
            #self.logger.debug(f"read_node_history: {len(rows)} rows")
            # Преобразуем в DataValue
            results = []
            for row in rows:
                #self.logger.debug(f"read_node_history: {row}")
                datavalue = ua.DataValue(
                    Value=variant_from_binary(Buffer(row['variantbinary'])),
                    StatusCode_=ua.StatusCode(row['statuscode']),
                    SourceTimestamp=row['sourcetimestamp'],
                    ServerTimestamp=row['servertimestamp']
                )
                results.append(datavalue)
                #self.logger.debug(f"read_node_history: {datavalue}")

            # Определяем время продолжения
            cont = None
            if len(results) == limit and len(rows) > 0:
                cont = rows[-1]['sourcetimestamp']

            #self.logger.debug(f"read_node_history: {len(results)} results")
            return results, cont

        except Exception as e:
            self.logger.error(f"Failed to read node history for {node_id}: {e}")
            return [], None

    async def read_event_history(
        self,
        source_id: ua.NodeId,
        start: Optional[datetime],
        end: Optional[datetime],
        nb_values: Optional[int],
        evfilter: Any
    ) -> Tuple[List[Any], Optional[datetime]]:
        """
        Чтение истории событий из единой таблицы событий.

        Args:
            source_id: Идентификатор источника событий
            start: Начальное время
            end: Конечное время
            nb_values: Количество значений
            evfilter: Фильтр событий

        Returns:
            Кортеж (список событий, время продолжения)
        """
        start_time, end_time, order, limit = self._get_bounds(start, end, nb_values)
        #self.logger.debug(f"read_event_history: {source_id} {start} {end} nb_values evfilter")
        try:
            # Получаем source_db_id
            source_data = self._datachanges_period.get(source_id)
            if source_data is None:
                # Если mapping не найден, получаем source_db_id из базы данных
                source_node_id_str = self._format_node_id(source_id)
                source_db_id = await self._fetchval(f'''
                    SELECT source_id FROM "{self._schema}".event_sources 
                    WHERE source_node_id = $1
                    LIMIT 1
                ''', source_node_id_str)
                
                if source_db_id is None:
                    self.logger.warning(f"No metadata found for source {source_id}")
                    return [], None
            else:
                # Проверяем формат данных
                if len(source_data) == 4:
                    #self.logger.debug(f"read_event_history: source_data: {source_data}")
                    period, count, source_db_id, event_ids = source_data
                    #self.logger.debug(f"read_event_history: using cached source_db_id: {source_db_id}")
                    
                elif len(source_data) == 3:
                    # Старый формат для переменных: (period, count, variable_id)
                    self.logger.warning(f"Source {source_id} is registered as variable, not event source")
                    return [], None
                else:
                    self.logger.warning(f"Unexpected data format for source {source_id}: {source_data}")
                    return [], None

            # Запрос к единой таблице событий
            select_sql = f'''
                SELECT event_timestamp, event_type_id, event_data
                FROM "{self._schema}".events_history
                WHERE source_id = $1 AND event_timestamp BETWEEN $2 AND $3
                ORDER BY event_timestamp {order}
                LIMIT $4
            '''

            rows = await self._fetch(select_sql, source_db_id, start_time, end_time, limit)
            #self.logger.debug(f"read_event_history: query: {select_sql}")
            #self.logger.debug(f"read_event_history: params: source_db_id={source_db_id}, start_time={start_time}, end_time={end_time}, limit={limit}")
            #self.logger.debug(f"read_event_history: {len(rows)} rows")
            # Преобразуем в события
            results = []
            for row in rows:
                data = row['event_data']
                if isinstance(data, str):
                    data = json.loads(data)
                values = self._binary_map_to_event_values(data)
                #payload = {"Time": row["event_timestamp"], "EventType": row["event_type_id"], **values}
                try:
                    #self.logger.debug(f"read_event_history: {values}")
                    event = Event.from_field_dict(values)
                    results.append(event)
                except Exception as e:
                    # Фоллбэк, если from_field_dict недоступен у конкретной реализации Event
                    #self.logger.debug(f"read_event_history fallback: {e}")
                    results.append(Event(**values))

            # Определяем время продолжения
            cont = None
            if len(results) == limit and len(rows) > 0:
                cont = rows[-1]['event_timestamp']

            return results, cont
        except Exception as e:
            self.logger.error(f"Failed to read event history for {source_id}: {e}")
            return [], None

    @staticmethod
    def _get_bounds(
        start: Optional[datetime], 
        end: Optional[datetime], 
        nb_values: Optional[int]
    ) -> Tuple[datetime, datetime, str, int]:
        """
        Определение границ и параметров для запроса истории.
        
        Args:
            start: Начальное время
            end: Конечное время
            nb_values: Количество значений
            
        Returns:
            Кортеж (начальное время, конечное время, порядок сортировки, лимит)
        """
        order = "ASC"
        if start is None or start == ua.get_win_epoch():
            order = "DESC"
            start = ua.get_win_epoch()
        if end is None or end == ua.get_win_epoch():
            end = datetime.now(timezone.utc) + timedelta(days=1)
        if start < end:
            start_time = start
            end_time = end
        else:
            order = "DESC"
            start_time = end
            end_time = start
        limit = nb_values if nb_values else 10000
        
        return start_time, end_time, order, limit

    async def execute_sql_delete(
        self, 
        condition: str, 
        args: Iterable, 
        table: str, 
        node_id: ua.NodeId
    ) -> None:
        """
        Выполнение SQL запроса удаления данных.
        
        Args:
            condition: SQL условие для удаления
            args: Аргументы для SQL запроса
            table: Имя таблицы (variables_history или events_history)
            node_id: Идентификатор узла для логирования
        """
        try:
            # Определяем полное имя таблицы со схемой
            if table == "variables_history":
                full_table = f'"{self._schema}".variables_history'
            elif table == "events_history":
                full_table = f'"{self._schema}".events_history'
            else:
                # Для обратной совместимости
                full_table = f'"{self._schema}".{table}'
            
            await self._execute(f'DELETE FROM {full_table} WHERE {condition}', *args)
        except Exception as e:
            self.logger.error(f"Failed to delete data for {node_id}: {e}")

    async def close(self) -> None:
        """Закрытие модуля историзации"""
        if self._pool:
            await self._pool.close()
            self.logger.info("HistoryTimescale closed")
