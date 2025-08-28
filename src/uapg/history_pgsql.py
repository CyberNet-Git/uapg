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

class HistoryPgSQL(HistoryStorageInterface):
    """
    Backend для хранения исторических данных OPC UA в PostgreSQL с поддержкой TimescaleDB.
    
    Этот класс реализует интерфейс HistoryStorageInterface и предоставляет
    функциональность для хранения и извлечения исторических данных OPC UA
    в PostgreSQL базе данных с оптимизацией для временных рядов.
    
    Особенности:
    - Таблицы создаются только при инициализации и не изменяются по структуре
    - Отдельные таблицы метаданных для переменных и типов событий
    - Оптимизировано для массовой записи от тысяч источников
    - Использует TimescaleDB для эффективной работы с временными рядами
    
    Attributes:
        max_history_data_response_size (int): Максимальный размер ответа с историческими данными
        logger (logging.Logger): Логгер для записи событий
        _datachanges_period (dict): Словарь периодов хранения данных по узлам
        _conn_params (dict): Параметры подключения к базе данных
        _event_fields (dict): Словарь полей событий по источникам
        _pool (asyncpg.Pool): Пул соединений с базой данных
        _min_size (int): Минимальное количество соединений в пуле
        _max_size (int): Максимальное количество соединений в пуле
        _initialized (bool): Флаг инициализации таблиц метаданных
    """

    def __init__(
        self, 
        user: str = 'postgres', 
        password: str = 'postmaster', 
        database: str = 'opcua', 
        host: str = 'localhost', 
        port: int = 5432,
        min_size: int = 1,
        max_size: int = 10,
        config_file: Optional[str] = None,
        encrypted_config: Optional[str] = None,
        master_password: Optional[str] = None,
        **kwargs
    ) -> None:
        """
        Инициализация HistoryPgSQL.
        
        Args:
            user: Имя пользователя базы данных
            password: Пароль пользователя
            database: Имя базы данных
            host: Хост базы данных
            port: Порт базы данных
            min_size: Минимальное количество соединений в пуле
            max_size: Максимальное количество соединений в пуле
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
        
        # Инициализация параметров подключения
        self._conn_params = self._init_connection_params(
            user, password, database, host, port,
            config_file, encrypted_config, master_password, **kwargs
        )
    
    def _init_connection_params(
        self,
        user: str,
        password: str,
        database: str,
        host: str,
        port: int,
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
                    'port': decrypted_config.get('port', port)
                }
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
                        'port': temp_manager.config.get('port', port)
                    }
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
            'port': port
        }
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
    ) -> 'HistoryPgSQL':
        """
        Создание экземпляра из файла зашифрованной конфигурации.
        
        Args:
            config_file: Путь к файлу зашифрованной конфигурации
            master_password: Главный пароль для расшифровки
            min_size: Минимальное количество соединений в пуле
            max_size: Максимальное количество соединений в пуле
            
        Returns:
            Экземпляр HistoryPgSQL с загруженной конфигурацией
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
    ) -> 'HistoryPgSQL':
        """
        Создание экземпляра из зашифрованной конфигурации в виде строки.
        
        Args:
            encrypted_config: Зашифрованная конфигурация в виде строки
            master_password: Главный пароль для расшифровки
            min_size: Минимальное количество соединений в пуле
            max_size: Максимальное количество соединений в пуле
            
        Returns:
            Экземпляр HistoryPgSQL с расшифрованной конфигурацией
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
            exclude_params = {'user', 'password', 'database', 'host', 'port', 'min_size', 'max_size', 'sslmode'}
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
                
            self.logger.info("HistoryPgSQL initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize HistoryPgSQL: {e}")
            raise

    async def stop(self) -> None:
        """Остановка и закрытие пула соединений."""
        if self._pool:
            await self._pool.close()
            self._pool = None
            self.logger.info("HistoryPgSQL stopped")

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
        """Создание таблиц метаданных для переменных и событий."""
        try:
            # Таблица метаданных переменных
            await self._execute('''
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
            await self._execute('''
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
            
            # Создаем индексы для метаданных
            await self._execute('CREATE INDEX IF NOT EXISTS idx_variable_metadata_node_id ON variable_metadata(node_id)')
            await self._execute('CREATE INDEX IF NOT EXISTS idx_event_type_metadata_source ON event_type_metadata(source_node_id)')
            await self._execute('CREATE INDEX IF NOT EXISTS idx_event_type_metadata_type ON event_type_metadata(event_type_id)')
            
            self.logger.info("Metadata tables created successfully")
        except Exception as e:
            self.logger.error(f"Failed to create metadata tables: {e}")
            raise

    async def _create_variable_table(self, table: str) -> None:
        """
        Создание таблицы переменных.
        
        Args:
            table: Имя таблицы
        """
        await self._execute(f'''
                CREATE TABLE IF NOT EXISTS "{table}" (
                    _id SERIAL,
                    servertimestamp TIMESTAMPTZ NOT NULL,
                    sourcetimestamp TIMESTAMPTZ NOT NULL,
                    statuscode INTEGER,
                    value TEXT,
                    varianttype TEXT,
                    variantbinary BYTEA,
                    PRIMARY KEY (_id, sourcetimestamp)
            )
        ''')

    async def _create_event_table(self, table: str) -> None:
        """
        Создание таблицы событий.
        
        Args:
            table: Имя таблицы
        """
        await self._execute(f'''
            CREATE TABLE IF NOT EXISTS "{table}" (
                _id SERIAL,
                _timestamp TIMESTAMPTZ NOT NULL,
                _eventtypename TEXT,
                _eventdata JSONB,
                PRIMARY KEY (_id, _timestamp)
            )
        ''')

    async def _setup_timescale_hypertable(self, table: str, partition_column: str) -> None:
        """
        Настройка TimescaleDB hypertable.
        
        Args:
            table: Имя таблицы
            partition_column: Колонка для партиционирования
        """
        await self._execute(
            f'SELECT create_hypertable(\'{table}\', \'{partition_column}\', if_not_exists => TRUE)'
        )

    async def _create_variable_indexes(self, table: str) -> None:
        """
        Создание индексов для таблицы переменных.
        
        Args:
            table: Имя таблицы
        """
        await self._execute(f'CREATE INDEX IF NOT EXISTS "{table}_id_idx" ON "{table}" (_id)')
        await self._execute(f'CREATE INDEX IF NOT EXISTS "{table}_timestamp_idx" ON "{table}" (sourcetimestamp)')

    async def _create_event_indexes(self, table: str) -> None:
        """
        Создание индексов для таблицы событий.
        
        Args:
            table: Имя таблицы
        """
        await self._execute(f'CREATE INDEX IF NOT EXISTS "{table}_id_idx" ON "{table}" (_id)')
        await self._execute(f'CREATE INDEX IF NOT EXISTS "{table}_timestamp_idx" ON "{table}" (_timestamp)')
        await self._execute(f'CREATE INDEX IF NOT EXISTS "{table}_eventtype_idx" ON "{table}" (_eventtypename)')

    async def _save_variable_metadata(self, node_id: ua.NodeId, table: str, period: Optional[timedelta], count: int) -> None:
        """
        Сохранение метаданных переменной.
        
        Args:
            node_id: Идентификатор узла
            table: Имя таблицы
            period: Период хранения
            count: Максимальное количество записей
        """
        await self._execute('''
            INSERT INTO variable_metadata (node_id, table_name, retention_period, max_records)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (node_id) DO UPDATE SET
                retention_period = EXCLUDED.retention_period,
                max_records = EXCLUDED.max_records
        ''', str(node_id), table, period, count)

    async def _save_event_metadata(self, event_type: ua.NodeId, source_id: ua.NodeId, table: str, fields: List[str], period: Optional[timedelta], count: int) -> None:
        """
        Сохранение метаданных события.
        
        Args:
            event_type: Тип события
            source_id: Идентификатор источника
            table: Имя таблицы
            fields: Список полей
            period: Период хранения
            count: Максимальное количество записей
        """
        await self._execute('''
            INSERT INTO event_type_metadata (event_type_id, source_node_id, table_name, fields, retention_period, max_records)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (event_type_id, source_node_id) DO UPDATE SET
                fields = EXCLUDED.fields,
                retention_period = EXCLUDED.retention_period,
                max_records = EXCLUDED.max_records
        ''', str(event_type), str(source_id), table, json.dumps(fields), period, count)

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
                    self.logger.debug(f"_event_to_binary_map: Processing ExtensionObject for key '{key}': {variant.Value}")
                    try:
                        binary_data = variant_to_binary(variant)
                        self.logger.debug(f"_event_to_binary_map: variant_to_binary success for '{key}', binary length: {len(binary_data)}")
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

    def _get_table_name(self, node_id: ua.NodeId, table_type: str = "var") -> str:
        """
        Генерация имени таблицы для узла.
        
        Args:
            node_id: Идентификатор узла OPC UA
            table_type: Тип таблицы ("var" для переменных, "evt" для событий)
            
        Returns:
            Имя таблицы в формате "Type_NamespaceIndex_Identifier"
        """
        return f"{table_type}_{node_id.NamespaceIndex}_{node_id.Identifier}"

    async def new_historized_node(
        self, 
        node_id: ua.NodeId, 
        period: Optional[timedelta], 
        count: int = 0
    ) -> None:
        """
        Создание новой таблицы для историзации узла.
        Таблица создается только один раз при инициализации.
        
        Args:
            node_id: Идентификатор узла OPC UA
            period: Период хранения данных (None для бесконечного хранения)
            count: Максимальное количество записей (0 для неограниченного)
        """
        table = self._get_table_name(node_id, "var")
        self.logger.debug(
            "new_historized_node: table=%s node_id=%s period=%s count=%s",
            table, node_id, period, count,
        )
        
        # Сохраняем период хранения
        self._datachanges_period[node_id] = period, count
        
        try:
            validate_table_name(table)
            
            # Создаем таблицу переменных
            await self._create_variable_table(table)
            
            # Преобразуем в hypertable TimescaleDB
            await self._setup_timescale_hypertable(table, 'sourcetimestamp')
            
            # Создаем индексы для производительности
            await self._create_variable_indexes(table)
            
            # Сохраняем метаданные
            await self._save_variable_metadata(node_id, table, period, count)
            
            self.logger.info(f"Variable table {table} created for node {node_id}")
        except Exception as e:
            self.logger.error(f"Failed to create variable table for {node_id}: {e}")
            raise

    async def new_historized_event(
        self, 
        source_id: ua.NodeId, 
        evtypes: List[ua.NodeId], 
        period: Optional[timedelta], 
        count: int = 0
    ) -> None:
        """
        Создание новой таблицы для историзации событий.
        Таблица создается только один раз при инициализации.
        
        Args:
            source_id: Идентификатор источника событий
            evtypes: Список типов событий
            period: Период хранения данных (None для бесконечного хранения)
            count: Максимальное количество записей (0 для неограниченного)
        """
        table = self._get_table_name(source_id, "evt")
        self.logger.debug(
            "new_historized_event: table=%s source_id=%s evtypes=%s period=%s count=%s",
            table, source_id, evtypes, period, count,
        )
        
        # Сохраняем период хранения
        self._datachanges_period[source_id] = period, count
        
        try:
            validate_table_name(table)
            
            # Получаем поля событий
            ev_fields = await self._get_event_fields(evtypes)
            self._event_fields[source_id] = ev_fields
            
            # Создаем таблицу событий с фиксированной структурой
            await self._create_event_table(table)
            
            # Преобразуем в hypertable TimescaleDB
            await self._setup_timescale_hypertable(table, '_timestamp')
            
            # Создаем индексы для производительности
            await self._create_event_indexes(table)
            
            # Сохраняем метаданные для каждого типа события
            for event_type in evtypes:
                await self._save_event_metadata(event_type, source_id, table, ev_fields, period, count)
            
            self.logger.info(f"Event table {table} created for source {source_id}")
        except Exception as e:
            self.logger.error(f"Failed to create event table for {source_id}: {e}")
            raise

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

    async def save_node_value(self, node_id: ua.NodeId, datavalue: ua.DataValue) -> None:
        """
        Сохранение значения узла в историю.
        
        Args:
            node_id: Идентификатор узла OPC UA
            datavalue: Значение данных для сохранения
        """
        table = self._get_table_name(node_id, "var")
        self.logger.debug(
            "save_node_value: table=%s node_id=%s source_ts=%s server_ts=%s status=%s",
            table, node_id, getattr(datavalue, 'SourceTimestamp', None), getattr(datavalue, 'ServerTimestamp', None), getattr(datavalue, 'StatusCode', None),
        )
        
        try:
            validate_table_name(table)
            
            # Простая вставка без проверок структуры
            await self._execute(
                f'INSERT INTO "{table}" (servertimestamp, sourcetimestamp, statuscode, value, varianttype, variantbinary) VALUES ($1, $2, $3, $4, $5, $6)',
                datavalue.ServerTimestamp,
                datavalue.SourceTimestamp,
                datavalue.StatusCode.value,
                str(datavalue.Value.Value),
                str(datavalue.Value.VariantType),
                variant_to_binary(datavalue.Value)
            )
            
            # Очистка старых данных по периоду хранения
            period, count = self._datachanges_period.get(node_id, (None, 0))
            if period:
                date_limit = datetime.now(timezone.utc) - period
                await self._execute(f'DELETE FROM "{table}" WHERE sourcetimestamp < $1', date_limit)
            elif count > 0:
                # Удаляем лишние записи по количеству
                await self._execute(f'DELETE FROM "{table}" WHERE _id NOT IN (SELECT _id FROM "{table}" ORDER BY _id DESC LIMIT $1)', count)
                
        except Exception as e:
            self.logger.error(f"Failed to save node value for {node_id}: {e}")

    async def save_event(self, event: Any) -> None:
        """
        Сохранение события в историю.
        
        Args:
            event: Событие OPC UA для сохранения
        """
        self.logger.debug(f"save_event: {type(event)}")
        self.logger.debug(f"save_event: {dir(event)}")
        self.logger.debug(f"save_event: {event.get_event_props_as_fields_dict()}")
        
        if event is None or not hasattr(event, 'SourceNode') or event.SourceNode is None:
            self.logger.error("save_event: invalid event")
            return
        
        table = self._get_table_name(event.SourceNode, "evt")
        event_type = getattr(event, 'EventType', None)
        
        if event_type is None:
            self.logger.error("save_event: event.EventType is None")
            return
        
        try:
            validate_table_name(table)
            
            # Получаем время события
            event_time = getattr(event, 'Time', None) or getattr(event, 'time', None) or datetime.now(timezone.utc)
            
            # Получаем все поля события (Variant) и сериализуем в бинарь (base64)
            raw_event_data = event.get_event_props_as_fields_dict() if hasattr(event, 'get_event_props_as_fields_dict') else {}
            bin_event_data = self._event_to_binary_map(raw_event_data)
            
            # Простая вставка без проверок структуры (JSONB dict, не dumps)
            await self._execute(
                f'INSERT INTO "{table}" (_timestamp, _eventtypename, _eventdata) VALUES ($1, $2, $3)',
                event_time,
                str(event_type),
                json.dumps(bin_event_data)  # asyncpg требует сериализованную строку для JSONB
            )
            
            # Очистка старых данных по периоду хранения
            period, count = self._datachanges_period.get(event.SourceNode, (None, 0))
            if period:
                date_limit = datetime.now(timezone.utc) - period
                await self._execute(f'DELETE FROM "{table}" WHERE _timestamp < $1', date_limit)
            elif count > 0:
                # Удаляем лишние записи по количеству
                await self._execute(f'DELETE FROM "{table}" WHERE _id NOT IN (SELECT _id FROM "{table}" ORDER BY _id DESC LIMIT $1)', count)
                
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
        Чтение истории узла.
        
        Args:
            node_id: Идентификатор узла
            start: Начальное время
            end: Конечное время
            nb_values: Количество значений
            return_bounds: Возвращать ли границы
            
        Returns:
            Кортеж (список значений, время продолжения)
        """
        table = self._get_table_name(node_id, "var")
        start_time, end_time, order, limit = self._get_bounds(start, end, nb_values)
        
        try:
            validate_table_name(table)
            
            # Простой запрос без проверок структуры
            select_sql = f'''
                SELECT servertimestamp, sourcetimestamp, statuscode, value, varianttype, variantbinary
                FROM "{table}" 
                WHERE sourcetimestamp BETWEEN $1 AND $2 
                ORDER BY sourcetimestamp {order} 
                LIMIT $3
            '''
            
            rows = await self._fetch(select_sql, start_time, end_time, limit)
            
            # Преобразуем в DataValue
            results = []
            for row in rows:
                datavalue = ua.DataValue(
                    Value=ua.Variant(row['value'], ua.VariantType.String),
                    StatusCode=ua.StatusCode(row['statuscode']),
                    SourceTimestamp=row['sourcetimestamp'],
                    ServerTimestamp=row['servertimestamp']
                )
                results.append(datavalue)
            
            # Определяем время продолжения
            cont = None
            if len(results) == limit and len(rows) > 0:
                cont = rows[-1]['sourcetimestamp']
            
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
        Чтение истории событий.
        
        Args:
            source_id: Идентификатор источника событий
            start: Начальное время
            end: Конечное время
            nb_values: Количество значений
            evfilter: Фильтр событий
            
        Returns:
            Кортеж (список событий, время продолжения)
        """
        table = self._get_table_name(source_id, "evt")
        start_time, end_time, order, limit = self._get_bounds(start, end, nb_values)
        
        try:
            validate_table_name(table)
            
            # Простой запрос без проверок структуры
            select_sql = f'''
                SELECT _timestamp, _eventtypename, _eventdata
                FROM "{table}" 
                WHERE _timestamp BETWEEN $1 AND $2 
                ORDER BY _timestamp {order} 
                LIMIT $3
            '''
            
            rows = await self._fetch(select_sql, start_time, end_time, limit)
            
            # Преобразуем в события
            results = []
            for row in rows:
                data = row['_eventdata']
                if isinstance(data, str):
                    data = json.loads(data)
                values = self._binary_map_to_event_values(data)
                #payload = {"Time": row["_timestamp"], "EventType": row["_eventtypename"], **values}
                try:
                    self.logger.debug(f"read_event_history: {values}")
                    event = Event.from_field_dict(values)
                    results.append(event)
                except Exception as e:
                    # Фоллбэк, если from_field_dict недоступен у конкретной реализации Event
                    self.logger.debug(f"read_event_history fallback: {e}")
                    results.append(Event(**values))
            
            # Определяем время продолжения
            cont = None
            if len(results) == limit and len(rows) > 0:
                cont = rows[-1]['_timestamp']
            
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
            table: Имя таблицы
            node_id: Идентификатор узла для логирования
        """
        try:
            validate_table_name(table)
            await self._execute(f'DELETE FROM "{table}" WHERE {condition}', *args)
        except Exception as e:
            self.logger.error(f"Failed to delete data for {node_id}: {e}")
