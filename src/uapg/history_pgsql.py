import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, List, Optional, Tuple

import asyncpg
from asyncua import ua


class Buffer:
    """Буфер для чтения бинарных данных."""
    
    def __init__(self, data: bytes) -> None:
        """
        Инициализация буфера.
        
        Args:
            data: Бинарные данные
        """
        self.data = data
        self.pos = 0

    def read(self, n: int) -> bytes:
        """
        Чтение n байт из буфера.
        
        Args:
            n: Количество байт для чтения
            
        Returns:
            Прочитанные байты
        """
        result = self.data[self.pos:self.pos+n]
        self.pos += n
        return result

from asyncua.server.history import HistoryStorageInterface
from asyncua.ua.ua_binary import variant_from_binary, variant_to_binary

# Импорт для работы с событиями
try:
    from asyncua.server.history import Event
except ImportError:
    # Fallback для старых версий asyncua
    class Event:
        """Простой класс для представления событий OPC UA"""
        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)
        
        @classmethod
        def from_field_dict(cls, field_dict: dict) -> 'Event':
            return cls(**field_dict)

# Импорт для получения свойств событий
try:
    from asyncua.server.history import get_event_properties_from_type_node
except ImportError:
    # Fallback функция
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
    
    Attributes:
        max_history_data_response_size (int): Максимальный размер ответа с историческими данными
        logger (logging.Logger): Логгер для записи событий
        _datachanges_period (dict): Словарь периодов хранения данных по узлам
        _conn_params (dict): Параметры подключения к базе данных
        _event_fields (dict): Словарь полей событий по источникам
        _pool (asyncpg.Pool): Пул соединений с базой данных
        _min_size (int): Минимальное количество соединений в пуле
        _max_size (int): Максимальное количество соединений в пуле
    """

    def __init__(
        self, 
        user: str = 'postgres', 
        password: str = 'postmaster', 
        database: str = 'opcua', 
        host: str = '127.0.0.1', 
        port: int = 5432,
        min_size: int = 5,
        max_size: int = 20,
        max_history_data_response_size: int = 10000
    ) -> None:
        self.max_history_data_response_size = max_history_data_response_size
        self.logger = logging.getLogger(__name__)
        self._datachanges_period = {}
        self._conn_params = dict(
            user=user, 
            password=password, 
            database=database, 
            host=host,
            port=port
        )
        self._event_fields = {}
        self._pool: asyncpg.Pool = None
        self._min_size = min_size
        self._max_size = max_size

    async def init(self) -> None:
        """Инициализация пула соединений с базой данных."""
        try:
            self._pool = await asyncpg.create_pool(
                **self._conn_params,
                min_size=self._min_size,
                max_size=self._max_size,
                command_timeout=60,
                statement_cache_size=0
            )
            self.logger.info(f"Historizing PgSQL pool initialized with {self._min_size}-{self._max_size} connections")
        except Exception as e:
            self.logger.error(f"Failed to initialize connection pool: {e}")
            raise

    async def stop(self) -> None:
        """Закрытие пула соединений с базой данных."""
        if self._pool:
            await self._pool.close()
            self.logger.info("Historizing PgSQL connection pool closed")

    async def _execute(self, query: str, *args) -> Any:
        """
        Выполнение SQL запроса с использованием соединения из пула.
        
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
        Выполнение SQL запроса с выборкой данных.
        
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
            Значение из запроса
        """
        async with self._pool.acquire() as conn:
            return await conn.fetchval(query, *args)

    async def new_historized_node(
        self, 
        node_id: ua.NodeId, 
        period: Optional[timedelta], 
        count: int = 0
    ) -> None:
        """
        Создание новой таблицы для историзации узла.
        
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
        self._datachanges_period[node_id] = period, count
        try:
            validate_table_name(table)
            await self._execute(
                f'''
                CREATE TABLE IF NOT EXISTS "{table}" (
                    _id SERIAL,
                    servertimestamp TIMESTAMPTZ NOT NULL,
                    sourcetimestamp TIMESTAMPTZ NOT NULL,
                    statuscode INTEGER,
                    value TEXT,
                    varianttype TEXT,
                    variantbinary BYTEA,
                    PRIMARY KEY (_id, sourcetimestamp)
                );
                '''
            )
            
            # Проверяем и исправляем структуру таблицы, если она уже существовала
            await self._ensure_variable_table_structure(table)
            # Удаляем конфликтующие уникальные индексы, не включающие колонку партиционирования
            await self._drop_conflicting_unique_indexes(table, 'sourcetimestamp')
            
            # Преобразуем таблицу в hypertable TimescaleDB
            await self._execute(
                f'SELECT create_hypertable(\'{table}\', \'sourcetimestamp\', if_not_exists => TRUE);'
            )
            # Индекс по _id (неуникальный) — уникальный недопустим без колонки партиционирования
            await self._execute(
                f'CREATE INDEX IF NOT EXISTS "{table}_id_idx" ON "{table}" ("_id");'
            )
            # Индекс по времени для ускорения запросов
            await self._execute(
                f'CREATE INDEX IF NOT EXISTS "{table}_source_ts_idx" ON "{table}" ("sourcetimestamp");'
            )
        except Exception as e:
            self.logger.info("Historizing PgSQL Table Creation Error for %s: %s", node_id, e)

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
            self.logger.error("Historizing PgSQL Delete Old Data Error for %s: %s", node_id, e)

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
            await self._execute(
                f'INSERT INTO "{table}" (servertimestamp, sourcetimestamp, statuscode, value, varianttype, variantbinary) VALUES ($1, $2, $3, $4, $5, $6)',
                datavalue.ServerTimestamp,
                datavalue.SourceTimestamp,
                datavalue.StatusCode.value,
                str(datavalue.Value.Value),
                datavalue.Value.VariantType.name,
                variant_to_binary(datavalue.Value),
            )
        except Exception as e:
            self.logger.error("Historizing PgSQL Insert Error for %s: %s", node_id, e)
        period, count = self._datachanges_period[node_id]
        if period:
            date_limit = datetime.now(timezone.utc) - period
            validate_table_name(table)
            await self.execute_sql_delete("sourcetimestamp < $1", (date_limit,), table, node_id)
        if count:
            validate_table_name(table)
            await self.execute_sql_delete(
                "sourcetimestamp = (SELECT CASE WHEN COUNT(*) > $1 THEN MIN(sourcetimestamp) ELSE NULL END FROM \"{}\")".format(table),
                (count,),
                table,
                node_id,
            )

    async def read_node_history(
        self, 
        node_id: ua.NodeId, 
        start: Optional[datetime], 
        end: Optional[datetime], 
        nb_values: Optional[int]
    ) -> Tuple[List[ua.DataValue], Optional[datetime]]:
        """
        Чтение исторических данных узла.
        
        Args:
            node_id: Идентификатор узла OPC UA
            start: Начальное время (None для самого раннего)
            end: Конечное время (None для текущего времени)
            nb_values: Количество значений для чтения (None для всех)
            
        Returns:
            Кортеж (список значений данных, время продолжения)
        """
        table = self._get_table_name(node_id, "var")
        start_time, end_time, order, limit = self._get_bounds(start, end, nb_values)
        cont = None
        results = []
        
        self.logger.debug(
            "read_node_history: table=%s node_id=%s start=%s end=%s order=%s limit=%s",
            table, node_id, start_time, end_time, order, limit
        )
        
        try:
            validate_table_name(table)
            
            # Проверяем, существует ли таблица
            table_exists = await self._fetchval(
                f'''
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = $1
                ''',
                table
            )
            
            if table_exists == 0:
                self.logger.warning(f"Table {table} does not exist for node {node_id}")
                return [], None
            
            # Проверяем количество записей в таблице
            total_rows = await self._fetchval(f'SELECT COUNT(*) FROM "{table}"')
            self.logger.debug(f"Table {table} contains {total_rows} total rows")
            
            # Проверяем количество записей в заданном временном диапазоне
            range_rows = await self._fetchval(
                f'SELECT COUNT(*) FROM "{table}" WHERE "sourcetimestamp" BETWEEN $1 AND $2',
                start_time, end_time
            )
            self.logger.debug(f"Table {table} contains {range_rows} rows in time range {start_time} to {end_time}")
            
            rows = await self._fetch(
                f'SELECT * FROM "{table}" WHERE "sourcetimestamp" BETWEEN $1 AND $2 ORDER BY "_id" {order} LIMIT $3',
                start_time, end_time, limit
            )
            
            self.logger.debug(f"Retrieved {len(rows)} rows from {table}")
            
            for row in rows:
                dv = ua.DataValue(
                    variant_from_binary(Buffer(row['variantbinary'])),
                    ServerTimestamp=row['servertimestamp'],
                    SourceTimestamp=row['sourcetimestamp'],
                    StatusCode_=ua.StatusCode(row['statuscode']),
                )
                results.append(dv)
                
        except Exception as e:
            self.logger.error("Historizing PgSQL Read Error for %s: %s", node_id, e)
            
        if len(results) > self.max_history_data_response_size:
            cont = results[self.max_history_data_response_size].SourceTimestamp
        results = results[: self.max_history_data_response_size]
        
        self.logger.debug(f"read_node_history: returning {len(results)} results for node {node_id}")
        return results, cont

    async def new_historized_event(
        self, 
        source_id: ua.NodeId, 
        evtypes: List[ua.NodeId], 
        period: Optional[timedelta], 
        count: int = 0
    ) -> None:
        """
        Создание новой таблицы для историзации событий.
        
        Args:
            source_id: Идентификатор источника событий
            evtypes: Список типов событий
            period: Период хранения данных (None для бесконечного хранения)
            count: Максимальное количество записей (0 для неограниченного)
        """
        ev_fields = await self._get_event_fields(evtypes)
        self._datachanges_period[source_id] = period
        self._event_fields[source_id] = ev_fields
        table = self._get_table_name(source_id, "evt")
        self.logger.debug(
            "new_historized_event: table=%s source_id=%s evtypes=%s period=%s count=%s",
            table, source_id, evtypes, period, count,
        )
        columns = self._get_event_columns(ev_fields)
        try:
            validate_table_name(table)
            # Формируем SQL для создания таблицы с учетом возможного отсутствия полей событий
            if columns.strip():
                create_table_sql = f'''
                CREATE TABLE IF NOT EXISTS "{table}" (
                    _id SERIAL,
                    _timestamp TIMESTAMPTZ NOT NULL,
                    _eventtypename TEXT,
                    {columns},
                    PRIMARY KEY (_id, _timestamp)
                );
                '''
            else:
                create_table_sql = f'''
                CREATE TABLE IF NOT EXISTS "{table}" (
                    _id SERIAL,
                    _timestamp TIMESTAMPTZ NOT NULL,
                    _eventtypename TEXT,
                    PRIMARY KEY (_id, _timestamp)
                );
                '''
            await self._execute(create_table_sql)
            
            # Проверяем и исправляем структуру таблицы, если она уже существовала
            await self._ensure_event_table_structure(table)
            # Удаляем конфликтующие уникальные индексы, не включающие колонку партиционирования
            await self._drop_conflicting_unique_indexes(table, '_timestamp')
            
            # Преобразуем таблицу событий в hypertable TimescaleDB
            await self._execute(
                f'SELECT create_hypertable(\'{table}\', \'_timestamp\', if_not_exists => TRUE);'
            )
            # Индекс по _id (неуникальный) — уникальный недопустим без колонки партиционирования
            await self._execute(
                f'CREATE INDEX IF NOT EXISTS "{table}_id_idx" ON "{table}" (_id);'
            )
            await self._execute(
                f'CREATE INDEX IF NOT EXISTS "{table}_timestamp_idx" ON "{table}" (_timestamp);'
            )
        except Exception as e:
            self.logger.info("Historizing PgSQL Table Creation Error for events from %s: %s", source_id, e)

    async def save_event(self, event: Any) -> None:
        """
        Сохранение события в историю.
        
        Args:
            event: Событие OPC UA для сохранения
        """
        # Проверяем, что событие не None
        if event is None:
            self.logger.error("save_event: event is None")
            return
        
        # Проверяем, что у события есть SourceNode
        if not hasattr(event, 'SourceNode') or event.SourceNode is None:
            self.logger.error("save_event: event.SourceNode is None or missing")
            return
        
        table = self._get_table_name(event.SourceNode, "evt")
        columns, placeholders, evtup, field_names = self._format_event(event)
        
        # Проверяем, что у события есть EventType
        if not hasattr(event, 'EventType') or event.EventType is None:
            self.logger.error("save_event: event.EventType is None or missing")
            return
        
        event_type = event.EventType
        # Получаем время события из различных возможных атрибутов
        raw_time = None
        if hasattr(event, 'Time') and event.Time is not None:
            raw_time = event.Time
        elif hasattr(event, 'time') and event.time is not None:
            raw_time = event.time
        elif hasattr(event, '_Time') and event._Time is not None:
            raw_time = event._Time
        
        # Логируем все доступные атрибуты события для отладки
        event_attrs = [attr for attr in dir(event) if not attr.startswith('_') and not callable(getattr(event, attr))]
        self.logger.debug("save_event: available event attributes: %s", event_attrs)
        
        if raw_time is None:
            self.logger.warning("save_event: event.Time is None; substituting current UTC time")
            insert_time = datetime.now(timezone.utc)
        else:
            insert_time = raw_time
            self.logger.debug("save_event: using event time: %s", insert_time)
        self.logger.debug(
            "save_event: table=%s source=%s type=%s raw_time=%s insert_time=%s fields=%s cols_str='%s' placeholders='%s' values_count=%d",
            table, getattr(event, 'SourceNode', None), event_type, raw_time, insert_time, field_names, columns, placeholders, len(evtup),
        )
        try:
            validate_table_name(table)
            
            # Проверяем, что время события не None
            if insert_time is None:
                self.logger.error("save_event: insert_time is None, cannot insert event")
                return
            
            # Дополнительная проверка и логирование времени
            self.logger.debug("save_event: final insert_time check - value: %s, type: %s", insert_time, type(insert_time))
            if insert_time is None:
                self.logger.error("save_event: insert_time is still None after all checks!")
                return
            
            # Гарантируем, что все требуемые колонки существуют
            if field_names:
                await self._ensure_event_dynamic_columns(table, field_names)
            
            # Проверяем, что колонка _timestamp существует и имеет правильное имя
            timestamp_col_exists = await self._fetchval(
                f'''
                SELECT COUNT(*) FROM information_schema.columns 
                WHERE table_name = $1 AND column_name = '_timestamp'
                ''',
                table
            )
            
            if timestamp_col_exists == 0:
                self.logger.error(f"Column '_timestamp' does not exist in table {table}")
                return
            
            # Формируем SQL для вставки с учетом возможного отсутствия полей событий
            if columns.strip():
                insert_sql = f'INSERT INTO "{table}" (_timestamp, _eventtypename, {columns}) VALUES ($1, $2, {placeholders})'
            else:
                insert_sql = f'INSERT INTO "{table}" (_timestamp, _eventtypename) VALUES ($1, $2)'
            
            # Финальная проверка перед выполнением SQL
            self.logger.debug("save_event: executing SQL: %s with params: time=%s, type=%s, values=%s", 
                            insert_sql, insert_time, str(event_type), evtup)
            
            await self._execute(insert_sql, insert_time, str(event_type), *evtup)
        except Exception as e:
            self.logger.error(
                "Historizing PgSQL Insert Error for events from %s: %s | table=%s time=%s type=%s fields=%s",
                getattr(event, 'SourceNode', None), e, table, insert_time, event_type, field_names,
            )
        period = self._datachanges_period.get(event.emitting_node)
        if period:
            date_limit = datetime.now(timezone.utc) - period
            try:
                validate_table_name(table)
                await self._execute(f'DELETE FROM "{table}" WHERE _timestamp < $1', date_limit)
            except Exception as e:
                self.logger.error("Historizing PgSQL Delete Old Data Error for events from %s: %s", event.SourceNode, e)

    async def read_event_history(
        self, 
        source_id: ua.NodeId, 
        start: Optional[datetime], 
        end: Optional[datetime], 
        nb_values: Optional[int], 
        evfilter: Any
    ) -> Tuple[List[Any], Optional[datetime]]:
        """
        Чтение исторических событий.
        
        Args:
            source_id: Идентификатор источника событий
            start: Начальное время (None для самого раннего)
            end: Конечное время (None для текущего времени)
            nb_values: Количество значений для чтения (None для всех)
            evfilter: Фильтр событий
            
        Returns:
            Кортеж (список событий, время продолжения)
        """
        table = self._get_table_name(source_id, "evt")
        start_time, end_time, order, limit = self._get_bounds(start, end, nb_values)
        clauses, clauses_str = await self._get_select_clauses(source_id, evfilter)
        cont = None
        cont_timestamps = []
        results = []
        
        self.logger.debug(
            "read_event_history: table=%s source_id=%s start=%s end=%s order=%s limit=%s clauses=%s",
            table, source_id, start_time, end_time, order, limit, clauses
        )
        
        try:
            validate_table_name(table)
            
            # Проверяем, существует ли таблица
            table_exists = await self._fetchval(
                f'''
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = $1
                ''',
                table
            )
            
            if table_exists == 0:
                self.logger.warning(f"Table {table} does not exist for source {source_id}")
                return [], None
            
            # Проверяем количество записей в таблице
            total_rows = await self._fetchval(f'SELECT COUNT(*) FROM "{table}"')
            self.logger.debug(f"Table {table} contains {total_rows} total rows")
            
            # Проверяем количество записей в заданном временном диапазоне
            range_rows = await self._fetchval(
                f'SELECT COUNT(*) FROM "{table}" WHERE _timestamp BETWEEN $1 AND $2',
                start_time, end_time
            )
            self.logger.debug(f"Table {table} contains {range_rows} rows in time range {start_time} to {end_time}")
            
            # Формируем SQL для выборки с учетом возможного отсутствия полей событий
            if clauses_str.strip():
                select_sql = f'SELECT _timestamp, {clauses_str} FROM "{table}" WHERE _timestamp BETWEEN $1 AND $2 ORDER BY "_id" {order} LIMIT $3'
            else:
                select_sql = f'SELECT _timestamp FROM "{table}" WHERE _timestamp BETWEEN $1 AND $2 ORDER BY "_id" {order} LIMIT $3'
            
            self.logger.debug(f"Executing SQL: {select_sql}")
            
            rows = await self._fetch(select_sql, start_time, end_time, limit)
            
            self.logger.debug(f"Retrieved {len(rows)} rows from {table}")
            
            for row in rows:
                fdict = {}
                cont_timestamps.append(row['_timestamp'])
                for i, field in enumerate(clauses):
                    val = row[field.lower()]
                    if val is not None:
                        fdict[clauses[i]] = variant_from_binary(Buffer(row[field.lower()]))
                    else:
                        fdict[clauses[i]] = ua.Variant(None)
                results.append(Event.from_field_dict(fdict))
                
        except Exception as e:
            self.logger.error("Historizing PgSQL Read Error events for node %s: %s", source_id, e)
            
        if len(results) > self.max_history_data_response_size:
            cont = cont_timestamps[self.max_history_data_response_size]
        results = results[: self.max_history_data_response_size]
        
        self.logger.debug(f"read_event_history: returning {len(results)} results for source {source_id}")
        return results, cont

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
            start_time = start  # возвращаем datetime, не строку
            end_time = end
        else:
            order = "DESC"
            start_time = end
            end_time = start
        limit = nb_values if nb_values else 10000
        
        # Логируем границы для отладки
        import logging
        logger = logging.getLogger(__name__)
        logger.debug(
            "_get_bounds: start=%s end=%s nb_values=%s -> start_time=%s end_time=%s order=%s limit=%s",
            start, end, nb_values, start_time, end_time, order, limit
        )
        
        return start_time, end_time, order, limit

    def _format_event(self, event: Any) -> Tuple[str, str, Tuple[Any, ...], List[str]]:
        """
        Форматирование события для вставки в базу данных.
        
        Args:
            event: Событие OPC UA
            
        Returns:
            Кортеж (строки колонок, плейсхолдеры, значения, список имён полей)
        """
        placeholders = []
        ev_variant_binaries = []
        ev_variant_dict = event.get_event_props_as_fields_dict()
        names = list(ev_variant_dict.keys())
        names.sort()
        
        # Если нет полей событий, возвращаем пустые строки
        if not names:
            return "", "", (), []
        
        # Начинаем с $3, так как $1 и $2 уже используются для времени и типа события
        placeholder_start = 3
        for i, name in enumerate(names):
            placeholders.append(f"${placeholder_start + i}")
            ev_variant_binaries.append(variant_to_binary(ev_variant_dict[name]))
        
        return self._list_to_sql_str(names), ", ".join(placeholders), tuple(ev_variant_binaries), names

    def _get_event_columns(self, ev_fields: List[str]) -> str:
        """
        Генерация SQL для колонок событий.
        
        Args:
            ev_fields: Список полей событий
            
        Returns:
            SQL строка с определением колонок
        """
        if not ev_fields:
            return ""
        fields = []
        for field in ev_fields:
            fields.append(f'"{field}" BYTEA')
        return ", ".join(fields)

    async def _ensure_event_dynamic_columns(self, table: str, fields: List[str]) -> None:
        """
        Обеспечивает наличие всех колонок событий (BYTEA) в таблице для динамического набора полей.
        """
        try:
            for field in fields:
                col_exists = await self._fetchval(
                    f'''
                    SELECT COUNT(*) FROM information_schema.columns 
                    WHERE table_name = $1 AND column_name = $2
                    ''',
                    table, field
                )
                if col_exists == 0:
                    self.logger.info(f"Adding missing event column '{field}' to table {table}")
                    await self._execute(
                        f'ALTER TABLE "{table}" ADD COLUMN "{field}" BYTEA'
                    )
        except Exception as e:
            self.logger.warning(f"Failed to ensure dynamic event columns for {table}: {e}")

    async def _get_select_clauses(self, source_id: ua.NodeId, evfilter: Any) -> Tuple[List[str], str]:
        """
        Получение SQL предложений для выборки событий.
        
        Args:
            source_id: Идентификатор источника событий
            evfilter: Фильтр событий
            
        Returns:
            Кортеж (список полей, SQL строка полей)
        """
        s_clauses = []
        for select_clause in evfilter.SelectClauses:
            try:
                if not select_clause.BrowsePath:
                    s_clauses.append(select_clause.Attribute.name)
                else:
                    name = select_clause.BrowsePath[0].Name
                    s_clauses.append(name)
            except AttributeError:
                self.logger.warning(
                    "Historizing PgSQL OPC UA Select Clause Warning for node %s, Clause: %s:", source_id, select_clause
                )
        
        self.logger.debug(
            "_get_select_clauses: source_id=%s s_clauses=%s _event_fields=%s",
            source_id, s_clauses, self._event_fields.get(source_id, [])
        )
        
        # Проверяем, что source_id существует в _event_fields
        if source_id not in self._event_fields:
            self.logger.warning(
                "_get_select_clauses: source_id=%s not found in _event_fields, available keys=%s",
                source_id, list(self._event_fields.keys())
            )
            # Попробуем автоматически зарегистрировать узел с базовыми полями событий
            try:
                await self._auto_register_event_node(source_id)
            except Exception as e:
                self.logger.warning(
                    "Failed to auto-register event node %s: %s",
                    source_id, e
                )
            
            # Проверяем еще раз после попытки регистрации
            if source_id not in self._event_fields:
                # Возвращаем пустой список, но не вызываем ошибку
                return [], ""
        
        # Получаем доступные поля для данного source_id
        available_fields = self._event_fields[source_id]
        if not available_fields:
            self.logger.warning(
                "_get_select_clauses: no fields available for source_id=%s, using fallback fields",
                source_id
            )
            # Используем базовые поля OPC UA событий как fallback
            available_fields = [
                "EventId", "EventType", "SourceName", "Time", "Message", 
                "Severity", "ConditionName", "BranchId", "Retain"
            ]
            # Обновляем _event_fields для будущих запросов
            self._event_fields[source_id] = available_fields
        
        clauses = [x for x in s_clauses if x in available_fields]
        
        if not clauses:
            self.logger.warning(
                "_get_select_clauses: no matching clauses found for source_id=%s, available_fields=%s",
                source_id, self._event_fields.get(source_id, [])
            )
            return [], ""
        
        self.logger.debug(
            "_get_select_clauses: returning clauses=%s for source_id=%s",
            clauses, source_id
        )
        
        return clauses, ", ".join([f'"{x}"' for x in clauses])

    async def _auto_register_event_node(self, source_id: ua.NodeId) -> None:
        """
        Автоматическая регистрация узла событий с базовыми полями.
        
        Args:
            source_id: Идентификатор узла для регистрации
        """
        try:
            # Регистрируем узел с базовыми полями событий OPC UA
            basic_fields = [
                "EventId", "EventType", "SourceName", "Time", "Message", 
                "Severity", "ConditionName", "BranchId", "Retain"
            ]
            
            self._event_fields[source_id] = basic_fields
            self.logger.info(
                "Auto-registered event node %s with basic fields: %s",
                source_id, basic_fields
            )
        except Exception as e:
            self.logger.warning(
                "Failed to auto-register event node %s: %s",
                source_id, e
            )

    @staticmethod
    def _list_to_sql_str(ls: List[str]) -> str:
        """
        Преобразование списка в SQL строку с кавычками.
        
        Args:
            ls: Список строк
            
        Returns:
            SQL строка с элементами в кавычках
        """
        return ", ".join([f'"{item}"' for item in ls])

    async def _drop_conflicting_unique_indexes(self, table: str, partition_col: str) -> None:
        """
        Удаляет уникальные индексы, которые не включают колонку партиционирования TimescaleDB.
        Это требуется, т.к. TimescaleDB запрещает уникальные индексы без включения партиционирующей колонки.
        """
        try:
            # Сначала исправляем дублирующиеся колонки
            await self._fix_duplicate_columns(table, partition_col)
            
            # Просто удаляем все уникальные индексы, которые не содержат partition_col
            # TimescaleDB автоматически создаст нужные индексы
            idx_rows = await self._fetch(
                f'''
                SELECT i.relname as index_name
                FROM pg_class t
                JOIN pg_index ix ON t.oid = ix.indrelid
                JOIN pg_class i ON i.oid = ix.indexrelid
                WHERE t.relname = $1 AND ix.indisunique = true
                ''',
                table
            )
            
            for row in idx_rows:
                index_name = row['index_name']
                self.logger.info(f"Dropping unique index {index_name} on {table}")
                try:
                    await self._execute(f'DROP INDEX IF EXISTS "{index_name}"')
                except Exception as drop_error:
                    self.logger.info(f"Index {index_name} was already dropped or doesn't exist")
                    
        except Exception as e:
            self.logger.warning(f"Failed to drop conflicting unique indexes for {table}: {e}")

    async def _fix_duplicate_columns(self, table: str, partition_col: str) -> None:
        """
        Исправляет дублирующиеся колонки с разным регистром в названии.
        Все колонки приводятся к нижнему регистру.
        """
        try:
            # Получаем все колонки таблицы
            columns = await self._fetch(
                f'''
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = $1
                ''',
                table
            )
            
            column_names = [row['column_name'] for row in columns]
            
            # Просто переименовываем все колонки в нижний регистр, кроме partition_col
            for col in column_names:
                if col == partition_col:
                    continue
                
                standard_name = col.lower()
                if col != standard_name:
                    try:
                        self.logger.info(f"Renaming column '{col}' to '{standard_name}' in table {table}")
                        await self._execute(f'ALTER TABLE "{table}" RENAME COLUMN "{col}" TO "{standard_name}"')
                    except Exception as rename_error:
                        # Если не удалось переименовать (например, колонка уже существует в chunk'е),
                        # просто логируем и продолжаем
                        self.logger.info(f"Column '{col}' already exists as '{standard_name}' in chunks, skipping rename")
                        
        except Exception as e:
            self.logger.warning(f"Failed to fix duplicate columns for {table}: {e}")

    async def _ensure_variable_table_structure(self, table: str) -> None:
        """
        Проверяет и исправляет структуру таблицы переменных.
        
        Args:
            table: Имя таблицы
        """
        try:
            # Проверяем существование колонки _id
            id_exists = await self._fetchval(
                f'''
                SELECT COUNT(*) FROM information_schema.columns 
                WHERE table_name = $1 AND column_name = '_id'
                ''',
                table
            )
            
            if id_exists == 0:
                # Колонка _id отсутствует, добавляем её
                self.logger.info(f"Adding missing column '_id' to table {table}")
                await self._execute(
                    f'ALTER TABLE "{table}" ADD COLUMN _id SERIAL'
                )
            
            # Проверяем существование колонки sourcetimestamp
            result = await self._fetchval(
                f'''
                SELECT COUNT(*) FROM information_schema.columns 
                WHERE table_name = $1 AND column_name = 'sourcetimestamp'
                ''',
                table
            )
            
            if result == 0:
                # Колонка sourcetimestamp отсутствует, добавляем её
                self.logger.info(f"Adding missing column 'sourcetimestamp' to table {table}")
                await self._execute(
                    f'ALTER TABLE "{table}" ADD COLUMN sourcetimestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()'
                )
                
                # Обновляем существующие записи, устанавливая sourcetimestamp = servertimestamp
                await self._execute(
                    f'UPDATE "{table}" SET sourcetimestamp = servertimestamp WHERE sourcetimestamp IS NULL'
                )
                
                # Убираем значение по умолчанию
                await self._execute(
                    f'ALTER TABLE "{table}" ALTER COLUMN sourcetimestamp DROP DEFAULT'
                )
                
            # Проверяем существование других базовых колонок
            for col_name, col_type in [
                ('servertimestamp', 'TIMESTAMPTZ'),
                ('statuscode', 'INTEGER'),
                ('value', 'TEXT'),
                ('varianttype', 'TEXT'),
                ('variantbinary', 'BYTEA')
            ]:
                col_exists = await self._fetchval(
                    f'''
                    SELECT COUNT(*) FROM information_schema.columns 
                    WHERE table_name = $1 AND column_name = $2
                    ''',
                    table, col_name
                )
                
                if col_exists == 0:
                    # Колонка отсутствует, добавляем её
                    self.logger.info(f"Adding missing column '{col_name}' to table {table}")
                    await self._execute(
                        f'ALTER TABLE "{table}" ADD COLUMN {col_name} {col_type}'
                    )
            
            # Проверяем и обновляем первичный ключ после всех изменений структуры
            await self._ensure_variable_table_primary_key(table)
                
        except Exception as e:
            self.logger.warning(f"Failed to ensure variable table structure for {table}: {e}")

    async def _ensure_event_table_structure(self, table: str) -> None:
        """
        Проверяет и исправляет структуру таблицы событий.
        Все колонки создаются в нижнем регистре.
        
        Args:
            table: Имя таблицы
        """
        try:
            # Проверяем существование колонки _id
            id_exists = await self._fetchval(
                f'''
                SELECT COUNT(*) FROM information_schema.columns 
                WHERE table_name = $1 AND column_name = '_id'
                ''',
                table
            )
            
            if id_exists == 0:
                # Колонка _id отсутствует, добавляем её
                self.logger.info(f"Adding missing column '_id' to table {table}")
                await self._execute(
                    f'ALTER TABLE "{table}" ADD COLUMN _id SERIAL'
                )
            
            # Проверяем существование колонки _timestamp (всегда в нижнем регистре)
            timestamp_exists = await self._fetchval(
                f'''
                SELECT COUNT(*) FROM information_schema.columns 
                WHERE table_name = $1 AND column_name = '_timestamp'
                ''',
                table
            )
            
            if timestamp_exists == 0:
                # Колонка _timestamp отсутствует, добавляем её
                self.logger.info(f"Adding missing column '_timestamp' to table {table}")
                await self._execute(
                    f'ALTER TABLE "{table}" ADD COLUMN _timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()'
                )
                
                # Обновляем существующие записи, устанавливая _timestamp = NOW()
                await self._execute(
                    f'UPDATE "{table}" SET _timestamp = NOW() WHERE _timestamp IS NULL'
                )
                
                # Убираем значение по умолчанию
                await self._execute(
                    f'ALTER TABLE "{table}" ALTER COLUMN _timestamp DROP DEFAULT'
                )
                
            # Проверяем существование колонки _eventtypename (всегда в нижнем регистре)
            event_type_exists = await self._fetchval(
                f'''
                SELECT COUNT(*) FROM information_schema.columns 
                WHERE table_name = $1 AND column_name = '_eventtypename'
                ''',
                table
            )
            
            if event_type_exists == 0:
                # Колонка _eventtypename отсутствует, добавляем её
                self.logger.info(f"Adding missing column '_eventtypename' to table {table}")
                await self._execute(
                    f'ALTER TABLE "{table}" ADD COLUMN _eventtypename TEXT'
                )
            
            # Проверяем и обновляем первичный ключ после всех изменений структуры
            await self._ensure_event_table_primary_key(table)
                
        except Exception as e:
            self.logger.warning(f"Failed to ensure event table structure for {table}: {e}")

    async def _ensure_variable_table_primary_key(self, table: str) -> None:
        """
        Проверяет и обновляет первичный ключ таблицы переменных.
        
        Args:
            table: Имя таблицы
        """
        try:
            # Просто удаляем существующий первичный ключ, если он есть
            pk_exists = await self._fetchval(
                f'''
                SELECT COUNT(*) FROM information_schema.table_constraints 
                WHERE table_name = $1 AND constraint_type = 'PRIMARY KEY'
                ''',
                table
            )
            
            if pk_exists > 0:
                # Получаем имя существующего первичного ключа
                old_pk_name = await self._fetchval(
                    f'''
                    SELECT constraint_name 
                    FROM information_schema.table_constraints 
                    WHERE table_name = $1 AND constraint_type = 'PRIMARY KEY'
                    ''',
                    table
                )
                
                if old_pk_name:
                    self.logger.info(f"Dropping old primary key {old_pk_name} from table {table}")
                    await self._execute(f'ALTER TABLE "{table}" DROP CONSTRAINT "{old_pk_name}"')
            
            # Создаем новый первичный ключ с sourcetimestamp
            self.logger.info(f"Adding primary key to table {table}")
            await self._execute(
                f'ALTER TABLE "{table}" ADD CONSTRAINT "{table}_pk" PRIMARY KEY (_id, sourcetimestamp)'
            )
                
        except Exception as e:
            self.logger.warning(f"Failed to ensure variable table primary key for {table}: {e}")

    async def _ensure_event_table_primary_key(self, table: str) -> None:
        """
        Проверяет и обновляет первичный ключ таблицы событий.
        
        Args:
            table: Имя таблицы
        """
        try:
            # Просто удаляем существующий первичный ключ, если он есть
            pk_exists = await self._fetchval(
                f'''
                SELECT COUNT(*) FROM information_schema.table_constraints 
                WHERE table_name = $1 AND constraint_type = 'PRIMARY KEY'
                ''',
                table
            )
            
            if pk_exists > 0:
                # Получаем имя существующего первичного ключа
                old_pk_name = await self._fetchval(
                    f'''
                    SELECT constraint_name 
                    FROM information_schema.table_constraints 
                    WHERE table_name = $1 AND constraint_type = 'PRIMARY KEY'
                    ''',
                    table
                )
                
                if old_pk_name:
                    self.logger.info(f"Dropping old primary key {old_pk_name} from table {table}")
                    await self._execute(f'ALTER TABLE "{table}" DROP CONSTRAINT "{old_pk_name}"')
            
            # Создаем новый первичный ключ с _timestamp
            self.logger.info(f"Adding primary key to table {table}")
            await self._execute(
                f'ALTER TABLE "{table}" ADD CONSTRAINT "{table}_pk" PRIMARY KEY (_id, _timestamp)'
            )
                
        except Exception as e:
            self.logger.warning(f"Failed to ensure event table primary key for {table}: {e}")
