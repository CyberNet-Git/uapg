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
        _db (asyncpg.Connection): Соединение с базой данных
    """

    def __init__(
        self, 
        user: str = 'postgres', 
        password: str = 'postmaster', 
        database: str = 'opcua', 
        host: str = '127.0.0.1', 
        max_history_data_response_size: int = 10000
    ) -> None:
        self.max_history_data_response_size = max_history_data_response_size
        self.logger = logging.getLogger(__name__)
        self._datachanges_period = {}
        self._conn_params = dict(user=user, password=password, database=database, host=host)
        self._event_fields = {}
        self._db: asyncpg.Connection = None

    async def init(self) -> None:
        """Инициализация соединения с базой данных."""
        self._db = await asyncpg.connect(**self._conn_params)

    async def stop(self) -> None:
        """Закрытие соединения с базой данных."""
        await self._db.close()
        self.logger.info("Historizing PgSQL connection closed")

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
        table = self._get_table_name(node_id)
        self._datachanges_period[node_id] = period, count
        try:
            validate_table_name(table)
            await self._db.execute(
                f'''
                CREATE TABLE IF NOT EXISTS "{table}" (
                    _id SERIAL PRIMARY KEY,
                    servertimestamp TIMESTAMPTZ NOT NULL,
                    sourcetimestamp TIMESTAMPTZ NOT NULL,
                    statuscode INTEGER,
                    value TEXT,
                    varianttype TEXT,
                    variantbinary BYTEA
                );
                '''
            )
            # Преобразуем таблицу в hypertable TimescaleDB
            await self._db.execute(
                f'SELECT create_hypertable(\'{table}\', \'sourcetimestamp\', if_not_exists => TRUE);'
            )
            # Индекс по времени для ускорения запросов
            await self._db.execute(
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
            await self._db.execute(f'DELETE FROM "{table}" WHERE {condition}', *args)
        except Exception as e:
            self.logger.error("Historizing PgSQL Delete Old Data Error for %s: %s", node_id, e)

    async def save_node_value(self, node_id: ua.NodeId, datavalue: ua.DataValue) -> None:
        """
        Сохранение значения узла в историю.
        
        Args:
            node_id: Идентификатор узла OPC UA
            datavalue: Значение данных для сохранения
        """
        table = self._get_table_name(node_id)
        try:
            validate_table_name(table)
            await self._db.execute(
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
        table = self._get_table_name(node_id)
        start_time, end_time, order, limit = self._get_bounds(start, end, nb_values)
        cont = None
        results = []
        try:
            validate_table_name(table)
            rows = await self._db.fetch(
                f'SELECT * FROM "{table}" WHERE "sourcetimestamp" BETWEEN $1 AND $2 ORDER BY "_id" {order} LIMIT $3',
                start_time, end_time, limit
            )
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
        table = self._get_table_name(source_id)
        columns = self._get_event_columns(ev_fields)
        try:
            validate_table_name(table)
            await self._db.execute(
                f'''
                CREATE TABLE IF NOT EXISTS "{table}" (
                    _id SERIAL PRIMARY KEY,
                    _Timestamp TIMESTAMPTZ NOT NULL,
                    _EventTypeName TEXT,
                    {columns}
                );
                '''
            )
            # Преобразуем таблицу событий в hypertable TimescaleDB
            await self._db.execute(
                f'SELECT create_hypertable(\'{table}\', \'_Timestamp\', if_not_exists => TRUE);'
            )
            await self._db.execute(
                f'CREATE INDEX IF NOT EXISTS "{table}_timestamp_idx" ON "{table}" ("_Timestamp");'
            )
        except Exception as e:
            self.logger.info("Historizing PgSQL Table Creation Error for events from %s: %s", source_id, e)

    async def save_event(self, event: Any) -> None:
        """
        Сохранение события в историю.
        
        Args:
            event: Событие OPC UA для сохранения
        """
        table = self._get_table_name(event.SourceNode)
        columns, placeholders, evtup = self._format_event(event)
        event_type = event.EventType
        try:
            validate_table_name(table)
            await self._db.execute(
                f'INSERT INTO "{table}" (_Timestamp, _EventTypeName, {columns}) VALUES ($1, $2, {placeholders})',
                event.Time, str(event_type), *evtup
            )
        except Exception as e:
            self.logger.error("Historizing PgSQL Insert Error for events from %s: %s", event.SourceNode, e)
        period = self._datachanges_period.get(event.emitting_node)
        if period:
            date_limit = datetime.now(timezone.utc) - period
            try:
                validate_table_name(table)
                await self._db.execute(f'DELETE FROM "{table}" WHERE _Timestamp < $1', date_limit)
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
        table = self._get_table_name(source_id)
        start_time, end_time, order, limit = self._get_bounds(start, end, nb_values)
        clauses, clauses_str = self._get_select_clauses(source_id, evfilter)
        cont = None
        cont_timestamps = []
        results = []
        try:
            validate_table_name(table)
            rows = await self._db.fetch(
                f'SELECT "_Timestamp", {clauses_str} FROM "{table}" WHERE "_Timestamp" BETWEEN $1 AND $2 ORDER BY "_id" {order} LIMIT $3',
                start_time, end_time, limit
            )
            for row in rows:
                fdict = {}
                cont_timestamps.append(row['_timestamp'])
                for i, field in enumerate(clauses):
                    val = row[field.lower()]
                    if val is not None:
                        fdict[clauses[i]] = variant_from_binary(Buffer(val))
                    else:
                        fdict[clauses[i]] = ua.Variant(None)
                results.append(Event.from_field_dict(fdict))
        except Exception as e:
            self.logger.error("Historizing PgSQL Read Error events for node %s: %s", source_id, e)
        if len(results) > self.max_history_data_response_size:
            cont = cont_timestamps[self.max_history_data_response_size]
        results = results[: self.max_history_data_response_size]
        return results, cont

    def _get_table_name(self, node_id: ua.NodeId) -> str:
        """
        Генерация имени таблицы для узла.
        
        Args:
            node_id: Идентификатор узла OPC UA
            
        Returns:
            Имя таблицы в формате "NamespaceIndex_Identifier"
        """
        return f"{node_id.NamespaceIndex}_{node_id.Identifier}"

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
        return start_time, end_time, order, limit

    def _format_event(self, event: Any) -> Tuple[str, str, Tuple[Any, ...]]:
        """
        Форматирование события для вставки в базу данных.
        
        Args:
            event: Событие OPC UA
            
        Returns:
            Кортеж (строки колонок, плейсхолдеры, значения)
        """
        placeholders = []
        ev_variant_binaries = []
        ev_variant_dict = event.get_event_props_as_fields_dict()
        names = list(ev_variant_dict.keys())
        names.sort()
        for name in names:
            placeholders.append(f"${{len(placeholders)+3}}")  # $1, $2 for time/type, $3... for fields
            ev_variant_binaries.append(variant_to_binary(ev_variant_dict[name]))
        return self._list_to_sql_str(names), ", ".join(placeholders), tuple(ev_variant_binaries)

    def _get_event_columns(self, ev_fields: List[str]) -> str:
        """
        Генерация SQL для колонок событий.
        
        Args:
            ev_fields: Список полей событий
            
        Returns:
            SQL строка с определением колонок
        """
        fields = []
        for field in ev_fields:
            fields.append(f'"{field}" BYTEA')
        return ", ".join(fields)

    def _get_select_clauses(self, source_id: ua.NodeId, evfilter: Any) -> Tuple[List[str], str]:
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
        clauses = [x for x in s_clauses if x in self._event_fields[source_id]]
        return clauses, ", ".join([f'"{x}"' for x in clauses])

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
