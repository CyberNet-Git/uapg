-- Sargable uapg_read_events_v2 (schema placeholder: {schema})
--
-- 003 сортировал результат через `ORDER BY CASE WHEN p_order = 'DESC' THEN ... END`.
-- Планировщик не сопоставляет такое выражение с индексом idx_events_ts_source_ts
-- (source_id, event_timestamp DESC, event_id DESC), поэтому на каждом чтении без
-- push-down шла полная сортировка окна вместо раннего выхода по LIMIT. Предикат
-- курсора по той же причине не использовался как индексный: вся пагинация
-- перечитывала окно с начала.
--
-- Здесь четыре отдельных RETURN QUERY (порядок × наличие курсора), в каждом
-- обычный ORDER BY. Для варианта с курсором граница окна сужается самим курсором,
-- а тай-брейк записан строковым сравнением, которое PostgreSQL умеет применять
-- как индексный предикат.
--
-- Сигнатура и RETURNS TABLE не меняются: ProcedureGateway.read_events_v2 вызывает
-- функцию как раньше. 003 на существующих БД уже отмечен применённым, поэтому
-- правка идёт отдельным файлом.

CREATE OR REPLACE FUNCTION "{schema}".uapg_read_events_v2(
    p_source_id BIGINT,
    p_start TIMESTAMPTZ,
    p_end TIMESTAMPTZ,
    p_limit INTEGER,
    p_order TEXT DEFAULT 'DESC',
    p_event_type_ids BIGINT[] DEFAULT NULL,
    p_cursor_ts TIMESTAMPTZ DEFAULT NULL,
    p_cursor_event_id BIGINT DEFAULT NULL
)
RETURNS TABLE (
    event_id BIGINT,
    event_timestamp TIMESTAMPTZ,
    event_type_id BIGINT,
    legacy_row_id BIGINT,
    schema_version INTEGER
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
    IF p_order = 'ASC' THEN
        IF p_cursor_ts IS NULL THEN
            RETURN QUERY
            SELECT e.event_id, e.event_timestamp, e.event_type_id,
                   e.legacy_row_id, e.schema_version
            FROM "{schema}".events_ts e
            WHERE e.source_id = p_source_id
              AND e.event_timestamp BETWEEN p_start AND p_end
              AND (p_event_type_ids IS NULL OR e.event_type_id = ANY (p_event_type_ids))
            ORDER BY e.event_timestamp ASC, e.event_id ASC
            LIMIT p_limit;
        ELSE
            RETURN QUERY
            SELECT e.event_id, e.event_timestamp, e.event_type_id,
                   e.legacy_row_id, e.schema_version
            FROM "{schema}".events_ts e
            WHERE e.source_id = p_source_id
              AND e.event_timestamp BETWEEN p_cursor_ts AND p_end
              AND (e.event_timestamp, e.event_id)
                  > (p_cursor_ts, COALESCE(p_cursor_event_id, (-9223372036854775808)::bigint))
              AND (p_event_type_ids IS NULL OR e.event_type_id = ANY (p_event_type_ids))
            ORDER BY e.event_timestamp ASC, e.event_id ASC
            LIMIT p_limit;
        END IF;
    ELSE
        IF p_cursor_ts IS NULL THEN
            RETURN QUERY
            SELECT e.event_id, e.event_timestamp, e.event_type_id,
                   e.legacy_row_id, e.schema_version
            FROM "{schema}".events_ts e
            WHERE e.source_id = p_source_id
              AND e.event_timestamp BETWEEN p_start AND p_end
              AND (p_event_type_ids IS NULL OR e.event_type_id = ANY (p_event_type_ids))
            ORDER BY e.event_timestamp DESC, e.event_id DESC
            LIMIT p_limit;
        ELSE
            RETURN QUERY
            SELECT e.event_id, e.event_timestamp, e.event_type_id,
                   e.legacy_row_id, e.schema_version
            FROM "{schema}".events_ts e
            WHERE e.source_id = p_source_id
              AND e.event_timestamp BETWEEN p_start AND p_cursor_ts
              AND (e.event_timestamp, e.event_id)
                  < (p_cursor_ts, COALESCE(p_cursor_event_id, 9223372036854775807::bigint))
              AND (p_event_type_ids IS NULL OR e.event_type_id = ANY (p_event_type_ids))
            ORDER BY e.event_timestamp DESC, e.event_id DESC
            LIMIT p_limit;
        END IF;
    END IF;
END;
$$;
