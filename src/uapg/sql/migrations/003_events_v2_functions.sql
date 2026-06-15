-- Events V2 stored functions

CREATE OR REPLACE FUNCTION "{schema}".uapg_save_event_v2(
    p_source_id BIGINT,
    p_event_type_id BIGINT,
    p_event_timestamp TIMESTAMPTZ,
    p_event_data JSONB,
    p_schema_version INTEGER DEFAULT 1
)
RETURNS TABLE (legacy_row_id BIGINT, event_id BIGINT)
LANGUAGE plpgsql
AS $$
DECLARE
    v_legacy_id BIGINT;
    v_event_id BIGINT;
BEGIN
    INSERT INTO "{schema}".events_history (
        source_id, event_type_id, event_timestamp, event_data
    ) VALUES (
        p_source_id, p_event_type_id, p_event_timestamp, p_event_data
    )
    ON CONFLICT (source_id, event_timestamp) DO NOTHING
    RETURNING id INTO v_legacy_id;

    IF v_legacy_id IS NULL THEN
        SELECT eh.id INTO v_legacy_id
        FROM "{schema}".events_history eh
        WHERE eh.source_id = p_source_id
          AND eh.event_timestamp = p_event_timestamp
        LIMIT 1;
    END IF;

    INSERT INTO "{schema}".events_ts (
        source_id, event_type_id, event_timestamp, schema_version, legacy_row_id
    ) VALUES (
        p_source_id, p_event_type_id, p_event_timestamp, p_schema_version, v_legacy_id
    )
    ON CONFLICT (source_id, event_timestamp) DO UPDATE SET
        legacy_row_id = COALESCE("{schema}".events_ts.legacy_row_id, EXCLUDED.legacy_row_id)
    RETURNING events_ts.event_id INTO v_event_id;

    legacy_row_id := v_legacy_id;
    event_id := v_event_id;
    RETURN NEXT;
END;
$$;

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
    RETURN QUERY
    SELECT
        e.event_id,
        e.event_timestamp,
        e.event_type_id,
        e.legacy_row_id,
        e.schema_version
    FROM "{schema}".events_ts e
    WHERE e.source_id = p_source_id
      AND e.event_timestamp BETWEEN p_start AND p_end
      AND (p_event_type_ids IS NULL OR e.event_type_id = ANY (p_event_type_ids))
      AND (
          p_cursor_ts IS NULL
          OR (
              p_order = 'DESC' AND (
                  e.event_timestamp < p_cursor_ts
                  OR (e.event_timestamp = p_cursor_ts AND e.event_id < p_cursor_event_id)
              )
          )
          OR (
              p_order = 'ASC' AND (
                  e.event_timestamp > p_cursor_ts
                  OR (e.event_timestamp = p_cursor_ts AND e.event_id > p_cursor_event_id)
              )
          )
      )
    ORDER BY
        CASE WHEN p_order = 'DESC' THEN e.event_timestamp END DESC,
        CASE WHEN p_order = 'DESC' THEN e.event_id END DESC,
        CASE WHEN p_order = 'ASC' THEN e.event_timestamp END ASC,
        CASE WHEN p_order = 'ASC' THEN e.event_id END ASC
    LIMIT p_limit;
END;
$$;

CREATE OR REPLACE FUNCTION "{schema}".uapg_explain_event_filter(
    p_source_id BIGINT,
    p_start TIMESTAMPTZ,
    p_end TIMESTAMPTZ,
    p_limit INTEGER,
    p_order TEXT DEFAULT 'DESC',
    p_event_type_ids BIGINT[] DEFAULT NULL
)
RETURNS TEXT
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_plan TEXT;
BEGIN
    EXECUTE format(
        'EXPLAIN (FORMAT JSON) SELECT * FROM %I.uapg_read_events_v2($1, $2, $3, $4, $5, $6, NULL, NULL)',
        '{schema}'
    )
    INTO v_plan
    USING p_source_id, p_start, p_end, p_limit, p_order, p_event_type_ids;
    RETURN v_plan;
END;
$$;

CREATE OR REPLACE PROCEDURE "{schema}".uapg_sync_event_type_schema(
    p_event_type_id BIGINT,
    p_node_id TEXT,
    p_parent_node_id TEXT,
    p_fields JSONB,
    p_schema_version INTEGER,
    p_physical_table TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO "{schema}".event_type_schema (
        event_type_id, schema_version, node_id, parent_node_id, fields
    ) VALUES (
        p_event_type_id, p_schema_version, p_node_id, p_parent_node_id, p_fields
    )
    ON CONFLICT (event_type_id, schema_version) DO UPDATE SET
        node_id = EXCLUDED.node_id,
        parent_node_id = EXCLUDED.parent_node_id,
        fields = EXCLUDED.fields,
        effective_from = NOW();

    INSERT INTO "{schema}".event_type_storage (
        event_type_id, physical_table, schema_version
    ) VALUES (
        p_event_type_id, p_physical_table, p_schema_version
    )
    ON CONFLICT (event_type_id) DO UPDATE SET
        physical_table = EXCLUDED.physical_table,
        schema_version = EXCLUDED.schema_version,
        updated_at = NOW();
END;
$$;

CREATE OR REPLACE PROCEDURE "{schema}".uapg_backfill_events_batch(
    p_batch_size INTEGER,
    INOUT p_last_legacy_id BIGINT,
    INOUT p_rows_processed BIGINT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_row RECORD;
    v_event_id BIGINT;
BEGIN
    FOR v_row IN
        SELECT eh.id, eh.source_id, eh.event_type_id, eh.event_timestamp
        FROM "{schema}".events_history eh
        WHERE eh.id > p_last_legacy_id
        ORDER BY eh.id
        LIMIT p_batch_size
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM "{schema}".events_ts et
            WHERE et.legacy_row_id = v_row.id
        ) THEN
            INSERT INTO "{schema}".events_ts (
                source_id, event_type_id, event_timestamp, schema_version, legacy_row_id
            ) VALUES (
                v_row.source_id, v_row.event_type_id, v_row.event_timestamp, 1, v_row.id
            )
            RETURNING event_id INTO v_event_id;
            p_rows_processed := p_rows_processed + 1;
        END IF;
        p_last_legacy_id := v_row.id;
    END LOOP;

    UPDATE "{schema}".uapg_backfill_state
    SET last_legacy_id = p_last_legacy_id,
        rows_processed = p_rows_processed,
        updated_at = NOW()
    WHERE domain = 'events';
END;
$$;
