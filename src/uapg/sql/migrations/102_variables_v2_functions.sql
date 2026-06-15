-- Variables V2 stored function stubs (roadmap release 2+)

CREATE OR REPLACE FUNCTION "{schema}".uapg_read_variables_raw_v2(
    p_variable_id BIGINT,
    p_start TIMESTAMPTZ,
    p_end TIMESTAMPTZ,
    p_limit INTEGER,
    p_order TEXT DEFAULT 'DESC',
    p_cursor_ts TIMESTAMPTZ DEFAULT NULL,
    p_cursor_value_id BIGINT DEFAULT NULL
)
RETURNS TABLE (
    value_id BIGINT,
    sourcetimestamp TIMESTAMPTZ,
    servertimestamp TIMESTAMPTZ,
    statuscode INTEGER,
    value_double DOUBLE PRECISION,
    value_int64 BIGINT,
    value_bool BOOLEAN,
    value_text TEXT,
    legacy_row_id BIGINT
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
    RETURN QUERY
    SELECT
        v.value_id,
        v.sourcetimestamp,
        v.servertimestamp,
        v.statuscode,
        v.value_double,
        v.value_int64,
        v.value_bool,
        v.value_text,
        v.legacy_row_id
    FROM "{schema}".variables_ts v
    WHERE v.variable_id = p_variable_id
      AND v.sourcetimestamp BETWEEN p_start AND p_end
    ORDER BY
        CASE WHEN p_order = 'DESC' THEN v.sourcetimestamp END DESC,
        CASE WHEN p_order = 'DESC' THEN v.value_id END DESC,
        CASE WHEN p_order = 'ASC' THEN v.sourcetimestamp END ASC,
        CASE WHEN p_order = 'ASC' THEN v.value_id END ASC
    LIMIT p_limit;
END;
$$;

CREATE OR REPLACE FUNCTION "{schema}".uapg_read_variables_processed_v2(
    p_plan JSONB
)
RETURNS TABLE (
    bucket TIMESTAMPTZ,
    variable_id BIGINT,
    aggregate_value DOUBLE PRECISION,
    sample_count BIGINT
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_variable_id BIGINT;
    v_start TIMESTAMPTZ;
    v_end TIMESTAMPTZ;
    v_interval INTERVAL;
    v_aggregate TEXT;
BEGIN
    v_variable_id := (p_plan->>'variable_id')::BIGINT;
    v_start := (p_plan->>'start')::TIMESTAMPTZ;
    v_end := (p_plan->>'end')::TIMESTAMPTZ;
    v_interval := ((p_plan->>'processing_interval_ms')::BIGINT || ' milliseconds')::INTERVAL;
    v_aggregate := COALESCE(p_plan->>'aggregate', 'Average');

    RETURN QUERY
    SELECT
        time_bucket(v_interval, v.sourcetimestamp) AS bucket,
        v.variable_id,
        CASE
            WHEN v_aggregate IN ('Average', 'TimeAverage') THEN avg(v.value_double)
            WHEN v_aggregate = 'Minimum' THEN min(v.value_double)
            WHEN v_aggregate = 'Maximum' THEN max(v.value_double)
            WHEN v_aggregate = 'Total' THEN sum(v.value_double)
            ELSE avg(v.value_double)
        END AS aggregate_value,
        count(*)::BIGINT AS sample_count
    FROM "{schema}".variables_ts v
    WHERE v.variable_id = v_variable_id
      AND v.sourcetimestamp BETWEEN v_start AND v_end
      AND v.value_double IS NOT NULL
    GROUP BY bucket, v.variable_id
    ORDER BY bucket;
END;
$$;

CREATE OR REPLACE FUNCTION "{schema}".uapg_read_variables_at_time_v2(
    p_variable_id BIGINT,
    p_timestamps TIMESTAMPTZ[]
)
RETURNS TABLE (
    requested_ts TIMESTAMPTZ,
    value_id BIGINT,
    sourcetimestamp TIMESTAMPTZ,
    value_double DOUBLE PRECISION,
    value_text TEXT
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
    RETURN QUERY
    SELECT
        t.ts AS requested_ts,
        v.value_id,
        v.sourcetimestamp,
        v.value_double,
        v.value_text
    FROM unnest(p_timestamps) AS t(ts)
    LEFT JOIN LATERAL (
        SELECT vt.*
        FROM "{schema}".variables_ts vt
        WHERE vt.variable_id = p_variable_id
          AND vt.sourcetimestamp <= t.ts
        ORDER BY vt.sourcetimestamp DESC, vt.value_id DESC
        LIMIT 1
    ) v ON TRUE;
END;
$$;
