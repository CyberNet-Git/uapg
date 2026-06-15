-- Timescale advanced policies for events_ts

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        BEGIN
            PERFORM add_retention_policy('"{schema}"."events_ts"', INTERVAL '365 days', if_not_exists => TRUE);
        EXCEPTION WHEN OTHERS THEN
            NULL;
        END;
        BEGIN
            PERFORM add_compression_policy('"{schema}"."events_ts"', INTERVAL '7 days', if_not_exists => TRUE);
        EXCEPTION WHEN OTHERS THEN
            NULL;
        END;
    END IF;
END $$;

-- Optional hourly continuous aggregate (analytics only)
CREATE MATERIALIZED VIEW IF NOT EXISTS "{schema}".uapg_events_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', event_timestamp) AS bucket,
    source_id,
    event_type_id,
    count(*) AS event_count
FROM "{schema}".events_ts
GROUP BY bucket, source_id, event_type_id
WITH NO DATA;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        BEGIN
            PERFORM add_continuous_aggregate_policy(
                '"{schema}"."uapg_events_hourly"',
                start_offset => INTERVAL '3 hours',
                end_offset => INTERVAL '1 hour',
                schedule_interval => INTERVAL '1 hour',
                if_not_exists => TRUE
            );
        EXCEPTION WHEN OTHERS THEN
            NULL;
        END;
    END IF;
END $$;
