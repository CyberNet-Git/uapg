-- Events V2 anchor + schema registry

CREATE TABLE IF NOT EXISTS "{schema}".events_ts (
    event_id BIGSERIAL,
    source_id BIGINT NOT NULL,
    event_type_id BIGINT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    legacy_row_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Timescale space partition on source_id requires PK to include source_id
    PRIMARY KEY (source_id, event_timestamp, event_id)
);

CREATE TABLE IF NOT EXISTS "{schema}".event_type_schema (
    id BIGSERIAL PRIMARY KEY,
    event_type_id BIGINT NOT NULL,
    schema_version INTEGER NOT NULL,
    node_id TEXT NOT NULL,
    parent_node_id TEXT,
    fields JSONB NOT NULL DEFAULT '[]'::jsonb,
    effective_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (event_type_id, schema_version)
);

CREATE TABLE IF NOT EXISTS "{schema}".event_type_storage (
    event_type_id BIGINT PRIMARY KEY,
    physical_table TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_ts_type_source
    ON "{schema}".events_ts (event_type_id, source_id, event_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_events_ts_legacy_row
    ON "{schema}".events_ts (legacy_row_id)
    WHERE legacy_row_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_event_type_schema_type
    ON "{schema}".event_type_schema (event_type_id, schema_version DESC);

-- Timescale hypertable (idempotent); indexes after conversion
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        IF NOT EXISTS (
            SELECT 1 FROM timescaledb_information.hypertables
            WHERE hypertable_schema = '{schema}' AND hypertable_name = 'events_ts'
        ) THEN
            PERFORM create_hypertable(
                '"{schema}"."events_ts"',
                'event_timestamp',
                partitioning_column => 'source_id',
                number_partitions => 64,
                if_not_exists => TRUE
            );
        END IF;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_events_ts_source_ts
    ON "{schema}".events_ts (source_id, event_timestamp DESC, event_id DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_events_ts_source_ts_uniq
    ON "{schema}".events_ts (source_id, event_timestamp);
