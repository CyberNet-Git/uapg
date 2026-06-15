-- Variables V2 skeleton (roadmap release 2+)

CREATE TABLE IF NOT EXISTS "{schema}".variables_ts (
    value_id BIGSERIAL,
    variable_id BIGINT NOT NULL,
    sourcetimestamp TIMESTAMPTZ NOT NULL,
    servertimestamp TIMESTAMPTZ,
    statuscode INTEGER,
    schema_version INTEGER NOT NULL DEFAULT 1,
    legacy_row_id BIGINT,
    value_double DOUBLE PRECISION,
    value_int64 BIGINT,
    value_bool BOOLEAN,
    value_text TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (value_id, sourcetimestamp)
);

CREATE TABLE IF NOT EXISTS "{schema}".variable_schema (
    id BIGSERIAL PRIMARY KEY,
    variable_id BIGINT NOT NULL,
    schema_version INTEGER NOT NULL,
    node_id TEXT NOT NULL,
    opc_datatype TEXT,
    sql_type TEXT,
    aggregate_eligible BOOLEAN NOT NULL DEFAULT FALSE,
    effective_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (variable_id, schema_version)
);

CREATE INDEX IF NOT EXISTS idx_variables_ts_var_ts
    ON "{schema}".variables_ts (variable_id, sourcetimestamp DESC);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        IF NOT EXISTS (
            SELECT 1 FROM timescaledb_information.hypertables
            WHERE hypertable_schema = '{schema}' AND hypertable_name = 'variables_ts'
        ) THEN
            PERFORM create_hypertable(
                '"{schema}"."variables_ts"',
                'sourcetimestamp',
                partitioning_column => 'variable_id',
                number_partitions => 128,
                if_not_exists => TRUE
            );
        END IF;
    END IF;
END $$;

INSERT INTO "{schema}".uapg_backfill_state (domain, last_legacy_id, rows_processed)
VALUES ('variables', 0, 0)
ON CONFLICT (domain) DO NOTHING;
