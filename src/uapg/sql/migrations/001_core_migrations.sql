-- Core migration tracking (schema placeholder: {schema})
CREATE TABLE IF NOT EXISTS "{schema}".uapg_schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS "{schema}".uapg_backfill_state (
    domain TEXT PRIMARY KEY,
    last_legacy_id BIGINT NOT NULL DEFAULT 0,
    rows_processed BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO "{schema}".uapg_backfill_state (domain, last_legacy_id, rows_processed)
VALUES ('events', 0, 0)
ON CONFLICT (domain) DO NOTHING;
