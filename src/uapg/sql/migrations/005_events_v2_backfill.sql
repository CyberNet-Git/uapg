-- Set-based events backfill + курсор typed-бэкфила (schema placeholder: {schema})
--
-- 003 определял uapg_backfill_events_batch процедурой с построчным циклом FOR ... LOOP,
-- которую никто не вызывал: ProcedureGateway дублировал ту же логику на Python и делал
-- 1+2N round-trip на батч. Процедура заменяется функцией — по образцу uapg_save_event_v2,
-- которую гейтвей вызывает как SELECT ... FROM ...(...).
--
-- 003 на существующих БД уже отмечен применённым, поэтому правка идёт отдельным файлом.

DROP PROCEDURE IF EXISTS "{schema}".uapg_backfill_events_batch(INTEGER, BIGINT, BIGINT);

CREATE OR REPLACE FUNCTION "{schema}".uapg_backfill_events_batch(
    p_batch_size INTEGER,
    p_last_legacy_id BIGINT,
    p_rows_processed BIGINT
)
RETURNS TABLE (last_legacy_id BIGINT, rows_processed BIGINT, rows_inserted BIGINT)
LANGUAGE plpgsql
AS $$
DECLARE
    v_last BIGINT := p_last_legacy_id;
    v_inserted BIGINT := 0;
BEGIN
    -- ORDER BY eh.id ... LIMIT обслуживается индексом idx_events_history_id.
    WITH batch AS (
        SELECT eh.id, eh.source_id, eh.event_type_id, eh.event_timestamp
        FROM "{schema}".events_history eh
        WHERE eh.id > p_last_legacy_id
        ORDER BY eh.id
        LIMIT p_batch_size
    ),
    ins AS (
        INSERT INTO "{schema}".events_ts (
            source_id, event_type_id, event_timestamp, schema_version, legacy_row_id
        )
        SELECT b.source_id, b.event_type_id, b.event_timestamp, 1, b.id
        FROM batch b
        WHERE NOT EXISTS (
            SELECT 1 FROM "{schema}".events_ts et
            WHERE et.legacy_row_id = b.id
        )
        ON CONFLICT DO NOTHING
        RETURNING 1
    )
    SELECT COALESCE(max(b.id), p_last_legacy_id), (SELECT count(*) FROM ins)
    INTO v_last, v_inserted
    FROM batch b;

    -- Watermark двигается до максимального id батча, включая строки, которые уже были
    -- в events_ts от dual-write; rows_processed растёт только на фактически вставленные.
    INSERT INTO "{schema}".uapg_backfill_state (domain, last_legacy_id, rows_processed)
    VALUES ('events', v_last, p_rows_processed + v_inserted)
    ON CONFLICT (domain) DO UPDATE SET
        last_legacy_id = EXCLUDED.last_legacy_id,
        rows_processed = EXCLUDED.rows_processed,
        updated_at = NOW();

    RETURN QUERY SELECT v_last, p_rows_processed + v_inserted, v_inserted;
END;
$$;

-- Курсор typed-бэкфила: отдельный domain в уже существующей uapg_backfill_state,
-- last_legacy_id хранит events_ts.legacy_row_id, до которого дошёл проход.
INSERT INTO "{schema}".uapg_backfill_state (domain, last_legacy_id, rows_processed)
VALUES ('events_typed', 0, 0)
ON CONFLICT (domain) DO NOTHING;
