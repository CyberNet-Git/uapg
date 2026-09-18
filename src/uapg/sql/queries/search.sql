-- Слой поиска событий: events_ts и типизированные таблицы.
-- Разделитель — строка вида "-- name: <имя>".

-- name: save_event
-- Одна процедура пишет событие в оба слоя: полный состав полей в
-- events_history и строку поиска в events_ts со ссылкой на неё.
SELECT legacy_row_id, event_id
FROM "{schema}".uapg_save_event_v2($1, $2, $3, $4::jsonb, $5)

-- name: read_events
SELECT event_id, event_timestamp, event_type_id, legacy_row_id
FROM "{schema}".uapg_read_events_v2($1, $2, $3, $4, $5, $6::bigint[], $7, $8)

-- name: explain_filter
SELECT "{schema}".uapg_explain_event_filter($1, $2, $3, $4, $5, $6::bigint[])

-- name: resolve_type
-- Клиент называет тип коротким именем, а в базе он хранится строкой NodeId.
SELECT event_type_id
FROM "{schema}".event_types
WHERE event_type_name = $1
   OR event_type_name LIKE $2
   OR event_type_name LIKE $3
LIMIT 1

-- name: types_with_storage
SELECT ets.event_type_id, ets.physical_table
FROM "{schema}".event_type_storage ets
JOIN "{schema}".event_types et ON et.event_type_id = ets.event_type_id
ORDER BY ets.event_type_id

-- name: backfill_state
SELECT last_legacy_id, rows_processed
FROM "{schema}".uapg_backfill_state
WHERE domain = 'events'

-- name: backfill_batch
CALL "{schema}".uapg_backfill_events_batch($1, $2, $3)

-- name: backfill_totals
SELECT
    (SELECT count(*) FROM "{schema}".events_history)::bigint AS total,
    (SELECT count(*)
       FROM "{schema}".events_history eh
      WHERE NOT EXISTS (
          SELECT 1 FROM "{schema}".events_ts et WHERE et.legacy_row_id = eh.id
      ))::bigint AS lag
