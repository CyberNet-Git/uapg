-- Запросы к истории событий.
-- Разделитель — строка вида "-- name: <имя>".

-- name: upsert_source
INSERT INTO "{schema}".event_sources (source_node_id, retention_period, max_records)
VALUES ($1, $2, $3)
ON CONFLICT (source_node_id) DO UPDATE
    SET retention_period = EXCLUDED.retention_period,
        max_records = EXCLUDED.max_records,
        updated_at = NOW()
RETURNING source_id

-- name: find_source
SELECT source_id FROM "{schema}".event_sources
WHERE source_node_id = $1
LIMIT 1

-- name: load_source_cache
SELECT source_node_id, source_id FROM "{schema}".event_sources

-- name: upsert_type
INSERT INTO "{schema}".event_types (event_type_name)
VALUES ($1)
ON CONFLICT (event_type_name) DO UPDATE SET updated_at = NOW()
RETURNING event_type_id

-- name: find_type
SELECT event_type_id FROM "{schema}".event_types
WHERE event_type_name = $1
LIMIT 1

-- name: load_type_cache
SELECT event_type_name, event_type_id FROM "{schema}".event_types

-- name: insert_history
-- Два события одного источника в одну и ту же миллисекунду неразличимы:
-- уникальный индекс по (source_id, event_timestamp) оставляет первое.
INSERT INTO "{schema}".events_history (source_id, event_type_id, event_timestamp, event_data)
VALUES ($1, $2, $3, $4)
ON CONFLICT (source_id, event_timestamp) DO NOTHING

-- name: read_history_asc
SELECT id, event_timestamp, event_type_id, event_data
FROM "{schema}".events_history
WHERE source_id = $1 AND event_timestamp BETWEEN $2 AND $3
ORDER BY event_timestamp ASC
LIMIT $4

-- name: read_history_desc
SELECT id, event_timestamp, event_type_id, event_data
FROM "{schema}".events_history
WHERE source_id = $1 AND event_timestamp BETWEEN $2 AND $3
ORDER BY event_timestamp DESC
LIMIT $4

-- name: read_payloads_by_legacy_ids
-- Полные поля события лежат только здесь: типизированные таблицы хранят
-- колонки под поиск, а не весь состав события.
SELECT id, event_data
FROM "{schema}".events_history
WHERE id = ANY($1::bigint[])

-- name: delete_history
DELETE FROM "{schema}".events_history
WHERE source_id = $1 AND event_timestamp BETWEEN $2 AND $3

-- name: backfill_lag
-- Сколько строк устаревшего хранения ещё не перенесено в слой поиска.
SELECT count(*)::bigint
FROM "{schema}".events_history eh
WHERE ($1::bigint IS NULL OR eh.source_id = $1)
  AND NOT EXISTS (
      SELECT 1 FROM "{schema}".events_ts et WHERE et.legacy_row_id = eh.id
  )
