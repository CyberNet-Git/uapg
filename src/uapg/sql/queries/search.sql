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

-- name: resolve_types_by_key
SELECT event_type_id
FROM "{schema}".event_types
WHERE event_type_name = ANY($1::text[])

-- name: types_with_storage
SELECT ets.event_type_id, ets.physical_table
FROM "{schema}".event_type_storage ets
JOIN "{schema}".event_types et ON et.event_type_id = ets.event_type_id
ORDER BY ets.event_type_id

-- name: backfill_state
SELECT last_legacy_id, rows_processed
FROM "{schema}".uapg_backfill_state
WHERE domain = $1

-- name: set_backfill_state
INSERT INTO "{schema}".uapg_backfill_state (domain, last_legacy_id, rows_processed)
VALUES ($1, $2, $3)
ON CONFLICT (domain) DO UPDATE
    SET last_legacy_id = EXCLUDED.last_legacy_id,
        rows_processed = EXCLUDED.rows_processed,
        updated_at = NOW()

-- name: backfill_batch
-- Один вызов на батч: функция из миграции 005 делает перенос одним
-- INSERT ... SELECT и сама двигает watermark. До 005 здесь была процедура с
-- построчным циклом, а гейтвей дублировал её логику в Python.
SELECT last_legacy_id, rows_processed, rows_inserted
FROM "{schema}".uapg_backfill_events_batch($1, $2, $3)

-- name: backfill_stats
-- Прогресс считается от watermark, а не полным anti-join
-- events_history × events_ts: на проде последний разворачивался в Parallel Hash
-- Anti Join по всем чанкам гипертаблицы и не укладывался в таймаут запроса.
-- Все три подзапроса обслуживаются индексом idx_events_history_id.
SELECT
    (SELECT count(*)::bigint FROM "{schema}".events_history WHERE id > $1) AS lag_rows,
    (SELECT min(id)::bigint FROM "{schema}".events_history) AS min_id,
    (SELECT max(id)::bigint FROM "{schema}".events_history) AS max_id

-- name: backfill_pending
-- Осталась ли у переноса работа: смотрим только первые $1 строк выше watermark —
-- ровно то, что взял бы следующий батч. Стоимость не зависит от размера
-- events_history.
--
-- Простое «есть строка выше watermark» здесь не годится: при двойной записи
-- новое событие сразу попадает и в events_history, и в events_ts, поэтому
-- признак сбрасывался бы после каждой записи, хотя переносить нечего.
SELECT EXISTS (
    SELECT 1
    FROM (
        SELECT eh.id
        FROM "{schema}".events_history eh
        WHERE eh.id > COALESCE((
            SELECT bs.last_legacy_id
            FROM "{schema}".uapg_backfill_state bs
            WHERE bs.domain = 'events'
        ), 0)
        ORDER BY eh.id
        LIMIT $1
    ) probe
    WHERE NOT EXISTS (
        SELECT 1 FROM "{schema}".events_ts et WHERE et.legacy_row_id = probe.id
    )
)

-- name: typed_backfill_rows
-- Восходящий курсор: до 0.2.17 здесь было ORDER BY legacy_row_id DESC LIMIT n без
-- курсора, поэтому каждый вызов брал одни и те же свежие строки и до старых не
-- доходил никогда. Keyset обслуживается частичным индексом
-- idx_events_ts_legacy_row, а существование таблицы проверяется to_regclass
-- вместо коррелированного подзапроса к information_schema на каждую строку.
SELECT et.legacy_row_id, et.event_id, et.event_timestamp, et.source_id,
       et.event_type_id, ets.physical_table
FROM "{schema}".events_ts et
JOIN "{schema}".event_type_storage ets ON ets.event_type_id = et.event_type_id
WHERE et.legacy_row_id > $1
  AND to_regclass(format('%I.%I', $2::text, ets.physical_table)) IS NOT NULL
ORDER BY et.legacy_row_id
LIMIT $3
