-- Запросы к истории переменных.
-- Разделитель — строка вида "-- name: <имя>".

-- name: upsert_metadata
-- Регистрация переменной. Повторный вызов возвращает уже выданный
-- идентификатор, а не создаёт второй: под node_id есть уникальный индекс.
-- Тип данных при повторной регистрации не затирается: он становится известен
-- только с первым значением, и 0.2.15 сбрасывал его в 'Unknown' на каждом
-- перезапуске сервера.
INSERT INTO "{schema}".variable_metadata (node_id, data_type, retention_period, max_records)
VALUES ($1, COALESCE($2::text, 'Unknown'), $3, $4)
ON CONFLICT (node_id) DO UPDATE
    SET data_type = COALESCE($2::text, "{schema}".variable_metadata.data_type),
        retention_period = EXCLUDED.retention_period,
        max_records = EXCLUDED.max_records,
        updated_at = NOW()
RETURNING variable_id

-- name: upsert_metadata_many
-- Пакетная регистрация: один запрос вместо запроса на узел. При старте сервера
-- узлов бывают десятки тысяч, и разница здесь — это разница во времени старта.
INSERT INTO "{schema}".variable_metadata (node_id, data_type, retention_period, max_records)
SELECT t.node_id, COALESCE(t.data_type, 'Unknown'), t.retention_period, t.max_records
FROM unnest($1::text[], $2::text[], $3::interval[], $4::integer[])
    AS t(node_id, data_type, retention_period, max_records)
ON CONFLICT (node_id) DO UPDATE
    SET retention_period = EXCLUDED.retention_period,
        max_records = EXCLUDED.max_records,
        updated_at = NOW()
RETURNING node_id, variable_id

-- name: find_metadata
SELECT variable_id, retention_period, max_records
FROM "{schema}".variable_metadata
WHERE node_id = $1
LIMIT 1

-- name: load_metadata_cache
SELECT node_id, variable_id
FROM "{schema}".variable_metadata
ORDER BY variable_id
LIMIT $1

-- name: insert_history
-- Повторная запись того же момента времени игнорируется: источник может
-- прислать значение дважды, и это не ошибка.
INSERT INTO "{schema}".variables_history
    (variable_id, servertimestamp, sourcetimestamp, statuscode, value, varianttype, variantbinary)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (variable_id, sourcetimestamp) DO NOTHING

-- name: upsert_last_value
-- Условие в WHERE не даёт запоздавшей записи откатить кэш назад: значения
-- приходят не строго по порядку, а последнее значение должно оставаться
-- последним по времени источника.
INSERT INTO "{schema}".variables_last_value
    (variable_id, sourcetimestamp, servertimestamp, statuscode, varianttype, variantbinary)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (variable_id) DO UPDATE
    SET sourcetimestamp = EXCLUDED.sourcetimestamp,
        servertimestamp = EXCLUDED.servertimestamp,
        statuscode = EXCLUDED.statuscode,
        varianttype = EXCLUDED.varianttype,
        variantbinary = EXCLUDED.variantbinary,
        is_seed = FALSE,
        updated_at = NOW()
    WHERE "{schema}".variables_last_value.is_seed
       OR "{schema}".variables_last_value.sourcetimestamp <= EXCLUDED.sourcetimestamp

-- name: read_history_asc
SELECT sourcetimestamp, servertimestamp, statuscode, varianttype, variantbinary
FROM "{schema}".variables_history
WHERE variable_id = $1 AND sourcetimestamp BETWEEN $2 AND $3
ORDER BY sourcetimestamp ASC
LIMIT $4

-- name: read_history_desc
SELECT sourcetimestamp, servertimestamp, statuscode, varianttype, variantbinary
FROM "{schema}".variables_history
WHERE variable_id = $1 AND sourcetimestamp BETWEEN $2 AND $3
ORDER BY sourcetimestamp DESC
LIMIT $4

-- name: read_last_value
SELECT sourcetimestamp, servertimestamp, statuscode, varianttype, variantbinary, is_seed
FROM "{schema}".variables_last_value
WHERE variable_id = $1

-- name: read_last_values_many
SELECT variable_id, sourcetimestamp, servertimestamp, statuscode, varianttype, variantbinary, is_seed
FROM "{schema}".variables_last_value
WHERE variable_id = ANY($1::bigint[])

-- name: read_latest_from_history
-- Запасной путь, когда строки в кэше последних значений ещё нет.
SELECT sourcetimestamp, servertimestamp, statuscode, varianttype, variantbinary
FROM "{schema}".variables_history
WHERE variable_id = $1
ORDER BY sourcetimestamp DESC
LIMIT 1

-- name: load_last_values_page
SELECT variable_id, sourcetimestamp, servertimestamp, statuscode, varianttype, variantbinary
FROM "{schema}".variables_last_value
WHERE variable_id > $1
ORDER BY variable_id
LIMIT $2

-- name: seed_last_values
-- Заглушки для переменных, у которых значения ещё не было: инвариант «на
-- каждую зарегистрированную переменную есть строка» позволяет чтению
-- последнего значения не ходить в историю.
INSERT INTO "{schema}".variables_last_value
    (variable_id, sourcetimestamp, servertimestamp, statuscode, varianttype, variantbinary, is_seed)
SELECT *, TRUE
FROM unnest($1::bigint[], $2::timestamptz[], $3::timestamptz[], $4::integer[], $5::integer[], $6::bytea[])
ON CONFLICT (variable_id) DO NOTHING
RETURNING variable_id

-- name: delete_history
DELETE FROM "{schema}".variables_history
WHERE variable_id = $1 AND sourcetimestamp BETWEEN $2 AND $3

-- name: update_data_type
UPDATE "{schema}".variable_metadata
SET data_type = $2, updated_at = NOW()
WHERE variable_id = $1 AND data_type IS DISTINCT FROM $2

-- name: latest_from_history_many
-- LATERAL с LIMIT 1 на каждую переменную: планировщик идёт по индексу
-- (variable_id, sourcetimestamp DESC) от свежих чанков и останавливается на
-- первом значении. DISTINCT ON по гипертаблице со множеством чанков
-- вырождается в слияние индексов всех чанков — секунды на вызов.
-- $2 ограничивает глубину поиска и позволяет TimescaleDB отбросить старые чанки.
SELECT v.variable_id, h.sourcetimestamp, h.servertimestamp,
       h.statuscode, h.varianttype, h.variantbinary
FROM unnest($1::bigint[]) AS v(variable_id)
CROSS JOIN LATERAL (
    SELECT sourcetimestamp, servertimestamp, statuscode, varianttype, variantbinary
    FROM "{schema}".variables_history
    WHERE variable_id = v.variable_id
      AND ($2::timestamptz IS NULL OR sourcetimestamp >= $2)
    ORDER BY sourcetimestamp DESC
    LIMIT 1
) h

-- name: upsert_last_values_many
-- Самозалечивание кэша: найденное в истории фиксируется в variables_last_value,
-- чтобы следующее чтение (и следующий старт) в историю уже не ходили.
INSERT INTO "{schema}".variables_last_value
    (variable_id, sourcetimestamp, servertimestamp, statuscode, varianttype, variantbinary)
SELECT * FROM unnest($1::bigint[], $2::timestamptz[], $3::timestamptz[],
                     $4::integer[], $5::integer[], $6::bytea[])
ON CONFLICT (variable_id) DO UPDATE
    SET sourcetimestamp = EXCLUDED.sourcetimestamp,
        servertimestamp = EXCLUDED.servertimestamp,
        statuscode = EXCLUDED.statuscode,
        varianttype = EXCLUDED.varianttype,
        variantbinary = EXCLUDED.variantbinary,
        is_seed = FALSE,
        updated_at = NOW()
    WHERE "{schema}".variables_last_value.is_seed
       OR "{schema}".variables_last_value.sourcetimestamp <= EXCLUDED.sourcetimestamp

-- name: seed_candidates
-- Сверяются только строки-заглушки: отсутствующие строки создаёт
-- seed_last_values при регистрации, иначе метаданные без истории гоняли бы
-- LATERAL по гипертаблице при каждой сверке.
SELECT m.variable_id, m.node_id
FROM "{schema}".variable_metadata m
JOIN "{schema}".variables_last_value lv ON lv.variable_id = m.variable_id
WHERE lv.is_seed
ORDER BY m.variable_id

-- name: confirm_seeds
UPDATE "{schema}".variables_last_value
SET is_seed = FALSE
WHERE variable_id = ANY($1::bigint[]) AND is_seed
