-- Ядро схемы историзации OPC UA.
--
-- Форма таблиц менять нельзя: существующие базы переезжают на новый код без
-- миграции. Особенности, которые легко потерять при переписывании, отмечены
-- комментариями.

CREATE SCHEMA IF NOT EXISTS "{schema}";

-- У таблиц истории нет PRIMARY KEY намеренно: уникальность держат индексы из
-- 020_indexes.sql, и именно на них опираются все ON CONFLICT.
CREATE TABLE IF NOT EXISTS "{schema}".variables_history (
    id BIGSERIAL,
    variable_id BIGINT NOT NULL,
    servertimestamp TIMESTAMPTZ NOT NULL,
    sourcetimestamp TIMESTAMPTZ NOT NULL,
    statuscode INTEGER,
    -- Человекочитаемая копия значения: код её не читает, восстановление идёт
    -- только из variantbinary.
    value TEXT,
    varianttype INTEGER,
    variantbinary BYTEA,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS "{schema}".events_history (
    id BIGSERIAL,
    source_id BIGINT NOT NULL,
    event_type_id BIGINT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    event_data JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- variable_id, source_id и event_type_id вычисляются из id. Это не украшение:
-- на них ссылаются таблицы истории, и замена их отдельной последовательностью
-- означала бы расхождение с уже записанными данными.
CREATE TABLE IF NOT EXISTS "{schema}".variable_metadata (
    id BIGSERIAL PRIMARY KEY,
    variable_id BIGINT GENERATED ALWAYS AS (id) STORED,
    node_id TEXT NOT NULL,
    data_type TEXT,
    retention_period INTERVAL,
    max_records INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (variable_id)
);

CREATE TABLE IF NOT EXISTS "{schema}".event_sources (
    id BIGSERIAL PRIMARY KEY,
    source_id BIGINT GENERATED ALWAYS AS (id) STORED,
    source_node_id TEXT NOT NULL,
    retention_period INTERVAL,
    max_records INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (source_id)
);

CREATE TABLE IF NOT EXISTS "{schema}".event_types (
    id BIGSERIAL PRIMARY KEY,
    event_type_id BIGINT GENERATED ALWAYS AS (id) STORED,
    event_type_name TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (event_type_id)
);

-- Кэш последних значений. Инвариант: для каждой зарегистрированной переменной
-- здесь есть строка, поэтому чтение последнего значения не идёт в историю.
CREATE TABLE IF NOT EXISTS "{schema}".variables_last_value (
    variable_id BIGINT PRIMARY KEY,
    sourcetimestamp TIMESTAMPTZ NOT NULL,
    servertimestamp TIMESTAMPTZ NOT NULL,
    statuscode INTEGER NOT NULL,
    varianttype INTEGER NOT NULL,
    variantbinary BYTEA NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- is_seed добавляется отдельно, а не в CREATE TABLE: в базах, созданных до
-- появления колонки, таблица уже существует и CREATE ничего не сделает.
-- TRUE означает строку-заглушку, ещё не сверенную с историей.
ALTER TABLE "{schema}".variables_last_value
    ADD COLUMN IF NOT EXISTS is_seed BOOLEAN NOT NULL DEFAULT FALSE;
