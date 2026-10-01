-- Индексы ядра.
--
-- Создаются по одному и по возможности: на горячей таблице CREATE INDEX ждёт
-- блокировку за живым INSERT, и падение здесь не должно валить запуск сервера.
-- Поэтому каждая инструкция выполняется отдельно, с предварительной проверкой
-- существования (см. storage/bootstrap.py).

-- Уникальность, под которой работает ON CONFLICT записи значений.
CREATE UNIQUE INDEX idx_variables_varid_sourcets
    ON "{schema}".variables_history (variable_id, sourcetimestamp);

CREATE INDEX idx_variables_variable_id
    ON "{schema}".variables_history (variable_id);

CREATE INDEX idx_variables_timestamp
    ON "{schema}".variables_history (sourcetimestamp);

CREATE INDEX idx_variables_server_timestamp
    ON "{schema}".variables_history (servertimestamp);

CREATE INDEX idx_variables_history_variable_id_timestamp
    ON "{schema}".variables_history (variable_id, sourcetimestamp);

-- Покрывающий индекс под чтение истории одной переменной от свежих к старым.
CREATE INDEX idx_variables_history_vid_ts_desc_covering
    ON "{schema}".variables_history (variable_id, sourcetimestamp DESC)
    INCLUDE (statuscode, varianttype, servertimestamp);

-- Уникальность, под которой работает ON CONFLICT записи событий.
CREATE UNIQUE INDEX idx_events_sourceid_eventts
    ON "{schema}".events_history (source_id, event_timestamp);

CREATE INDEX idx_events_source_id
    ON "{schema}".events_history (source_id);

CREATE INDEX idx_events_event_type_id
    ON "{schema}".events_history (event_type_id);

CREATE INDEX idx_events_timestamp
    ON "{schema}".events_history (event_timestamp);

-- events_history.id — BIGSERIAL без PRIMARY KEY. Без индекса по нему полным
-- сканом всех чанков идут и батч переноса в слой поиска, и восстановление
-- полей события по идентификаторам, и проба готовности переноса.
CREATE INDEX idx_events_history_id
    ON "{schema}".events_history (id);

CREATE INDEX idx_events_data_gin
    ON "{schema}".events_history USING GIN (event_data);

CREATE INDEX idx_events_history_source_timestamp
    ON "{schema}".events_history (source_id, event_timestamp);

-- В 0.2.15 таких индексов было два с разными именами и одинаковым определением;
-- дубль стоил лишней записи на каждой вставке и здесь не создаётся.
CREATE INDEX idx_events_history_type_source
    ON "{schema}".events_history (event_type_id, source_id);

CREATE UNIQUE INDEX idx_variable_metadata_node_id
    ON "{schema}".variable_metadata (node_id);

CREATE INDEX idx_variable_metadata_variable_id
    ON "{schema}".variable_metadata (variable_id);

CREATE INDEX idx_variable_metadata_created
    ON "{schema}".variable_metadata (created_at);

CREATE UNIQUE INDEX idx_event_sources_node_id
    ON "{schema}".event_sources (source_node_id);

CREATE INDEX idx_event_sources_source_id
    ON "{schema}".event_sources (source_id);

CREATE INDEX idx_event_sources_created
    ON "{schema}".event_sources (created_at);

CREATE UNIQUE INDEX idx_event_types_name
    ON "{schema}".event_types (event_type_name);

CREATE INDEX idx_event_types_event_type_id
    ON "{schema}".event_types (event_type_id);

CREATE INDEX idx_event_types_created
    ON "{schema}".event_types (created_at);

CREATE INDEX idx_variables_last_value_updated
    ON "{schema}".variables_last_value (updated_at);
