"""Осознанные отступления новой реализации от эталона 0.2.15.

Контрактные тесты падают на любом расхождении, которого нет в этих списках.
Список — единственное место, где фиксируется «да, мы знаем и так и хотели»,
поэтому каждая строка обязана нести причину.
"""

from __future__ import annotations

from typing import Dict, Set

# --- публичный API -----------------------------------------------------------

API: Dict[str, str] = {
    # Легаси-бэкенд без TimescaleDB удалён: в проде им никто не пользовался,
    # переносить данные не требуется.
    "module_all removed: HistoryPgSQL": "удалён legacy-бэкенд HistoryPgSQL",
    # Админ-утилиты вынесены из пакета: uapg v3 — только бэкенд историзации.
    # Код остаётся доступен на main (18df1cf) и переезжает в opc-vibroiot-db.
    "module_all removed: DatabaseManager": "админ-утилиты вынесены из пакета",
    "module_all removed: create_database_standalone": "админ-утилиты вынесены из пакета",
    "module_all removed: backup_database_standalone": "админ-утилиты вынесены из пакета",
}

# --- поведение записи ---

BEHAVIOUR: Dict[str, str] = {
    # В 0.2.15 значение с кодом качества Bad не записывалось вовсе: код не
    # помещается в INTEGER без знака, PostgreSQL отвергал параметр, и вместе с
    # ним терялась вся пачка. Теперь те же 32 бита пишутся со знаком; схема не
    # меняется, коды Good и Uncertain записываются как прежде.
    "statuscode Bad записывается вместо потери батча": "исправление, схема не меняется",
}

# --- схема БД ----------------------------------------------------------------

SCHEMA: Dict[str, str] = {
    # В 0.2.15 создавались два одинаковых индекса на (event_type_id, source_id);
    # второй — чистая потеря на каждой записи. В уже существующих базах он
    # останется и вреда не принесёт, новые базы получают один.
    "indexes events_history removed: CREATE INDEX idx_events_history_event_type_source "
    "ON {schema}.events_history USING btree (event_type_id, source_id)": "дубль индекса",
}

# Объекты миграций 101/102 (variables v2 по ADR-003): создавались в каждой базе,
# но ни одна строка Python их не использует. В v3 не создаются до реализации фичи.
SCHEMA_DEAD_VARIABLES_V2: Set[str] = {
    "variables_ts",
    "variable_schema",
    "uapg_read_variables_raw_v2",
    "uapg_read_variables_processed_v2",
    "uapg_read_variables_at_time_v2",
}
