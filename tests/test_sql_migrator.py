"""Tests for SQL migrator helpers."""

from uapg.v2.sql_migrator import load_migration_sql


def test_load_migration_sql_substitutes_schema() -> None:
    sql = load_migration_sql("001_core_migrations.sql", "opcua_history")
    assert '"opcua_history".uapg_schema_migrations' in sql
    assert "{schema}" not in sql
