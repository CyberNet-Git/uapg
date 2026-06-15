# ADR-002: Платформа History V2 (shared infra)

## Статус

Принято

## Решение

Общая инфраструктура для events (релиз 1) и variables (релиз 2+):

| Компонент | Назначение |
|-----------|------------|
| `uapg_schema_migrations` | версионирование SQL |
| `SqlMigrator` | применение `sql/migrations/*.sql` |
| `ProcedureGateway` | вызов SQL functions / Python fallback |
| `StorageMode` | `legacy` / `dual` / `v2` per domain |
| `extension_schema` | deployment-specific procedures |

Префиксы миграций: `001_events_*`, `101_variables_*`.

## Границы

- Core uapg без доменной логики продуктов.
- Extension schema для VibroIoT и др.
