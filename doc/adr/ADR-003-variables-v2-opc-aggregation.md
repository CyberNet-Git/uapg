# ADR-003: Variables History V2 + OPC UA Aggregation (roadmap)

## Статус

Принято (roadmap, не блокирует релиз events V2)

## Контекст

`variables_history` хранит `variantbinary BYTEA`; `read_node_history` не реализует `ReadProcessed` / `AggregateFunction` из OPC UA Part 11.

## Решение (релиз 2+)

- `variables_ts` hypertable + typed scalar columns.
- `AggregatePlan` JSON из `ReadProcessedDetails`.
- SQL: `time_bucket`, continuous aggregates, `uapg_read_variables_processed_v2`.
- Dual-write с `variables_history`.

## MVP AggregateFunction

| OPC UA | SQL/Timescale |
|--------|---------------|
| Average, TimeAverage | `avg(time_bucket(...))` |
| Minimum, Maximum | `min` / `max` |
| Total, Count | `sum` / `count` |
| Start, End | `first` / `last` ordered by time |

Отложено: Variance, Interpolative, ReadModified, Annotations.

## Зависимости

Стабилизация events V2 (фазы 0–6).
