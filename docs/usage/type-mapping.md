---
icon: lucide/arrow-right-left
---

# Type Mapping

pg2iceberg maps PostgreSQL column types to Iceberg types automatically during schema discovery. Aliases (e.g. `integer`, `serial`) are normalized to their canonical form before mapping.

## Type map

| PostgreSQL type | Iceberg type | Notes |
|----------------|--------------|-------|
| `smallint` | `int` | |
| `integer`, `serial`, `oid` | `int` | |
| `bigint`, `bigserial` | `long` | |
| `real` | `float` | |
| `double precision` | `double` | |
| `numeric(p,s)` where p ≤ 38 | `decimal(p,s)` | Precision preserved exactly |
| `numeric(p,s)` where p > 38 | — | **Pipeline refuses to start** |
| `numeric` (unconstrained) | `decimal(38,18)` | Warning logged; overflow will error |
| `boolean` | `boolean` | |
| `text`, `varchar`, `char`, `name` | `string` | |
| `bytea` | `binary` | |
| `date` | `date` | |
| `time`, `timetz` | `time` | Microsecond precision |
| `timestamp` | `timestamp` | Microsecond precision |
| `timestamptz` | `timestamptz` | Microsecond precision |
| `uuid` | `uuid` | |
| `json`, `jsonb` | `string` | Stored as text |
| Other (`inet`, `interval`, `xml`, …) | `string` | Stored as text representation |

## Decimal precision limit

Iceberg supports a maximum decimal precision of 38. If a PostgreSQL table has a `numeric(p,s)` column with `p > 38`, pg2iceberg will refuse to start and will also reject schema evolution that introduces such a column. This is intentional — silently truncating precision would cause data corruption.

Unconstrained `numeric` columns (no precision specified) are mapped to `decimal(38,18)`. A warning is logged at startup. Values that exceed 38 digits of precision will cause a runtime error.

!!! tip
    If you have unconstrained `numeric` columns, add an explicit precision constraint in PostgreSQL (`ALTER TABLE ... ALTER COLUMN ... TYPE numeric(p,s)`) before starting replication to avoid the `decimal(38,18)` fallback.
