---
icon: lucide/grid-3x3
---

# Iceberg Partitioning

pg2iceberg can materialize tables into partitioned Iceberg tables. Partitions are defined per table in the config and are applied during materialization — the staging layer is unaffected.

## Configuration

Add a `partition` list under each table's `iceberg` block:

```yaml
tables:
  - name: public.orders
    iceberg:
      partition:
        - "day(created_at)"
        - "region"
```

Multiple partition fields are supported and are applied in the order listed.

## Transforms

### Time-based

Apply to `date`, `timestamp`, or `timestamptz` columns.

| Expression | Partition by |
|------------|-------------|
| `year(col)` | Year (e.g. 2024) |
| `month(col)` | Year-month (e.g. 2024-03) |
| `day(col)` | Calendar day |
| `hour(col)` | Hour |

```yaml
partition:
  - "day(created_at)"
```

### Identity

Partitions by the raw column value. Works with any column type. Use the column name without a transform function.

```yaml
partition:
  - "region"
  - "status"
```

### Bucket

Hashes the column value into `N` buckets using Murmur3 (per the Iceberg spec). Useful for high-cardinality columns like UUIDs or string IDs where identity partitioning would produce too many small partitions.

Supported column types: `integer`, `bigint`, `text`, `varchar`, `uuid`, `date`, `timestamp`, `timestamptz`, `numeric`, `bytea`.

```yaml
partition:
  - "bucket[16](customer_id)"
```

### Truncate

Truncates the column value to a width `W`. For strings, `W` is the number of UTF-8 code points. For integers and decimals, `W` is a rounding modulus (values are rounded down to the nearest multiple of `W`).

Supported column types: `integer`, `bigint`, `numeric`, `text`, `varchar`, `char`, `bytea`.

```yaml
partition:
  - "truncate[4](name)"    # first 4 characters of name
  - "truncate[1000](id)"   # id rounded down to nearest 1000
```

## Combining transforms

Partition fields can be freely combined:

```yaml
tables:
  - name: public.events
    iceberg:
      partition:
        - "day(ts)"
        - "bucket[8](user_id)"
```

## Compaction and partitioning

!!! warning "Known limitation"
    Compaction currently writes unpartitioned output for partitioned tables. Query correctness is not affected — compaction only merges existing files — but the output files will not carry partition metadata. This will be fixed in a future release.
