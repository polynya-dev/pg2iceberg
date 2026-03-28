# Architecture

## TOAST handling

The recommended way of dealing with TOAST is to use `REPLICA IDENTITY FULL`. This guarantees that updates that don't touch TOAST columns will still contain the TOAST column values.

If for some reason you cannot enable `REPLICA IDENTITY FULL`, pg2iceberg has built-in support for handling TOAST. When it sees an update event, it performs the following:

1. Check if TOAST cache contains that value. If yes, load it from memory.
2. If not, query the target Iceberg table for the TOAST value.
3. Cache this in the TOAST cache.

This approach has a few caveats worth noting:

- It only works if the table is fully replicated. If the table was replicated without taking a snapshot, or if for some reason pg2iceberg was interrupted and resumed improperly, lookup can fail, leading to null value.
- It is inefficient if the Iceberg table is not partitioned
- It is inefficient if cache miss happen more often than cache hit, e.g. updates tend to touch old rows

### Why not query the source table?

The issue with this is if WAL events are delayed (e.g. database upgrade), by the time the WAL event is processed later, the TOAST value lookup will see the value now instead of the value at the time the WAL event happened, which may be different. This leads to (temporarily) incorrect data that is visible to the reader. There is no way to do a point-in-time TOAST value lookup with Postgres.

### Why is lookup needed? Why not just update the changed columns?

Because Iceberg (as of V3) has no way of performing partial column update, i.e. it is not possible to do this with Iceberg:

```sql
UPDATE foo SET a = 1, b = 2 WHERE id = 123;
```

Partial updates are handled either via copy-on-write or merge-on-read (pg2iceberg uses MoR), both of which requires lookup.

There is an ongoing discussion to support partial column update in Iceberg in the future, which should solve this: https://github.com/apache/iceberg/issues/15146
