# Failover

A physical standby can take over from the source when it fails. On PostgreSQL 17 and later, pg2iceberg's replication slot can follow it there, so the pipeline carries on after a failover without a new snapshot.

## Failover slots

On PostgreSQL 17+, pg2iceberg creates its replication slot as a **failover slot**: standbys that sync slots keep a copy of it. A slot created before, or under the other setting, is altered to match at the next start. The `pg2iceberg_slot_failover` gauge is 1 while the slot is a failover slot.

To keep a plain slot instead:

```yaml
source:
  logical:
    failover: false        # or PG2ICEBERG_SLOT_FAILOVER=false
```

Before PostgreSQL 17 there are no failover slots, and the setting does nothing. A failover there loses the slot: pg2iceberg refuses to start (`SlotGoneButLsnExists`), and the pipeline needs `pg2iceberg cleanup` and a new snapshot.

## Setting up Postgres

A failover slot survives a failover only when Postgres is set up for it:

| Where | Setting |
|---|---|
| Source | `synchronized_standby_slots = '<the standby's physical slot>'` |
| Standby | `sync_replication_slots = on` |
| Standby | `hot_standby_feedback = on` |
| Standby | `primary_slot_name = '<its physical slot>'` |
| Standby | `primary_conninfo` naming a `dbname` |

`synchronized_standby_slots` matters most. With it, Postgres sends pg2iceberg nothing the standby hasn't received, so a failover can't leave Iceberg with changes the new primary never had. pg2iceberg also relies on it to tell what the standby has (see below).

Before failing over, check that the standby has synced the slot. A slot it hasn't synced yet is lost:

```sql
-- On the standby
SELECT slot_name, synced, temporary FROM pg_replication_slots;
```

Point pg2iceberg at an address that follows the primary: a DNS name or virtual IP the failover moves, a proxy, or a multi-host connection string with `target_session_attrs=read-write`.

## What happens on a failover

The replication stream drops. When pg2iceberg reconnects and finds the source on a new timeline, it exits with an error, and a new process starts from what pg2iceberg's state says. Run it under something that restarts it: Kubernetes, systemd, ECS. The new process resumes from where the slot and that state agree, with no new snapshot.

Set `synchronized_standby_slots` on the new primary to its own standbys' slots, or to nothing until it has one. A standby built from the old primary inherits that setting, naming a slot the new primary doesn't have, and Postgres then holds pg2iceberg's stream back until it's changed.

## pg2iceberg's state in the source

pg2iceberg keeps its state (the `_pg2iceberg` schema) on the source's server — in the source database, or the one `state.database` names there — so it fails over with the source. With an asynchronous standby, a failover can lose the last few writes to it. Anything pg2iceberg did elsewhere on those writes would survive, though: an acked slot, a commit to Iceberg, a deleted staged file. The state would then remember less than the slot, Iceberg and the object store show.

So pg2iceberg acks the slot, commits to Iceberg and deletes staged files only once the standby has the writes to its state behind them. It tells by the replication stream: once the stream has sent a position, the standby has the WAL before it. A failover then loses only writes nothing has acted on, as a crash would.

The cost is freshness: a commit to Iceberg waits for the standby to catch up, so a lagging standby delays Iceberg by its lag, and the slot is acked a little later. If the standby stops, Postgres holds the replication stream back too, and Iceberg waits with it.

A process whose state's database failed over writes nothing more to it: its connection comes back on another timeline, and the process exits.

## Limits

- **The initial snapshot isn't held back.** A failover during a new pipeline's first snapshot can need `pg2iceberg cleanup` and a new snapshot.
- **`materializer-only` workers aren't held back.** In [distributed mode](modes.md), they don't read the replication stream, so a failover to an asynchronous standby can leave the state behind their commits. Run them with a synchronous standby.
- **Commits to the state must be durable.** Leave `synchronous_commit` on for pg2iceberg's role.
