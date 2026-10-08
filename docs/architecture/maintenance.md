---
icon: lucide/wrench
---

# Table Maintenance

Table maintenance handles two housekeeping tasks. It runs as a background goroutine (default: every hour) and can also be triggered manually with `--maintain`.

## Snapshot expiry

Iceberg tables accumulate snapshots over time — one per materializer commit. Old snapshots are expired by removing their entries from the snapshot list and deleting the corresponding manifest files from S3.

The retention period defaults to 7 days (`168h`). Snapshots newer than the retention cutoff are kept; older ones are removed along with any manifest or data files that are no longer referenced by a retained snapshot.

## Orphan file cleanup

Orphan files are S3 objects that exist in the table's storage location but are not referenced by any current snapshot — typically left behind by interrupted writes. Maintenance deletes any file whose modification time is older than the orphan grace period (default: 30 minutes).

## When the catalog maintains the tables

Managed catalogs maintain their tables themselves: S3 Tables compacts them, expires their snapshots and removes unreferenced files by default; R2 Data Catalog compacts and expires when you turn it on; so do Glue's table optimizers. They commit to the table as another engine would, and pg2iceberg's own maintenance would get in their way:

- Orphan cleanup would delete the catalog's compaction output before the catalog commits it — both write under the table's location — and the table would then name files that are gone.
- pg2iceberg's compaction would rewrite the same files as the catalog's, doubling the work, and one of the two would lose the race.

Set `sink.maintenance: managed` (or `PG2ICEBERG_MAINTENANCE=managed`) and pg2iceberg leaves maintenance to the catalog:

- Its compaction rewrites no data files. It only retires delete files no data file is left for: the catalog's compaction leaves the deletes it applied behind, as Iceberg's Java rewrite does, and pg2iceberg writes one with every change. It runs with the materializer, under the same `compaction_delete_files` threshold.
- `pg2iceberg maintain` refuses to run.
- The materializer rereads what it knows of a table at every cycle that works on it, instead of keeping it until a pg2iceberg process writes the table: the catalog's commits tell no pg2iceberg process.

pg2iceberg still records how far each table applied the change log in the table's `pg2iceberg.log-ends` property, so a restart doesn't apply a commit twice even once the catalog has expired every snapshot pg2iceberg wrote.

A catalog that removes unreferenced files must keep them longer than pg2iceberg can lag. Change chunks staged under the table's location (with vended credentials) are unreferenced until materialized, and so are files written for a commit still in flight.

The catalog's compaction has to keep pg2iceberg's deletes applying to the rows it rewrites. Iceberg's Java rewrite does: its outputs keep the sequence number of the snapshot it read, or it refuses to commit if a delete landed meanwhile. A compactor that did neither would bring back rows deleted while it ran.
