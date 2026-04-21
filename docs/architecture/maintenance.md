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
