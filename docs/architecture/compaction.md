---
icon: lucide/archive
---

# Compaction

Compaction merges accumulated data files and equality delete files into fewer, larger data files. It runs automatically after each materializer cycle when file count thresholds are exceeded (default: 10 data files or 100 delete files), and can also be triggered manually with `--compact`.

## How compaction works

1. **Evaluate** — load the current table snapshot and count data and delete files; skip if below thresholds
2. **Plan** — separate files into carry-forward (large, no deletions) and rewrite (small or affected by deletes)
3. **Apply deletes** — for rewrite candidates: download files, deduplicate rows by primary key, apply equality deletes respecting Iceberg sequence number ordering (a delete only removes a row if the delete's sequence number is strictly greater than the row's)
4. **Re-serialize** — write merged rows into new Parquet files targeting the configured target file size (default: 128 MB)
5. **Commit** — replace the old file set with the new one in a single snapshot commit

The result is a smaller set of data files with no equality delete files, reducing read amplification for downstream query engines.

!!! warning "Known limitation"
    Compaction currently writes unpartitioned output for partitioned tables. This will be fixed in a future release.
