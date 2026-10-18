---
icon: lucide/archive
---

# Compaction

Compaction merges small data files into larger ones, and drops the rows updates and deletes left behind. It runs for each table where it's due: beside the materializer in `pg2iceberg run`, and on demand with [`pg2iceberg compact`](../usage/modes.md#compact-one-shot-compaction-pass).

## Beside the materializer

A pass reads and rewrites files for seconds or minutes; a materializer catching up shouldn't stop for it. In `run`, passes over every table are prepared on a task of their own — planned from the materializer's [file index](materializer.md) as a cycle left it, inputs read and rewritten, outputs uploaded — while cycles go on, and the next cycle after they're done commits them before it starts. The materializer stays the only committer, so its file index takes each commit in turn. The next passes start once those are committed, and not before the next cycle: planned earlier, they could pick the same files again.

A pass reads the table as of the snapshot its file index reflected when it was planned, never later: what's committed meanwhile waits for the next pass. Rows the cycles meanwhile updated or deleted were rewritten in their old versions, which the cycles' own deletes hide; the file index keeps them where the cycles put them.

## Why rows need dropping

pg2iceberg writes [merge-on-read](materializer.md#merge-on-read): an update or delete doesn't touch the data file holding the old row, it adds an equality delete — the row's key — that readers apply. The old row stays in its file, and every reader skips it.

An equality delete applies to every older data file, so a delete file can only go once no older file holds a row it hides: those files must be rewritten without their deleted rows first. Under updates spread across a large table, that's most of the table.

## When it runs

A table is compacted when:

- it has `compaction_data_files` data files (default 8): its small files — under half of `target_file_size` — are merged; or
- the rows deletes hide are `compaction_delete_ratio` (default 0.1) of the rows in the files holding them, or there are `compaction_delete_files` delete files (default 32): those files are rewritten without the hidden rows, and the delete files no remaining file needs are dropped.

While the materializer catches up on a backlog, only the delete-file count compacts a table: the next cycle follows straight away and would dirty again what a pass rewrote. Merges and ratio-triggered rewrites wait until it has caught up, and then cover everything the backlog deleted at once.

The ratio bounds the work: rewriting reads at most 1 / `compaction_delete_ratio` rows per row deleted (10, by default). pg2iceberg knows which file holds each row (its [file index](materializer.md)), so it counts hidden rows without reading any file, and rewrites only files that have some.

## A pass

1. **Plan** — oldest files first, up to four `target_file_size`s of input; the rest wait for the next pass
2. **Apply deletes** — read the keys the deletes hide, up to eight delete files at once; a delete hides a row only if its sequence number is greater than the row's
3. **Rewrite** — stream each input file through, in order, in batches of 1024 rows, dropping hidden rows, into new files of up to `target_file_size`, one partition each. Files from before a schema change come out in the current schema
4. **Commit** — one snapshot replaces the inputs and the dropped delete files with the outputs, once they've all uploaded

Each file is a round trip to the object store, so a pass overlaps them: the inputs are read ahead of the one being rewritten — up to eight files, and `target_file_size` bytes of them — starting while the deletes load, and an output uploads while the next is written, up to `target_file_size` bytes uploading. A pass holds those in memory as well as the file it's reading and the one it's writing: about four `target_file_size`s at most.

## When the catalog compacts

With `sink.maintenance: managed`, the catalog compacts the table and pg2iceberg rewrites no data files. Its pass keeps one job: retiring the delete files no data file is left for — the catalog's rewrites leave them behind. See [Table Maintenance](maintenance.md#when-the-catalog-maintains-the-tables).
