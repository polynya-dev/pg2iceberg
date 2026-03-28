package sink

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/pg2iceberg/pg2iceberg/metrics"
)

// resolveToast resolves unchanged TOAST columns for pending rows by scanning
// the destination Iceberg table. This is the cold-path fallback — most TOAST
// resolution happens inline in writeDirect() via the in-memory rowCache.
//
// This path is only hit when the rowCache doesn't have the previous row version,
// e.g. after a process restart.
func (s *Sink) resolveToast(ctx context.Context, pgTable string, ts *tableSink) error {
	if len(ts.toastPending) == 0 {
		return nil
	}

	pk := ts.schema.PK
	if len(pk) == 0 {
		log.Printf("[sink] TOAST: skipping resolution for %s (no primary key)", pgTable)
		return nil
	}

	// Collect unique PKs that need resolution.
	type pendingInfo struct {
		rows          []toastPendingRow
		partitionKey  string // for filtering data files
	}
	pendingByPK := make(map[string]*pendingInfo)
	for _, p := range ts.toastPending {
		pkKey := buildPKKey(p.row, pk)
		info, ok := pendingByPK[pkKey]
		if !ok {
			// Compute partition key from the row to narrow file scan.
			partKey := ""
			if ts.partSpec != nil && !ts.partSpec.IsUnpartitioned() {
				partKey, _ = ts.partSpec.PartitionKey(p.row, ts.schema)
			}
			info = &pendingInfo{partitionKey: partKey}
			pendingByPK[pkKey] = info
		}
		info.rows = append(info.rows, p)
	}

	if len(pendingByPK) == 0 {
		return nil
	}

	startTime := time.Now()

	// Load current table metadata to find data files.
	tm, err := s.catalog.LoadTable(s.cfg.Namespace, ts.icebergName)
	if err != nil {
		return fmt.Errorf("TOAST: load table: %w", err)
	}
	if tm == nil || tm.Metadata.CurrentSnapshotID == 0 {
		log.Printf("[sink] TOAST: no snapshots for %s, %d rows unresolved", pgTable, len(pendingByPK))
		return nil
	}

	manifestListURI := tm.CurrentManifestList()
	if manifestListURI == "" {
		return nil
	}

	mlKey, err := KeyFromURI(manifestListURI)
	if err != nil {
		return fmt.Errorf("TOAST: parse manifest list URI: %w", err)
	}
	mlData, err := s.s3.Download(ctx, mlKey)
	if err != nil {
		return fmt.Errorf("TOAST: download manifest list: %w", err)
	}
	manifestInfos, err := ReadManifestList(mlData)
	if err != nil {
		return fmt.Errorf("TOAST: read manifest list: %w", err)
	}

	// Collect partition keys we need to find.
	neededPartitions := make(map[string]bool)
	for _, info := range pendingByPK {
		neededPartitions[info.partitionKey] = true
	}

	// Collect data files, filtering by partition where possible.
	// Process manifests in reverse order (newest first) for efficiency.
	var dataFiles []DataFileInfo
	for i := len(manifestInfos) - 1; i >= 0; i-- {
		mfi := manifestInfos[i]
		if mfi.Content != 0 { // skip delete manifests
			continue
		}
		mKey, err := KeyFromURI(mfi.Path)
		if err != nil {
			continue
		}
		mData, err := s.s3.Download(ctx, mKey)
		if err != nil {
			continue
		}
		entries, err := ReadManifest(mData)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.Status == 2 || e.DataFile.Content != 0 {
				continue
			}
			// Filter by partition if possible.
			if ts.partSpec != nil && !ts.partSpec.IsUnpartitioned() && len(e.DataFile.PartitionValues) > 0 {
				filePartKey := partitionKeyFromAvroValues(e.DataFile.PartitionValues, ts.partSpec)
				if !neededPartitions[filePartKey] {
					continue
				}
			}
			dataFiles = append(dataFiles, e.DataFile)
		}
	}

	// Scan data files for matching PKs. Stop early once all resolved.
	resolved := 0
	filesScanned := 0
	for _, df := range dataFiles {
		if len(pendingByPK) == 0 {
			break // all resolved
		}

		dfKey, err := KeyFromURI(df.Path)
		if err != nil {
			continue
		}
		data, err := s.s3.Download(ctx, dfKey)
		if err != nil {
			log.Printf("[sink] TOAST: failed to download %s: %v", df.Path, err)
			continue
		}
		filesScanned++

		rows, err := readParquetRows(data, ts.schema)
		if err != nil {
			log.Printf("[sink] TOAST: failed to read %s: %v", df.Path, err)
			continue
		}

		for _, row := range rows {
			pkKey := buildPKKey(row, pk)
			info, ok := pendingByPK[pkKey]
			if !ok {
				continue
			}

			// Found the row — patch all pending entries for this PK.
			for _, p := range info.rows {
				for _, col := range p.unchangedCols {
					if val, exists := row[col]; exists {
						p.row[col] = val
					}
				}
				resolved++
			}

			// Cache for future lookups.
			ts.rowCache[pkKey] = copyRow(row)

			delete(pendingByPK, pkKey)
		}
	}

	duration := time.Since(startTime)
	metrics.ToastLookupsTotal.WithLabelValues(s.pipelineID, pgTable).Inc()
	metrics.ToastRowsResolvedTotal.WithLabelValues(s.pipelineID, pgTable).Add(float64(resolved))
	metrics.ToastLookupDurationSeconds.WithLabelValues(s.pipelineID, pgTable).Observe(duration.Seconds())

	unresolved := len(pendingByPK)
	if unresolved > 0 {
		log.Printf("[sink] TOAST: %d/%d rows unresolved for %s (row may have been deleted or not yet flushed)",
			unresolved, unresolved+resolved, pgTable)
	}

	log.Printf("[sink] TOAST: resolved %d rows for %s via Iceberg scan (%d files scanned)",
		resolved, pgTable, filesScanned)

	return nil
}

// partitionKeyFromAvroValues reconstructs a partition key string from
// manifest Avro partition values, matching the format produced by
// PartitionSpec.PartitionKey().
func partitionKeyFromAvroValues(avroValues map[string]any, partSpec *PartitionSpec) string {
	parts := make([]string, len(partSpec.Fields))
	for i, field := range partSpec.Fields {
		val := avroValues[field.Name]
		if val == nil {
			parts[i] = "__null__"
		} else {
			parts[i] = fmt.Sprintf("%v", val)
		}
	}
	return strings.Join(parts, "/")
}

// buildPKKey creates a unique key for a row based on its PK columns.
// Uses null byte as separator to avoid collisions.
func buildPKKey(row map[string]any, pk []string) string {
	parts := make([]string, len(pk))
	for i, col := range pk {
		parts[i] = fmt.Sprintf("%v", row[col])
	}
	return strings.Join(parts, "\x00")
}
