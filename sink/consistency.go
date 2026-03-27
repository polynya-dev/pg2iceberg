package sink

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/pg2iceberg/pg2iceberg/schema"
)

const consistencyTableName = "_consistency"

// consistencySchema returns the hardcoded schema for the _consistency table.
func consistencySchema() *schema.TableSchema {
	return &schema.TableSchema{
		Table: consistencyTableName,
		PK:    []string{"flush_id", "table_name"},
		Columns: []schema.Column{
			{Name: "flush_id", PGType: "bigint", FieldID: 1},
			{Name: "pg_xid_start", PGType: "bigint", FieldID: 2},
			{Name: "pg_xid_end", PGType: "bigint", FieldID: 3},
			{Name: "pg_commit_ts", PGType: "timestamptz", FieldID: 4, IsNullable: true},
			{Name: "table_name", PGType: "text", FieldID: 5},
			{Name: "snapshot_id", PGType: "bigint", FieldID: 6},
			{Name: "created_at", PGType: "timestamptz", FieldID: 7},
		},
	}
}

// RegisterConsistencyTable creates the _consistency table in the catalog.
func (s *Sink) RegisterConsistencyTable(ctx context.Context) error {
	ts := consistencySchema()

	if err := s.catalog.EnsureNamespace(s.cfg.Namespace); err != nil {
		return fmt.Errorf("ensure namespace: %w", err)
	}

	icebergTable := consistencyTableName
	tm, err := s.catalog.LoadTable(s.cfg.Namespace, icebergTable)
	if err != nil {
		return fmt.Errorf("load consistency table: %w", err)
	}

	partSpec, _ := BuildPartitionSpec(nil, ts)

	if tm == nil {
		location := fmt.Sprintf("%s%s.db/%s", s.cfg.Warehouse, s.cfg.Namespace, icebergTable)
		_, err = s.catalog.CreateTable(s.cfg.Namespace, icebergTable, ts, location, partSpec)
		if err != nil {
			return fmt.Errorf("create consistency table: %w", err)
		}
		log.Printf("[sink] created consistency table %s.%s", s.cfg.Namespace, icebergTable)
	} else {
		log.Printf("[sink] using existing consistency table %s.%s", s.cfg.Namespace, icebergTable)
	}

	targetSize := s.cfg.TargetFileSizeOrDefault()
	tSink := &tableSink{
		schema:      ts,
		icebergName: icebergTable,
		partSpec:    partSpec,
		targetSize:  targetSize,
		partitions:  make(map[string]*partitionedWriter),
	}
	tSink.partitions[""] = &partitionedWriter{
		dataWriter: NewRollingDataWriter(ts, targetSize),
		delWriter:  NewRollingDeleteWriter(ts, targetSize),
	}
	s.tables[consistencyTableName] = tSink
	return nil
}

// writeConsistencyRecords writes one row per tracked table to the _consistency
// table, then commits it as an Iceberg snapshot. Tables that were not flushed
// get their current snapshot_id from the catalog so every flush_id is a
// complete point-in-time view.
func (s *Sink) writeConsistencyRecords(ctx context.Context, txns []*txBuffer, snapshotIDs map[string]int64) error {
	ts, ok := s.tables[consistencyTableName]
	if !ok {
		return fmt.Errorf("consistency table not registered")
	}

	// Build the full snapshot map: start with flushed tables, then fill in
	// unchanged tables from the catalog.
	allSnapshots := make(map[string]int64, len(s.tables))
	for k, v := range snapshotIDs {
		allSnapshots[k] = v
	}
	for pgTable, tSink := range s.tables {
		if pgTable == consistencyTableName {
			continue
		}
		if _, ok := allSnapshots[pgTable]; ok {
			continue
		}
		// Table had no changes — fetch current snapshot from catalog.
		tm, err := s.catalog.LoadTable(s.cfg.Namespace, tSink.icebergName)
		if err != nil {
			return fmt.Errorf("load table %s for consistency: %w", pgTable, err)
		}
		if tm != nil && tm.Metadata.CurrentSnapshotID > 0 {
			allSnapshots[pgTable] = tm.Metadata.CurrentSnapshotID
		}
		// If table has no snapshots yet (never flushed), skip it.
	}

	if len(allSnapshots) == 0 {
		return nil
	}

	flushID := time.Now().UnixMilli()
	now := time.Now()

	var minXID, maxXID uint32
	var latestCommitTS time.Time
	for _, tx := range txns {
		if minXID == 0 || tx.xid < minXID {
			minXID = tx.xid
		}
		if tx.xid > maxXID {
			maxXID = tx.xid
		}
		if tx.commitTS.After(latestCommitTS) {
			latestCommitTS = tx.commitTS
		}
	}

	for tableName, snapID := range allSnapshots {
		row := map[string]any{
			"flush_id":     int64(flushID),
			"pg_xid_start": int64(minXID),
			"pg_xid_end":   int64(maxXID),
			"pg_commit_ts":  latestCommitTS,
			"table_name":   tableName,
			"snapshot_id":  snapID,
			"created_at":   now,
		}
		pw := ts.getPartitionWriter(row)
		if err := pw.dataWriter.Add(row); err != nil {
			return fmt.Errorf("buffer consistency record: %w", err)
		}
	}

	snapID, err := s.flushTable(ctx, consistencyTableName, ts)
	if err != nil {
		return fmt.Errorf("flush consistency table: %w", err)
	}

	log.Printf("[sink] wrote %d consistency records (flush_id=%d, snapshot=%d, xid_range=[%d,%d])",
		len(allSnapshots), flushID, snapID, minXID, maxXID)
	return nil
}
