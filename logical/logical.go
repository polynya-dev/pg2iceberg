package logical

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/postgres"
	"github.com/pg2iceberg/pg2iceberg/snapshot"
	"github.com/pg2iceberg/pg2iceberg/utils"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// LogicalSource implements Source using PostgreSQL logical replication.
// It manages the full lifecycle of the replication slot.
type LogicalSource struct {
	pgCfg      config.PostgresConfig
	cfg        config.LogicalConfig
	tableCfgs  []config.TableConfig
	tables     map[string]*postgres.TableSchema
	pipelineID string

	replConn  *pgconn.PgConn
	queryConn *pgx.Conn

	// relations caches RelationMessage by OID for schema change detection.
	relations map[uint32]*pglogrepl.RelationMessageV2

	// decoder is the zero-allocation WAL message decoder.
	decoder *WALDecoder

	// startLSN is the position to start streaming from.
	startLSN pglogrepl.LSN
	// receivedLSN is the latest WAL position received from PG.
	// Accessed atomically: written by the capture goroutine (handleCopyData),
	// read by the pipeline goroutine (idle-advance path) and by standbyStatus.
	receivedLSN atomic.Uint64
	// flushedLSN is the latest WAL position that has been durably written
	// to Iceberg. Only this value is confirmed back to PG via standby
	// status updates, so PG will not recycle WAL that hasn't been persisted.
	// Accessed atomically: written by the pipeline goroutine (SetFlushedLSN),
	// read by the capture goroutine (standbyStatus).
	flushedLSN atomic.Uint64

	// snapshotComplete is set from the checkpoint to skip the initial snapshot.
	snapshotComplete bool
	// snapshotedTables tracks which tables already completed their snapshot
	// (restored from checkpoint for crash recovery).
	snapshotedTables map[string]bool

	// currentTxCommitTime is the PostgreSQL commit timestamp of the in-flight
	// transaction, captured from BeginMessage. This is the source-of-truth
	// timestamp (equivalent to Debezium's source.ts_ms).
	currentTxCommitTime time.Time
	// currentTxXID is the PostgreSQL transaction ID (XID) of the in-flight
	// transaction, captured from BeginMessage.
	currentTxXID uint32

	// standbyInterval controls how often standby status updates are sent to PG.
	// Defaults to 10s. Shorter values make confirmed_flush_lsn update faster.
	standbyInterval time.Duration

	// snapshotDeps holds dependencies for direct-to-Iceberg snapshot writes.
	// When set, the snapshot bypasses the events table and writes directly
	// to materialized tables.
	snapshotDeps *snapshot.Deps
}

func NewLogicalSource(pgCfg config.PostgresConfig, logicalCfg config.LogicalConfig, tableCfgs []config.TableConfig, pipelineID ...string) *LogicalSource {
	pid := ""
	if len(pipelineID) > 0 {
		pid = pipelineID[0]
	}
	return &LogicalSource{
		pgCfg:           pgCfg,
		cfg:             logicalCfg,
		tableCfgs:       tableCfgs,
		tables:          make(map[string]*postgres.TableSchema),
		relations:       make(map[uint32]*pglogrepl.RelationMessageV2),
		decoder:         NewWALDecoder(),
		pipelineID:      pid,
		standbyInterval: logicalCfg.StandbyIntervalDuration(),
	}
}

// SetStandbyInterval overrides the default 10s standby status interval.
func (l *LogicalSource) SetStandbyInterval(d time.Duration) {
	l.standbyInterval = d
}

// SetStartLSN restores position from a checkpoint.
func (l *LogicalSource) SetStartLSN(lsn uint64) {
	l.startLSN = pglogrepl.LSN(lsn)
	l.receivedLSN.Store(lsn)
	l.flushedLSN.Store(lsn)
}

// SetSnapshotComplete marks the initial snapshot as already done (from checkpoint).
func (l *LogicalSource) SetSnapshotComplete(done bool) {
	l.snapshotComplete = done
}

// SetSnapshotedTables restores per-table snapshot progress from checkpoint.
func (l *LogicalSource) SetSnapshotedTables(tables map[string]bool) {
	l.snapshotedTables = tables
}

// SetSnapshotDeps configures the snapshot to write directly to materialized
// Iceberg tables, bypassing the events table.
func (l *LogicalSource) SetSnapshotDeps(deps *snapshot.Deps) {
	l.snapshotDeps = deps
}

// ReceivedLSN returns the latest WAL position received from PG.
func (l *LogicalSource) ReceivedLSN() uint64 {
	return l.receivedLSN.Load()
}

// FlushedLSN returns the latest WAL position durably written to Iceberg.
func (l *LogicalSource) FlushedLSN() uint64 {
	return l.flushedLSN.Load()
}

// SetFlushedLSN advances the flushed position after a successful Iceberg write.
// This is the position confirmed back to PG, so WAL before it can be recycled.
func (l *LogicalSource) SetFlushedLSN(lsn uint64) {
	l.flushedLSN.Store(lsn)
}

func (l *LogicalSource) Capture(ctx context.Context, events chan<- postgres.ChangeEvent) error {
	// Open a regular connection for schema discovery and slot management.
	var err error
	l.queryConn, err = pgx.Connect(ctx, l.pgCfg.DSN())
	if err != nil {
		return fmt.Errorf("query connect: %w", err)
	}
	defer l.queryConn.Close(ctx)

	// Discover schemas for configured tables.
	for _, tc := range l.tableCfgs {
		ts, err := postgres.DiscoverSchema(ctx, l.queryConn, tc.Name)
		if err != nil {
			return fmt.Errorf("discover schema for %s: %w", tc.Name, err)
		}
		if err := ts.Validate(); err != nil {
			return err
		}
		l.tables[tc.Name] = ts
	}

	// Ensure the source-side marker table exists so operators can insert
	// snapshot-alignment markers. The table is included in the publication
	// below so marker rows flow through logical decoding like any user row.
	if err := l.ensureSwitchoverMarkersTable(ctx); err != nil {
		return fmt.Errorf("ensure switchover_markers: %w", err)
	}

	// Ensure publication exists, creating or updating it if needed.
	if err := l.ensurePublication(ctx); err != nil {
		return fmt.Errorf("ensure publication: %w", err)
	}

	// Open replication connection first — ensureSlot uses it so the exported
	// snapshot stays valid until we finish the initial COPY.
	l.replConn, err = pgconn.Connect(ctx, l.pgCfg.ReplicationDSN())
	if err != nil {
		return fmt.Errorf("replication connect: %w", err)
	}

	// Create or reuse replication slot.
	snapshotName, err := l.ensureSlot(ctx)
	if err != nil {
		return fmt.Errorf("ensure slot: %w", err)
	}

	// If the snapshot hasn't been completed yet, copy all existing rows
	// before starting WAL streaming. On a fresh slot we pin to the exported
	// snapshot; on crash recovery (slot exists, no exported snapshot) we use
	// a plain REPEATABLE READ transaction — duplicates are safe because the
	// sink merges on PK.
	if !l.snapshotComplete {
		if err := l.snapshotTables(ctx, snapshotName); err != nil {
			return fmt.Errorf("initial snapshot: %w", err)
		}
		// Signal to the pipeline that the snapshot is done so it can persist
		// the flag in the checkpoint.
		select {
		case events <- postgres.ChangeEvent{Operation: postgres.OpSnapshotComplete}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Start streaming.
	err = pglogrepl.StartReplication(ctx, l.replConn, l.cfg.SlotName, l.startLSN,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '2'",
				fmt.Sprintf("publication_names '%s'", l.cfg.PublicationName),
				"messages 'true'",
			},
		},
	)
	if err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	log.Printf("[logical] streaming from LSN %s", l.startLSN)

	standbyInterval := l.standbyInterval
	if standbyInterval == 0 {
		standbyInterval = 10 * time.Second
	}
	nextStandby := time.Now().Add(standbyInterval)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Send standby status periodically so PG knows we're alive.
		if time.Now().After(nextStandby) {
			if err := utils.Do(ctx, 3, 100*time.Millisecond, 5*time.Second, func() error {
				return l.sendStandby(ctx)
			}); err != nil {
				return fmt.Errorf("standby status: %w", err)
			}
			nextStandby = time.Now().Add(standbyInterval)
		}

		// Use a short deadline for ReceiveMessage so we can periodically
		// send standby updates even when no WAL data is arriving.
		recvCtx, recvCancel := context.WithDeadline(ctx, nextStandby)
		rawMsg, err := l.replConn.ReceiveMessage(recvCtx)
		recvCancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Timeout is expected — just loop back to send standby.
			if recvCtx.Err() != nil {
				continue
			}
			return fmt.Errorf("receive message: %w", err)
		}

		switch msg := rawMsg.(type) {
		case *pgproto3.CopyData:
			if err := l.handleCopyData(ctx, msg, events); err != nil {
				return err
			}
		}
	}
}

func (l *LogicalSource) handleCopyData(ctx context.Context, msg *pgproto3.CopyData, events chan<- postgres.ChangeEvent) error {
	switch msg.Data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
		if err != nil {
			return fmt.Errorf("parse keepalive: %w", err)
		}
		// Advance receivedLSN from the server's sent-ptr so idle-advance can
		// ack non-tracked-table WAL back to PG. Without this, publications
		// that filter out most activity see receivedLSN stuck at the last
		// tracked-table LSN, pinning confirmed_flush_lsn and leaking WAL.
		if end := uint64(pkm.ServerWALEnd); end > l.receivedLSN.Load() {
			l.receivedLSN.Store(end)
		}
		if pkm.ReplyRequested {
			if err := utils.Do(ctx, 3, 100*time.Millisecond, 5*time.Second, func() error {
				return l.sendStandby(ctx)
			}); err != nil {
				return fmt.Errorf("standby status: %w", err)
			}
		}

	case pglogrepl.XLogDataByteID:
		xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
		if err != nil {
			return fmt.Errorf("parse xlog: %w", err)
		}

		if err := l.processWAL(ctx, xld, events); err != nil {
			return err
		}

		// Track the latest WAL position received.
		end := uint64(xld.WALStart) + uint64(len(xld.WALData))
		if end > l.receivedLSN.Load() {
			l.receivedLSN.Store(end)
		}
	}
	return nil
}

func (l *LogicalSource) processWAL(ctx context.Context, xld pglogrepl.XLogData, events chan<- postgres.ChangeEvent) error {
	msg, err := l.decoder.Decode(xld.WALData)
	if err != nil {
		return fmt.Errorf("decode WAL: %w", err)
	}

	walEnd := uint64(xld.WALStart) + uint64(len(xld.WALData))

	switch msg.Type {
	case msgRelation:
		rel := msg.Relation
		pgRel := rel.ToRelationMessageV2()
		table := fqTable(rel.Namespace, rel.RelationName)
		if l.isTracked(table) {
			if old, exists := l.relations[rel.RelationID]; exists {
				if diff := diffRelation(old, pgRel); diff != nil {
					diff.Table = table
					log.Printf("[logical] schema change detected for %s: +%d columns, -%d columns, %d type changes",
						table, len(diff.AddedColumns), len(diff.DroppedColumns), len(diff.TypeChanges))
					select {
					case events <- postgres.ChangeEvent{
						Table:        table,
						Operation:    postgres.OpSchemaChange,
						SchemaChange: diff,
						LSN:          walEnd,
					}:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
		}
		l.relations[rel.RelationID] = pgRel

	case msgInsert:
		rel, ok := l.relations[msg.RelationID]
		if !ok {
			return nil
		}
		table := fqTable(rel.Namespace, rel.RelationName)
		// Marker table: emit a synthetic OpMarker event instead of a
		// user-table INSERT. The marker row itself is never written to the
		// sink — the pipeline uses it as a flush fence at the transaction
		// COMMIT that contains it.
		if table == MarkerTableQualified {
			uuidVal, _ := msg.After["uuid"].(string)
			if uuidVal == "" {
				log.Printf("[logical] marker row missing uuid at LSN %d, ignoring", walEnd)
				return nil
			}
			select {
			case events <- postgres.ChangeEvent{
				Table:               table,
				Operation:           postgres.OpMarker,
				MarkerUUID:          uuidVal,
				LSN:                 walEnd,
				SourceTimestamp:     l.currentTxCommitTime,
				ProcessingTimestamp: time.Now(),
				TransactionID:       l.currentTxXID,
			}:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		}
		if !l.isTracked(table) {
			return nil
		}
		ts := l.schemaForRelation(rel)

		select {
		case events <- postgres.ChangeEvent{
			Table:              table,
			Operation:          postgres.OpInsert,
			After:              msg.After,
			PK:                 ts.PK,
			LSN:                walEnd,
			SourceTimestamp:     l.currentTxCommitTime,
			ProcessingTimestamp: time.Now(),
			TransactionID:      l.currentTxXID,
		}:
		case <-ctx.Done():
			return ctx.Err()
		}

	case msgUpdate:
		rel, ok := l.relations[msg.RelationID]
		if !ok {
			return nil
		}
		table := fqTable(rel.Namespace, rel.RelationName)
		if !l.isTracked(table) {
			return nil
		}
		ts := l.schemaForRelation(rel)
		unchangedCols := ExtractUnchangedCols(msg.After)

		select {
		case events <- postgres.ChangeEvent{
			Table:              table,
			Operation:          postgres.OpUpdate,
			Before:             msg.Before,
			PK:                 ts.PK,
			After:              msg.After,
			LSN:                walEnd,
			SourceTimestamp:     l.currentTxCommitTime,
			ProcessingTimestamp: time.Now(),
			UnchangedCols:      unchangedCols,
			TransactionID:      l.currentTxXID,
		}:
		case <-ctx.Done():
			return ctx.Err()
		}

	case msgDelete:
		rel, ok := l.relations[msg.RelationID]
		if !ok {
			return nil
		}
		table := fqTable(rel.Namespace, rel.RelationName)
		if !l.isTracked(table) {
			return nil
		}
		ts := l.schemaForRelation(rel)

		select {
		case events <- postgres.ChangeEvent{
			Table:              table,
			Operation:          postgres.OpDelete,
			Before:             msg.Before,
			PK:                 ts.PK,
			LSN:                walEnd,
			SourceTimestamp:     l.currentTxCommitTime,
			ProcessingTimestamp: time.Now(),
			TransactionID:      l.currentTxXID,
		}:
		case <-ctx.Done():
			return ctx.Err()
		}

	case msgBegin:
		l.currentTxCommitTime = msg.CommitTime
		l.currentTxXID = msg.Xid
		select {
		case events <- postgres.ChangeEvent{
			Operation:      postgres.OpBegin,
			LSN:            walEnd,
			TransactionID:  msg.Xid,
			SourceTimestamp: msg.CommitTime,
		}:
		case <-ctx.Done():
			return ctx.Err()
		}

	case msgCommit:
		select {
		case events <- postgres.ChangeEvent{
			Operation:      postgres.OpCommit,
			LSN:            walEnd,
			TransactionID:  l.currentTxXID,
			SourceTimestamp: msg.CommitTime,
		}:
		case <-ctx.Done():
			return ctx.Err()
		}
		l.currentTxCommitTime = time.Time{}
		l.currentTxXID = 0

	case msgTruncate:
		log.Printf("[logical] TRUNCATE detected (not yet handled)")
	}

	return nil
}

// ensureSlot creates the replication slot if it doesn't already exist.
// It uses l.replConn (which must already be open) so that the exported
// snapshot remains valid for the initial COPY.
// Returns the snapshot name (non-empty only when a new slot was created).
// MarkerSchemaName and MarkerTableName identify the source-side marker table
// that operators insert into to trigger a snapshot-alignment flush. The table
// lives on both the publisher (blue) and subscriber (green) sides — on green
// the row arrives via logical replication from blue.
const (
	MarkerSchemaName = "_pg2iceberg"
	MarkerTableName  = "switchover_markers"
)

// MarkerTableQualified is the schema-qualified name used in publications and
// decoder filtering.
var MarkerTableQualified = MarkerSchemaName + "." + MarkerTableName

// ensureSwitchoverMarkersTable creates the _pg2iceberg schema and the
// switchover_markers table if they don't already exist. Idempotent. The
// table is the source-side rendezvous point for blue/green verification —
// an operator inserts a UUID, pg2iceberg observes it in the WAL stream on
// both sides, and produces aligned Iceberg snapshots.
func (l *LogicalSource) ensureSwitchoverMarkersTable(ctx context.Context) error {
	if _, err := l.queryConn.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+pgx.Identifier{MarkerSchemaName}.Sanitize()); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}
	_, err := l.queryConn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			uuid       TEXT PRIMARY KEY,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`, pgx.Identifier{MarkerSchemaName}.Sanitize(), pgx.Identifier{MarkerTableName}.Sanitize()))
	if err != nil {
		return fmt.Errorf("create marker table: %w", err)
	}
	return nil
}

// ensurePublication creates the publication if it doesn't exist, and ensures
// it covers all configured tables. If tables are added to the config, they are
// added to the publication; tables removed from config are left in the publication
// (safe — they just produce ignored events).
//
// On PostgreSQL 13+, the publication is created/updated with
// publish_via_partition_root = true so that WAL events for partitioned tables
// are tagged with the root parent table name (matching the configured table).
func (l *LogicalSource) ensurePublication(ctx context.Context) error {
	pubName := l.cfg.PublicationName

	// Check PostgreSQL version for publish_via_partition_root support (PG 13+).
	var pgVersionNum int
	if err := l.queryConn.QueryRow(ctx,
		"SELECT current_setting('server_version_num')::int",
	).Scan(&pgVersionNum); err != nil {
		return fmt.Errorf("query pg version: %w", err)
	}
	supportsPartitionRoot := pgVersionNum >= 130000

	if !supportsPartitionRoot {
		for _, ts := range l.tables {
			if ts.Partitioned {
				return fmt.Errorf("table %s is partitioned but PostgreSQL %d does not support publish_via_partition_root (requires 13+); "+
					"partitioned table replication is not possible on this version", ts.Table, pgVersionNum)
			}
		}
	}

	var pubExists bool
	err := l.queryConn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)",
		pubName,
	).Scan(&pubExists)
	if err != nil {
		return fmt.Errorf("check publication: %w", err)
	}

	// Collect configured table names, plus the marker table so operators
	// can drive snapshot alignment through logical replication.
	tableNames := make([]string, 0, len(l.tableCfgs)+1)
	for _, tc := range l.tableCfgs {
		tableNames = append(tableNames, tc.Name)
	}
	tableNames = append(tableNames, MarkerTableQualified)

	if !pubExists {
		// CREATE PUBLICATION requires table names inlined (not parameterized).
		tablesSQL := strings.Join(tableNames, ", ")
		createSQL := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pgx.Identifier{pubName}.Sanitize(), tablesSQL)
		if supportsPartitionRoot {
			createSQL += " WITH (publish_via_partition_root = true)"
		}
		_, err := l.queryConn.Exec(ctx, createSQL)
		if err != nil {
			return fmt.Errorf("create publication: %w", err)
		}
		log.Printf("[logical] created publication %q for tables: %s", pubName, tablesSQL)
		return nil
	}

	// Publication exists — check if all configured tables are included.
	rows, err := l.queryConn.Query(ctx,
		"SELECT schemaname || '.' || tablename FROM pg_publication_tables WHERE pubname = $1",
		pubName)
	if err != nil {
		return fmt.Errorf("list publication tables: %w", err)
	}
	defer rows.Close()

	published := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("scan publication table: %w", err)
		}
		published[name] = true
	}

	var missing []string
	for _, t := range tableNames {
		if !published[t] {
			missing = append(missing, t)
		}
	}

	if len(missing) > 0 {
		tablesSQL := strings.Join(missing, ", ")
		_, err := l.queryConn.Exec(ctx,
			fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s", pgx.Identifier{pubName}.Sanitize(), tablesSQL))
		if err != nil {
			return fmt.Errorf("alter publication: %w", err)
		}
		log.Printf("[logical] added tables to publication %q: %s", pubName, tablesSQL)
	} else {
		log.Printf("[logical] publication %q exists with all configured tables", pubName)
	}

	// Ensure publish_via_partition_root is set on existing publications.
	// This is idempotent and harmless for non-partitioned tables.
	if supportsPartitionRoot {
		_, err = l.queryConn.Exec(ctx,
			fmt.Sprintf("ALTER PUBLICATION %s SET (publish_via_partition_root = true)", pgx.Identifier{pubName}.Sanitize()))
		if err != nil {
			return fmt.Errorf("set publish_via_partition_root: %w", err)
		}
	}

	return nil
}

func (l *LogicalSource) ensureSlot(ctx context.Context) (string, error) {
	var exists bool
	err := l.queryConn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
		l.cfg.SlotName,
	).Scan(&exists)
	if err != nil {
		return "", fmt.Errorf("check slot: %w", err)
	}

	if exists {
		log.Printf("[logical] reusing existing slot %q", l.cfg.SlotName)
		return "", nil
	}

	result, err := pglogrepl.CreateReplicationSlot(ctx, l.replConn, l.cfg.SlotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{
			Temporary: false,
		},
	)
	if err != nil {
		return "", fmt.Errorf("create slot: %w", err)
	}

	if l.startLSN == 0 {
		lsn, err := pglogrepl.ParseLSN(result.ConsistentPoint)
		if err != nil {
			return "", fmt.Errorf("parse consistent point LSN %q: %w", result.ConsistentPoint, err)
		}
		l.startLSN = lsn
		l.receivedLSN.Store(uint64(lsn))
		l.flushedLSN.Store(uint64(lsn))
	}

	log.Printf("[logical] created slot %q at LSN %s (snapshot %s)", l.cfg.SlotName, result.ConsistentPoint, result.SnapshotName)
	return result.SnapshotName, nil
}

// snapshotTables performs an initial full-table copy for all tracked tables
// using CTID range chunks, writing directly to materialized Iceberg tables.
// When snapshotName is non-empty (fresh slot), each per-table transaction is
// pinned to the exported snapshot. On crash recovery (empty snapshotName),
// a plain REPEATABLE READ transaction is used instead.
func (l *LogicalSource) snapshotTables(ctx context.Context, snapshotName string) error {
	if l.snapshotDeps == nil {
		return fmt.Errorf("snapshot deps not set; call SetSnapshotDeps before Capture")
	}

	var tables []snapshot.Table
	for _, tc := range l.tableCfgs {
		if tc.SkipSnapshot {
			log.Printf("[logical] snapshot: skipping %s (skip_snapshot=true)", tc.Name)
			continue
		}
		if l.snapshotedTables[tc.Name] {
			log.Printf("[logical] snapshot: skipping %s (already completed)", tc.Name)
			continue
		}
		tables = append(tables, snapshot.Table{
			Name:   tc.Name,
			Schema: l.tables[tc.Name],
		})
	}

	if len(tables) == 0 {
		return nil
	}

	dsn := l.pgCfg.DSN()
	txFactory := func(ctx context.Context) (pgx.Tx, func(context.Context), error) {
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return nil, nil, fmt.Errorf("snapshot connect: %w", err)
		}
		cleanup := func(ctx context.Context) { conn.Close(ctx) }

		tx, err := conn.Begin(ctx)
		if err != nil {
			cleanup(ctx)
			return nil, nil, fmt.Errorf("begin snapshot tx: %w", err)
		}

		if _, err := tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
			tx.Rollback(ctx)
			cleanup(ctx)
			return nil, nil, fmt.Errorf("set isolation level: %w", err)
		}

		// Pin to exported snapshot if available (fresh slot).
		if snapshotName != "" {
			if _, err := tx.Exec(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotName)); err != nil {
				tx.Rollback(ctx)
				cleanup(ctx)
				return nil, nil, fmt.Errorf("set transaction snapshot: %w", err)
			}
		}

		return tx, cleanup, nil
	}

	snap := snapshot.NewSnapshotter(tables, txFactory, l.cfg.SnapshotConcurrencyOrDefault(), *l.snapshotDeps)
	log.Printf("[logical] snapshot: %d tables, concurrency=%d", len(tables), l.cfg.SnapshotConcurrencyOrDefault())
	if _, err := snap.Run(ctx); err != nil {
		return err
	}

	return nil
}

// standbyStatus builds the status update that will be sent to PG.
// WALWritePosition uses receivedLSN so PG knows we're alive and reading,
// but WALFlushPosition (which controls WAL recycling) uses flushedLSN.
func (l *LogicalSource) standbyStatus() pglogrepl.StandbyStatusUpdate {
	flushed := pglogrepl.LSN(l.flushedLSN.Load())
	received := pglogrepl.LSN(l.receivedLSN.Load())
	return pglogrepl.StandbyStatusUpdate{
		WALWritePosition: received,
		WALFlushPosition: flushed,
		WALApplyPosition: flushed,
		ClientTime:       time.Now(),
		ReplyRequested:   false,
	}
}

func (l *LogicalSource) sendStandby(ctx context.Context) error {
	// Only confirm the flushed LSN back to PG. This ensures PG will not
	// recycle WAL that hasn't been durably written to Iceberg.
	return pglogrepl.SendStandbyStatusUpdate(ctx, l.replConn, l.standbyStatus())
}

// SendStandbyNow sends a standby status update to PG immediately.
// This is useful in tests to force PG to update confirmed_flush_lsn
// without waiting for the next periodic heartbeat.
func (l *LogicalSource) SendStandbyNow(ctx context.Context) error {
	return pglogrepl.SendStandbyStatusUpdate(ctx, l.replConn, l.standbyStatus())
}

// Close terminates the replication connection. The replication slot is
// intentionally never dropped — it must persist across restarts so the
// pipeline can resume from its last confirmed LSN without data loss.
// WAL accumulation from a long-lived slot should be managed via
// PostgreSQL's max_slot_wal_keep_size and replication lag monitoring.
func (l *LogicalSource) Close() error {
	if l.replConn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := l.replConn.Close(ctx); err != nil {
			cancel()
			l.replConn = nil
			return fmt.Errorf("close replication connection: %w", err)
		}
		cancel()
		l.replConn = nil
	}
	return nil
}

// AddTable adds a table to the tracked set. The table must already be in
// the PG publication; this only controls which replicated events pg2iceberg processes.
func (l *LogicalSource) AddTable(table string) {
	if !l.isTracked(table) {
		l.tableCfgs = append(l.tableCfgs, config.TableConfig{Name: table})
	}
}

// RemoveTable removes a table from the tracked set.
func (l *LogicalSource) RemoveTable(table string) {
	for i, tc := range l.tableCfgs {
		if tc.Name == table {
			l.tableCfgs = append(l.tableCfgs[:i], l.tableCfgs[i+1:]...)
			delete(l.tables, table)
			return
		}
	}
}

func (l *LogicalSource) isTracked(table string) bool {
	for _, tc := range l.tableCfgs {
		if tc.Name == table {
			return true
		}
	}
	return false
}

func (l *LogicalSource) schemaForRelation(rel *pglogrepl.RelationMessageV2) *postgres.TableSchema {
	table := fqTable(rel.Namespace, rel.RelationName)
	if ts, ok := l.tables[table]; ok {
		return ts
	}
	// Build a minimal schema from the relation message
	ts := &postgres.TableSchema{Table: table}
	for i, col := range rel.Columns {
		pgType := oidToPGType(col.DataType)
		precision, scale := numericTypmod(pgType, col.TypeModifier)
		ts.Columns = append(ts.Columns, postgres.Column{
			Name:      col.Name,
			PGType:    pgType,
			FieldID:   i + 1,
			Precision: precision,
			Scale:     scale,
		})
	}
	l.tables[table] = ts
	return ts
}

// numericTypmod decodes precision and scale from a PostgreSQL type modifier
// for numeric types. Returns (0, 0) for non-numeric types or unconstrained numeric.
func numericTypmod(pgType postgres.Type, typmod int32) (precision, scale int) {
	if pgType != postgres.Numeric || typmod < 0 {
		return 0, 0
	}
	// PostgreSQL stores numeric typmod as ((precision << 16) | scale) + VARHDRSZ(4)
	mod := int(typmod - 4)
	return (mod >> 16) & 0xFFFF, mod & 0xFFFF
}



func fqTable(namespace, name string) string {
	if namespace == "" || strings.EqualFold(namespace, "public") {
		return "public." + name
	}
	return namespace + "." + name
}

// pgOIDToType maps well-known PostgreSQL type OIDs to their canonical type.
// Unknown OIDs default to Text.
var pgOIDToType = map[uint32]postgres.Type{
	16:   postgres.Bool,
	17:   postgres.Bytea,
	20:   postgres.Int8,
	21:   postgres.Int2,
	23:   postgres.Int4,
	25:   postgres.Text,
	26:   postgres.OID,
	114:  postgres.JSON,
	700:  postgres.Float4,
	701:  postgres.Float8,
	1042: postgres.Bpchar,
	1043: postgres.Varchar,
	1082: postgres.Date,
	1083: postgres.Time,
	1114: postgres.Timestamp,
	1184: postgres.TimestampTZ,
	1266: postgres.TimeTZ,
	1700: postgres.Numeric,
	2950: postgres.UUID,
	3802: postgres.JSONB,
}

func oidToPGType(oid uint32) postgres.Type {
	if t, ok := pgOIDToType[oid]; ok {
		return t
	}
	return postgres.Text
}

// diffRelation compares two RelationMessageV2 and returns a postgres.SchemaChange if
// columns were added, dropped, or changed type. Returns nil when identical.
func diffRelation(old, new *pglogrepl.RelationMessageV2) *postgres.SchemaChange {
	oldCols := make(map[string]uint32, len(old.Columns))
	for _, c := range old.Columns {
		oldCols[c.Name] = c.DataType
	}
	newCols := make(map[string]uint32, len(new.Columns))
	for _, c := range new.Columns {
		newCols[c.Name] = c.DataType
	}

	var sc postgres.SchemaChange

	// Detect added columns and type changes.
	for _, c := range new.Columns {
		oldOID, existed := oldCols[c.Name]
		if !existed {
			sc.AddedColumns = append(sc.AddedColumns, postgres.SchemaColumn{
				Name:   c.Name,
				PGType: oidToPGType(c.DataType),
			})
		} else if oldOID != c.DataType {
			sc.TypeChanges = append(sc.TypeChanges, postgres.TypeChange{
				Name:    c.Name,
				OldType: oidToPGType(oldOID),
				NewType: oidToPGType(c.DataType),
			})
		}
	}

	// Detect dropped columns.
	for _, c := range old.Columns {
		if _, exists := newCols[c.Name]; !exists {
			sc.DroppedColumns = append(sc.DroppedColumns, c.Name)
		}
	}

	if len(sc.AddedColumns) == 0 && len(sc.DroppedColumns) == 0 && len(sc.TypeChanges) == 0 {
		return nil
	}
	return &sc
}
