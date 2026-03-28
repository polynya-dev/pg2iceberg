package source

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/schema"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

// LogicalSource implements Source using PostgreSQL logical replication.
// It manages the full lifecycle of the replication slot.
type LogicalSource struct {
	pgCfg      config.PostgresConfig
	cfg        config.LogicalConfig
	tableCfgs  []config.TableConfig
	tables     map[string]*schema.TableSchema
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
	receivedLSN pglogrepl.LSN
	// flushedLSN is the latest WAL position that has been durably written
	// to Iceberg. Only this value is confirmed back to PG via standby
	// status updates, so PG will not recycle WAL that hasn't been persisted.
	// Accessed atomically: written by the pipeline goroutine (SetFlushedLSN),
	// read by the capture goroutine (standbyStatus).
	flushedLSN atomic.Uint64

	// slotCreated tracks whether we created the slot (for cleanup).
	slotCreated bool

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
		tables:          make(map[string]*schema.TableSchema),
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
	l.receivedLSN = pglogrepl.LSN(lsn)
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

// ReceivedLSN returns the latest WAL position received from PG.
func (l *LogicalSource) ReceivedLSN() uint64 {
	return uint64(l.receivedLSN)
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

func (l *LogicalSource) Capture(ctx context.Context, events chan<- ChangeEvent) error {
	// Open a regular connection for schema discovery and slot management.
	var err error
	l.queryConn, err = pgx.Connect(ctx, l.pgCfg.DSN())
	if err != nil {
		return fmt.Errorf("query connect: %w", err)
	}
	defer l.queryConn.Close(ctx)

	// Discover schemas for configured tables.
	for _, tc := range l.tableCfgs {
		ts, err := schema.DiscoverSchema(ctx, l.queryConn, tc.Name)
		if err != nil {
			return fmt.Errorf("discover schema for %s: %w", tc.Name, err)
		}
		l.tables[tc.Name] = ts
	}

	// Validate publication exists.
	var pubExists bool
	err = l.queryConn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)",
		l.cfg.PublicationName,
	).Scan(&pubExists)
	if err != nil {
		return fmt.Errorf("check publication: %w", err)
	}
	if !pubExists {
		return fmt.Errorf("publication %q does not exist; create it with: CREATE PUBLICATION %s FOR TABLE ...",
			l.cfg.PublicationName, l.cfg.PublicationName)
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
		if err := l.snapshotTables(ctx, snapshotName, events); err != nil {
			return fmt.Errorf("initial snapshot: %w", err)
		}
		// Signal to the pipeline that the snapshot is done so it can persist
		// the flag in the checkpoint.
		select {
		case events <- ChangeEvent{Operation: OpSnapshotComplete}:
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
			l.sendStandby(ctx)
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

func (l *LogicalSource) handleCopyData(ctx context.Context, msg *pgproto3.CopyData, events chan<- ChangeEvent) error {
	switch msg.Data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
		if err != nil {
			return fmt.Errorf("parse keepalive: %w", err)
		}
		if pkm.ReplyRequested {
			l.sendStandby(ctx)
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
		if xld.WALStart+pglogrepl.LSN(len(xld.WALData)) > l.receivedLSN {
			l.receivedLSN = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
		}
	}
	return nil
}

func (l *LogicalSource) processWAL(ctx context.Context, xld pglogrepl.XLogData, events chan<- ChangeEvent) error {
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
					case events <- ChangeEvent{
						Table:        table,
						Operation:    OpSchemaChange,
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
		if !l.isTracked(table) {
			return nil
		}
		ts := l.schemaForRelation(rel)

		select {
		case events <- ChangeEvent{
			Table:              table,
			Operation:          OpInsert,
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
		case events <- ChangeEvent{
			Table:              table,
			Operation:          OpUpdate,
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
		case events <- ChangeEvent{
			Table:              table,
			Operation:          OpDelete,
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
		case events <- ChangeEvent{
			Operation:      OpBegin,
			LSN:            walEnd,
			TransactionID:  msg.Xid,
			SourceTimestamp: msg.CommitTime,
		}:
		case <-ctx.Done():
			return ctx.Err()
		}

	case msgCommit:
		select {
		case events <- ChangeEvent{
			Operation:      OpCommit,
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
		if err == nil {
			l.startLSN = lsn
			l.receivedLSN = lsn
			l.flushedLSN.Store(uint64(lsn))
		}
	}

	l.slotCreated = true
	log.Printf("[logical] created slot %q at LSN %s (snapshot %s)", l.cfg.SlotName, result.ConsistentPoint, result.SnapshotName)
	return result.SnapshotName, nil
}

// snapshotTables performs an initial full-table copy for all tracked tables.
// When snapshotName is non-empty (fresh slot), each per-table transaction is
// pinned to the exported snapshot. On crash recovery (empty snapshotName),
// a plain REPEATABLE READ transaction is used instead.
func (l *LogicalSource) snapshotTables(ctx context.Context, snapshotName string, events chan<- ChangeEvent) error {
	var tables []SnapshotTable
	for _, tc := range l.tableCfgs {
		if tc.SkipSnapshot {
			log.Printf("[logical] snapshot: skipping %s (skip_snapshot=true)", tc.Name)
			continue
		}
		if l.snapshotedTables[tc.Name] {
			log.Printf("[logical] snapshot: skipping %s (already completed)", tc.Name)
			continue
		}
		tables = append(tables, SnapshotTable{
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

	snap := NewSnapshotter(tables, txFactory, l.cfg.SnapshotConcurrencyOrDefault(), l.pipelineID)
	log.Printf("[logical] snapshot: %d tables, concurrency=%d", len(tables), l.cfg.SnapshotConcurrencyOrDefault())
	if _, err := snap.Run(ctx, events); err != nil {
		return err
	}

	return nil
}

// standbyStatus builds the status update that will be sent to PG.
// WALWritePosition uses receivedLSN so PG knows we're alive and reading,
// but WALFlushPosition (which controls WAL recycling) uses flushedLSN.
func (l *LogicalSource) standbyStatus() pglogrepl.StandbyStatusUpdate {
	flushed := pglogrepl.LSN(l.flushedLSN.Load())
	return pglogrepl.StandbyStatusUpdate{
		WALWritePosition: l.receivedLSN,
		WALFlushPosition: flushed,
		WALApplyPosition: flushed,
		ClientTime:       time.Now(),
		ReplyRequested:   false,
	}
}

func (l *LogicalSource) sendStandby(ctx context.Context) {
	// Only confirm the flushed LSN back to PG. This ensures PG will not
	// recycle WAL that hasn't been durably written to Iceberg.
	err := pglogrepl.SendStandbyStatusUpdate(ctx, l.replConn, l.standbyStatus())
	if err != nil {
		log.Printf("[logical] standby status error: %v", err)
	}
}

// SendStandbyNow sends a standby status update to PG immediately.
// This is useful in tests to force PG to update confirmed_flush_lsn
// without waiting for the next periodic heartbeat.
func (l *LogicalSource) SendStandbyNow(ctx context.Context) error {
	return pglogrepl.SendStandbyStatusUpdate(ctx, l.replConn, l.standbyStatus())
}

// Close terminates the replication connection and drops the slot if we created it.
func (l *LogicalSource) Close() error {
	// Close the replication connection first so the slot becomes inactive.
	if l.replConn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		l.replConn.Close(ctx)
		cancel()
		l.replConn = nil
	}

	if !l.slotCreated {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, l.pgCfg.DSN())
	if err != nil {
		return fmt.Errorf("connect for cleanup: %w", err)
	}
	defer conn.Close(ctx)

	// Slot should be inactive now since we closed replConn above.
	_, err = conn.Exec(ctx,
		"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1 AND NOT active",
		l.cfg.SlotName,
	)
	if err != nil {
		return fmt.Errorf("drop slot: %w", err)
	}

	log.Printf("[logical] dropped slot %q", l.cfg.SlotName)
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

func (l *LogicalSource) schemaForRelation(rel *pglogrepl.RelationMessageV2) *schema.TableSchema {
	table := fqTable(rel.Namespace, rel.RelationName)
	if ts, ok := l.tables[table]; ok {
		return ts
	}
	// Build a minimal schema from the relation message
	ts := &schema.TableSchema{Table: table}
	for i, col := range rel.Columns {
		ts.Columns = append(ts.Columns, schema.Column{
			Name:    col.Name,
			PGType:  oidToPGType(col.DataType),
			FieldID: i + 1,
		})
	}
	l.tables[table] = ts
	return ts
}


// pgValueToString converts a pgx native Go value to its text representation,
// matching the format produced by the WAL decoder (which always returns strings).
func pgValueToString(v any) any {
	if v == nil {
		return nil
	}
	switch x := v.(type) {
	case string:
		return x
	case int16:
		return fmt.Sprintf("%d", x)
	case int32:
		return fmt.Sprintf("%d", x)
	case int64:
		return fmt.Sprintf("%d", x)
	case float32:
		return fmt.Sprintf("%g", x)
	case float64:
		return fmt.Sprintf("%g", x)
	case bool:
		if x {
			return "t"
		}
		return "f"
	case time.Time:
		return x.Format("2006-01-02 15:04:05.999999-07")
	case pgtype.Numeric:
		// Reconstruct the text representation preserving scale (trailing zeros).
		// Exp is negative for fractional digits: e.g. Int=10050, Exp=-2 → "100.50"
		if !x.Valid {
			return nil
		}
		intStr := x.Int.String()
		if x.Exp >= 0 {
			return intStr + strings.Repeat("0", int(x.Exp))
		}
		scale := int(-x.Exp)
		if len(intStr) <= scale {
			intStr = strings.Repeat("0", scale-len(intStr)+1) + intStr
		}
		pos := len(intStr) - scale
		return intStr[:pos] + "." + intStr[pos:]
	case []byte:
		return string(x)
	default:
		return fmt.Sprintf("%v", x)
	}
}

func fqTable(namespace, name string) string {
	if namespace == "" || strings.EqualFold(namespace, "public") {
		return "public." + name
	}
	return namespace + "." + name
}

// pgOIDToType maps well-known PostgreSQL type OIDs to their canonical type names.
// Unknown OIDs default to "text".
var pgOIDToType = map[uint32]string{
	16:   "bool",
	17:   "bytea",
	20:   "int8",
	21:   "int2",
	23:   "int4",
	25:   "text",
	114:  "json",
	700:  "float4",
	701:  "float8",
	1042: "bpchar",
	1043: "varchar",
	1082: "date",
	1114: "timestamp",
	1184: "timestamptz",
	1700: "numeric",
	2950: "uuid",
	3802: "jsonb",
}

func oidToPGType(oid uint32) string {
	if t, ok := pgOIDToType[oid]; ok {
		return t
	}
	return "text"
}

// diffRelation compares two RelationMessageV2 and returns a SchemaChange if
// columns were added, dropped, or changed type. Returns nil when identical.
func diffRelation(old, new *pglogrepl.RelationMessageV2) *SchemaChange {
	oldCols := make(map[string]uint32, len(old.Columns))
	for _, c := range old.Columns {
		oldCols[c.Name] = c.DataType
	}
	newCols := make(map[string]uint32, len(new.Columns))
	for _, c := range new.Columns {
		newCols[c.Name] = c.DataType
	}

	var sc SchemaChange

	// Detect added columns and type changes.
	for _, c := range new.Columns {
		oldOID, existed := oldCols[c.Name]
		if !existed {
			sc.AddedColumns = append(sc.AddedColumns, SchemaColumn{
				Name:   c.Name,
				PGType: oidToPGType(c.DataType),
			})
		} else if oldOID != c.DataType {
			sc.TypeChanges = append(sc.TypeChanges, TypeChange{
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
