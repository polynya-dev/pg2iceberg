package source

import (
	"context"
	"fmt"
	"log"
	"strings"
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
	pgCfg     config.PostgresConfig
	cfg       config.LogicalConfig
	tableCfgs []config.TableConfig
	tables    map[string]*schema.TableSchema

	replConn  *pgconn.PgConn
	queryConn *pgx.Conn

	// relations caches RelationMessage by OID for decoding tuples.
	relations map[uint32]*pglogrepl.RelationMessageV2

	// startLSN is the position to start streaming from.
	startLSN pglogrepl.LSN
	// confirmedLSN is the last position we confirmed to PG.
	confirmedLSN pglogrepl.LSN

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
}

func NewLogicalSource(pgCfg config.PostgresConfig, logicalCfg config.LogicalConfig, tableCfgs []config.TableConfig) *LogicalSource {
	return &LogicalSource{
		pgCfg:     pgCfg,
		cfg:       logicalCfg,
		tableCfgs: tableCfgs,
		tables:    make(map[string]*schema.TableSchema),
		relations: make(map[uint32]*pglogrepl.RelationMessageV2),
	}
}

// SetStartLSN restores position from a checkpoint.
func (l *LogicalSource) SetStartLSN(lsn uint64) {
	l.startLSN = pglogrepl.LSN(lsn)
	l.confirmedLSN = pglogrepl.LSN(lsn)
}

// SetSnapshotComplete marks the initial snapshot as already done (from checkpoint).
func (l *LogicalSource) SetSnapshotComplete(done bool) {
	l.snapshotComplete = done
}

// SetSnapshotedTables restores per-table snapshot progress from checkpoint.
func (l *LogicalSource) SetSnapshotedTables(tables map[string]bool) {
	l.snapshotedTables = tables
}

// ConfirmedLSN returns the current confirmed LSN for checkpointing.
func (l *LogicalSource) ConfirmedLSN() uint64 {
	return uint64(l.confirmedLSN)
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

	standbyTicker := time.NewTicker(10 * time.Second)
	defer standbyTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-standbyTicker.C:
			l.sendStandby(ctx)
		default:
		}

		rawMsg, err := l.replConn.ReceiveMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
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

		// Track position for standby updates.
		if xld.WALStart+pglogrepl.LSN(len(xld.WALData)) > l.confirmedLSN {
			l.confirmedLSN = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
		}
	}
	return nil
}

func (l *LogicalSource) processWAL(ctx context.Context, xld pglogrepl.XLogData, events chan<- ChangeEvent) error {
	logicalMsg, err := pglogrepl.ParseV2(xld.WALData, false)
	if err != nil {
		return fmt.Errorf("parse logical msg: %w", err)
	}

	switch m := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		l.relations[m.RelationID] = m

	case *pglogrepl.InsertMessageV2:
		rel, ok := l.relations[m.RelationID]
		if !ok {
			return nil // skip unknown relation
		}
		table := fqTable(rel.Namespace, rel.RelationName)
		if !l.isTracked(table) {
			return nil
		}
		ts := l.schemaForRelation(rel)
		after, _ := decodeTuple(rel, m.Tuple)

		select {
		case events <- ChangeEvent{
			Table:              table,
			Operation:          OpInsert,
			After:              after,
			PK:                 ts.PK,
			SourceTimestamp:     l.currentTxCommitTime,
			ProcessingTimestamp: time.Now(),
			TransactionID:      l.currentTxXID,
		}:
		case <-ctx.Done():
			return ctx.Err()
		}

	case *pglogrepl.UpdateMessageV2:
		rel, ok := l.relations[m.RelationID]
		if !ok {
			return nil
		}
		table := fqTable(rel.Namespace, rel.RelationName)
		if !l.isTracked(table) {
			return nil
		}
		ts := l.schemaForRelation(rel)

		var before map[string]any
		if m.OldTuple != nil {
			before, _ = decodeTuple(rel, m.OldTuple)
		}
		after, unchangedCols := decodeTuple(rel, m.NewTuple)

		select {
		case events <- ChangeEvent{
			Table:              table,
			Operation:          OpUpdate,
			Before:             before,
			PK:                 ts.PK,
			After:              after,
			SourceTimestamp:     l.currentTxCommitTime,
			ProcessingTimestamp: time.Now(),
			UnchangedCols:      unchangedCols,
			TransactionID:      l.currentTxXID,
		}:
		case <-ctx.Done():
			return ctx.Err()
		}

	case *pglogrepl.DeleteMessageV2:
		rel, ok := l.relations[m.RelationID]
		if !ok {
			return nil
		}
		table := fqTable(rel.Namespace, rel.RelationName)
		if !l.isTracked(table) {
			return nil
		}
		ts := l.schemaForRelation(rel)

		var before map[string]any
		if m.OldTuple != nil {
			before, _ = decodeTuple(rel, m.OldTuple)
		}

		select {
		case events <- ChangeEvent{
			Table:              table,
			Operation:          OpDelete,
			Before:             before,
			PK:                 ts.PK,
			SourceTimestamp:     l.currentTxCommitTime,
			ProcessingTimestamp: time.Now(),
			TransactionID:      l.currentTxXID,
		}:
		case <-ctx.Done():
			return ctx.Err()
		}

	case *pglogrepl.BeginMessage:
		l.currentTxCommitTime = m.CommitTime
		l.currentTxXID = m.Xid
		select {
		case events <- ChangeEvent{
			Operation:      OpBegin,
			TransactionID:  m.Xid,
			SourceTimestamp: m.CommitTime,
		}:
		case <-ctx.Done():
			return ctx.Err()
		}
	case *pglogrepl.CommitMessage:
		select {
		case events <- ChangeEvent{
			Operation:      OpCommit,
			TransactionID:  l.currentTxXID,
			SourceTimestamp: m.CommitTime,
		}:
		case <-ctx.Done():
			return ctx.Err()
		}
		l.currentTxCommitTime = time.Time{}
		l.currentTxXID = 0
	case *pglogrepl.TruncateMessageV2:
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
			l.confirmedLSN = lsn
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

	snap := NewSnapshotter(tables, txFactory, l.cfg.SnapshotConcurrencyOrDefault())
	log.Printf("[logical] snapshot: %d tables, concurrency=%d", len(tables), l.cfg.SnapshotConcurrencyOrDefault())
	if _, err := snap.Run(ctx, events); err != nil {
		return err
	}

	return nil
}

func (l *LogicalSource) sendStandby(ctx context.Context) {
	err := pglogrepl.SendStandbyStatusUpdate(ctx, l.replConn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: l.confirmedLSN,
		WALFlushPosition: l.confirmedLSN,
		WALApplyPosition: l.confirmedLSN,
		ClientTime:       time.Now(),
		ReplyRequested:   false,
	})
	if err != nil {
		log.Printf("[logical] standby status error: %v", err)
	}
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
			FieldID: i + 1,
		})
	}
	l.tables[table] = ts
	return ts
}

func decodeTuple(rel *pglogrepl.RelationMessageV2, tuple *pglogrepl.TupleData) (map[string]any, []string) {
	if tuple == nil {
		return nil, nil
	}
	row := make(map[string]any, len(tuple.Columns))
	var unchangedCols []string
	for i, col := range tuple.Columns {
		if i >= len(rel.Columns) {
			break
		}
		colName := rel.Columns[i].Name
		switch col.DataType {
		case 'n': // null
			row[colName] = nil
		case 'u': // unchanged TOAST column
			row[colName] = nil
			unchangedCols = append(unchangedCols, colName)
		case 't': // text
			row[colName] = string(col.Data)
		}
	}
	return row, unchangedCols
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
