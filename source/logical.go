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
	pgCfg  config.PostgresConfig
	cfg    config.LogicalConfig
	tables map[string]*schema.TableSchema

	replConn *pgconn.PgConn
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
}

func NewLogicalSource(pgCfg config.PostgresConfig, logicalCfg config.LogicalConfig) *LogicalSource {
	return &LogicalSource{
		pgCfg:     pgCfg,
		cfg:       logicalCfg,
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
	for _, table := range l.cfg.Tables {
		ts, err := schema.DiscoverSchema(ctx, l.queryConn, table)
		if err != nil {
			return fmt.Errorf("discover schema for %s: %w", table, err)
		}
		l.tables[table] = ts
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

	// If this is a fresh slot and the snapshot hasn't been completed yet,
	// copy all existing rows before starting WAL streaming.
	if !l.snapshotComplete && snapshotName != "" {
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
			Table:     table,
			Operation: OpInsert,
			After:     after,
			PK:        ts.PK,
			Timestamp: time.Now(),
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
			Table:         table,
			Operation:     OpUpdate,
			Before:        before,
			PK:            ts.PK,
			After:         after,
			Timestamp:     time.Now(),
			UnchangedCols: unchangedCols,
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
			Table:     table,
			Operation: OpDelete,
			Before:    before,
			PK:        ts.PK,
			Timestamp: time.Now(),
		}:
		case <-ctx.Done():
			return ctx.Err()
		}

	case *pglogrepl.BeginMessage:
		// no-op: we could track transaction boundaries in the future
	case *pglogrepl.CommitMessage:
		// no-op
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

// snapshotTables performs an initial full-table copy for all tracked tables
// using the exported snapshot from slot creation. This guarantees we see exactly
// the rows that existed at the slot's ConsistentPoint.
func (l *LogicalSource) snapshotTables(ctx context.Context, snapshotName string, events chan<- ChangeEvent) error {
	tx, err := l.queryConn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin snapshot tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// Importing a snapshot requires REPEATABLE READ or SERIALIZABLE.
	if _, err := tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		return fmt.Errorf("set isolation level: %w", err)
	}

	// Pin to the exported snapshot so we see exactly the same data as the
	// slot's consistent point.
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotName)); err != nil {
		return fmt.Errorf("set transaction snapshot: %w", err)
	}

	for _, table := range l.cfg.Tables {
		ts := l.tables[table]

		rows, err := tx.Query(ctx, fmt.Sprintf("SELECT * FROM %s", table))
		if err != nil {
			return fmt.Errorf("snapshot query %s: %w", table, err)
		}

		descs := rows.FieldDescriptions()
		count := 0

		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				rows.Close()
				return fmt.Errorf("snapshot scan %s: %w", table, err)
			}

			row := make(map[string]any, len(descs))
			for i, desc := range descs {
				row[string(desc.Name)] = pgValueToString(values[i])
			}

			select {
			case events <- ChangeEvent{
				Table:     table,
				Operation: OpInsert,
				After:     row,
				PK:        ts.PK,
				Timestamp: time.Now(),
			}:
			case <-ctx.Done():
				rows.Close()
				return ctx.Err()
			}
			count++
		}
		rows.Close()

		if err := rows.Err(); err != nil {
			return fmt.Errorf("snapshot rows %s: %w", table, err)
		}

		log.Printf("[logical] snapshot: emitted %d rows for %s", count, table)
	}

	return tx.Commit(ctx)
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
		l.cfg.Tables = append(l.cfg.Tables, table)
	}
}

// RemoveTable removes a table from the tracked set.
func (l *LogicalSource) RemoveTable(table string) {
	for i, t := range l.cfg.Tables {
		if t == table {
			l.cfg.Tables = append(l.cfg.Tables[:i], l.cfg.Tables[i+1:]...)
			delete(l.tables, table)
			return
		}
	}
}

func (l *LogicalSource) isTracked(table string) bool {
	for _, t := range l.cfg.Tables {
		if t == table {
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
