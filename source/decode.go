package source

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
)

// rowPool recycles map[string]any instances used for decoded rows.
// Maps are cleared (keys deleted, buckets retained) before returning to the pool.
var rowPool = sync.Pool{
	New: func() any {
		return make(map[string]any, 8) // pre-size for typical column count
	},
}

// AcquireRow returns a map from the pool, ready for use as a decoded row.
func AcquireRow(size int) map[string]any {
	m := rowPool.Get().(map[string]any)
	// If the pooled map is too small for the column count, the runtime
	// will grow it automatically — no need to check.
	return m
}

// ReleaseRow returns a row map to the pool after clearing it.
// The caller must not reference the map after this call.
func ReleaseRow(m map[string]any) {
	if m == nil {
		return
	}
	clear(m)
	rowPool.Put(m)
}


// WAL message type bytes (same as pglogrepl constants).
const (
	msgBegin    = 'B'
	msgCommit   = 'C'
	msgRelation = 'R'
	msgInsert   = 'I'
	msgUpdate   = 'U'
	msgDelete   = 'D'
	msgTruncate = 'T'
	msgOrigin   = 'O'
	msgType     = 'Y'
	msgMessage  = 'M'

	// Tuple column types.
	colNull      = 'n'
	colUnchanged = 'u'
	colText      = 't'
	colBinary    = 'b'
)

// pgEpochMicros is microseconds between Unix epoch (1970-01-01) and PG epoch (2000-01-01).
const pgEpochMicros = 946684800 * 1_000_000

// WALDecoder decodes PostgreSQL logical replication WAL messages directly from
// wire bytes, bypassing pglogrepl.ParseV2 to eliminate per-message heap
// allocations. Row maps are acquired from a sync.Pool and must be released
// by the caller (via ReleaseRow) after the data has been consumed.
//
// The decoder is NOT goroutine-safe — it reuses an internal DecodedMessage
// to avoid per-call allocations. The returned *DecodedMessage is only valid
// until the next call to Decode.
//
// It is designed to be unit-testable: feed it raw WAL bytes and assert the
// decoded results.
type WALDecoder struct {
	// relations caches relation metadata keyed by OID.
	// Must be populated by processing RelationMessages before DML messages.
	relations map[uint32]*RelationInfo

	// msg is reused across Decode calls to avoid allocations.
	// Only valid until the next Decode call.
	msg DecodedMessage
}

// RelationInfo is a lightweight replacement for pglogrepl.RelationMessageV2
// that holds only what we need for tuple decoding.
type RelationInfo struct {
	RelationID      uint32
	Namespace       string
	RelationName    string
	ReplicaIdentity uint8
	Columns         []RelationColumn
}

// RelationColumn holds per-column metadata from a RelationMessage.
type RelationColumn struct {
	Flags        uint8
	Name         string
	DataType     uint32
	TypeModifier int32
}

// DecodedMessage is the result of decoding a single WAL message.
type DecodedMessage struct {
	// Type is the WAL message type byte.
	Type byte

	// For Begin/Commit:
	Xid        uint32
	CommitTime time.Time
	CommitLSN  uint64

	// For Insert/Update/Delete:
	RelationID uint32
	After      map[string]any   // new tuple (Insert, Update)
	Before     map[string]any   // old tuple (Update with old, Delete)
	Unchanged  []string         // unchanged TOAST column names

	// For Relation:
	Relation *RelationInfo

	// For Truncate:
	RelationIDs []uint32
}

// NewWALDecoder creates a new zero-allocation WAL decoder.
func NewWALDecoder() *WALDecoder {
	return &WALDecoder{
		relations: make(map[uint32]*RelationInfo),
	}
}

// Decode decodes a single WAL message from raw wire bytes.
// The data slice must start with the message type byte.
//
// The returned *DecodedMessage is reused across calls — it is only valid until
// the next call to Decode. The caller must extract all needed data before
// calling Decode again. Row maps (After/Before) are from a pool and must be
// released via ReleaseRow after consumption.
func (d *WALDecoder) Decode(data []byte) (*DecodedMessage, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty WAL message")
	}

	// Reset reusable message. Nil out pointer/slice fields to avoid stale references.
	d.msg = DecodedMessage{}

	msgType := data[0]
	src := data[1:]

	switch msgType {
	case msgBegin:
		return d.decodeBegin(src)
	case msgCommit:
		return d.decodeCommit(src)
	case msgRelation:
		return d.decodeRelation(src)
	case msgInsert:
		return d.decodeInsert(src)
	case msgUpdate:
		return d.decodeUpdate(src)
	case msgDelete:
		return d.decodeDelete(src)
	case msgTruncate:
		return d.decodeTruncate(src)
	case msgOrigin, msgType, msgMessage:
		d.msg.Type = msgType
		return &d.msg, nil
	default:
		return nil, fmt.Errorf("unsupported WAL message type: %c", msgType)
	}
}

// GetRelation returns cached relation info by OID, or nil if not found.
func (d *WALDecoder) GetRelation(oid uint32) *RelationInfo {
	return d.relations[oid]
}

// SetRelation stores a relation (useful for tests).
func (d *WALDecoder) SetRelation(rel *RelationInfo) {
	d.relations[rel.RelationID] = rel
}

func (d *WALDecoder) decodeBegin(src []byte) (*DecodedMessage, error) {
	if len(src) < 20 {
		return nil, fmt.Errorf("BeginMessage too short: %d bytes", len(src))
	}
	d.msg.Type = msgBegin
	d.msg.CommitTime = pgTimeToGoTime(int64(binary.BigEndian.Uint64(src[8:])))
	d.msg.Xid = binary.BigEndian.Uint32(src[16:])
	return &d.msg, nil
}

func (d *WALDecoder) decodeCommit(src []byte) (*DecodedMessage, error) {
	if len(src) < 25 {
		return nil, fmt.Errorf("CommitMessage too short: %d bytes", len(src))
	}
	d.msg.Type = msgCommit
	d.msg.CommitLSN = binary.BigEndian.Uint64(src[1:])
	d.msg.CommitTime = pgTimeToGoTime(int64(binary.BigEndian.Uint64(src[17:])))
	return &d.msg, nil
}

func (d *WALDecoder) decodeRelation(src []byte) (*DecodedMessage, error) {
	if len(src) < 7 {
		return nil, fmt.Errorf("RelationMessage too short: %d bytes", len(src))
	}

	low := 0
	relID := binary.BigEndian.Uint32(src[low:])
	low += 4

	namespace, n := decodeNullString(src[low:])
	if n < 0 {
		return nil, fmt.Errorf("RelationMessage: bad namespace string")
	}
	low += n

	relName, n := decodeNullString(src[low:])
	if n < 0 {
		return nil, fmt.Errorf("RelationMessage: bad relation name string")
	}
	low += n

	replicaIdentity := src[low]
	low++

	colNum := int(binary.BigEndian.Uint16(src[low:]))
	low += 2

	cols := make([]RelationColumn, colNum)
	for i := 0; i < colNum; i++ {
		cols[i].Flags = src[low]
		low++

		name, n := decodeNullString(src[low:])
		if n < 0 {
			return nil, fmt.Errorf("RelationMessage: bad column name string at %d", i)
		}
		low += n

		cols[i].Name = name
		cols[i].DataType = binary.BigEndian.Uint32(src[low:])
		low += 4
		cols[i].TypeModifier = int32(binary.BigEndian.Uint32(src[low:]))
		low += 4
	}

	rel := &RelationInfo{
		RelationID:      relID,
		Namespace:       namespace,
		RelationName:    relName,
		ReplicaIdentity: replicaIdentity,
		Columns:         cols,
	}
	d.relations[relID] = rel

	d.msg.Type = msgRelation
	d.msg.Relation = rel
	return &d.msg, nil
}

func (d *WALDecoder) decodeInsert(src []byte) (*DecodedMessage, error) {
	if len(src) < 7 {
		return nil, fmt.Errorf("InsertMessage too short: %d bytes", len(src))
	}

	relID := binary.BigEndian.Uint32(src)
	d.msg.Type = msgInsert
	d.msg.RelationID = relID

	rel := d.relations[relID]
	if rel == nil {
		return &d.msg, nil
	}

	row, _, err := decodeTupleData(src[5:], rel.Columns)
	if err != nil {
		return nil, fmt.Errorf("InsertMessage tuple: %w", err)
	}
	d.msg.After = row
	return &d.msg, nil
}

func (d *WALDecoder) decodeUpdate(src []byte) (*DecodedMessage, error) {
	if len(src) < 6 {
		return nil, fmt.Errorf("UpdateMessage too short: %d bytes", len(src))
	}

	relID := binary.BigEndian.Uint32(src)
	d.msg.Type = msgUpdate
	d.msg.RelationID = relID

	rel := d.relations[relID]
	if rel == nil {
		return &d.msg, nil
	}

	low := 4
	tupleType := src[low]
	low++

	switch tupleType {
	case 'K', 'O':
		before, used, err := decodeTupleData(src[low:], rel.Columns)
		if err != nil {
			return nil, fmt.Errorf("UpdateMessage old tuple: %w", err)
		}
		d.msg.Before = before
		low += used
		low++ // skip 'N' marker for new tuple
		fallthrough
	case 'N':
		after, _, err := decodeTupleData(src[low:], rel.Columns)
		if err != nil {
			return nil, fmt.Errorf("UpdateMessage new tuple: %w", err)
		}
		d.msg.After = after
	default:
		return nil, fmt.Errorf("UpdateMessage: unexpected tuple type %c", tupleType)
	}

	return &d.msg, nil
}

func (d *WALDecoder) decodeDelete(src []byte) (*DecodedMessage, error) {
	if len(src) < 6 {
		return nil, fmt.Errorf("DeleteMessage too short: %d bytes", len(src))
	}

	relID := binary.BigEndian.Uint32(src)
	d.msg.Type = msgDelete
	d.msg.RelationID = relID

	rel := d.relations[relID]
	if rel == nil {
		return &d.msg, nil
	}

	before, _, err := decodeTupleData(src[5:], rel.Columns)
	if err != nil {
		return nil, fmt.Errorf("DeleteMessage tuple: %w", err)
	}
	d.msg.Before = before
	return &d.msg, nil
}

func (d *WALDecoder) decodeTruncate(src []byte) (*DecodedMessage, error) {
	if len(src) < 9 {
		return nil, fmt.Errorf("TruncateMessage too short: %d bytes", len(src))
	}

	relNum := binary.BigEndian.Uint32(src)
	// src[4] = option byte
	low := 5
	relIDs := make([]uint32, relNum)
	for i := 0; i < int(relNum); i++ {
		relIDs[i] = binary.BigEndian.Uint32(src[low:])
		low += 4
	}

	d.msg.Type = msgTruncate
	d.msg.RelationIDs = relIDs
	return &d.msg, nil
}

// decodeTupleData decodes tuple column data directly into a map[string]any.
// Returns the row map, bytes consumed, and any error.
// Unchanged TOAST columns are set to nil in the map, and their names are NOT
// tracked here — the caller uses the separate unchangedCols mechanism.
func decodeTupleData(src []byte, cols []RelationColumn) (map[string]any, int, error) {
	if len(src) < 2 {
		return nil, 0, fmt.Errorf("tuple data too short")
	}

	colNum := int(binary.BigEndian.Uint16(src))
	low := 2

	row := AcquireRow(colNum)
	var unchangedCols []string

	for i := 0; i < colNum; i++ {
		if low >= len(src) {
			return nil, 0, fmt.Errorf("tuple data truncated at column %d", i)
		}

		colType := src[low]
		low++

		var colName string
		if i < len(cols) {
			colName = cols[i].Name
		} else {
			colName = fmt.Sprintf("_col%d", i)
		}

		switch colType {
		case colNull:
			row[colName] = nil
		case colUnchanged:
			row[colName] = nil
			unchangedCols = append(unchangedCols, colName)
		case colText, colBinary:
			if low+4 > len(src) {
				return nil, 0, fmt.Errorf("tuple data truncated at column %d length", i)
			}
			length := int(binary.BigEndian.Uint32(src[low:]))
			low += 4
			if low+length > len(src) {
				return nil, 0, fmt.Errorf("tuple data truncated at column %d data", i)
			}
			// Must copy: the underlying buffer (from pgconn.Frontend.Receive)
			// is reused on the next ReceiveMessage call.
			row[colName] = string(src[low : low+length])
			low += length
		}
	}

	// Stash unchanged cols in a sentinel key that processWAL will extract.
	// This avoids returning a third value and keeps the map as the single output.
	if len(unchangedCols) > 0 {
		row[unchangedColsKey] = unchangedCols
	}

	return row, low, nil
}

// unchangedColsKey is a sentinel map key used to pass unchanged TOAST column
// names through the row map. It uses a null byte prefix that cannot collide
// with any real PostgreSQL column name.
const unchangedColsKey = "\x00__unchanged_cols__"

// ExtractUnchangedCols removes and returns the unchanged TOAST column names
// from a decoded row, if any.
func ExtractUnchangedCols(row map[string]any) []string {
	v, ok := row[unchangedColsKey]
	if !ok {
		return nil
	}
	cols := v.([]string)
	delete(row, unchangedColsKey)
	return cols
}

// decodeNullString reads a null-terminated string from src.
// Returns the string and total bytes consumed (including the null byte).
// Returns ("", -1) if no null byte is found.
func decodeNullString(src []byte) (string, int) {
	end := bytes.IndexByte(src, 0)
	if end == -1 {
		return "", -1
	}
	return string(src[:end]), end + 1
}

// pgTimeToGoTime converts PG epoch microseconds to time.Time.
func pgTimeToGoTime(microsecSinceY2K int64) time.Time {
	microsecSinceUnixEpoch := pgEpochMicros + microsecSinceY2K
	return time.Unix(0, microsecSinceUnixEpoch*1000)
}

// ToRelationMessageV2 converts a RelationInfo to a pglogrepl.RelationMessageV2.
// Used for compatibility with existing code that expects pglogrepl types
// (e.g. diffRelation, schemaForRelation).
func (r *RelationInfo) ToRelationMessageV2() *pglogrepl.RelationMessageV2 {
	cols := make([]*pglogrepl.RelationMessageColumn, len(r.Columns))
	for i, c := range r.Columns {
		cols[i] = &pglogrepl.RelationMessageColumn{
			Flags:        c.Flags,
			Name:         c.Name,
			DataType:     c.DataType,
			TypeModifier: c.TypeModifier,
		}
	}
	return &pglogrepl.RelationMessageV2{
		RelationMessage: pglogrepl.RelationMessage{
			RelationID:      r.RelationID,
			Namespace:       r.Namespace,
			RelationName:    r.RelationName,
			ReplicaIdentity: r.ReplicaIdentity,
			ColumnNum:       uint16(len(r.Columns)),
			Columns:         cols,
		},
	}
}
