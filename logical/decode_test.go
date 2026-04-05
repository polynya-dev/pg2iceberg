package logical

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
)

// buildBeginMsg constructs a raw WAL Begin message.
// Format: 'B' + FinalLSN(8) + CommitTime(8) + Xid(4) = 21 bytes
func buildBeginMsg(finalLSN uint64, commitTime time.Time, xid uint32) []byte {
	buf := make([]byte, 21)
	buf[0] = 'B'
	binary.BigEndian.PutUint64(buf[1:], finalLSN)
	binary.BigEndian.PutUint64(buf[9:], timeToPgMicros(commitTime))
	binary.BigEndian.PutUint32(buf[17:], xid)
	return buf
}

// buildCommitMsg constructs a raw WAL Commit message.
// Format: 'C' + Flags(1) + CommitLSN(8) + TransactionEndLSN(8) + CommitTime(8) = 26 bytes
func buildCommitMsg(commitLSN, txEndLSN uint64, commitTime time.Time) []byte {
	buf := make([]byte, 26)
	buf[0] = 'C'
	buf[1] = 0 // flags
	binary.BigEndian.PutUint64(buf[2:], commitLSN)
	binary.BigEndian.PutUint64(buf[10:], txEndLSN)
	binary.BigEndian.PutUint64(buf[18:], timeToPgMicros(commitTime))
	return buf
}

// buildRelationMsg constructs a raw WAL Relation message.
func buildRelationMsg(relID uint32, namespace, relName string, cols []testCol) []byte {
	var buf []byte
	buf = append(buf, 'R')
	buf = appendUint32(buf, relID)
	buf = appendNullString(buf, namespace)
	buf = appendNullString(buf, relName)
	buf = append(buf, 0) // replica identity
	buf = appendUint16(buf, uint16(len(cols)))
	for _, c := range cols {
		buf = append(buf, c.flags)
		buf = appendNullString(buf, c.name)
		buf = appendUint32(buf, c.dataType)
		buf = appendInt32(buf, c.typeMod)
	}
	return buf
}

type testCol struct {
	flags    uint8
	name     string
	dataType uint32
	typeMod  int32
}

// buildInsertMsg constructs a raw WAL Insert message.
// Format: 'I' + RelationID(4) + 'N' + TupleData
func buildInsertMsg(relID uint32, colValues []testTupleCol) []byte {
	var buf []byte
	buf = append(buf, 'I')
	buf = appendUint32(buf, relID)
	buf = append(buf, 'N')
	buf = appendTupleData(buf, colValues)
	return buf
}

// buildUpdateMsg constructs a raw WAL Update message (new tuple only).
// Format: 'U' + RelationID(4) + 'N' + TupleData
func buildUpdateMsg(relID uint32, colValues []testTupleCol) []byte {
	var buf []byte
	buf = append(buf, 'U')
	buf = appendUint32(buf, relID)
	buf = append(buf, 'N')
	buf = appendTupleData(buf, colValues)
	return buf
}

// buildUpdateMsgWithOld constructs an Update message with old + new tuples.
// Format: 'U' + RelationID(4) + 'O' + OldTupleData + 'N' + NewTupleData
func buildUpdateMsgWithOld(relID uint32, oldCols, newCols []testTupleCol) []byte {
	var buf []byte
	buf = append(buf, 'U')
	buf = appendUint32(buf, relID)
	buf = append(buf, 'O')
	buf = appendTupleData(buf, oldCols)
	buf = append(buf, 'N')
	buf = appendTupleData(buf, newCols)
	return buf
}

// buildDeleteMsg constructs a raw WAL Delete message.
// Format: 'D' + RelationID(4) + 'K'/'O' + TupleData
func buildDeleteMsg(relID uint32, tupleType byte, colValues []testTupleCol) []byte {
	var buf []byte
	buf = append(buf, 'D')
	buf = appendUint32(buf, relID)
	buf = append(buf, tupleType)
	buf = appendTupleData(buf, colValues)
	return buf
}

type testTupleCol struct {
	dataType byte   // 'n', 'u', 't'
	data     string // only for 't'
}

func appendTupleData(buf []byte, cols []testTupleCol) []byte {
	buf = appendUint16(buf, uint16(len(cols)))
	for _, c := range cols {
		buf = append(buf, c.dataType)
		switch c.dataType {
		case 't':
			buf = appendUint32(buf, uint32(len(c.data)))
			buf = append(buf, c.data...)
		case 'n', 'u':
			// no additional data
		}
	}
	return buf
}

// --- encoding helpers ---

func appendUint16(buf []byte, v uint16) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, v)
	return append(buf, b...)
}

func appendUint32(buf []byte, v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return append(buf, b...)
}

func appendInt32(buf []byte, v int32) []byte {
	return appendUint32(buf, uint32(v))
}

func appendNullString(buf []byte, s string) []byte {
	buf = append(buf, s...)
	buf = append(buf, 0)
	return buf
}

func timeToPgMicros(t time.Time) uint64 {
	microsecSinceUnixEpoch := t.Unix()*1_000_000 + int64(t.Nanosecond())/1000
	return uint64(microsecSinceUnixEpoch - pgEpochMicros)
}

// --- tests ---

func TestDecodeBegin(t *testing.T) {
	d := NewWALDecoder()
	ts := time.Date(2026, 3, 29, 10, 0, 0, 0, time.UTC)
	data := buildBeginMsg(100, ts, 42)

	msg, err := d.Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if msg.Type != msgBegin {
		t.Fatalf("Type = %c, want B", msg.Type)
	}
	if msg.Xid != 42 {
		t.Errorf("Xid = %d, want 42", msg.Xid)
	}
	// Allow 1ms tolerance for time conversion rounding.
	if diff := msg.CommitTime.Sub(ts); diff < -time.Millisecond || diff > time.Millisecond {
		t.Errorf("CommitTime = %v, want ~%v (diff=%v)", msg.CommitTime, ts, diff)
	}
}

func TestDecodeCommit(t *testing.T) {
	d := NewWALDecoder()
	ts := time.Date(2026, 3, 29, 10, 0, 0, 0, time.UTC)
	data := buildCommitMsg(200, 300, ts)

	msg, err := d.Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if msg.Type != msgCommit {
		t.Fatalf("Type = %c, want C", msg.Type)
	}
	if msg.CommitLSN != 200 {
		t.Errorf("CommitLSN = %d, want 200", msg.CommitLSN)
	}
}

func TestDecodeRelation(t *testing.T) {
	d := NewWALDecoder()
	data := buildRelationMsg(1001, "public", "users", []testCol{
		{flags: 1, name: "id", dataType: 23, typeMod: -1},  // int4
		{flags: 0, name: "name", dataType: 25, typeMod: -1}, // text
		{flags: 0, name: "age", dataType: 23, typeMod: -1},  // int4
	})

	msg, err := d.Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if msg.Type != msgRelation {
		t.Fatalf("Type = %c, want R", msg.Type)
	}
	rel := msg.Relation
	if rel.RelationID != 1001 {
		t.Errorf("RelationID = %d, want 1001", rel.RelationID)
	}
	if rel.Namespace != "public" {
		t.Errorf("Namespace = %q, want public", rel.Namespace)
	}
	if rel.RelationName != "users" {
		t.Errorf("RelationName = %q, want users", rel.RelationName)
	}
	if len(rel.Columns) != 3 {
		t.Fatalf("len(Columns) = %d, want 3", len(rel.Columns))
	}
	if rel.Columns[0].Name != "id" || rel.Columns[1].Name != "name" {
		t.Errorf("Columns[0].Name=%q, Columns[1].Name=%q", rel.Columns[0].Name, rel.Columns[1].Name)
	}
	// Verify it's cached.
	if d.GetRelation(1001) == nil {
		t.Error("relation not cached after decode")
	}
}

func TestDecodeInsert(t *testing.T) {
	d := NewWALDecoder()
	d.SetRelation(&RelationInfo{
		RelationID:   1001,
		Namespace:    "public",
		RelationName: "users",
		Columns: []RelationColumn{
			{Name: "id"},
			{Name: "name"},
			{Name: "age"},
		},
	})

	data := buildInsertMsg(1001, []testTupleCol{
		{dataType: 't', data: "42"},
		{dataType: 't', data: "Alice"},
		{dataType: 't', data: "30"},
	})

	msg, err := d.Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if msg.Type != msgInsert {
		t.Fatalf("Type = %c, want I", msg.Type)
	}
	if msg.After["id"] != "42" {
		t.Errorf("After[id] = %v, want 42", msg.After["id"])
	}
	if msg.After["name"] != "Alice" {
		t.Errorf("After[name] = %v, want Alice", msg.After["name"])
	}
	if msg.After["age"] != "30" {
		t.Errorf("After[age] = %v, want 30", msg.After["age"])
	}
}

func TestDecodeInsertWithNull(t *testing.T) {
	d := NewWALDecoder()
	d.SetRelation(&RelationInfo{
		RelationID: 1001,
		Columns: []RelationColumn{
			{Name: "id"},
			{Name: "name"},
			{Name: "bio"},
		},
	})

	data := buildInsertMsg(1001, []testTupleCol{
		{dataType: 't', data: "1"},
		{dataType: 't', data: "Bob"},
		{dataType: 'n'},
	})

	msg, err := d.Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if msg.After["bio"] != nil {
		t.Errorf("After[bio] = %v, want nil", msg.After["bio"])
	}
}

func TestDecodeUpdateWithUnchangedTOAST(t *testing.T) {
	d := NewWALDecoder()
	d.SetRelation(&RelationInfo{
		RelationID: 1001,
		Columns: []RelationColumn{
			{Name: "id"},
			{Name: "name"},
			{Name: "large_text"},
		},
	})

	data := buildUpdateMsg(1001, []testTupleCol{
		{dataType: 't', data: "1"},
		{dataType: 't', data: "Alice Updated"},
		{dataType: 'u'}, // unchanged TOAST
	})

	msg, err := d.Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if msg.Type != msgUpdate {
		t.Fatalf("Type = %c, want U", msg.Type)
	}
	if msg.After["name"] != "Alice Updated" {
		t.Errorf("After[name] = %v, want Alice Updated", msg.After["name"])
	}

	// Extract unchanged cols.
	unchanged := ExtractUnchangedCols(msg.After)
	if len(unchanged) != 1 || unchanged[0] != "large_text" {
		t.Errorf("unchanged = %v, want [large_text]", unchanged)
	}
	// Sentinel key should be removed.
	if _, ok := msg.After[unchangedColsKey]; ok {
		t.Error("sentinel key still present after ExtractUnchangedCols")
	}
}

func TestDecodeUpdateWithOldTuple(t *testing.T) {
	d := NewWALDecoder()
	d.SetRelation(&RelationInfo{
		RelationID: 1001,
		Columns: []RelationColumn{
			{Name: "id"},
			{Name: "name"},
		},
	})

	data := buildUpdateMsgWithOld(1001,
		[]testTupleCol{
			{dataType: 't', data: "1"},
			{dataType: 't', data: "OldName"},
		},
		[]testTupleCol{
			{dataType: 't', data: "1"},
			{dataType: 't', data: "NewName"},
		},
	)

	msg, err := d.Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if msg.Before["name"] != "OldName" {
		t.Errorf("Before[name] = %v, want OldName", msg.Before["name"])
	}
	if msg.After["name"] != "NewName" {
		t.Errorf("After[name] = %v, want NewName", msg.After["name"])
	}
}

func TestDecodeDelete(t *testing.T) {
	d := NewWALDecoder()
	d.SetRelation(&RelationInfo{
		RelationID: 1001,
		Columns: []RelationColumn{
			{Name: "id"},
			{Name: "name"},
		},
	})

	data := buildDeleteMsg(1001, 'K', []testTupleCol{
		{dataType: 't', data: "42"},
		{dataType: 'n'},
	})

	msg, err := d.Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if msg.Type != msgDelete {
		t.Fatalf("Type = %c, want D", msg.Type)
	}
	if msg.Before["id"] != "42" {
		t.Errorf("Before[id] = %v, want 42", msg.Before["id"])
	}
}

func TestDecodeUnknownRelation(t *testing.T) {
	d := NewWALDecoder()
	// No relation set for OID 9999
	data := buildInsertMsg(9999, []testTupleCol{
		{dataType: 't', data: "hello"},
	})

	msg, err := d.Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	// Should return message with nil After (unknown relation).
	if msg.After != nil {
		t.Errorf("After should be nil for unknown relation, got %v", msg.After)
	}
}

func TestDecodeFullTransaction(t *testing.T) {
	d := NewWALDecoder()
	ts := time.Date(2026, 3, 29, 12, 0, 0, 0, time.UTC)

	// 1. Relation
	relData := buildRelationMsg(1001, "public", "users", []testCol{
		{flags: 1, name: "id", dataType: 23, typeMod: -1},
		{flags: 0, name: "name", dataType: 25, typeMod: -1},
	})
	if _, err := d.Decode(relData); err != nil {
		t.Fatalf("Relation: %v", err)
	}

	// 2. Begin
	beginData := buildBeginMsg(100, ts, 10)
	beginMsg, err := d.Decode(beginData)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if beginMsg.Xid != 10 {
		t.Errorf("Begin Xid = %d, want 10", beginMsg.Xid)
	}

	// 3. Insert
	insertData := buildInsertMsg(1001, []testTupleCol{
		{dataType: 't', data: "1"},
		{dataType: 't', data: "Alice"},
	})
	insertMsg, err := d.Decode(insertData)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if insertMsg.After["name"] != "Alice" {
		t.Errorf("Insert After[name] = %v, want Alice", insertMsg.After["name"])
	}

	// 4. Update
	updateData := buildUpdateMsg(1001, []testTupleCol{
		{dataType: 't', data: "1"},
		{dataType: 't', data: "Alice Updated"},
	})
	updateMsg, err := d.Decode(updateData)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updateMsg.After["name"] != "Alice Updated" {
		t.Errorf("Update After[name] = %v, want Alice Updated", updateMsg.After["name"])
	}

	// 5. Delete
	deleteData := buildDeleteMsg(1001, 'K', []testTupleCol{
		{dataType: 't', data: "1"},
		{dataType: 'n'},
	})
	deleteMsg, err := d.Decode(deleteData)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if deleteMsg.Before["id"] != "1" {
		t.Errorf("Delete Before[id] = %v, want 1", deleteMsg.Before["id"])
	}

	// 6. Commit
	commitData := buildCommitMsg(200, 300, ts)
	commitMsg, err := d.Decode(commitData)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if commitMsg.Type != msgCommit {
		t.Errorf("Commit Type = %c, want C", commitMsg.Type)
	}
}

func TestToRelationMessageV2(t *testing.T) {
	rel := &RelationInfo{
		RelationID:      1001,
		Namespace:       "public",
		RelationName:    "users",
		ReplicaIdentity: 'd',
		Columns: []RelationColumn{
			{Flags: 1, Name: "id", DataType: 23, TypeModifier: -1},
			{Flags: 0, Name: "name", DataType: 25, TypeModifier: -1},
		},
	}

	pgRel := rel.ToRelationMessageV2()
	if pgRel.RelationID != 1001 {
		t.Errorf("RelationID = %d, want 1001", pgRel.RelationID)
	}
	if pgRel.Namespace != "public" {
		t.Errorf("Namespace = %q, want public", pgRel.Namespace)
	}
	if len(pgRel.Columns) != 2 {
		t.Fatalf("len(Columns) = %d, want 2", len(pgRel.Columns))
	}
	if pgRel.Columns[0].Name != "id" {
		t.Errorf("Columns[0].Name = %q, want id", pgRel.Columns[0].Name)
	}
}

func BenchmarkParseV2_Insert(b *testing.B) {
	// Benchmark the old pglogrepl.ParseV2 + decodeTuple path for comparison.
	rel := &pglogrepl.RelationMessageV2{
		RelationMessage: pglogrepl.RelationMessage{
			RelationID: 1001,
			Columns: []*pglogrepl.RelationMessageColumn{
				{Name: "id"},
				{Name: "name"},
				{Name: "email"},
				{Name: "age"},
				{Name: "created_at"},
			},
		},
	}

	data := buildInsertMsg(1001, []testTupleCol{
		{dataType: 't', data: "12345"},
		{dataType: 't', data: "Alice Johnson"},
		{dataType: 't', data: "alice@example.com"},
		{dataType: 't', data: "30"},
		{dataType: 't', data: "2026-03-29 10:00:00+00"},
	})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m, err := pglogrepl.ParseV2(data, false)
		if err != nil {
			b.Fatal(err)
		}
		ins := m.(*pglogrepl.InsertMessageV2)
		decodeTupleLegacy(rel, ins.Tuple)
	}
}

// decodeTupleLegacy is the old decodeTuple implementation, kept for benchmarking.
func decodeTupleLegacy(rel *pglogrepl.RelationMessageV2, tuple *pglogrepl.TupleData) (map[string]any, []string) {
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
		case 'n':
			row[colName] = nil
		case 'u':
			row[colName] = nil
			unchangedCols = append(unchangedCols, colName)
		case 't':
			row[colName] = string(col.Data)
		}
	}
	return row, unchangedCols
}

func BenchmarkDecode_Insert(b *testing.B) {
	d := NewWALDecoder()
	d.SetRelation(&RelationInfo{
		RelationID: 1001,
		Columns: []RelationColumn{
			{Name: "id"},
			{Name: "name"},
			{Name: "email"},
			{Name: "age"},
			{Name: "created_at"},
		},
	})

	data := buildInsertMsg(1001, []testTupleCol{
		{dataType: 't', data: "12345"},
		{dataType: 't', data: "Alice Johnson"},
		{dataType: 't', data: "alice@example.com"},
		{dataType: 't', data: "30"},
		{dataType: 't', data: "2026-03-29 10:00:00+00"},
	})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = d.Decode(data)
	}
}

func BenchmarkDecode_Insert_Pooled(b *testing.B) {
	d := NewWALDecoder()
	d.SetRelation(&RelationInfo{
		RelationID: 1001,
		Columns: []RelationColumn{
			{Name: "id"},
			{Name: "name"},
			{Name: "email"},
			{Name: "age"},
			{Name: "created_at"},
		},
	})

	data := buildInsertMsg(1001, []testTupleCol{
		{dataType: 't', data: "12345"},
		{dataType: 't', data: "Alice Johnson"},
		{dataType: 't', data: "alice@example.com"},
		{dataType: 't', data: "30"},
		{dataType: 't', data: "2026-03-29 10:00:00+00"},
	})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		msg, _ := d.Decode(data)
		ReleaseRow(msg.After) // simulate flush releasing the row back
	}
}

func BenchmarkDecode_Update_TOAST(b *testing.B) {
	d := NewWALDecoder()
	d.SetRelation(&RelationInfo{
		RelationID: 1001,
		Columns: []RelationColumn{
			{Name: "id"},
			{Name: "name"},
			{Name: "large_text"},
			{Name: "another_text"},
		},
	})

	data := buildUpdateMsg(1001, []testTupleCol{
		{dataType: 't', data: "1"},
		{dataType: 't', data: "Updated Name"},
		{dataType: 'u'}, // unchanged TOAST
		{dataType: 'u'}, // unchanged TOAST
	})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		msg, _ := d.Decode(data)
		ExtractUnchangedCols(msg.After)
	}
}
