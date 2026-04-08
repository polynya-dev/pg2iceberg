package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestCheckpoint_SealAndVerify(t *testing.T) {
	cp := &Checkpoint{
		Mode:             "logical",
		LSN:              12345,
		SnapshotComplete: true,
	}
	cp.Seal()

	if cp.Version != CheckpointVersion {
		t.Fatalf("expected version %d, got %d", CheckpointVersion, cp.Version)
	}
	if cp.Checksum == "" {
		t.Fatal("expected non-empty checksum after Seal")
	}
	if err := cp.Verify(); err != nil {
		t.Fatalf("verify failed: %v", err)
	}
}

func TestCheckpoint_VerifyDetectsTamper(t *testing.T) {
	cp := &Checkpoint{
		Mode:             "logical",
		LSN:              12345,
		SnapshotComplete: true,
	}
	cp.Seal()

	// Tamper with a field.
	cp.LSN = 99999

	err := cp.Verify()
	if err == nil {
		t.Fatal("expected checksum mismatch error")
	}
	t.Logf("got expected error: %v", err)
}

func TestCheckpoint_VerifyFreshCheckpoint(t *testing.T) {
	cp := &Checkpoint{}
	if err := cp.Verify(); err != nil {
		t.Fatalf("fresh checkpoint should pass verify, got: %v", err)
	}
}

func TestCheckpoint_VerifyOldCheckpointNoChecksum(t *testing.T) {
	// Simulate a checkpoint from before versioning.
	cp := &Checkpoint{
		Mode: "logical",
		LSN:  100,
	}
	// No Seal() called — Version=0, Checksum="".
	if err := cp.Verify(); err != nil {
		t.Fatalf("old checkpoint without checksum should pass verify, got: %v", err)
	}
}

func TestCheckpoint_VerifyFutureVersion(t *testing.T) {
	cp := &Checkpoint{
		Version: CheckpointVersion + 1,
		Mode:    "logical",
	}
	err := cp.Verify()
	if err == nil {
		t.Fatal("expected error for future version")
	}
	t.Logf("got expected error: %v", err)
}

func TestCheckpoint_ChecksumDeterministic(t *testing.T) {
	// computeChecksum should be deterministic for the same fields (including map ordering).
	cp := &Checkpoint{
		Version:          1,
		Revision:         5,
		Mode:             "logical",
		LSN:              12345,
		SnapshotComplete: true,
		MaterializerSnapshots: map[string]int64{
			"public.b": 2,
			"public.a": 1,
		},
	}
	first := cp.computeChecksum()
	second := cp.computeChecksum()

	if first != second {
		t.Fatalf("checksum not deterministic: %s != %s", first, second)
	}
}

func TestFileCheckpointStore_SealAndVerify(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.json")
	store := NewFileCheckpointStore(path)

	cp := &Checkpoint{
		Mode:             "logical",
		LSN:              5000,
		SnapshotComplete: true,
	}
	if err := store.Save(context.Background(), "test", cp); err != nil {
		t.Fatalf("save: %v", err)
	}

	loaded, err := store.Load(context.Background(), "test")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded.Version != CheckpointVersion {
		t.Fatalf("expected version %d, got %d", CheckpointVersion, loaded.Version)
	}
	if loaded.LSN != 5000 {
		t.Fatalf("expected LSN 5000, got %d", loaded.LSN)
	}
}

func TestFileCheckpointStore_DetectsTamper(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.json")
	store := NewFileCheckpointStore(path)

	cp := &Checkpoint{
		Mode:             "logical",
		LSN:              5000,
		SnapshotComplete: true,
	}
	if err := store.Save(context.Background(), "test", cp); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Tamper with the file.
	data, _ := os.ReadFile(path)
	tampered := []byte(string(data[:len(data)-10]) + "999999999}")
	os.WriteFile(path, tampered, 0644)

	_, err := store.Load(context.Background(), "test")
	if err == nil {
		t.Fatal("expected error after tampering")
	}
	t.Logf("got expected error: %v", err)
}

func TestFileCheckpointStore_ConcurrentUpdateDetected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.json")

	store1 := NewFileCheckpointStore(path)
	store2 := NewFileCheckpointStore(path)

	// Instance 1 saves.
	cp1 := &Checkpoint{Mode: "logical", LSN: 1000}
	if err := store1.Save(context.Background(), "test", cp1); err != nil {
		t.Fatalf("save 1: %v", err)
	}

	// Both instances load the same revision.
	loaded1, err := store1.Load(context.Background(), "test")
	if err != nil {
		t.Fatalf("load 1: %v", err)
	}
	loaded2, err := store2.Load(context.Background(), "test")
	if err != nil {
		t.Fatalf("load 2: %v", err)
	}

	// Instance 1 saves (advances revision).
	loaded1.LSN = 2000
	if err := store1.Save(context.Background(), "test", loaded1); err != nil {
		t.Fatalf("save from instance 1: %v", err)
	}

	// Instance 2 tries to save with stale revision — should fail.
	loaded2.LSN = 3000
	err = store2.Save(context.Background(), "test", loaded2)
	if err != ErrConcurrentUpdate {
		t.Fatalf("expected ErrConcurrentUpdate, got: %v", err)
	}
}

func TestCheckpoint_RevisionIncrements(t *testing.T) {
	store := NewMemCheckpointStore()

	cp := &Checkpoint{Mode: "logical", LSN: 100}
	if err := store.Save(context.Background(), "test", cp); err != nil {
		t.Fatalf("save 1: %v", err)
	}
	if cp.Revision != 1 {
		t.Fatalf("expected revision 1, got %d", cp.Revision)
	}

	loaded, _ := store.Load(context.Background(), "test")
	loaded.LSN = 200
	if err := store.Save(context.Background(), "test", loaded); err != nil {
		t.Fatalf("save 2: %v", err)
	}
	if loaded.Revision != 2 {
		t.Fatalf("expected revision 2, got %d", loaded.Revision)
	}
}

func TestMemCheckpointStore_SealAndVerify(t *testing.T) {
	store := NewMemCheckpointStore()

	cp := &Checkpoint{
		Mode: "logical",
		LSN:  3000,
	}
	if err := store.Save(context.Background(), "test", cp); err != nil {
		t.Fatalf("save: %v", err)
	}

	loaded, err := store.Load(context.Background(), "test")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded.Version != CheckpointVersion {
		t.Fatalf("expected version %d, got %d", CheckpointVersion, loaded.Version)
	}
	if loaded.Checksum == "" {
		t.Fatal("expected non-empty checksum")
	}
}
