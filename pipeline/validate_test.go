package pipeline

import (
	"strings"
	"testing"
)

func TestValidateStartup(t *testing.T) {
	table := func(name string, existed bool, snapID, seqNum int64) TableExistence {
		return TableExistence{
			PGTable:     "public." + name,
			IcebergName: name,
			Existed:     existed,
			SnapshotID:  snapID,
			SeqNum:      seqNum,
		}
	}

	tests := []struct {
		name    string
		v       StartupValidation
		wantErr string // substring match; empty = no error
	}{
		{
			name: "fresh start, no tables, no slot",
			v: StartupValidation{
				Checkpoint: &Checkpoint{},
				Tables:     []TableExistence{table("orders", false, 0, 0)},
				Slot:       &SlotState{Exists: false},
				ConfigMode: "logical",
				SlotName:   "test_slot",
			},
		},
		{
			name: "fresh start with orphaned tables",
			v: StartupValidation{
				Checkpoint: &Checkpoint{},
				Tables:     []TableExistence{table("orders", true, 1, 1)},
				Slot:       &SlotState{Exists: false},
				ConfigMode: "logical",
				SlotName:   "test_slot",
			},
			wantErr: "no checkpoint found but Iceberg table(s) already exist",
		},
		{
			name: "fresh start with orphaned slot",
			v: StartupValidation{
				Checkpoint: &Checkpoint{},
				Tables:     []TableExistence{table("orders", false, 0, 0)},
				Slot:       &SlotState{Exists: true, RestartLSN: 100},
				ConfigMode: "logical",
				SlotName:   "test_slot",
			},
			wantErr: "replication slot \"test_slot\" already exists",
		},
		{
			name: "resume, all consistent",
			v: StartupValidation{
				Checkpoint: &Checkpoint{Mode: "logical", LSN: 1000, SnapshotComplete: true},
				Tables:     []TableExistence{table("orders", true, 5, 3)},
				Slot:       &SlotState{Exists: true, RestartLSN: 900, ConfirmedFlushLSN: 1000},
				ConfigMode: "logical",
				SlotName:   "test_slot",
			},
		},
		{
			name: "checkpoint but table missing",
			v: StartupValidation{
				Checkpoint: &Checkpoint{Mode: "logical", LSN: 1000, SnapshotComplete: true},
				Tables:     []TableExistence{table("orders", false, 0, 0)},
				Slot:       &SlotState{Exists: true, RestartLSN: 900},
				ConfigMode: "logical",
				SlotName:   "test_slot",
			},
			wantErr: "Iceberg table(s) missing: orders",
		},
		{
			name: "checkpoint LSN but no slot",
			v: StartupValidation{
				Checkpoint: &Checkpoint{Mode: "logical", LSN: 1000, SnapshotComplete: true},
				Tables:     []TableExistence{table("orders", true, 5, 3)},
				Slot:       &SlotState{Exists: false},
				ConfigMode: "logical",
				SlotName:   "test_slot",
			},
			wantErr: "replication slot \"test_slot\" does not exist",
		},
		{
			name: "slot restart_lsn ahead of checkpoint",
			v: StartupValidation{
				Checkpoint: &Checkpoint{Mode: "logical", LSN: 1000, SnapshotComplete: true},
				Tables:     []TableExistence{table("orders", true, 5, 3)},
				Slot:       &SlotState{Exists: true, RestartLSN: 2000},
				ConfigMode: "logical",
				SlotName:   "test_slot",
			},
			wantErr: "WAL has been recycled",
		},
		{
			name: "mode mismatch",
			v: StartupValidation{
				Checkpoint: &Checkpoint{Mode: "query"},
				Tables:     []TableExistence{table("orders", true, 5, 3)},
				Slot:       nil,
				ConfigMode: "logical",
			},
			wantErr: "checkpoint was created by \"query\" mode but config specifies \"logical\"",
		},
		{
			name: "snapshot complete but table has no snapshots",
			v: StartupValidation{
				Checkpoint: &Checkpoint{Mode: "logical", LSN: 1000, SnapshotComplete: true},
				Tables:     []TableExistence{table("orders", true, 0, 0)},
				Slot:       &SlotState{Exists: true, RestartLSN: 900},
				ConfigMode: "logical",
				SlotName:   "test_slot",
			},
			wantErr: "table \"orders\" has no snapshots",
		},
		{
			name: "snapshot in progress (crash recovery) is valid",
			v: StartupValidation{
				Checkpoint: &Checkpoint{
					Mode:             "logical",
					LSN:              1000,
					SnapshotComplete: false,
					SnapshotedTables: map[string]bool{"public.orders": true},
				},
				Tables:     []TableExistence{table("orders", true, 1, 1)},
				Slot:       &SlotState{Exists: true, RestartLSN: 900},
				ConfigMode: "logical",
				SlotName:   "test_slot",
			},
		},
		{
			name: "snapshot complete but LSN is 0",
			v: StartupValidation{
				Checkpoint: &Checkpoint{Mode: "logical", LSN: 0, SnapshotComplete: true},
				Tables:     []TableExistence{table("orders", true, 1, 1)},
				Slot:       &SlotState{Exists: true, RestartLSN: 100},
				ConfigMode: "logical",
				SlotName:   "test_slot",
			},
			wantErr: "LSN is 0",
		},
		{
			name: "query mode skips slot checks",
			v: StartupValidation{
				Checkpoint: &Checkpoint{Mode: "query", Watermark: "2026-01-01T00:00:00Z"},
				Tables:     []TableExistence{table("orders", true, 5, 3)},
				Slot:       nil,
				ConfigMode: "query",
			},
		},
		{
			name: "multiple violations reported together",
			v: StartupValidation{
				Checkpoint: &Checkpoint{Mode: "logical", LSN: 1000, SnapshotComplete: true},
				Tables: []TableExistence{
					table("orders", false, 0, 0),
					table("orders_events", false, 0, 0),
				},
				Slot:       &SlotState{Exists: false},
				ConfigMode: "logical",
				SlotName:   "test_slot",
			},
			wantErr: "orders, orders_events",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateStartup(tt.v)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("expected no error, got: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got: %v", tt.wantErr, err)
			}
		})
	}
}
