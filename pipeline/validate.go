package pipeline

import (
	"fmt"
	"strings"
)

// TableExistence records whether an Iceberg table existed before registration.
type TableExistence struct {
	PGTable     string
	IcebergName string
	Existed     bool
	SnapshotID  int64 // CurrentSnapshotID, 0 if !Existed
	SeqNum      int64 // LastSequenceNumber, 0 if !Existed
}

// SlotState holds replication slot info from PostgreSQL.
type SlotState struct {
	Exists            bool
	RestartLSN        uint64
	ConfirmedFlushLSN uint64
}

// StartupValidation captures the state of the world at pipeline startup.
type StartupValidation struct {
	Checkpoint *Checkpoint
	Tables     []TableExistence
	Slot       *SlotState // nil for query mode
	ConfigMode string     // "logical" or "query"
	SlotName   string
}

// ValidateStartup checks that checkpoint state is consistent with
// Iceberg catalog state and PostgreSQL replication slot state.
// Returns nil if valid, or an actionable error describing all violations.
func ValidateStartup(v StartupValidation) error {
	cp := v.Checkpoint
	fresh := cp.Mode == ""
	var errs []string

	// 1. Mode mismatch.
	if !fresh && cp.Mode != v.ConfigMode {
		errs = append(errs, fmt.Sprintf(
			"checkpoint was created by %q mode but config specifies %q mode; "+
				"change source.mode back to %q, or delete the checkpoint to start fresh",
			cp.Mode, v.ConfigMode, cp.Mode))
	}

	// 2. No checkpoint but Iceberg tables exist (orphaned tables).
	if fresh {
		var orphaned []string
		for _, t := range v.Tables {
			if t.Existed {
				orphaned = append(orphaned, t.IcebergName)
			}
		}
		if len(orphaned) > 0 {
			errs = append(errs, fmt.Sprintf(
				"no checkpoint found but Iceberg table(s) already exist: %s; "+
					"delete the tables to start fresh, or restore the checkpoint",
				strings.Join(orphaned, ", ")))
		}
	}

	// 3. No checkpoint but replication slot exists (orphaned slot).
	if fresh && v.Slot != nil && v.Slot.Exists {
		errs = append(errs, fmt.Sprintf(
			"no checkpoint found but replication slot %q already exists; "+
				"drop the slot with: SELECT pg_drop_replication_slot('%s'); "+
				"or restore the checkpoint",
			v.SlotName, v.SlotName))
	}

	// 4. Checkpoint exists but tables missing.
	if !fresh {
		var missing []string
		for _, t := range v.Tables {
			if !t.Existed {
				missing = append(missing, t.IcebergName)
			}
		}
		if len(missing) > 0 {
			errs = append(errs, fmt.Sprintf(
				"checkpoint exists but Iceberg table(s) missing: %s; "+
					"delete the checkpoint to re-snapshot, or recreate the tables",
				strings.Join(missing, ", ")))
		}
	}

	// 5. Checkpoint has LSN but slot is gone.
	if !fresh && cp.LSN > 0 && v.Slot != nil && !v.Slot.Exists {
		errs = append(errs, fmt.Sprintf(
			"checkpoint has LSN %d but replication slot %q does not exist; "+
				"WAL data since that position is lost; "+
				"delete the checkpoint and Iceberg tables to re-snapshot",
			cp.LSN, v.SlotName))
	}

	// 6. Slot restart_lsn ahead of checkpoint LSN (WAL recycled).
	if !fresh && cp.LSN > 0 && v.Slot != nil && v.Slot.Exists && v.Slot.RestartLSN > cp.LSN {
		errs = append(errs, fmt.Sprintf(
			"replication slot %q restart_lsn (%d) is ahead of checkpoint LSN (%d); "+
				"WAL has been recycled and data is lost; "+
				"delete the checkpoint and Iceberg tables to re-snapshot",
			v.SlotName, v.Slot.RestartLSN, cp.LSN))
	}

	// 7. Snapshot complete but LSN is 0 (crashed after snapshot, before first CDC flush).
	if !fresh && cp.SnapshotComplete && cp.LSN == 0 && v.ConfigMode == "logical" {
		errs = append(errs, "checkpoint says snapshot is complete but LSN is 0; "+
			"the pipeline crashed after the snapshot but before the first CDC flush; "+
			"delete the checkpoint and Iceberg tables to re-snapshot")
	}

	// 8. Checkpoint says snapshot complete but table has no snapshots.
	if !fresh && cp.SnapshotComplete {
		for _, t := range v.Tables {
			if t.Existed && t.SnapshotID == 0 {
				errs = append(errs, fmt.Sprintf(
					"checkpoint says snapshot is complete but table %q has no snapshots; "+
						"table may have been recreated externally; "+
						"delete the checkpoint to re-snapshot",
					t.IcebergName))
			}
		}
	}

	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("startup validation failed:\n  - %s", strings.Join(errs, "\n  - "))
}
