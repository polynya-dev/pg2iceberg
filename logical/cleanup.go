package logical

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5"
)

// Cleanup drops the replication slot, publication, and coordinator schema
// created by a pg2iceberg pipeline. The replication slot must be inactive
// before calling this function — it will fail immediately if the slot is
// still active.
func Cleanup(ctx context.Context, conn *pgx.Conn, slotName, publicationName, coordinatorSchema string) error {
	// Check that slot is not active.
	var active bool
	err := conn.QueryRow(ctx,
		"SELECT active FROM pg_replication_slots WHERE slot_name = $1", slotName,
	).Scan(&active)
	if err != nil {
		// Slot doesn't exist — skip to publication.
		log.Printf("[cleanup] slot %q not found, skipping", slotName)
		goto dropPublication
	}
	if active {
		return fmt.Errorf("replication slot %q is still active", slotName)
	}

	// Drop replication slot.
	if _, err := conn.Exec(ctx, "SELECT pg_drop_replication_slot($1)", slotName); err != nil {
		return fmt.Errorf("drop slot %q: %w", slotName, err)
	}
	log.Printf("[cleanup] dropped slot %q", slotName)

dropPublication:
	// Drop publication.
	if _, err := conn.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pgx.Identifier{publicationName}.Sanitize())); err != nil {
		return fmt.Errorf("drop publication %q: %w", publicationName, err)
	}
	log.Printf("[cleanup] dropped publication %q", publicationName)

	// Drop coordinator schema.
	if _, err := conn.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", pgx.Identifier{coordinatorSchema}.Sanitize())); err != nil {
		return fmt.Errorf("drop schema %q: %w", coordinatorSchema, err)
	}
	log.Printf("[cleanup] dropped schema %q", coordinatorSchema)

	log.Println("[cleanup] done")
	return nil
}
