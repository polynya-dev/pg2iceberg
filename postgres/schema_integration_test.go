//go:build integration

package postgres_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pg2iceberg/pg2iceberg/postgres"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

func startPostgres(t *testing.T, ctx context.Context) (*pgx.Conn, func()) {
	t.Helper()

	ctr, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("testdb"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		tcpostgres.BasicWaitStrategies(),
		testcontainers.WithCmd("postgres",
			"-c", "wal_level=logical",
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("get host: %v", err)
	}
	port, err := ctr.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("get port: %v", err)
	}

	dsn := "postgres://postgres:postgres@" + host + ":" + port.Port() + "/testdb?sslmode=disable"
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		ctr.Terminate(ctx)
		t.Fatalf("connect: %v", err)
	}

	return conn, func() {
		conn.Close(ctx)
		ctr.Terminate(ctx)
	}
}

// TestDiscoverSchema_AllTypes creates a wide table covering every supported PG
// type, discovers its schema, and asserts each column's Iceberg type mapping.
func TestDiscoverSchema_AllTypes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn, cleanup := startPostgres(t, ctx)
	defer cleanup()

	_, err := conn.Exec(ctx, `
		CREATE TABLE public.all_types (
			-- integer types
			col_smallint     SMALLINT,
			col_integer      INTEGER,
			col_bigint       BIGINT,
			col_serial       SERIAL,
			col_bigserial    BIGSERIAL,
			col_oid          OID,

			-- floating point
			col_real         REAL,
			col_double       DOUBLE PRECISION,

			-- decimal with precision
			col_numeric      NUMERIC,
			col_numeric_10_2 NUMERIC(10, 2),
			col_numeric_38_0 NUMERIC(38, 0),

			-- boolean
			col_bool         BOOLEAN,

			-- text types
			col_text         TEXT,
			col_varchar      VARCHAR(255),
			col_char         CHAR(10),
			col_name         NAME,

			-- binary
			col_bytea        BYTEA,

			-- date/time
			col_date         DATE,
			col_time         TIME,
			col_timetz       TIMETZ,
			col_timestamp    TIMESTAMP,
			col_timestamptz  TIMESTAMPTZ,

			-- uuid
			col_uuid         UUID,

			-- json
			col_json         JSON,
			col_jsonb        JSONB,

			-- types that fall back to string
			col_inet         INET,
			col_interval     INTERVAL,
			col_xml          XML,

			-- not-null column for coverage
			col_required     INTEGER NOT NULL DEFAULT 0
		)
	`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	ts, err := postgres.DiscoverSchema(ctx, conn, "public.all_types")
	if err != nil {
		t.Fatalf("discover schema: %v", err)
	}

	want := map[string]struct {
		icebergType string
		pgType      postgres.Type
		nullable    bool
		truncated   bool
	}{
		"col_smallint":     {"int", postgres.Int2, true, false},
		"col_integer":      {"int", postgres.Int4, true, false},
		"col_bigint":       {"long", postgres.Int8, true, false},
		"col_serial":       {"int", postgres.Int4, false, false},
		"col_bigserial":    {"long", postgres.Int8, false, false},
		"col_oid":          {"int", postgres.OID, true, false},
		"col_real":         {"float", postgres.Float4, true, false},
		"col_double":       {"double", postgres.Float8, true, false},
		"col_numeric":      {"decimal(38,18)", postgres.Numeric, true, true},
		"col_numeric_10_2": {"decimal(10,2)", postgres.Numeric, true, false},
		"col_numeric_38_0": {"decimal(38,0)", postgres.Numeric, true, false},
		"col_bool":         {"boolean", postgres.Bool, true, false},
		"col_text":         {"string", postgres.Text, true, false},
		"col_varchar":      {"string", postgres.Varchar, true, false},
		"col_char":         {"string", postgres.Bpchar, true, false},
		"col_name":         {"string", postgres.Name, true, false},
		"col_bytea":        {"binary", postgres.Bytea, true, false},
		"col_date":         {"date", postgres.Date, true, false},
		"col_time":         {"time", postgres.Time, true, false},
		"col_timetz":       {"time", postgres.TimeTZ, true, false},
		"col_timestamp":    {"timestamp", postgres.Timestamp, true, false},
		"col_timestamptz":  {"timestamptz", postgres.TimestampTZ, true, false},
		"col_uuid":         {"uuid", postgres.UUID, true, false},
		"col_json":         {"string", postgres.JSON, true, false},
		"col_jsonb":        {"string", postgres.JSONB, true, false},
		"col_inet":         {"string", postgres.Text, true, false}, // unknown → Text
		"col_interval":     {"string", postgres.Text, true, false},
		"col_xml":          {"string", postgres.Text, true, false},
		"col_required":     {"int", postgres.Int4, false, false},
	}

	if len(ts.Columns) != len(want) {
		t.Fatalf("expected %d columns, got %d", len(want), len(ts.Columns))
	}

	for _, col := range ts.Columns {
		w, ok := want[col.Name]
		if !ok {
			t.Errorf("unexpected column %q", col.Name)
			continue
		}

		iceType, truncated := col.IcebergType()
		if iceType != w.icebergType {
			t.Errorf("column %q: iceberg type = %q, want %q", col.Name, iceType, w.icebergType)
		}
		if truncated != w.truncated {
			t.Errorf("column %q: truncated = %v, want %v", col.Name, truncated, w.truncated)
		}
		if col.PGType != w.pgType {
			t.Errorf("column %q: PGType = %q, want %q", col.Name, col.PGType, w.pgType)
		}
		if col.IsNullable != w.nullable {
			t.Errorf("column %q: nullable = %v, want %v", col.Name, col.IsNullable, w.nullable)
		}
	}

	// FieldIDs should be 1-based and sequential.
	for i, col := range ts.Columns {
		if col.FieldID != i+1 {
			t.Errorf("column %q: FieldID = %d, want %d", col.Name, col.FieldID, i+1)
		}
	}
}

// TestDiscoverSchema_NoPK verifies that a table without a primary key is
// discovered successfully with an empty PK slice.
func TestDiscoverSchema_NoPK(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn, cleanup := startPostgres(t, ctx)
	defer cleanup()

	_, err := conn.Exec(ctx, `
		CREATE TABLE public.no_pk (
			id   INTEGER,
			name TEXT,
			val  NUMERIC(5, 3)
		)
	`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	ts, err := postgres.DiscoverSchema(ctx, conn, "public.no_pk")
	if err != nil {
		t.Fatalf("discover schema: %v", err)
	}

	if len(ts.PK) != 0 {
		t.Errorf("expected empty PK, got %v", ts.PK)
	}
	if len(ts.PKFieldIDs()) != 0 {
		t.Errorf("expected empty PKFieldIDs, got %v", ts.PKFieldIDs())
	}
	if len(ts.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(ts.Columns))
	}

	// Verify numeric precision is preserved.
	valCol := ts.Columns[2]
	if valCol.Name != "val" {
		t.Fatalf("expected column 'val', got %q", valCol.Name)
	}
	iceType, _ := valCol.IcebergType()
	if iceType != "decimal(5,3)" {
		t.Errorf("val iceberg type = %q, want %q", iceType, "decimal(5,3)")
	}
}

// TestDiscoverSchema_CompositePK verifies discovery of a table with a
// composite primary key and that PKFieldIDs returns the correct IDs.
func TestDiscoverSchema_CompositePK(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn, cleanup := startPostgres(t, ctx)
	defer cleanup()

	_, err := conn.Exec(ctx, `
		CREATE TABLE public.composite_pk (
			tenant_id  INTEGER NOT NULL,
			order_id   BIGINT NOT NULL,
			line_num   SMALLINT NOT NULL,
			product    TEXT,
			PRIMARY KEY (tenant_id, order_id, line_num)
		)
	`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	ts, err := postgres.DiscoverSchema(ctx, conn, "public.composite_pk")
	if err != nil {
		t.Fatalf("discover schema: %v", err)
	}

	wantPK := []string{"tenant_id", "order_id", "line_num"}
	if len(ts.PK) != len(wantPK) {
		t.Fatalf("PK = %v, want %v", ts.PK, wantPK)
	}
	for i, pk := range ts.PK {
		if pk != wantPK[i] {
			t.Errorf("PK[%d] = %q, want %q", i, pk, wantPK[i])
		}
	}

	// PKFieldIDs should match column positions (1-based).
	wantIDs := []int{1, 2, 3}
	ids := ts.PKFieldIDs()
	if len(ids) != len(wantIDs) {
		t.Fatalf("PKFieldIDs = %v, want %v", ids, wantIDs)
	}
	for i, id := range ids {
		if id != wantIDs[i] {
			t.Errorf("PKFieldIDs[%d] = %d, want %d", i, id, wantIDs[i])
		}
	}
}

// TestDiscoverSchema_DecimalTruncation verifies that Validate() rejects
// tables with numeric precision exceeding Iceberg's limit of 38.
func TestDiscoverSchema_DecimalTruncation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn, cleanup := startPostgres(t, ctx)
	defer cleanup()

	_, err := conn.Exec(ctx, `
		CREATE TABLE public.big_decimal (
			id  SERIAL PRIMARY KEY,
			val NUMERIC(50, 10)
		)
	`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	ts, err := postgres.DiscoverSchema(ctx, conn, "public.big_decimal")
	if err != nil {
		t.Fatalf("discover schema: %v", err)
	}

	// Validate should reject precision > 38.
	err = ts.Validate()
	if err == nil {
		t.Fatal("expected Validate() to return error for NUMERIC(50,10)")
	}
	if !strings.Contains(err.Error(), "exceeds Iceberg") {
		t.Errorf("unexpected error: %v", err)
	}
}
