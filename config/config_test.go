package config

import (
	"strings"
	"testing"
)

func validConfig() Config {
	cfg := Config{
		Tables: []TableConfig{{Name: "public.orders"}},
		Source: SourceConfig{
			Mode: "logical",
			Postgres: PostgresConfig{
				Host:     "localhost",
				Port:     5432,
				Database: "testdb",
				User:     "postgres",
				Password: "postgres",
			},
			Logical: LogicalConfig{
				SlotName:        "test_slot",
				PublicationName: "test_pub",
			},
		},
		Sink: SinkConfig{
			CatalogURI:    "http://localhost:8181",
			Warehouse:     "s3://warehouse/",
			Namespace:     "default",
			S3Endpoint:    "http://localhost:9000",
			FlushInterval: "10s",
			FlushRows:     1000,
		},
	}
	cfg.ApplyDefaults()
	return cfg
}

func TestValidate_ValidConfig(t *testing.T) {
	cfg := validConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid config, got: %v", err)
	}
}

func TestValidate_NoTables(t *testing.T) {
	cfg := validConfig()
	cfg.Tables = nil
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for no tables")
	}
	if !strings.Contains(err.Error(), "at least one table") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidate_DuplicateTable(t *testing.T) {
	cfg := validConfig()
	cfg.Tables = append(cfg.Tables, TableConfig{Name: "public.orders"})
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for duplicate table")
	}
	if !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidate_InvalidMode(t *testing.T) {
	cfg := validConfig()
	cfg.Source.Mode = "invalid"
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for invalid mode")
	}
	if !strings.Contains(err.Error(), "source.mode") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidate_MissingPostgresHost(t *testing.T) {
	cfg := validConfig()
	cfg.Source.Postgres.Host = ""
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for missing host")
	}
	if !strings.Contains(err.Error(), "source.postgres.host") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidate_LogicalMissingSlot(t *testing.T) {
	cfg := validConfig()
	cfg.Source.Logical.SlotName = ""
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for missing slot_name")
	}
	if !strings.Contains(err.Error(), "slot_name") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidate_QueryModeMissingPK(t *testing.T) {
	cfg := validConfig()
	cfg.Source.Mode = "query"
	cfg.Source.Logical = LogicalConfig{} // not needed for query
	cfg.Tables = []TableConfig{{Name: "public.orders", WatermarkColumn: "updated_at"}}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for missing primary_key in query mode")
	}
	if !strings.Contains(err.Error(), "primary_key") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidate_MissingSinkFields(t *testing.T) {
	cfg := validConfig()
	cfg.Sink.CatalogURI = ""
	cfg.Sink.Warehouse = ""
	cfg.Sink.Namespace = ""
	cfg.Sink.S3Endpoint = ""
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for missing sink fields")
	}
	for _, field := range []string{"catalog_uri", "warehouse", "namespace", "s3_endpoint"} {
		if !strings.Contains(err.Error(), field) {
			t.Errorf("expected error to mention %s: %v", field, err)
		}
	}
}

func TestValidate_InvalidDuration(t *testing.T) {
	cfg := validConfig()
	cfg.Sink.FlushInterval = "not-a-duration"
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for invalid flush_interval")
	}
	if !strings.Contains(err.Error(), "flush_interval") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidate_MultipleErrors(t *testing.T) {
	cfg := Config{}
	cfg.ApplyDefaults()
	cfg.Source.Postgres = PostgresConfig{} // all empty
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected errors")
	}
	// Should report multiple issues
	lines := strings.Split(err.Error(), "\n")
	if len(lines) < 3 {
		t.Fatalf("expected multiple errors, got: %v", err)
	}
}
