package config

import (
	"os"
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

func TestApplyDefaults_LogicalSlotAndPub(t *testing.T) {
	cfg := Config{}
	cfg.ApplyDefaults()
	if cfg.Source.Logical.SlotName != "pg2iceberg_slot" {
		t.Errorf("SlotName = %q, want pg2iceberg_slot", cfg.Source.Logical.SlotName)
	}
	if cfg.Source.Logical.PublicationName != "pg2iceberg_pub" {
		t.Errorf("PublicationName = %q, want pg2iceberg_pub", cfg.Source.Logical.PublicationName)
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

func TestValidate_QueryModeRejectsLogicalSettings(t *testing.T) {
	cfg := validConfig()
	cfg.Source.Mode = "query"
	cfg.Source.Logical = LogicalConfig{}
	cfg.Tables = []TableConfig{{Name: "public.orders", PrimaryKey: []string{"id"}, WatermarkColumn: "updated_at"}}
	cfg.Sink.EventsPartition = "day(_ts)"
	cfg.Sink.MaterializerInterval = "10s"
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for logical-only settings in query mode")
	}
	if !strings.Contains(err.Error(), "events_partition") {
		t.Errorf("expected error to mention events_partition: %v", err)
	}
	if !strings.Contains(err.Error(), "materializer_interval") {
		t.Errorf("expected error to mention materializer_interval: %v", err)
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

func TestParsePostgresURL(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		wantHost string
		wantPort int
		wantDB   string
		wantUser string
		wantPass string
		wantErr  bool
	}{
		{
			name:     "full URL",
			url:      "postgresql://myuser:mypass@db.example.com:5433/mydb",
			wantHost: "db.example.com",
			wantPort: 5433,
			wantDB:   "mydb",
			wantUser: "myuser",
			wantPass: "mypass",
		},
		{
			name:     "no port",
			url:      "postgresql://user:pass@localhost/testdb",
			wantHost: "localhost",
			wantPort: 0,
			wantDB:   "testdb",
			wantUser: "user",
			wantPass: "pass",
		},
		{
			name:     "no password",
			url:      "postgresql://user@localhost:5432/testdb",
			wantHost: "localhost",
			wantPort: 5432,
			wantDB:   "testdb",
			wantUser: "user",
			wantPass: "",
		},
		{
			name:     "postgres scheme",
			url:      "postgres://user:pass@localhost:5432/db",
			wantHost: "localhost",
			wantPort: 5432,
			wantDB:   "db",
			wantUser: "user",
			wantPass: "pass",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pg, err := ParsePostgresURL(tt.url)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParsePostgresURL() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if pg.Host != tt.wantHost {
				t.Errorf("Host = %q, want %q", pg.Host, tt.wantHost)
			}
			if pg.Port != tt.wantPort {
				t.Errorf("Port = %d, want %d", pg.Port, tt.wantPort)
			}
			if pg.Database != tt.wantDB {
				t.Errorf("Database = %q, want %q", pg.Database, tt.wantDB)
			}
			if pg.User != tt.wantUser {
				t.Errorf("User = %q, want %q", pg.User, tt.wantUser)
			}
			if pg.Password != tt.wantPass {
				t.Errorf("Password = %q, want %q", pg.Password, tt.wantPass)
			}
		})
	}
}

func TestParseTables(t *testing.T) {
	tables := ParseTables("public.orders, public.users , public.products")
	if len(tables) != 3 {
		t.Fatalf("expected 3 tables, got %d", len(tables))
	}
	want := []string{"public.orders", "public.users", "public.products"}
	for i, tc := range tables {
		if tc.Name != want[i] {
			t.Errorf("tables[%d].Name = %q, want %q", i, tc.Name, want[i])
		}
	}
}

func TestApplyEnv(t *testing.T) {
	// Save and restore env.
	envVars := []string{
		"POSTGRES_URL", "POSTGRES_HOST", "POSTGRES_PORT",
		"POSTGRES_DATABASE", "POSTGRES_USER", "POSTGRES_PASSWORD",
		"MODE", "TABLES", "SLOT_NAME", "PUBLICATION_NAME",
		"ICEBERG_CATALOG_URL", "WAREHOUSE", "NAMESPACE",
		"S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY", "S3_REGION",
		"STATE_POSTGRES_URL", "METRICS_ADDR",
	}
	saved := make(map[string]string)
	for _, k := range envVars {
		saved[k] = os.Getenv(k)
	}
	t.Cleanup(func() {
		for _, k := range envVars {
			if saved[k] != "" {
				os.Setenv(k, saved[k])
			} else {
				os.Unsetenv(k)
			}
		}
	})

	// Clear all env vars first.
	for _, k := range envVars {
		os.Unsetenv(k)
	}

	t.Run("POSTGRES_URL overrides postgres config", func(t *testing.T) {
		os.Setenv("POSTGRES_URL", "postgresql://envuser:envpass@envhost:5555/envdb")
		defer os.Unsetenv("POSTGRES_URL")

		var cfg Config
		if err := cfg.ApplyEnv(); err != nil {
			t.Fatal(err)
		}
		if cfg.Source.Postgres.Host != "envhost" {
			t.Errorf("Host = %q, want envhost", cfg.Source.Postgres.Host)
		}
		if cfg.Source.Postgres.Port != 5555 {
			t.Errorf("Port = %d, want 5555", cfg.Source.Postgres.Port)
		}
		if cfg.Source.Postgres.User != "envuser" {
			t.Errorf("User = %q, want envuser", cfg.Source.Postgres.User)
		}
	})

	t.Run("individual env vars override YAML", func(t *testing.T) {
		os.Setenv("ICEBERG_CATALOG_URL", "http://catalog:8181")
		os.Setenv("WAREHOUSE", "s3://mybucket/")
		os.Setenv("NAMESPACE", "prod")
		os.Setenv("TABLES", "public.orders,public.users")
		defer func() {
			os.Unsetenv("ICEBERG_CATALOG_URL")
			os.Unsetenv("WAREHOUSE")
			os.Unsetenv("NAMESPACE")
			os.Unsetenv("TABLES")
		}()

		cfg := validConfig()
		if err := cfg.ApplyEnv(); err != nil {
			t.Fatal(err)
		}
		if cfg.Sink.CatalogURI != "http://catalog:8181" {
			t.Errorf("CatalogURI = %q", cfg.Sink.CatalogURI)
		}
		if cfg.Sink.Warehouse != "s3://mybucket/" {
			t.Errorf("Warehouse = %q", cfg.Sink.Warehouse)
		}
		if cfg.Sink.Namespace != "prod" {
			t.Errorf("Namespace = %q", cfg.Sink.Namespace)
		}
		if len(cfg.Tables) != 2 || cfg.Tables[0].Name != "public.orders" {
			t.Errorf("Tables = %v", cfg.Tables)
		}
	})

	t.Run("individual postgres fields override URL fields", func(t *testing.T) {
		os.Setenv("POSTGRES_URL", "postgresql://urluser:urlpass@urlhost:5432/urldb")
		os.Setenv("POSTGRES_HOST", "overridehost")
		defer func() {
			os.Unsetenv("POSTGRES_URL")
			os.Unsetenv("POSTGRES_HOST")
		}()

		var cfg Config
		if err := cfg.ApplyEnv(); err != nil {
			t.Fatal(err)
		}
		if cfg.Source.Postgres.Host != "overridehost" {
			t.Errorf("Host = %q, want overridehost", cfg.Source.Postgres.Host)
		}
		if cfg.Source.Postgres.User != "urluser" {
			t.Errorf("User = %q, want urluser (from URL)", cfg.Source.Postgres.User)
		}
	})
}
