package config

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Tables       []TableConfig `yaml:"tables" json:"tables"`
	Source       SourceConfig  `yaml:"source" json:"source"`
	Sink         SinkConfig    `yaml:"sink" json:"sink"`
	State        StateConfig   `yaml:"state" json:"state"`
	MetricsAddr  string        `yaml:"metrics_addr" json:"metrics_addr,omitempty"`
	SnapshotOnly bool          `yaml:"snapshot_only" json:"snapshot_only,omitempty"`
}

// TableConfig describes a table to replicate.
type TableConfig struct {
	Name string `yaml:"name" json:"name"` // e.g. "public.orders"

	// Logical mode options.
	SkipSnapshot bool `yaml:"skip_snapshot" json:"skip_snapshot,omitempty"`

	// Query mode options.
	PrimaryKey      []string `yaml:"primary_key" json:"primary_key,omitempty"`
	WatermarkColumn string   `yaml:"watermark_column" json:"watermark_column,omitempty"`

	// Iceberg table options.
	Iceberg IcebergTableConfig `yaml:"iceberg" json:"iceberg,omitempty"`
}

// IcebergTableConfig holds Iceberg-specific per-table settings.
type IcebergTableConfig struct {
	// Partition defines partition fields using function syntax:
	//   - "day(created_at)"    → day transform on created_at column
	//   - "month(event_time)"  → month transform
	//   - "region"             → identity transform (column name only)
	// Supported transforms: identity, year, month, day, hour.
	Partition []string `yaml:"partition" json:"partition,omitempty"`
}

type SourceConfig struct {
	Mode     string         `yaml:"mode" json:"mode"`
	Postgres PostgresConfig `yaml:"postgres" json:"postgres"`
	Query    QueryConfig    `yaml:"query" json:"query,omitempty"`
	Logical  LogicalConfig  `yaml:"logical" json:"logical,omitempty"`
}

type PostgresConfig struct {
	Host     string `yaml:"host" json:"host"`
	Port     int    `yaml:"port" json:"port"`
	Database string `yaml:"database" json:"database"`
	User     string `yaml:"user" json:"user"`
	Password string `yaml:"password" json:"password"`
	SSLMode  string `yaml:"sslmode" json:"sslmode,omitempty"` // disable, require, verify-ca, verify-full
}

func (p PostgresConfig) sslmode() string {
	if p.SSLMode != "" {
		return p.SSLMode
	}
	return "disable"
}

func (p PostgresConfig) DSN() string {
	return fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		p.Host, p.Port, p.Database, p.User, p.Password, p.sslmode())
}

func (p PostgresConfig) ReplicationDSN() string {
	return fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s replication=database",
		p.Host, p.Port, p.Database, p.User, p.Password, p.sslmode())
}

type QueryConfig struct {
	PollInterval string `yaml:"poll_interval" json:"poll_interval"`
}

func (q QueryConfig) PollDuration() time.Duration {
	d, err := time.ParseDuration(q.PollInterval)
	if err != nil {
		return 5 * time.Second
	}
	return d
}

type LogicalConfig struct {
	PublicationName        string `yaml:"publication_name" json:"publication_name"`
	SlotName               string `yaml:"slot_name" json:"slot_name"`
	SnapshotConcurrency    int    `yaml:"snapshot_concurrency" json:"snapshot_concurrency,omitempty"`
	SnapshotChunkPages     int    `yaml:"snapshot_chunk_pages" json:"snapshot_chunk_pages,omitempty"`
	SnapshotTargetFileSize int64  `yaml:"snapshot_target_file_size" json:"snapshot_target_file_size,omitempty"`
	StandbyInterval        string `yaml:"standby_interval" json:"standby_interval,omitempty"`
}

// StandbyIntervalDuration returns the configured standby interval,
// defaulting to 10s if not set or invalid.
func (c LogicalConfig) StandbyIntervalDuration() time.Duration {
	if c.StandbyInterval == "" {
		return 10 * time.Second
	}
	d, err := time.ParseDuration(c.StandbyInterval)
	if err != nil {
		return 10 * time.Second
	}
	return d
}

// SnapshotConcurrencyOrDefault returns the configured snapshot concurrency,
// defaulting to GOMAXPROCS if not set.
func (c LogicalConfig) SnapshotConcurrencyOrDefault() int {
	if c.SnapshotConcurrency > 0 {
		return c.SnapshotConcurrency
	}
	return runtime.GOMAXPROCS(0)
}

// SnapshotChunkPagesOrDefault returns the configured CTID chunk size in pages.
// Default: 2048 pages (~16MB at 8KB/page).
func (c LogicalConfig) SnapshotChunkPagesOrDefault() int {
	if c.SnapshotChunkPages > 0 {
		return c.SnapshotChunkPages
	}
	return 2048
}

// SnapshotTargetFileSizeOrDefault returns the target Parquet file size for
// snapshot writes. Default: 128MB (larger than streaming since this is bulk).
func (c LogicalConfig) SnapshotTargetFileSizeOrDefault() int64 {
	if c.SnapshotTargetFileSize > 0 {
		return c.SnapshotTargetFileSize
	}
	return 128 * 1024 * 1024
}

type SinkConfig struct {
	CatalogURI   string `yaml:"catalog_uri" json:"catalog_uri"`
	CatalogAuth  string `yaml:"catalog_auth" json:"catalog_auth,omitempty"`   // "" (none), "sigv4", or "bearer"
	CatalogToken string `yaml:"catalog_token" json:"catalog_token,omitempty"` // Bearer token (required when catalog_auth=bearer)

	CredentialMode string `yaml:"credential_mode" json:"credential_mode,omitempty"` // "static" (default) or "vended"

	Warehouse   string `yaml:"warehouse" json:"warehouse,omitempty"`
	Namespace   string `yaml:"namespace" json:"namespace"`
	S3Endpoint  string `yaml:"s3_endpoint" json:"s3_endpoint,omitempty"`
	S3AccessKey string `yaml:"s3_access_key" json:"s3_access_key,omitempty"`
	S3SecretKey string `yaml:"s3_secret_key" json:"s3_secret_key,omitempty"`
	S3Region    string `yaml:"s3_region" json:"s3_region"`
	FlushInterval  string `yaml:"flush_interval" json:"flush_interval"`
	FlushRows      int    `yaml:"flush_rows" json:"flush_rows"`
	FlushBytes     int64  `yaml:"flush_bytes" json:"flush_bytes,omitempty"`
	TargetFileSize int64  `yaml:"target_file_size" json:"target_file_size,omitempty"`

	// Events table partition expression, e.g. "day(_ts)" or "month(_ts)".
	// Defaults to "day(_ts)" if not set.
	EventsPartition string `yaml:"events_partition" json:"events_partition,omitempty"`

	// Materializer settings
	MaterializerInterval       string `yaml:"materializer_interval" json:"materializer_interval,omitempty"`
	MaterializerTargetFileSize int64  `yaml:"materializer_target_file_size" json:"materializer_target_file_size,omitempty"`
	MaterializerConcurrency    int    `yaml:"materializer_concurrency" json:"materializer_concurrency,omitempty"`

	// Compaction thresholds — compact when file counts exceed these.
	CompactionDataFiles   int `yaml:"compaction_data_files" json:"compaction_data_files,omitempty"`
	CompactionDeleteFiles int `yaml:"compaction_delete_files" json:"compaction_delete_files,omitempty"`

	// Maintenance settings — snapshot expiry and orphan file cleanup.
	MaintenanceRetention string `yaml:"maintenance_retention" json:"maintenance_retention,omitempty"` // e.g. "168h" (7 days)
	MaintenanceInterval  string `yaml:"maintenance_interval" json:"maintenance_interval,omitempty"`   // e.g. "1h"
	MaintenanceGrace     string `yaml:"maintenance_grace" json:"maintenance_grace,omitempty"`         // e.g. "30m"
}

func (s SinkConfig) EventsPartitionOrDefault() string {
	if s.EventsPartition != "" {
		return s.EventsPartition
	}
	return "day(_ts)"
}

func (s SinkConfig) FlushBytesOrDefault() int64 {
	if s.FlushBytes > 0 {
		return s.FlushBytes
	}
	return 64 * 1024 * 1024 // 64MB
}

func (s SinkConfig) TargetFileSizeOrDefault() int64 {
	if s.TargetFileSize > 0 {
		return s.TargetFileSize
	}
	return 128 * 1024 * 1024 // 128MB
}

func (s SinkConfig) MaterializerDuration() time.Duration {
	if s.MaterializerInterval == "" {
		return 30 * time.Second // default 30s
	}
	d, err := time.ParseDuration(s.MaterializerInterval)
	if err != nil {
		return 30 * time.Second
	}
	return d
}

// MaterializerTargetFileSizeOrDefault returns the target file size for
// materialized table data and delete files. Default: 8MB.
func (s SinkConfig) MaterializerTargetFileSizeOrDefault() int64 {
	if s.MaterializerTargetFileSize > 0 {
		return s.MaterializerTargetFileSize
	}
	return 8 * 1024 * 1024 // 8MB
}

// MaterializerConcurrencyOrDefault returns the configured materializer I/O
// concurrency, defaulting to 16. Since this is I/O-bound (S3 range reads),
// it should be higher than core count.
func (s SinkConfig) MaterializerConcurrencyOrDefault() int {
	if s.MaterializerConcurrency > 0 {
		return s.MaterializerConcurrency
	}
	return 16
}

func (s SinkConfig) CompactionDataFilesOrDefault() int {
	if s.CompactionDataFiles > 0 {
		return s.CompactionDataFiles
	}
	return 20
}

func (s SinkConfig) CompactionDeleteFilesOrDefault() int {
	if s.CompactionDeleteFiles > 0 {
		return s.CompactionDeleteFiles
	}
	return 10
}

func (s SinkConfig) MaintenanceRetentionOrDefault() time.Duration {
	if s.MaintenanceRetention != "" {
		d, err := time.ParseDuration(s.MaintenanceRetention)
		if err == nil {
			return d
		}
	}
	return 7 * 24 * time.Hour // 7 days
}

func (s SinkConfig) MaintenanceIntervalOrDefault() time.Duration {
	if s.MaintenanceInterval != "" {
		d, err := time.ParseDuration(s.MaintenanceInterval)
		if err == nil {
			return d
		}
	}
	return 1 * time.Hour
}

func (s SinkConfig) MaintenanceGraceOrDefault() time.Duration {
	if s.MaintenanceGrace != "" {
		d, err := time.ParseDuration(s.MaintenanceGrace)
		if err == nil {
			return d
		}
	}
	return 30 * time.Minute
}

func (s SinkConfig) FlushDuration() time.Duration {
	d, err := time.ParseDuration(s.FlushInterval)
	if err != nil {
		return 10 * time.Second
	}
	return d
}

type StateConfig struct {
	Path        string `yaml:"path" json:"path,omitempty"`
	PostgresURL string `yaml:"postgres_url" json:"postgres_url,omitempty"`
}

// TableNames returns the list of table names from the top-level config.
func (cfg *Config) TableNames() []string {
	names := make([]string, len(cfg.Tables))
	for i, t := range cfg.Tables {
		names[i] = t.Name
	}
	return names
}

// FindTable returns the TableConfig for a given table name, or nil if not found.
func (cfg *Config) FindTable(name string) *TableConfig {
	for i := range cfg.Tables {
		if cfg.Tables[i].Name == name {
			return &cfg.Tables[i]
		}
	}
	return nil
}

// Load reads a config from a YAML file, applies defaults, and validates.
func Load(path string) (*Config, error) {
	cfg, err := LoadYAML(path)
	if err != nil {
		return nil, err
	}

	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// LoadYAML reads and parses a YAML config file without applying defaults or validation.
func LoadYAML(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	return &cfg, nil
}

// ParsePostgresURL parses a PostgreSQL connection URL into a PostgresConfig.
// Accepts URLs like: postgresql://user:password@host:port/database
func ParsePostgresURL(connURL string) (PostgresConfig, error) {
	u, err := url.Parse(connURL)
	if err != nil {
		return PostgresConfig{}, fmt.Errorf("invalid postgres URL: %w", err)
	}

	pg := PostgresConfig{
		Host:     u.Hostname(),
		Database: strings.TrimPrefix(u.Path, "/"),
		User:     u.User.Username(),
	}

	if p := u.Port(); p != "" {
		port, err := strconv.Atoi(p)
		if err != nil {
			return PostgresConfig{}, fmt.Errorf("invalid port in postgres URL: %w", err)
		}
		pg.Port = port
	}

	if pw, ok := u.User.Password(); ok {
		pg.Password = pw
	}

	if sslmode := u.Query().Get("sslmode"); sslmode != "" {
		pg.SSLMode = sslmode
	}

	return pg, nil
}

// ApplyEnv overrides config fields from environment variables.
// Call this after loading YAML (if any) and before ApplyDefaults/Validate.
func (cfg *Config) ApplyEnv() error {
	// Postgres connection — URL first, then individual fields can override.
	if v := os.Getenv("POSTGRES_URL"); v != "" {
		pg, err := ParsePostgresURL(v)
		if err != nil {
			return err
		}
		cfg.Source.Postgres = pg
	}
	if v := os.Getenv("POSTGRES_HOST"); v != "" {
		cfg.Source.Postgres.Host = v
	}
	if v := os.Getenv("POSTGRES_PORT"); v != "" {
		port, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid POSTGRES_PORT: %w", err)
		}
		cfg.Source.Postgres.Port = port
	}
	if v := os.Getenv("POSTGRES_DATABASE"); v != "" {
		cfg.Source.Postgres.Database = v
	}
	if v := os.Getenv("POSTGRES_USER"); v != "" {
		cfg.Source.Postgres.User = v
	}
	if v := os.Getenv("POSTGRES_SSLMODE"); v != "" {
		cfg.Source.Postgres.SSLMode = v
	}
	if v := os.Getenv("POSTGRES_PASSWORD"); v != "" {
		cfg.Source.Postgres.Password = v
	}

	if v := os.Getenv("MODE"); v != "" {
		cfg.Source.Mode = v
	}
	if v := os.Getenv("TABLES"); v != "" {
		cfg.Tables = ParseTables(v)
	}
	if v := os.Getenv("SLOT_NAME"); v != "" {
		cfg.Source.Logical.SlotName = v
	}
	if v := os.Getenv("PUBLICATION_NAME"); v != "" {
		cfg.Source.Logical.PublicationName = v
	}

	if v := os.Getenv("ICEBERG_CATALOG_URL"); v != "" {
		cfg.Sink.CatalogURI = v
	}
	if v := os.Getenv("CATALOG_AUTH"); v != "" {
		cfg.Sink.CatalogAuth = v
	}
	if v := os.Getenv("CATALOG_TOKEN"); v != "" {
		cfg.Sink.CatalogToken = v
	}
	if v := os.Getenv("CREDENTIAL_MODE"); v != "" {
		cfg.Sink.CredentialMode = v
	}
	if v := os.Getenv("WAREHOUSE"); v != "" {
		cfg.Sink.Warehouse = v
	}
	if v := os.Getenv("NAMESPACE"); v != "" {
		cfg.Sink.Namespace = v
	}
	if v := os.Getenv("S3_ENDPOINT"); v != "" {
		cfg.Sink.S3Endpoint = v
	}
	if v := os.Getenv("S3_ACCESS_KEY"); v != "" {
		cfg.Sink.S3AccessKey = v
	}
	if v := os.Getenv("S3_SECRET_KEY"); v != "" {
		cfg.Sink.S3SecretKey = v
	}
	if v := os.Getenv("S3_REGION"); v != "" {
		cfg.Sink.S3Region = v
	}

	if v := os.Getenv("SNAPSHOT_ONLY"); v != "" {
		cfg.SnapshotOnly = v == "1" || v == "true"
	}

	if v := os.Getenv("STATE_POSTGRES_URL"); v != "" {
		cfg.State.PostgresURL = v
	}
	if v := os.Getenv("METRICS_ADDR"); v != "" {
		cfg.MetricsAddr = v
	}

	if v := os.Getenv("MAINTENANCE_RETENTION"); v != "" {
		cfg.Sink.MaintenanceRetention = v
	}
	if v := os.Getenv("MAINTENANCE_INTERVAL"); v != "" {
		cfg.Sink.MaintenanceInterval = v
	}
	if v := os.Getenv("MAINTENANCE_GRACE"); v != "" {
		cfg.Sink.MaintenanceGrace = v
	}

	return nil
}

// ParseTables splits a comma-separated table list into TableConfigs.
func ParseTables(s string) []TableConfig {
	var tables []TableConfig
	for _, name := range strings.Split(s, ",") {
		name = strings.TrimSpace(name)
		if name != "" {
			tables = append(tables, TableConfig{Name: name})
		}
	}
	return tables
}

// LoadJSON parses a config from JSON bytes (used by the API).
func LoadJSON(data []byte) (*Config, error) {
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// ApplyDefaults fills in default values for any unset fields.
func (cfg *Config) ApplyDefaults() {
	if cfg.Source.Mode == "" {
		cfg.Source.Mode = "logical"
	}
	if cfg.Source.Postgres.Port == 0 {
		cfg.Source.Postgres.Port = 5432
	}
	if cfg.Sink.FlushRows == 0 {
		cfg.Sink.FlushRows = 1000
	}
	if cfg.Sink.CredentialMode == "" {
		cfg.Sink.CredentialMode = "static"
	}
	if cfg.Sink.S3Region == "" {
		cfg.Sink.S3Region = "us-east-1"
	}
	if cfg.Source.Logical.SlotName == "" {
		cfg.Source.Logical.SlotName = "pg2iceberg_slot"
	}
	if cfg.Source.Logical.PublicationName == "" {
		cfg.Source.Logical.PublicationName = "pg2iceberg_pub"
	}
}

// Validate checks that the config has all required fields and valid values.
// It should be called after ApplyDefaults.
func (cfg *Config) Validate() error {
	var errs []string

	// Tables
	if len(cfg.Tables) == 0 {
		errs = append(errs, "tables: at least one table is required")
	}
	seen := make(map[string]bool)
	for i, t := range cfg.Tables {
		if t.Name == "" {
			errs = append(errs, fmt.Sprintf("tables[%d].name: required", i))
		} else if seen[t.Name] {
			errs = append(errs, fmt.Sprintf("tables[%d].name: duplicate table %q", i, t.Name))
		} else {
			seen[t.Name] = true
		}
	}

	// Source
	pg := cfg.Source.Postgres
	if pg.Host == "" {
		errs = append(errs, "source.postgres.host: required")
	}
	if pg.Database == "" {
		errs = append(errs, "source.postgres.database: required")
	}
	if pg.User == "" {
		errs = append(errs, "source.postgres.user: required")
	}

	// In snapshot-only mode, mode-specific validation is skipped because
	// the snapshot path doesn't use replication slots or watermark queries.
	if !cfg.SnapshotOnly {
		switch cfg.Source.Mode {
		case "logical", "query":
		default:
			errs = append(errs, fmt.Sprintf("source.mode: must be \"logical\" or \"query\", got %q", cfg.Source.Mode))
		}

		if cfg.Source.Mode == "query" {
			if cfg.Source.Query.PollInterval != "" {
				if _, err := time.ParseDuration(cfg.Source.Query.PollInterval); err != nil {
					errs = append(errs, fmt.Sprintf("source.query.poll_interval: invalid duration %q", cfg.Source.Query.PollInterval))
				}
			}
			for i, t := range cfg.Tables {
				if len(t.PrimaryKey) == 0 {
					errs = append(errs, fmt.Sprintf("tables[%d].primary_key: required for query mode", i))
				}
				if t.WatermarkColumn == "" {
					errs = append(errs, fmt.Sprintf("tables[%d].watermark_column: required for query mode", i))
				}
			}
			// Warn about logical-only settings that have no effect in query mode.
			if cfg.Sink.EventsPartition != "" {
				errs = append(errs, "sink.events_partition: not applicable in query mode (no events table)")
			}
			if cfg.Sink.MaterializerInterval != "" {
				errs = append(errs, "sink.materializer_interval: not applicable in query mode (no materializer)")
			}
		}
	}

	// Sink
	if cfg.Sink.CatalogURI == "" {
		errs = append(errs, "sink.catalog_uri: required")
	}
	if cfg.Sink.Namespace == "" {
		errs = append(errs, "sink.namespace: required")
	}

	// Validate catalog_auth.
	switch cfg.Sink.CatalogAuth {
	case "", "none", "sigv4", "bearer":
	default:
		errs = append(errs, fmt.Sprintf("sink.catalog_auth: must be \"sigv4\", \"bearer\", or empty, got %q", cfg.Sink.CatalogAuth))
	}
	if cfg.Sink.CatalogAuth == "bearer" && cfg.Sink.CatalogToken == "" {
		errs = append(errs, "sink.catalog_token: required when catalog_auth is \"bearer\"")
	}

	// Validate credential_mode.
	switch cfg.Sink.CredentialMode {
	case "static", "vended", "iam":
	default:
		errs = append(errs, fmt.Sprintf("sink.credential_mode: must be \"static\", \"vended\", or \"iam\", got %q", cfg.Sink.CredentialMode))
	}

	// S3 fields and warehouse are only required in static credential mode.
	if cfg.Sink.CredentialMode == "static" {
		if cfg.Sink.Warehouse == "" {
			errs = append(errs, "sink.warehouse: required for static credential mode")
		}
		if cfg.Sink.S3Endpoint == "" {
			errs = append(errs, "sink.s3_endpoint: required for static credential mode")
		}
	}

	if cfg.Sink.FlushInterval != "" {
		if _, err := time.ParseDuration(cfg.Sink.FlushInterval); err != nil {
			errs = append(errs, fmt.Sprintf("sink.flush_interval: invalid duration %q", cfg.Sink.FlushInterval))
		}
	}
	if cfg.Sink.MaterializerInterval != "" {
		if _, err := time.ParseDuration(cfg.Sink.MaterializerInterval); err != nil {
			errs = append(errs, fmt.Sprintf("sink.materializer_interval: invalid duration %q", cfg.Sink.MaterializerInterval))
		}
	}
	if cfg.Sink.MaintenanceRetention != "" {
		if _, err := time.ParseDuration(cfg.Sink.MaintenanceRetention); err != nil {
			errs = append(errs, fmt.Sprintf("sink.maintenance_retention: invalid duration %q", cfg.Sink.MaintenanceRetention))
		}
	}
	if cfg.Sink.MaintenanceInterval != "" {
		if _, err := time.ParseDuration(cfg.Sink.MaintenanceInterval); err != nil {
			errs = append(errs, fmt.Sprintf("sink.maintenance_interval: invalid duration %q", cfg.Sink.MaintenanceInterval))
		}
	}
	if cfg.Sink.MaintenanceGrace != "" {
		if _, err := time.ParseDuration(cfg.Sink.MaintenanceGrace); err != nil {
			errs = append(errs, fmt.Sprintf("sink.maintenance_grace: invalid duration %q", cfg.Sink.MaintenanceGrace))
		}
	}
	if cfg.Sink.FlushRows < 0 {
		errs = append(errs, "sink.flush_rows: must be positive")
	}
	if cfg.Sink.FlushBytes < 0 {
		errs = append(errs, "sink.flush_bytes: must be positive")
	}
	if cfg.Sink.TargetFileSize < 0 {
		errs = append(errs, "sink.target_file_size: must be positive")
	}

	if len(errs) > 0 {
		return fmt.Errorf("config validation failed:\n  %s", strings.Join(errs, "\n  "))
	}
	return nil
}
