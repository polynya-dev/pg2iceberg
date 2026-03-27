package config

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Tables      []TableConfig `yaml:"tables" json:"tables"`
	Source      SourceConfig  `yaml:"source" json:"source"`
	Sink        SinkConfig    `yaml:"sink" json:"sink"`
	State       StateConfig   `yaml:"state" json:"state"`
	MetricsAddr string        `yaml:"metrics_addr" json:"metrics_addr,omitempty"`
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
}

func (p PostgresConfig) DSN() string {
	return fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=disable",
		p.Host, p.Port, p.Database, p.User, p.Password)
}

func (p PostgresConfig) ReplicationDSN() string {
	return fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=disable replication=database",
		p.Host, p.Port, p.Database, p.User, p.Password)
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
	PublicationName     string `yaml:"publication_name" json:"publication_name"`
	SlotName            string `yaml:"slot_name" json:"slot_name"`
	SnapshotConcurrency int    `yaml:"snapshot_concurrency" json:"snapshot_concurrency,omitempty"`
}

// SnapshotConcurrencyOrDefault returns the configured snapshot concurrency,
// defaulting to GOMAXPROCS if not set.
func (c LogicalConfig) SnapshotConcurrencyOrDefault() int {
	if c.SnapshotConcurrency > 0 {
		return c.SnapshotConcurrency
	}
	return runtime.GOMAXPROCS(0)
}

type SinkConfig struct {
	CatalogURI  string `yaml:"catalog_uri" json:"catalog_uri"`
	CatalogAuth string `yaml:"catalog_auth" json:"catalog_auth,omitempty"` // "" (none) or "sigv4" (AWS SigV4)
	Warehouse   string `yaml:"warehouse" json:"warehouse"`
	Namespace   string `yaml:"namespace" json:"namespace"`
	S3Endpoint  string `yaml:"s3_endpoint" json:"s3_endpoint"`
	S3AccessKey string `yaml:"s3_access_key" json:"s3_access_key"`
	S3SecretKey string `yaml:"s3_secret_key" json:"s3_secret_key"`
	S3Region    string `yaml:"s3_region" json:"s3_region"`
	FlushInterval  string `yaml:"flush_interval" json:"flush_interval"`
	FlushRows      int    `yaml:"flush_rows" json:"flush_rows"`
	FlushBytes     int64  `yaml:"flush_bytes" json:"flush_bytes,omitempty"`
	TargetFileSize int64  `yaml:"target_file_size" json:"target_file_size,omitempty"`

	// Compaction settings
	CompactionInterval   string `yaml:"compaction_interval" json:"compaction_interval,omitempty"`
	CompactionTargetSize int64  `yaml:"compaction_target_size" json:"compaction_target_size,omitempty"`
	CompactionMinFiles   int    `yaml:"compaction_min_files" json:"compaction_min_files,omitempty"`
	MaxSnapshots         int    `yaml:"max_snapshots" json:"max_snapshots,omitempty"`
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

func (s SinkConfig) CompactionDuration() time.Duration {
	if s.CompactionInterval == "" {
		return 0 // disabled
	}
	d, err := time.ParseDuration(s.CompactionInterval)
	if err != nil {
		return 0
	}
	return d
}

func (s SinkConfig) CompactionTargetSizeOrDefault() int64 {
	if s.CompactionTargetSize > 0 {
		return s.CompactionTargetSize
	}
	return 256 * 1024 * 1024 // 256MB
}

func (s SinkConfig) CompactionMinFilesOrDefault() int {
	if s.CompactionMinFiles > 0 {
		return s.CompactionMinFiles
	}
	return 4
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

// Load reads a config from a YAML file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
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
	if cfg.Sink.S3Region == "" {
		cfg.Sink.S3Region = "us-east-1"
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
	switch cfg.Source.Mode {
	case "logical", "query":
	default:
		errs = append(errs, fmt.Sprintf("source.mode: must be \"logical\" or \"query\", got %q", cfg.Source.Mode))
	}

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

	if cfg.Source.Mode == "logical" {
		if cfg.Source.Logical.SlotName == "" {
			errs = append(errs, "source.logical.slot_name: required for logical mode")
		}
		if cfg.Source.Logical.PublicationName == "" {
			errs = append(errs, "source.logical.publication_name: required for logical mode")
		}
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
	}

	// Sink
	if cfg.Sink.CatalogURI == "" {
		errs = append(errs, "sink.catalog_uri: required")
	}
	if cfg.Sink.Warehouse == "" {
		errs = append(errs, "sink.warehouse: required")
	}
	if cfg.Sink.Namespace == "" {
		errs = append(errs, "sink.namespace: required")
	}
	if cfg.Sink.S3Endpoint == "" {
		errs = append(errs, "sink.s3_endpoint: required")
	}

	if cfg.Sink.FlushInterval != "" {
		if _, err := time.ParseDuration(cfg.Sink.FlushInterval); err != nil {
			errs = append(errs, fmt.Sprintf("sink.flush_interval: invalid duration %q", cfg.Sink.FlushInterval))
		}
	}
	if cfg.Sink.CompactionInterval != "" {
		if _, err := time.ParseDuration(cfg.Sink.CompactionInterval); err != nil {
			errs = append(errs, fmt.Sprintf("sink.compaction_interval: invalid duration %q", cfg.Sink.CompactionInterval))
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
