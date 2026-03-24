package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Source SourceConfig `yaml:"source" json:"source"`
	Sink   SinkConfig   `yaml:"sink" json:"sink"`
	State  StateConfig  `yaml:"state" json:"state"`
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
	Tables       []QueryTableConfig `yaml:"tables" json:"tables"`
	PollInterval string             `yaml:"poll_interval" json:"poll_interval"`
}

func (q QueryConfig) PollDuration() time.Duration {
	d, err := time.ParseDuration(q.PollInterval)
	if err != nil {
		return 5 * time.Second
	}
	return d
}

type QueryTableConfig struct {
	Name            string   `yaml:"name" json:"name"`
	PrimaryKey      []string `yaml:"primary_key" json:"primary_key"`
	WatermarkColumn string   `yaml:"watermark_column" json:"watermark_column"`
}

type LogicalConfig struct {
	PublicationName string   `yaml:"publication_name" json:"publication_name"`
	SlotName        string   `yaml:"slot_name" json:"slot_name"`
	Tables          []string `yaml:"tables" json:"tables"`
}

type SinkConfig struct {
	CatalogURI     string `yaml:"catalog_uri" json:"catalog_uri"`
	Warehouse      string `yaml:"warehouse" json:"warehouse"`
	Namespace      string `yaml:"namespace" json:"namespace"`
	S3Endpoint     string `yaml:"s3_endpoint" json:"s3_endpoint"`
	S3AccessKey    string `yaml:"s3_access_key" json:"s3_access_key"`
	S3SecretKey    string `yaml:"s3_secret_key" json:"s3_secret_key"`
	S3Region       string `yaml:"s3_region" json:"s3_region"`
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
	Path string `yaml:"path" json:"path"`
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

	applyDefaults(&cfg)
	return &cfg, nil
}

// LoadJSON parses a config from JSON bytes (used by the API).
func LoadJSON(data []byte) (*Config, error) {
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	applyDefaults(&cfg)
	return &cfg, nil
}

func applyDefaults(cfg *Config) {
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
