package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Source SourceConfig `yaml:"source"`
	Sink   SinkConfig   `yaml:"sink"`
	State  StateConfig  `yaml:"state"`
}

type SourceConfig struct {
	Mode    string         `yaml:"mode"` // "query" or "logical"
	Postgres PostgresConfig `yaml:"postgres"`
	Query   QueryConfig    `yaml:"query"`
	Logical LogicalConfig  `yaml:"logical"`
}

type PostgresConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
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
	Tables       []QueryTableConfig `yaml:"tables"`
	PollInterval string             `yaml:"poll_interval"`
}

func (q QueryConfig) PollDuration() time.Duration {
	d, err := time.ParseDuration(q.PollInterval)
	if err != nil {
		return 5 * time.Second
	}
	return d
}

type QueryTableConfig struct {
	Name            string   `yaml:"name"`
	PrimaryKey      []string `yaml:"primary_key"`
	WatermarkColumn string   `yaml:"watermark_column"`
}

type LogicalConfig struct {
	PublicationName string   `yaml:"publication_name"`
	SlotName        string   `yaml:"slot_name"`
	Tables          []string `yaml:"tables"`
}

type SinkConfig struct {
	CatalogURI    string `yaml:"catalog_uri"`
	Warehouse     string `yaml:"warehouse"`
	Namespace     string `yaml:"namespace"`
	S3Endpoint    string `yaml:"s3_endpoint"`
	S3AccessKey   string `yaml:"s3_access_key"`
	S3SecretKey   string `yaml:"s3_secret_key"`
	S3Region      string `yaml:"s3_region"`
	FlushInterval string `yaml:"flush_interval"`
	FlushRows     int    `yaml:"flush_rows"`
}

func (s SinkConfig) FlushDuration() time.Duration {
	d, err := time.ParseDuration(s.FlushInterval)
	if err != nil {
		return 10 * time.Second
	}
	return d
}

type StateConfig struct {
	Path string `yaml:"path"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

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

	return &cfg, nil
}
