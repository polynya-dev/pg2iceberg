package pipeline

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// ClickHouseProvisioner creates per-tenant databases in ClickHouse
// via the HTTP interface.
type ClickHouseProvisioner struct {
	addr       string // e.g. "http://localhost:8123"
	catalogURI string // Iceberg REST catalog URI as seen by ClickHouse (e.g. "http://iceberg-rest:8181/v1")
	s3Endpoint string // S3 endpoint as seen by ClickHouse (e.g. "http://minio:9000")
	warehouse  string // e.g. "s3://warehouse/"
	client     *http.Client
}

func NewClickHouseProvisioner(addr, catalogURI, s3Endpoint, warehouse string) *ClickHouseProvisioner {
	return &ClickHouseProvisioner{
		addr:       addr,
		catalogURI: catalogURI,
		s3Endpoint: s3Endpoint,
		warehouse:  warehouse,
		client:     &http.Client{Timeout: 10 * time.Second},
	}
}

// EnsureDatabase creates a ClickHouse database for the given Iceberg namespace
// if it doesn't already exist.
func (c *ClickHouseProvisioner) EnsureDatabase(namespace string) error {
	query := fmt.Sprintf(
		`CREATE DATABASE IF NOT EXISTS %s ENGINE = DataLakeCatalog('%s') SETTINGS catalog_type = 'rest', warehouse = '%s', storage_endpoint = '%s'`,
		namespace, c.catalogURI, c.warehouse, c.s3Endpoint,
	)
	return c.exec(query)
}

// DropDatabase drops a ClickHouse database for the given namespace.
func (c *ClickHouseProvisioner) DropDatabase(namespace string) error {
	query := fmt.Sprintf(`DROP DATABASE IF EXISTS %s`, namespace)
	return c.exec(query)
}

func (c *ClickHouseProvisioner) exec(query string) error {
	u := fmt.Sprintf("%s/?query=%s", c.addr, url.QueryEscape(query))
	resp, err := c.client.Post(u, "", nil)
	if err != nil {
		return fmt.Errorf("clickhouse request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("clickhouse error (%d): %s", resp.StatusCode, body)
	}
	return nil
}
