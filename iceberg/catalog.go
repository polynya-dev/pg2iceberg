package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/pg2iceberg/pg2iceberg/pipeline"
	"github.com/pg2iceberg/pg2iceberg/postgres"
)

var tracer = otel.Tracer("pg2iceberg/catalog")

// Catalog abstracts Iceberg catalog operations.
type Catalog interface {
	EnsureNamespace(ctx context.Context, ns string) error
	LoadTable(ctx context.Context, ns, table string) (*TableMetadata, error)
	CreateTable(ctx context.Context, ns, table string, ts *postgres.TableSchema, location string, partSpec *PartitionSpec) (*TableMetadata, error)
	CommitSnapshot(ctx context.Context, ns, table string, currentSnapshotID int64, snapshot SnapshotCommit) error
	CommitTransaction(ctx context.Context, ns string, commits []TableCommit) error
	EvolveSchema(ctx context.Context, ns, table string, currentSchemaID int, newSchema *postgres.TableSchema) (int, error)
}

// CatalogClient interacts with the Iceberg REST catalog.
type CatalogClient struct {
	baseURL string
	prefix  string // optional path prefix from GET /v1/config (e.g. "502bab72-...")
	client  *http.Client
}

func NewCatalogClient(baseURL string, httpClient *http.Client) *CatalogClient {
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	return &CatalogClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  httpClient,
	}
}

// SetPrefix sets the path prefix returned by GET /v1/config.
// When set, all catalog paths use /v1/{prefix}/... instead of /v1/...
func (c *CatalogClient) SetPrefix(prefix string) {
	c.prefix = prefix
}

// v1Path builds a versioned catalog path, inserting the prefix if set.
// e.g. with prefix "abc": "/v1/abc/namespaces/default/tables/orders"
func (c *CatalogClient) v1Path(path string) string {
	if c.prefix != "" {
		return "/v1/" + c.prefix + path
	}
	return "/v1" + path
}

// TableMetadata holds the relevant fields from the Iceberg REST catalog response.
type TableMetadata struct {
	MetadataLocation string            `json:"metadata-location"`
	Config           map[string]string `json:"config"` // vended credentials and storage config
	Metadata         struct {
		FormatVersion      int    `json:"format-version"`
		TableUUID          string `json:"table-uuid"`
		Location           string `json:"location"`
		LastSequenceNumber int64  `json:"last-sequence-number"`
		LastUpdatedMs      int64  `json:"last-updated-ms"`
		LastColumnID       int    `json:"last-column-id"`
		CurrentSchemaID    int    `json:"current-schema-id"`
		CurrentSnapshotID  int64  `json:"current-snapshot-id"`
		Snapshots          []struct {
			SnapshotID     int64             `json:"snapshot-id"`
			TimestampMs    int64             `json:"timestamp-ms"`
			ManifestList   string            `json:"manifest-list"`
			Summary        map[string]string `json:"summary"`
			SchemaID       int               `json:"schema-id"`
			SequenceNumber int64             `json:"sequence-number"`
		} `json:"snapshots"`
		Properties map[string]string `json:"properties"`
	} `json:"metadata"`
}

// VendedCreds holds temporary storage credentials returned by the catalog.
type VendedCreds struct {
	AccessKeyID    string
	SecretAccessKey string
	SessionToken   string
	Region         string
	Endpoint       string
}

// VendedCredentials extracts S3 credentials from the catalog config map.
// Returns nil if no credentials are present.
func (tm *TableMetadata) VendedCredentials() *VendedCreds {
	if tm == nil || tm.Config == nil {
		return nil
	}
	ak := tm.Config["s3.access-key-id"]
	sk := tm.Config["s3.secret-access-key"]
	if ak == "" || sk == "" {
		return nil
	}
	return &VendedCreds{
		AccessKeyID:    ak,
		SecretAccessKey: sk,
		SessionToken:   tm.Config["s3.session-token"],
		Region:         tm.Config["s3.region"],
		Endpoint:       tm.Config["s3.endpoint"],
	}
}

// CurrentManifestList returns the manifest-list URI of the current snapshot, or empty if no snapshots.
func (tm *TableMetadata) CurrentManifestList() string {
	if tm.Metadata.CurrentSnapshotID <= 0 {
		return ""
	}
	for _, snap := range tm.Metadata.Snapshots {
		if snap.SnapshotID == tm.Metadata.CurrentSnapshotID {
			return snap.ManifestList
		}
	}
	return ""
}

// EnsureNamespace creates a namespace if it doesn't exist.
func (c *CatalogClient) EnsureNamespace(ctx context.Context, ns string) error {
	ctx, span := tracer.Start(ctx, "catalog.EnsureNamespace", trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(
		attribute.String("iceberg.namespace", ns),
		attribute.String("service.name", "iceberg"),
	))
	defer span.End()

	// Check existence first — the Iceberg REST catalog's JdbcCatalog
	// backend returns 500 (not 409) on concurrent create attempts due
	// to a primary key constraint violation in the properties table.
	req, err := http.NewRequestWithContext(ctx, "HEAD", c.baseURL+c.v1Path(fmt.Sprintf("/namespaces/%s", ns)), nil)
	if err != nil {
		return err
	}
	headResp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	headResp.Body.Close()
	if headResp.StatusCode == 200 {
		return nil
	}

	body := map[string]any{
		"namespace": []string{ns},
	}
	resp, err := c.post(ctx, c.v1Path("/namespaces"), body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// 200 = created, 409 = already exists — both fine
	if resp.StatusCode != 200 && resp.StatusCode != 409 {
		return c.readError(resp)
	}
	return nil
}

// LoadTable fetches table metadata from the catalog.
func (c *CatalogClient) LoadTable(ctx context.Context, ns, table string) (*TableMetadata, error) {
	ctx, span := tracer.Start(ctx, "catalog.LoadTable "+table, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(
		attribute.String("iceberg.namespace", ns),
		attribute.String("iceberg.table", table),
		attribute.String("service.name", "iceberg"),
	))
	defer span.End()

	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL+c.v1Path(fmt.Sprintf("/namespaces/%s/tables/%s", ns, table)), nil)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Iceberg-Access-Delegation", "vended-credentials")
	resp, err := c.client.Do(req)
	pipeline.CatalogOperationDurationSeconds.WithLabelValues("load_table").Observe(time.Since(start).Seconds())
	if err != nil {
		pipeline.CatalogErrorsTotal.WithLabelValues("load_table").Inc()
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return nil, nil // table doesn't exist
	}
	if resp.StatusCode != 200 {
		pipeline.CatalogErrorsTotal.WithLabelValues("load_table").Inc()
		err := c.readError(resp)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	var tm TableMetadata
	if err := json.NewDecoder(resp.Body).Decode(&tm); err != nil {
		return nil, fmt.Errorf("decode table metadata: %w", err)
	}
	return &tm, nil
}

// CreateTable creates a new Iceberg table.
func (c *CatalogClient) CreateTable(ctx context.Context, ns, table string, ts *postgres.TableSchema, location string, partSpec *PartitionSpec) (*TableMetadata, error) {
	ctx, span := tracer.Start(ctx, "catalog.CreateTable "+table, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(
		attribute.String("iceberg.namespace", ns),
		attribute.String("iceberg.table", table),
		attribute.String("service.name", "iceberg"),
	))
	defer span.End()
	icebergSchema := postgres.IcebergSchemaJSON(ts)

	partitionSpec := map[string]any{
		"spec-id": 0,
		"fields":  []any{},
	}
	if partSpec != nil && !partSpec.IsUnpartitioned() {
		partitionSpec = partSpec.CatalogPartitionSpec()
	}

	body := map[string]any{
		"name":           table,
		"schema":         icebergSchema,
		"partition-spec": partitionSpec,
		"write-order": map[string]any{
			"order-id": 0,
			"fields":   []any{},
		},
		"properties": map[string]string{
			"format-version":       "2",
			"write.format.default": "parquet",
		},
	}
	if location != "" {
		body["location"] = location
	}

	resp, err := c.post(ctx, c.v1Path(fmt.Sprintf("/namespaces/%s/tables", ns)), body)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		err := c.readError(resp)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	var tm TableMetadata
	if err := json.NewDecoder(resp.Body).Decode(&tm); err != nil {
		return nil, fmt.Errorf("decode create response: %w", err)
	}
	if tm.MetadataLocation == "" {
		return nil, fmt.Errorf("create table %s.%s: server returned 200 but no metadata-location (catalog may not support this endpoint)", ns, table)
	}
	log.Printf("[catalog] created table %s.%s (uuid=%s, location=%s)", ns, table, tm.Metadata.TableUUID, tm.Metadata.Location)
	return &tm, nil
}

// CommitSnapshot commits a new snapshot to the table.
func (c *CatalogClient) CommitSnapshot(ctx context.Context, ns, table string, currentSnapshotID int64, snapshot SnapshotCommit) error {
	ctx, span := tracer.Start(ctx, "catalog.CommitSnapshot "+table, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(
		attribute.String("iceberg.namespace", ns),
		attribute.String("iceberg.table", table),
		attribute.Int64("iceberg.snapshot_id", snapshot.SnapshotID),
		attribute.String("service.name", "iceberg"),
	))
	defer span.End()

	defer func(start time.Time) {
		pipeline.CatalogOperationDurationSeconds.WithLabelValues("commit_snapshot").Observe(time.Since(start).Seconds())
	}(time.Now())
	// Build requirements
	var requirements []map[string]any
	if currentSnapshotID <= 0 {
		// First commit: assert no current snapshot on main
		requirements = []map[string]any{
			{
				"type": "assert-ref-snapshot-id",
				"ref":  "main",
				"snapshot-id": nil,
			},
		}
	} else {
		requirements = []map[string]any{
			{
				"type":        "assert-ref-snapshot-id",
				"ref":         "main",
				"snapshot-id": currentSnapshotID,
			},
		}
	}

	updates := []map[string]any{
		{
			"action": "add-snapshot",
			"snapshot": map[string]any{
				"snapshot-id":     snapshot.SnapshotID,
				"timestamp-ms":   snapshot.TimestampMs,
				"manifest-list":  snapshot.ManifestListPath,
				"summary":        snapshot.Summary,
				"schema-id":      snapshot.SchemaID,
				"sequence-number": snapshot.SequenceNumber,
			},
		},
		{
			"action":      "set-snapshot-ref",
			"ref-name":    "main",
			"type":        "branch",
			"snapshot-id": snapshot.SnapshotID,
		},
	}

	body := map[string]any{
		"requirements": requirements,
		"updates":      updates,
	}

	resp, err := c.post(ctx, c.v1Path(fmt.Sprintf("/namespaces/%s/tables/%s", ns, table)), body)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		err := c.readError(resp)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

// SnapshotCommit holds the data needed to commit a new snapshot.
type SnapshotCommit struct {
	SnapshotID       int64
	SequenceNumber   int64
	TimestampMs      int64
	ManifestListPath string
	Summary          map[string]string
	SchemaID         int
}

// TableCommit holds the data needed to commit a snapshot for one table
// within a multi-table transaction.
type TableCommit struct {
	Table             string
	CurrentSnapshotID int64
	Snapshot          SnapshotCommit
}

// CommitTransaction atomically commits snapshots to multiple tables using
// the Iceberg REST catalog's multi-table transaction endpoint.
func (c *CatalogClient) CommitTransaction(ctx context.Context, ns string, commits []TableCommit) error {
	ctx, span := tracer.Start(ctx, "catalog.CommitTransaction", trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(
		attribute.String("iceberg.namespace", ns),
		attribute.Int("iceberg.table_count", len(commits)),
		attribute.String("service.name", "iceberg"),
	))
	defer span.End()

	defer func(start time.Time) {
		pipeline.CatalogOperationDurationSeconds.WithLabelValues("commit_transaction").Observe(time.Since(start).Seconds())
	}(time.Now())
	if len(commits) == 0 {
		return nil
	}

	// Single table — use the normal commit path.
	if len(commits) == 1 {
		tc := commits[0]
		return c.CommitSnapshot(ctx, ns, tc.Table, tc.CurrentSnapshotID, tc.Snapshot)
	}

	var tableChanges []map[string]any
	for _, tc := range commits {
		var requirements []map[string]any
		if tc.CurrentSnapshotID <= 0 {
			requirements = []map[string]any{
				{
					"type":        "assert-ref-snapshot-id",
					"ref":         "main",
					"snapshot-id": nil,
				},
			}
		} else {
			requirements = []map[string]any{
				{
					"type":        "assert-ref-snapshot-id",
					"ref":         "main",
					"snapshot-id": tc.CurrentSnapshotID,
				},
			}
		}

		updates := []map[string]any{
			{
				"action": "add-snapshot",
				"snapshot": map[string]any{
					"snapshot-id":      tc.Snapshot.SnapshotID,
					"timestamp-ms":    tc.Snapshot.TimestampMs,
					"manifest-list":   tc.Snapshot.ManifestListPath,
					"summary":         tc.Snapshot.Summary,
					"schema-id":       tc.Snapshot.SchemaID,
					"sequence-number": tc.Snapshot.SequenceNumber,
				},
			},
			{
				"action":      "set-snapshot-ref",
				"ref-name":    "main",
				"type":        "branch",
				"snapshot-id": tc.Snapshot.SnapshotID,
			},
		}

		tableChanges = append(tableChanges, map[string]any{
			"identifier": map[string]any{
				"namespace": []string{ns},
				"name":      tc.Table,
			},
			"requirements": requirements,
			"updates":      updates,
		})
	}

	body := map[string]any{
		"table-changes": tableChanges,
	}

	resp, err := c.post(ctx, c.v1Path("/transactions/commit"), body)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 && resp.StatusCode != 204 {
		err := c.readError(resp)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

// EvolveSchema updates the Iceberg table schema via the REST catalog.
// It adds a new schema version and sets it as the current postgres.
// Returns the new schema ID.
func (c *CatalogClient) EvolveSchema(ctx context.Context, ns, table string, currentSchemaID int, newSchema *postgres.TableSchema) (int, error) {
	ctx, span := tracer.Start(ctx, "catalog.EvolveSchema "+table, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(
		attribute.String("iceberg.namespace", ns),
		attribute.String("iceberg.table", table),
		attribute.String("service.name", "iceberg"),
	))
	defer span.End()

	defer func(start time.Time) {
		pipeline.CatalogOperationDurationSeconds.WithLabelValues("evolve_schema").Observe(time.Since(start).Seconds())
	}(time.Now())
	newSchemaID := currentSchemaID + 1

	body := map[string]any{
		"requirements": []map[string]any{
			{
				"type":              "assert-current-schema-id",
				"current-schema-id": currentSchemaID,
			},
		},
		"updates": []map[string]any{
			{
				"action": "add-schema",
				"schema": postgres.IcebergSchemaJSONWithID(newSchema, newSchemaID),
			},
			{
				"action":    "set-current-schema",
				"schema-id": -1, // -1 means "use the last added schema"
			},
		},
	}

	resp, err := c.post(ctx, c.v1Path(fmt.Sprintf("/namespaces/%s/tables/%s", ns, table)), body)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, fmt.Errorf("evolve schema: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		err := c.readError(resp)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, err
	}
	return newSchemaID, nil
}

func (c *CatalogClient) get(ctx context.Context, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.client.Do(req)
}

func (c *CatalogClient) post(ctx context.Context, path string, body any) (*http.Response, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+path, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.client.Do(req)
}

func (c *CatalogClient) readError(resp *http.Response) error {
	body, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("catalog error %d: %s", resp.StatusCode, string(body))
}

// CatalogConfig holds the response from GET /v1/config.
type CatalogConfig struct {
	Defaults  map[string]string `json:"defaults"`
	Overrides map[string]string `json:"overrides"`
}

// GetConfig fetches catalog-wide configuration defaults.
// Returns nil without error if the endpoint is not available (404/405).
func (c *CatalogClient) GetConfig(ctx context.Context, warehouse string) (*CatalogConfig, error) {
	path := "/v1/config"
	if warehouse != "" {
		path += "?warehouse=" + url.QueryEscape(warehouse)
	}
	resp, err := c.get(ctx, path)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 || resp.StatusCode == 405 {
		return nil, nil
	}
	if resp.StatusCode != 200 {
		return nil, c.readError(resp)
	}

	var cc CatalogConfig
	if err := json.NewDecoder(resp.Body).Decode(&cc); err != nil {
		return nil, fmt.Errorf("decode catalog config: %w", err)
	}
	return &cc, nil
}
