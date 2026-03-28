package sink

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/pg2iceberg/pg2iceberg/metrics"
	"github.com/pg2iceberg/pg2iceberg/schema"
)

// CatalogClient interacts with the Iceberg REST catalog.
type CatalogClient struct {
	baseURL string
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

// TableMetadata holds the relevant fields from the Iceberg REST catalog response.
type TableMetadata struct {
	MetadataLocation string `json:"metadata-location"`
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
func (c *CatalogClient) EnsureNamespace(ns string) error {
	body := map[string]any{
		"namespace": []string{ns},
	}
	resp, err := c.post(fmt.Sprintf("/v1/namespaces"), body)
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
func (c *CatalogClient) LoadTable(ns, table string) (*TableMetadata, error) {
	start := time.Now()
	resp, err := c.get(fmt.Sprintf("/v1/namespaces/%s/tables/%s", ns, table))
	metrics.CatalogOperationDurationSeconds.WithLabelValues("load_table").Observe(time.Since(start).Seconds())
	if err != nil {
		metrics.CatalogErrorsTotal.WithLabelValues("load_table").Inc()
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return nil, nil // table doesn't exist
	}
	if resp.StatusCode != 200 {
		metrics.CatalogErrorsTotal.WithLabelValues("load_table").Inc()
		return nil, c.readError(resp)
	}

	var tm TableMetadata
	if err := json.NewDecoder(resp.Body).Decode(&tm); err != nil {
		return nil, fmt.Errorf("decode table metadata: %w", err)
	}
	return &tm, nil
}

// CreateTable creates a new Iceberg table.
func (c *CatalogClient) CreateTable(ns, table string, ts *schema.TableSchema, location string, partSpec *PartitionSpec) (*TableMetadata, error) {
	icebergSchema := schema.IcebergSchemaJSON(ts)

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

	resp, err := c.post(fmt.Sprintf("/v1/namespaces/%s/tables", ns), body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, c.readError(resp)
	}

	var tm TableMetadata
	if err := json.NewDecoder(resp.Body).Decode(&tm); err != nil {
		return nil, fmt.Errorf("decode create response: %w", err)
	}
	return &tm, nil
}

// CommitSnapshot commits a new snapshot to the table.
func (c *CatalogClient) CommitSnapshot(ns, table string, currentSnapshotID int64, snapshot SnapshotCommit) error {
	defer func(start time.Time) {
		metrics.CatalogOperationDurationSeconds.WithLabelValues("commit_snapshot").Observe(time.Since(start).Seconds())
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

	resp, err := c.post(fmt.Sprintf("/v1/namespaces/%s/tables/%s", ns, table), body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return c.readError(resp)
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
func (c *CatalogClient) CommitTransaction(ns string, commits []TableCommit) error {
	defer func(start time.Time) {
		metrics.CatalogOperationDurationSeconds.WithLabelValues("commit_transaction").Observe(time.Since(start).Seconds())
	}(time.Now())
	if len(commits) == 0 {
		return nil
	}

	// Single table — use the normal commit path.
	if len(commits) == 1 {
		tc := commits[0]
		return c.CommitSnapshot(ns, tc.Table, tc.CurrentSnapshotID, tc.Snapshot)
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

	resp, err := c.post("/v1/transactions/commit", body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 && resp.StatusCode != 204 {
		return c.readError(resp)
	}
	return nil
}

// EvolveSchema updates the Iceberg table schema via the REST catalog.
// It adds a new schema version and sets it as the current schema.
// Returns the new schema ID.
func (c *CatalogClient) EvolveSchema(ns, table string, currentSchemaID int, newSchema *schema.TableSchema) (int, error) {
	defer func(start time.Time) {
		metrics.CatalogOperationDurationSeconds.WithLabelValues("evolve_schema").Observe(time.Since(start).Seconds())
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
				"schema": schema.IcebergSchemaJSONWithID(newSchema, newSchemaID),
			},
			{
				"action":    "set-current-schema",
				"schema-id": -1, // -1 means "use the last added schema"
			},
		},
	}

	resp, err := c.post(fmt.Sprintf("/v1/namespaces/%s/tables/%s", ns, table), body)
	if err != nil {
		return 0, fmt.Errorf("evolve schema: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return 0, c.readError(resp)
	}
	return newSchemaID, nil
}

func (c *CatalogClient) get(path string) (*http.Response, error) {
	req, err := http.NewRequest("GET", c.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.client.Do(req)
}

func (c *CatalogClient) post(path string, body any) (*http.Response, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	req, err := http.NewRequest("POST", c.baseURL+path, bytes.NewReader(data))
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
