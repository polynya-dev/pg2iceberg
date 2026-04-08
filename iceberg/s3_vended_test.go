package iceberg

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockCatalogForVended implements TableLoader with configurable LoadTable responses.
type mockCatalogForVended struct {
	loadCount atomic.Int64
	mu        sync.Mutex
	creds     *VendedCreds
	location  string
}

func (m *mockCatalogForVended) LoadTable(_ context.Context, ns, table string) (*TableMetadata, error) {
	m.loadCount.Add(1)
	m.mu.Lock()
	creds := m.creds
	loc := m.location
	m.mu.Unlock()

	config := map[string]string{
		"s3.access-key-id":     creds.AccessKeyID,
		"s3.secret-access-key": creds.SecretAccessKey,
		"s3.session-token":     creds.SessionToken,
	}
	if creds.Region != "" {
		config["s3.region"] = creds.Region
	}
	if creds.Endpoint != "" {
		config["s3.endpoint"] = creds.Endpoint
	}

	return &TableMetadata{
		Config: config,
		Metadata: struct {
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
		}{
			Location: loc,
		},
	}, nil
}

func (m *mockCatalogForVended) setCreds(creds *VendedCreds) {
	m.mu.Lock()
	m.creds = creds
	m.mu.Unlock()
}

func TestVendedS3Client_Creation(t *testing.T) {
	creds := &VendedCreds{
		AccessKeyID:    "AKID1",
		SecretAccessKey: "SECRET1",
		SessionToken:   "TOKEN1",
		Region:         "us-east-1",
	}

	mock := &mockCatalogForVended{
		creds:    creds,
		location: "s3://test-bucket/warehouse/ns.db/table",
	}

	client, err := NewVendedS3Client(mock, "ns", "table", creds, "s3://test-bucket/warehouse/ns.db/table")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestVendedS3Client_RefreshOnExpiry(t *testing.T) {
	creds1 := &VendedCreds{
		AccessKeyID:    "AKID1",
		SecretAccessKey: "SECRET1",
		SessionToken:   "TOKEN1",
		Region:         "us-east-1",
	}
	creds2 := &VendedCreds{
		AccessKeyID:    "AKID2",
		SecretAccessKey: "SECRET2",
		SessionToken:   "TOKEN2",
		Region:         "us-east-1",
	}

	mock := &mockCatalogForVended{
		creds:    creds1,
		location: "s3://test-bucket/warehouse/ns.db/table",
	}

	client, err := NewVendedS3Client(mock, "ns", "table", creds1, "s3://test-bucket/warehouse/ns.db/table")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// No LoadTable calls yet (initial creds were passed directly).
	if got := mock.loadCount.Load(); got != 0 {
		t.Fatalf("expected 0 LoadTable calls, got %d", got)
	}

	// Force expiry by setting expiry to the past.
	client.mu.Lock()
	client.expiry = time.Now().Add(-1 * time.Minute)
	client.mu.Unlock()

	// Update mock to return new creds.
	mock.setCreds(creds2)

	// The next operation should trigger a refresh.
	// We can't actually call Upload/Download without a real S3,
	// but we can call ensureFresh directly.
	if err := client.ensureFresh(context.Background()); err != nil {
		t.Fatalf("ensureFresh error: %v", err)
	}

	if got := mock.loadCount.Load(); got != 1 {
		t.Fatalf("expected 1 LoadTable call after refresh, got %d", got)
	}

	// A second call should NOT trigger another refresh (creds are fresh now).
	if err := client.ensureFresh(context.Background()); err != nil {
		t.Fatalf("ensureFresh error: %v", err)
	}
	if got := mock.loadCount.Load(); got != 1 {
		t.Fatalf("expected still 1 LoadTable call, got %d", got)
	}
}

func TestVendedS3Client_ConcurrentRefresh(t *testing.T) {
	creds := &VendedCreds{
		AccessKeyID:    "AKID1",
		SecretAccessKey: "SECRET1",
		SessionToken:   "TOKEN1",
		Region:         "us-east-1",
	}

	mock := &mockCatalogForVended{
		creds:    creds,
		location: "s3://test-bucket/warehouse/ns.db/table",
	}

	client, err := NewVendedS3Client(mock, "ns", "table", creds, "s3://test-bucket/warehouse/ns.db/table")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Force expiry.
	client.mu.Lock()
	client.expiry = time.Now().Add(-1 * time.Minute)
	client.mu.Unlock()

	// Launch concurrent refreshes.
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := client.ensureFresh(context.Background()); err != nil {
				t.Errorf("ensureFresh error: %v", err)
			}
		}()
	}
	wg.Wait()

	// Should only have refreshed once due to double-check locking.
	if got := mock.loadCount.Load(); got != 1 {
		t.Fatalf("expected 1 LoadTable call with concurrent access, got %d", got)
	}
}

func TestVendedCredentials_ParseConfig(t *testing.T) {
	tm := &TableMetadata{
		Config: map[string]string{
			"s3.access-key-id":     "AKID",
			"s3.secret-access-key": "SECRET",
			"s3.session-token":     "TOKEN",
			"s3.region":            "eu-west-1",
			"s3.endpoint":          "https://s3.example.com",
		},
	}

	creds := tm.VendedCredentials()
	if creds == nil {
		t.Fatal("expected non-nil creds")
	}
	if creds.AccessKeyID != "AKID" {
		t.Errorf("AccessKeyID = %q", creds.AccessKeyID)
	}
	if creds.SecretAccessKey != "SECRET" {
		t.Errorf("SecretAccessKey = %q", creds.SecretAccessKey)
	}
	if creds.SessionToken != "TOKEN" {
		t.Errorf("SessionToken = %q", creds.SessionToken)
	}
	if creds.Region != "eu-west-1" {
		t.Errorf("Region = %q", creds.Region)
	}
	if creds.Endpoint != "https://s3.example.com" {
		t.Errorf("Endpoint = %q", creds.Endpoint)
	}
}

func TestVendedCredentials_NilConfig(t *testing.T) {
	tm := &TableMetadata{}
	if creds := tm.VendedCredentials(); creds != nil {
		t.Fatal("expected nil creds for empty config")
	}

	if creds := (*TableMetadata)(nil).VendedCredentials(); creds != nil {
		t.Fatal("expected nil creds for nil TableMetadata")
	}
}

func TestVendedCredentials_MissingKeys(t *testing.T) {
	tm := &TableMetadata{
		Config: map[string]string{
			"s3.access-key-id": "AKID",
			// missing secret key
		},
	}
	if creds := tm.VendedCredentials(); creds != nil {
		t.Fatal("expected nil creds when secret key is missing")
	}
}
