package iceberg

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/pg2iceberg/pg2iceberg/config"
)

// IcebergClients holds the catalog and storage clients needed by all pipelines.
type IcebergClients struct {
	Catalog Catalog
	S3      ObjectStorage // nil in vended mode until EnsureStorage is called

	credentialMode string
	catalogClient  *CatalogClient // retained for vended credential refresh
	sinkCfg        config.SinkConfig

	// DiscoveredWarehouse is the warehouse URI obtained from GET /v1/config.
	// Used as a fallback when Warehouse is not set in user config.
	DiscoveredWarehouse string
}

// NewClients constructs catalog and S3 clients from config.
// In vended credential mode, S3 is nil until EnsureStorage is called.
func NewClients(cfg config.SinkConfig) (*IcebergClients, error) {
	var httpClient *http.Client
	switch cfg.CatalogAuth {
	case "sigv4":
		transport, err := NewSigV4Transport(cfg.S3Region)
		if err != nil {
			return nil, fmt.Errorf("create sigv4 transport: %w", err)
		}
		httpClient = &http.Client{Transport: transport}
	case "bearer":
		httpClient = &http.Client{Transport: NewBearerTransport(cfg.CatalogToken)}
	case "", "none":
		// No auth.
	default:
		return nil, fmt.Errorf("unsupported catalog_auth: %q", cfg.CatalogAuth)
	}

	catalogClient := NewCatalogClient(cfg.CatalogURI, httpClient)

	ic := &IcebergClients{
		Catalog:        catalogClient,
		credentialMode: cfg.CredentialMode,
		catalogClient:  catalogClient,
		sinkCfg:        cfg,
	}

	// Attempt catalog config discovery for warehouse, prefix, and other defaults.
	// For sigv4 (Glue), skip sending S3 URIs as the warehouse param — Glue
	// expects a catalog identifier, not a storage path.
	configWarehouse := cfg.Warehouse
	if cfg.CatalogAuth == "sigv4" && IsStorageURI(configWarehouse) {
		configWarehouse = ""
	}
	cc, err := catalogClient.GetConfig(configWarehouse)
	if err != nil {
		log.Printf("[iceberg] GET /v1/config failed (non-fatal): %v", err)
	} else if cc != nil {
		if wh, ok := cc.Defaults["warehouse"]; ok && cfg.Warehouse == "" {
			ic.DiscoveredWarehouse = wh
			log.Printf("[iceberg] discovered warehouse from catalog: %s", wh)
		}
		// Apply prefix override (required by some catalogs like Cloudflare R2).
		if prefix, ok := cc.Overrides["prefix"]; ok && prefix != "" {
			catalogClient.SetPrefix(prefix)
			log.Printf("[iceberg] using catalog prefix: %s", prefix)
		}
	}

	switch cfg.CredentialMode {
	case "vended":
		// S3 is initialized lazily via EnsureStorage after first LoadTable.
	case "iam":
		s3Client, err := NewIAMS3Client(context.Background(), cfg.S3Region, cfg.Warehouse)
		if err != nil {
			return nil, fmt.Errorf("create iam s3 client: %w", err)
		}
		ic.S3 = s3Client
	default: // static
		s3Client, err := NewS3Client(cfg.S3Endpoint, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3Region, cfg.Warehouse)
		if err != nil {
			return nil, fmt.Errorf("create s3 client: %w", err)
		}
		ic.S3 = s3Client
	}

	return ic, nil
}

// EnsureStorage ensures the S3 storage client is initialized.
// In static mode this is a no-op (already created in NewClients).
// In vended mode, it calls LoadTable to obtain temporary credentials
// from the catalog and creates a VendedS3Client.
func (ic *IcebergClients) EnsureStorage(ctx context.Context, ns, table string) error {
	if ic.S3 != nil {
		return nil
	}
	if ic.credentialMode != "vended" {
		return fmt.Errorf("S3 client not configured and credential mode is %q", ic.credentialMode)
	}

	tm, err := ic.catalogClient.LoadTable(ns, table)
	if err != nil {
		return fmt.Errorf("load table for vended credentials: %w", err)
	}
	if tm == nil {
		return fmt.Errorf("table %s.%s does not exist, cannot obtain vended credentials", ns, table)
	}

	creds := tm.VendedCredentials()
	if creds == nil {
		return fmt.Errorf("catalog did not return vended credentials for %s.%s", ns, table)
	}

	vendedClient, err := NewVendedS3Client(ic.catalogClient, ns, table, creds, tm.Metadata.Location)
	if err != nil {
		return fmt.Errorf("create vended s3 client: %w", err)
	}
	ic.S3 = vendedClient
	return nil
}

// IsStorageURI returns true if the string looks like a storage URI (s3://, gs://, etc.)
// that can be used as a location prefix. Returns false for catalog identifiers.
func IsStorageURI(s string) bool {
	return strings.HasPrefix(s, "s3://") || strings.HasPrefix(s, "s3a://") || strings.HasPrefix(s, "gs://")
}

// Warehouse returns the effective warehouse URI for building table locations.
// Returns empty string if the warehouse is not an S3-style URI (e.g. when it's
// just a catalog identifier like R2's warehouse name).
func (ic *IcebergClients) Warehouse() string {
	wh := ic.sinkCfg.Warehouse
	if wh == "" {
		wh = ic.DiscoveredWarehouse
	}
	if IsStorageURI(wh) {
		return wh
	}
	return ""
}

// WarehouseName returns the raw warehouse identifier for catalog config discovery.
func (ic *IcebergClients) WarehouseName() string {
	if ic.sinkCfg.Warehouse != "" {
		return ic.sinkCfg.Warehouse
	}
	return ic.DiscoveredWarehouse
}
