package iceberg

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	// Default credential TTL when the catalog doesn't provide an expiry.
	defaultVendedTTL = 55 * time.Minute
	// Refresh credentials this far before expiry.
	refreshBuffer = 5 * time.Minute
)

// VendedS3Client implements ObjectStorage using catalog-vended credentials.
// It refreshes credentials by calling LoadTable when they approach expiry.
type VendedS3Client struct {
	catalog   Catalog
	namespace string
	table     string // used for credential refresh via LoadTable

	mu     sync.RWMutex
	inner  *S3Client
	expiry time.Time
}

// NewVendedS3Client creates an ObjectStorage client using vended credentials.
// The bucket is extracted from tableLocation (e.g. "s3://bucket/path").
func NewVendedS3Client(catalog Catalog, ns, table string, creds *VendedCreds, tableLocation string) (*VendedS3Client, error) {
	inner, err := buildS3Client(creds, tableLocation)
	if err != nil {
		return nil, err
	}

	return &VendedS3Client{
		catalog:   catalog,
		namespace: ns,
		table:     table,
		inner:     inner,
		expiry:    time.Now().Add(defaultVendedTTL),
	}, nil
}

// buildS3Client creates an S3Client from vended credentials.
func buildS3Client(creds *VendedCreds, tableLocation string) (*S3Client, error) {
	u, err := url.Parse(tableLocation)
	if err != nil {
		return nil, fmt.Errorf("parse table location %q: %w", tableLocation, err)
	}
	bucket := u.Host

	region := creds.Region
	if region == "" {
		region = "us-east-1"
	}

	opts := s3.Options{
		Region:       region,
		Credentials:  credentials.NewStaticCredentialsProvider(creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken),
		UsePathStyle: true,
	}
	if creds.Endpoint != "" {
		opts.BaseEndpoint = &creds.Endpoint
	}

	return &S3Client{
		client: s3.New(opts),
		bucket: bucket,
	}, nil
}

// ensureFresh refreshes credentials if they are near expiry.
func (v *VendedS3Client) ensureFresh(ctx context.Context) error {
	v.mu.RLock()
	needsRefresh := time.Now().After(v.expiry.Add(-refreshBuffer))
	v.mu.RUnlock()

	if !needsRefresh {
		return nil
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	// Double-check after acquiring write lock.
	if !time.Now().After(v.expiry.Add(-refreshBuffer)) {
		return nil
	}

	tm, err := v.catalog.LoadTable(ctx, v.namespace, v.table)
	if err != nil {
		return fmt.Errorf("refresh vended credentials: %w", err)
	}

	creds := tm.VendedCredentials()
	if creds == nil {
		return fmt.Errorf("catalog did not return vended credentials on refresh")
	}

	inner, err := buildS3Client(creds, tm.Metadata.Location)
	if err != nil {
		return fmt.Errorf("rebuild s3 client on refresh: %w", err)
	}

	v.inner = inner
	v.expiry = time.Now().Add(defaultVendedTTL)
	log.Printf("[vended-s3] refreshed credentials for %s.%s", v.namespace, v.table)
	return nil
}

func (v *VendedS3Client) client(ctx context.Context) (*S3Client, error) {
	if err := v.ensureFresh(ctx); err != nil {
		return nil, err
	}
	v.mu.RLock()
	c := v.inner
	v.mu.RUnlock()
	return c, nil
}

func (v *VendedS3Client) Upload(ctx context.Context, key string, data []byte) (string, error) {
	c, err := v.client(ctx)
	if err != nil {
		return "", err
	}
	return c.Upload(ctx, key, data)
}

func (v *VendedS3Client) Download(ctx context.Context, key string) ([]byte, error) {
	c, err := v.client(ctx)
	if err != nil {
		return nil, err
	}
	return c.Download(ctx, key)
}

func (v *VendedS3Client) DownloadRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	c, err := v.client(ctx)
	if err != nil {
		return nil, err
	}
	return c.DownloadRange(ctx, key, offset, length)
}

func (v *VendedS3Client) StatObject(ctx context.Context, key string) (int64, error) {
	c, err := v.client(ctx)
	if err != nil {
		return 0, err
	}
	return c.StatObject(ctx, key)
}
