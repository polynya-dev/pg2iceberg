package iceberg

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// VendedS3Router implements ObjectStorage by routing each operation to the
// correct per-table VendedS3Client. Vended credentials are scoped to a
// single table's S3 prefix, so a shared client can't serve multiple tables.
// The router matches each S3 key to the right client by longest prefix.
type VendedS3Router struct {
	mu      sync.RWMutex
	entries []routerEntry
}

type routerEntry struct {
	basePath string
	client   ObjectStorage
}

// NewVendedS3Router creates an empty router. Use Add to register tables.
func NewVendedS3Router() *VendedS3Router {
	return &VendedS3Router{}
}

// Add registers a per-table ObjectStorage client under its base path.
func (r *VendedS3Router) Add(basePath string, client ObjectStorage) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, routerEntry{basePath: basePath, client: client})
	// Sort longest prefix first for correct matching.
	sort.Slice(r.entries, func(i, j int) bool {
		return len(r.entries[i].basePath) > len(r.entries[j].basePath)
	})
}

// clientFor returns the ObjectStorage whose basePath is a prefix of key.
func (r *VendedS3Router) clientFor(key string) (ObjectStorage, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, e := range r.entries {
		if strings.HasPrefix(key, e.basePath) {
			return e.client, nil
		}
	}
	if len(r.entries) > 0 {
		// Fallback: use the first client (all share the same bucket).
		return r.entries[0].client, nil
	}
	return nil, fmt.Errorf("vended-s3-router: no client registered for key %q", key)
}

func (r *VendedS3Router) Upload(ctx context.Context, key string, data []byte) (string, error) {
	c, err := r.clientFor(key)
	if err != nil {
		return "", err
	}
	return c.Upload(ctx, key, data)
}

func (r *VendedS3Router) Download(ctx context.Context, key string) ([]byte, error) {
	c, err := r.clientFor(key)
	if err != nil {
		return nil, err
	}
	return c.Download(ctx, key)
}

func (r *VendedS3Router) DownloadRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	c, err := r.clientFor(key)
	if err != nil {
		return nil, err
	}
	return c.DownloadRange(ctx, key, offset, length)
}

func (r *VendedS3Router) StatObject(ctx context.Context, key string) (int64, error) {
	c, err := r.clientFor(key)
	if err != nil {
		return 0, err
	}
	return c.StatObject(ctx, key)
}

func (r *VendedS3Router) URIForKey(key string) string {
	c, err := r.clientFor(key)
	if err != nil {
		return ""
	}
	return c.URIForKey(key)
}

func (r *VendedS3Router) ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	c, err := r.clientFor(prefix)
	if err != nil {
		return nil, err
	}
	return c.ListObjects(ctx, prefix)
}

func (r *VendedS3Router) DeleteObjects(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	// All keys in a single DeleteObjects call belong to the same table.
	c, err := r.clientFor(keys[0])
	if err != nil {
		return err
	}
	return c.DeleteObjects(ctx, keys)
}
