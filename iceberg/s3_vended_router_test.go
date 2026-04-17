package iceberg

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// scopedStorage is an in-memory ObjectStorage that only allows access to keys
// under its prefix. Operations on keys outside the prefix return AccessDenied,
// simulating how STS session policies scope vended credentials per table.
type scopedStorage struct {
	prefix string

	mu   sync.Mutex
	data map[string][]byte
}

func newScopedStorage(prefix string) *scopedStorage {
	return &scopedStorage{prefix: prefix, data: make(map[string][]byte)}
}

func (s *scopedStorage) check(key string) error {
	if !strings.HasPrefix(key, s.prefix) {
		return fmt.Errorf("AccessDenied: key %q is outside scoped prefix %q", key, s.prefix)
	}
	return nil
}

func (s *scopedStorage) Upload(_ context.Context, key string, data []byte) (string, error) {
	if err := s.check(key); err != nil {
		return "", err
	}
	s.mu.Lock()
	s.data[key] = append([]byte(nil), data...)
	s.mu.Unlock()
	return "s3://bucket/" + key, nil
}

func (s *scopedStorage) Download(_ context.Context, key string) ([]byte, error) {
	if err := s.check(key); err != nil {
		return nil, err
	}
	s.mu.Lock()
	d, ok := s.data[key]
	s.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("not found: %s", key)
	}
	return d, nil
}

func (s *scopedStorage) DownloadRange(_ context.Context, key string, offset, length int64) ([]byte, error) {
	if err := s.check(key); err != nil {
		return nil, err
	}
	s.mu.Lock()
	d := s.data[key]
	s.mu.Unlock()
	if d == nil {
		return nil, fmt.Errorf("not found: %s", key)
	}
	end := offset + length
	if end > int64(len(d)) {
		end = int64(len(d))
	}
	return d[offset:end], nil
}

func (s *scopedStorage) StatObject(_ context.Context, key string) (int64, error) {
	if err := s.check(key); err != nil {
		return 0, err
	}
	s.mu.Lock()
	d, ok := s.data[key]
	s.mu.Unlock()
	if !ok {
		return 0, fmt.Errorf("not found: %s", key)
	}
	return int64(len(d)), nil
}

func (s *scopedStorage) URIForKey(key string) string {
	return "s3://bucket/" + key
}

func (s *scopedStorage) ListObjects(_ context.Context, prefix string) ([]ObjectInfo, error) {
	if err := s.check(prefix); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []ObjectInfo
	for k, v := range s.data {
		if strings.HasPrefix(k, prefix) {
			out = append(out, ObjectInfo{Key: k, Size: int64(len(v))})
		}
	}
	return out, nil
}

func (s *scopedStorage) DeleteObjects(_ context.Context, keys []string) error {
	for _, k := range keys {
		if err := s.check(k); err != nil {
			return err
		}
	}
	s.mu.Lock()
	for _, k := range keys {
		delete(s.data, k)
	}
	s.mu.Unlock()
	return nil
}

func TestVendedS3Router_RoutesToCorrectTable(t *testing.T) {
	ctx := context.Background()

	ordersStore := newScopedStorage("warehouse/ns-uuid/orders-uuid/")
	paymentsStore := newScopedStorage("warehouse/ns-uuid/payments-uuid/")
	usersStore := newScopedStorage("warehouse/ns-uuid/users-uuid/")

	router := NewVendedS3Router()
	router.Add("warehouse/ns-uuid/orders-uuid/", ordersStore)
	router.Add("warehouse/ns-uuid/payments-uuid/", paymentsStore)
	router.Add("warehouse/ns-uuid/users-uuid/", usersStore)

	// Write to each table's data and staged paths.
	paths := map[string][]byte{
		"warehouse/ns-uuid/orders-uuid/data/snap-0.parquet":      []byte("orders-snap"),
		"warehouse/ns-uuid/orders-uuid/staged/001.parquet":       []byte("orders-staged"),
		"warehouse/ns-uuid/payments-uuid/data/snap-0.parquet":    []byte("payments-snap"),
		"warehouse/ns-uuid/payments-uuid/staged/001.parquet":     []byte("payments-staged"),
		"warehouse/ns-uuid/users-uuid/data/snap-0.parquet":       []byte("users-snap"),
		"warehouse/ns-uuid/users-uuid/metadata/00000.metadata":   []byte("users-meta"),
	}

	for key, data := range paths {
		if _, err := router.Upload(ctx, key, data); err != nil {
			t.Fatalf("Upload(%s): %v", key, err)
		}
	}

	// Read back and verify data.
	for key, want := range paths {
		got, err := router.Download(ctx, key)
		if err != nil {
			t.Fatalf("Download(%s): %v", key, err)
		}
		if string(got) != string(want) {
			t.Errorf("Download(%s) = %q, want %q", key, got, want)
		}
	}

	// Verify each scoped store only has its own keys.
	if len(ordersStore.data) != 2 {
		t.Errorf("ordersStore has %d keys, want 2", len(ordersStore.data))
	}
	if len(paymentsStore.data) != 2 {
		t.Errorf("paymentsStore has %d keys, want 2", len(paymentsStore.data))
	}
	if len(usersStore.data) != 2 {
		t.Errorf("usersStore has %d keys, want 2", len(usersStore.data))
	}
}

func TestVendedS3Router_ScopedStorageRejectsWrongPrefix(t *testing.T) {
	ctx := context.Background()

	// Verify the scoped mock actually rejects cross-table writes.
	ordersStore := newScopedStorage("warehouse/ns-uuid/orders-uuid/")
	_, err := ordersStore.Upload(ctx, "warehouse/ns-uuid/payments-uuid/data/file.parquet", []byte("nope"))
	if err == nil {
		t.Fatal("expected AccessDenied for cross-table write, got nil")
	}
	if !strings.Contains(err.Error(), "AccessDenied") {
		t.Fatalf("expected AccessDenied error, got: %v", err)
	}
}

func TestVendedS3Router_DeleteRoutesCorrectly(t *testing.T) {
	ctx := context.Background()

	ordersStore := newScopedStorage("warehouse/ns-uuid/orders-uuid/")
	paymentsStore := newScopedStorage("warehouse/ns-uuid/payments-uuid/")

	router := NewVendedS3Router()
	router.Add("warehouse/ns-uuid/orders-uuid/", ordersStore)
	router.Add("warehouse/ns-uuid/payments-uuid/", paymentsStore)

	// Upload files to both tables.
	router.Upload(ctx, "warehouse/ns-uuid/orders-uuid/staged/a.parquet", []byte("a"))
	router.Upload(ctx, "warehouse/ns-uuid/orders-uuid/staged/b.parquet", []byte("b"))
	router.Upload(ctx, "warehouse/ns-uuid/payments-uuid/staged/c.parquet", []byte("c"))

	// Delete orders staged files.
	err := router.DeleteObjects(ctx, []string{
		"warehouse/ns-uuid/orders-uuid/staged/a.parquet",
		"warehouse/ns-uuid/orders-uuid/staged/b.parquet",
	})
	if err != nil {
		t.Fatalf("DeleteObjects: %v", err)
	}

	if len(ordersStore.data) != 0 {
		t.Errorf("ordersStore has %d keys after delete, want 0", len(ordersStore.data))
	}
	if len(paymentsStore.data) != 1 {
		t.Errorf("paymentsStore has %d keys, want 1 (untouched)", len(paymentsStore.data))
	}
}

func TestVendedS3Router_ListObjectsScoped(t *testing.T) {
	ctx := context.Background()

	ordersStore := newScopedStorage("warehouse/ns-uuid/orders-uuid/")
	paymentsStore := newScopedStorage("warehouse/ns-uuid/payments-uuid/")

	router := NewVendedS3Router()
	router.Add("warehouse/ns-uuid/orders-uuid/", ordersStore)
	router.Add("warehouse/ns-uuid/payments-uuid/", paymentsStore)

	router.Upload(ctx, "warehouse/ns-uuid/orders-uuid/data/1.parquet", []byte("o1"))
	router.Upload(ctx, "warehouse/ns-uuid/orders-uuid/data/2.parquet", []byte("o2"))
	router.Upload(ctx, "warehouse/ns-uuid/payments-uuid/data/1.parquet", []byte("p1"))

	// List orders only — should not see payments.
	objects, err := router.ListObjects(ctx, "warehouse/ns-uuid/orders-uuid/")
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(objects) != 2 {
		t.Errorf("ListObjects returned %d objects, want 2", len(objects))
	}
}

func TestVendedS3Router_EmptyRouterErrors(t *testing.T) {
	ctx := context.Background()
	router := NewVendedS3Router()

	_, err := router.Upload(ctx, "any/key", []byte("data"))
	if err == nil {
		t.Fatal("expected error from empty router, got nil")
	}
}
