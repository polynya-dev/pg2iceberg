package sink

import (
	"container/list"
	"strings"
	"unsafe"

	"github.com/pg2iceberg/pg2iceberg/schema"
)

// toastCache is a byte-bounded LRU cache shared across all tables.
// It stores only TOAST-able column values (text, bytea, json, jsonb, etc.)
// keyed by table name + PK.
//
// Not goroutine-safe — all access is on the single Sink goroutine.
type toastCache struct {
	maxBytes  int64
	usedBytes int64
	items     map[string]*list.Element // cacheKey -> list element
	order     *list.List               // front = most recently used
}

type toastCacheEntry struct {
	key    string
	values map[string]any // only TOAST-able columns
	bytes  int64
}

func newToastCache(maxBytes int64) *toastCache {
	return &toastCache{
		maxBytes: maxBytes,
		items:    make(map[string]*list.Element),
		order:    list.New(),
	}
}

// put inserts or updates a cache entry, extracting only TOAST-able columns.
func (c *toastCache) put(pgTable string, pkKey string, row map[string]any, columns []schema.Column) {
	key := toastCacheKey(pgTable, pkKey)

	vals, size := extractToastColumns(row, columns)
	if len(vals) == 0 {
		return // nothing TOAST-able to cache
	}

	// If already present, remove old entry first.
	if elem, ok := c.items[key]; ok {
		c.removeElement(elem)
	}

	// Evict LRU entries until we have room.
	for c.usedBytes+size > c.maxBytes && c.order.Len() > 0 {
		c.removeElement(c.order.Back())
	}

	entry := &toastCacheEntry{key: key, values: vals, bytes: size}
	elem := c.order.PushFront(entry)
	c.items[key] = elem
	c.usedBytes += size
}

// get returns cached TOAST-able column values and moves the entry to front.
func (c *toastCache) get(pgTable string, pkKey string) (map[string]any, bool) {
	key := toastCacheKey(pgTable, pkKey)
	elem, ok := c.items[key]
	if !ok {
		return nil, false
	}
	c.order.MoveToFront(elem)
	return elem.Value.(*toastCacheEntry).values, true
}

// delete removes an entry (e.g. on DELETE events).
func (c *toastCache) delete(pgTable string, pkKey string) {
	key := toastCacheKey(pgTable, pkKey)
	if elem, ok := c.items[key]; ok {
		c.removeElement(elem)
	}
}

func (c *toastCache) removeElement(elem *list.Element) {
	entry := c.order.Remove(elem).(*toastCacheEntry)
	delete(c.items, entry.key)
	c.usedBytes -= entry.bytes
}

func toastCacheKey(pgTable, pkKey string) string {
	return pgTable + "\x00" + pkKey
}

// isToastable returns true for PG types that can be stored out-of-line via TOAST.
func isToastable(pgType string) bool {
	switch strings.ToLower(pgType) {
	case "text", "varchar", "character varying", "bpchar", "char", "character",
		"bytea", "jsonb", "json", "xml", "name":
		return true
	}
	return false
}

// extractToastColumns copies only TOAST-able column values from a row
// and estimates the byte size of the cached entry.
func extractToastColumns(row map[string]any, columns []schema.Column) (map[string]any, int64) {
	var vals map[string]any
	var size int64

	for _, col := range columns {
		if !isToastable(col.PGType) {
			continue
		}
		v, ok := row[col.Name]
		if !ok || v == nil {
			continue
		}
		if vals == nil {
			vals = make(map[string]any, 4)
		}
		vals[col.Name] = v
		size += estimateValueBytes(col.Name, v)
	}

	if vals != nil {
		// Fixed overhead: map header + cache key + list element.
		size += 128
	}
	return vals, size
}

// estimateValueBytes estimates the memory footprint of a single cached value.
func estimateValueBytes(key string, v any) int64 {
	// Map entry overhead (key string header + value interface + hash bucket).
	n := int64(len(key)) + 64

	switch x := v.(type) {
	case string:
		n += int64(len(x)) + int64(unsafe.Sizeof(x))
	case []byte:
		n += int64(len(x)) + int64(unsafe.Sizeof(x))
	default:
		n += 32 // small scalar fallback
	}
	return n
}
