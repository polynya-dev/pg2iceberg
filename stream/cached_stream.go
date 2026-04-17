package stream

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/pg2iceberg/pg2iceberg/iceberg"
	"github.com/pg2iceberg/pg2iceberg/pipeline"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

// CachedStream wraps a Stream with an in-memory cache of pre-parsed events.
// In combined mode (single process running both WAL writer and materializer),
// the materializer reads events directly from memory — skipping S3 download,
// Parquet parsing, and JSON decoding entirely.
//
// On cold start (restart/recovery), the cache is empty and Download falls back
// to S3 + Parquet + JSON decode — this is the correct recovery behavior.
type CachedStream struct {
	coord     Coordinator
	s3        iceberg.ObjectStorage
	namespace string

	mu         sync.Mutex
	eventCache map[string]any    // s3Key -> pre-parsed events ([]MatEvent from logical pkg)
	dataCache  map[string][]byte // s3Key -> raw Parquet bytes (fallback for Download)
}

// NewCachedStream creates a Stream with an in-memory event cache.
func NewCachedStream(coord Coordinator, s3 iceberg.ObjectStorage, namespace string) *CachedStream {
	return &CachedStream{
		coord:      coord,
		s3:         s3,
		namespace:  namespace,
		eventCache: make(map[string]any),
		dataCache:  make(map[string][]byte),
	}
}

// Coordinator returns the underlying coordinator for direct cursor/lock access.
func (cs *CachedStream) Coordinator() Coordinator { return cs.coord }

// Append stages Parquet files to S3, atomically registers them in the log
// index, and caches the pre-parsed events in memory.
func (cs *CachedStream) Append(ctx context.Context, batches []WriteBatch) error {
	if len(batches) == 0 {
		return nil
	}

	ctx, span := streamTracer.Start(ctx, "stream.Append",
		trace.WithAttributes(
			attribute.Int("stream.batch_count", len(batches)),
			attribute.Bool("stream.cached", true),
		))
	defer span.End()

	type staged struct {
		key      string
		byteSize int64
	}
	files := make([]staged, len(batches))

	g, gctx := errgroup.WithContext(ctx)
	for i, b := range batches {
		i, b := i, b
		g.Go(func() error {
			id, err := uuid.NewV7()
			if err != nil {
				return fmt.Errorf("generate uuid: %w", err)
			}
			key := stagingKey(b.BasePath, b.Table, id.String())
			if _, err := cs.s3.Upload(gctx, key, b.Data); err != nil {
				return fmt.Errorf("upload staged file for %s: %w", b.Table, err)
			}
			files[i] = staged{key: key, byteSize: int64(len(b.Data))}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	appends := make([]LogAppend, len(batches))
	for i, b := range batches {
		appends[i] = LogAppend{
			Table:       b.Table,
			S3Path:      files[i].key,
			RecordCount: b.RecordCount,
			ByteSize:    files[i].byteSize,
		}
	}

	if _, err := cs.coord.ClaimOffsets(ctx, appends); err != nil {
		return fmt.Errorf("claim offsets: %w", err)
	}

	// Record staging metrics.
	for i, b := range batches {
		pipeline.StreamStagedFilesTotal.WithLabelValues(b.Table).Inc()
		pipeline.StreamStagedBytesTotal.WithLabelValues(b.Table).Add(float64(files[i].byteSize))
	}

	// Cache pre-parsed events and raw data for the co-located materializer.
	cs.mu.Lock()
	for i, b := range batches {
		key := files[i].key
		if b.Events != nil {
			cs.eventCache[key] = b.Events
		}
		// Also cache raw bytes as fallback for Download.
		cp := make([]byte, len(b.Data))
		copy(cp, b.Data)
		cs.dataCache[key] = cp
	}
	cs.mu.Unlock()

	return nil
}

// Read returns log entries after the given offset for a table.
func (cs *CachedStream) Read(ctx context.Context, table string, afterOffset int64) ([]LogEntry, error) {
	return cs.coord.ReadLog(ctx, table, afterOffset)
}

// CachedEvents returns pre-parsed events for a staged file, or nil on cache miss.
// The caller should type-assert the result to []MatEvent.
func (cs *CachedStream) CachedEvents(s3Path string) any {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.eventCache[s3Path]
}

// Download returns staged Parquet data from the in-memory cache if available,
// falling back to S3 on cache miss (cold start / recovery).
func (cs *CachedStream) Download(ctx context.Context, s3Path string) ([]byte, error) {
	cs.mu.Lock()
	data, ok := cs.dataCache[s3Path]
	cs.mu.Unlock()
	if ok {
		return data, nil
	}
	return cs.s3.Download(ctx, s3Path)
}

// Evict removes entries from both caches.
func (cs *CachedStream) Evict(paths []string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	for _, p := range paths {
		delete(cs.eventCache, p)
		delete(cs.dataCache, p)
	}
}

// Truncate removes processed log entries and deletes their S3 files.
func (cs *CachedStream) Truncate(ctx context.Context, table string, atOrBeforeOffset int64) error {
	paths, err := cs.coord.TruncateLog(ctx, table, atOrBeforeOffset)
	if err != nil {
		return err
	}
	cs.Evict(paths)
	if len(paths) > 0 {
		if err := cs.s3.DeleteObjects(ctx, paths); err != nil {
			return fmt.Errorf("delete staged files: %w", err)
		}
	}
	return nil
}
