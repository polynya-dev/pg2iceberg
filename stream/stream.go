package stream

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/pg2iceberg/pg2iceberg/iceberg"
	"golang.org/x/sync/errgroup"
)

// Stream is an append-only distributed log for staging WAL change events.
// It combines S3 storage (for Parquet data) with a Coordinator (for atomic
// offset assignment and indexing). Replaces the Iceberg events table.
//
// Writers call Append to stage files. Readers call Read + Download to consume.
// No Iceberg catalog operations are on the write path.
type Stream struct {
	coord     Coordinator
	s3        iceberg.ObjectStorage
	namespace string // Iceberg namespace (e.g. "rideshare")
}

// NewStream creates a Stream backed by the given coordinator and S3 client.
func NewStream(coord Coordinator, s3 iceberg.ObjectStorage, namespace string) *Stream {
	return &Stream{coord: coord, s3: s3, namespace: namespace}
}

// Coordinator returns the underlying coordinator for direct cursor/lock access.
func (s *Stream) Coordinator() Coordinator { return s.coord }

// WriteBatch is a Parquet file chunk to stage in S3.
type WriteBatch struct {
	Table       string // PG table name (e.g. "public.orders")
	Data        []byte // serialized Parquet bytes
	RecordCount int    // number of change events
}

// Append stages Parquet files to S3 and atomically registers them in the
// log index. The ordering guarantees safety:
//  1. Upload files to S3 (if this fails, no coordination state is created)
//  2. ClaimOffsets on coordinator (single PG transaction for all files)
//
// If step 2 fails, orphan S3 files are left behind but never indexed.
// A periodic GC can clean these up.
func (s *Stream) Append(ctx context.Context, batches []WriteBatch) error {
	if len(batches) == 0 {
		return nil
	}

	// Step 1: Upload all files to S3 in parallel.
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
			key := fmt.Sprintf("staged/%s/%s.parquet", b.Table, id.String())
			if _, err := s.s3.Upload(gctx, key, b.Data); err != nil {
				return fmt.Errorf("upload staged file for %s: %w", b.Table, err)
			}
			files[i] = staged{key: key, byteSize: int64(len(b.Data))}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// Step 2: Atomically register all files in the log index.
	appends := make([]LogAppend, len(batches))
	for i, b := range batches {
		appends[i] = LogAppend{
			Table:       b.Table,
			S3Path:      files[i].key,
			RecordCount: b.RecordCount,
			ByteSize:    files[i].byteSize,
		}
	}

	if _, err := s.coord.ClaimOffsets(ctx, appends); err != nil {
		return fmt.Errorf("claim offsets: %w", err)
	}
	return nil
}

// Read returns log entries after the given offset for a table.
// Use Coordinator().GetCursor() to obtain the current offset.
func (s *Stream) Read(ctx context.Context, table string, afterOffset int64) ([]LogEntry, error) {
	return s.coord.ReadLog(ctx, table, afterOffset)
}

// Download fetches a staged Parquet file from S3.
func (s *Stream) Download(ctx context.Context, s3Path string) ([]byte, error) {
	return s.s3.Download(ctx, s3Path)
}

// Truncate removes processed log entries and deletes their S3 files.
func (s *Stream) Truncate(ctx context.Context, table string, atOrBeforeOffset int64) error {
	paths, err := s.coord.TruncateLog(ctx, table, atOrBeforeOffset)
	if err != nil {
		return err
	}
	if len(paths) > 0 {
		if err := s.s3.DeleteObjects(ctx, paths); err != nil {
			return fmt.Errorf("delete staged files: %w", err)
		}
	}
	return nil
}
