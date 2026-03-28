package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	mode := flag.String("mode", "stream", "Mode: init (create schema), seed (bulk insert), or stream (sustained writes)")
	seedRows := flag.Int("seed-rows", 0, "Number of rows to seed (seed mode, overrides --seed-size)")
	seedSize := flag.String("seed-size", "", "Target data size to seed, e.g. 10GB, 1GB, 500MB (seed mode)")
	rate := flag.Int("rate", 1000, "Target rows/sec (stream mode)")
	duration := flag.Duration("duration", 15*time.Minute, "How long to stream (stream mode)")
	insertPct := flag.Int("insert-pct", 70, "Percentage of inserts (stream mode)")
	updatePct := flag.Int("update-pct", 20, "Percentage of updates (stream mode)")
	largeTextPct := flag.Int("large-text-pct", 0, "Percentage of rows with 4KB+ large_text (TOAST)")
	batchSize := flag.Int("batch-size", 100, "Rows per batch insert")
	concurrency := flag.Int("concurrency", 4, "Number of parallel workers (seed mode)")
	flag.Parse()

	if os.Getenv("PPROF") != "" {
		go func() {
			log.Println("pprof listening on :6060")
			http.ListenAndServe(":6060", nil)
		}()
	}

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is required")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		cancel()
	}()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	writerID := uuid.New()
	log.Printf("bench-writer id=%s mode=%s", writerID, *mode)

	switch *mode {
	case "init":
		if err := runInit(ctx, pool); err != nil {
			log.Fatalf("init: %v", err)
		}
		return
	case "seed":
		targetBytes := int64(0)
		if *seedSize != "" {
			var err error
			targetBytes, err = parseSize(*seedSize)
			if err != nil {
				log.Fatalf("invalid --seed-size %q: %v", *seedSize, err)
			}
		}
		if *seedRows <= 0 && targetBytes <= 0 {
			log.Fatal("--seed-rows or --seed-size is required for seed mode")
		}
		if err := runSeed(ctx, pool, writerID, *seedRows, targetBytes, *batchSize, *largeTextPct, *concurrency); err != nil {
			log.Fatalf("seed: %v", err)
		}
	case "stream":
		if err := runStream(ctx, pool, writerID, *rate, *duration, *insertPct, *updatePct, *batchSize, *largeTextPct); err != nil && ctx.Err() == nil {
			log.Fatalf("stream: %v", err)
		}
	default:
		log.Fatalf("unknown mode: %s", *mode)
	}
}

const initSQL = `
CREATE TABLE IF NOT EXISTS bench_events (
    id          BIGSERIAL PRIMARY KEY,
    seq         BIGINT NOT NULL,
    writer_id   UUID NOT NULL,
    op_type     TEXT NOT NULL,
    payload     JSONB NOT NULL,
    large_text  TEXT,
    value       INTEGER NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE bench_events ALTER COLUMN large_text SET STORAGE EXTERNAL;

CREATE INDEX IF NOT EXISTS idx_bench_events_seq ON bench_events (seq);

CREATE PUBLICATION pg2iceberg_bench FOR TABLE bench_events;

CREATE TABLE IF NOT EXISTS bench_operations (
    id          BIGSERIAL PRIMARY KEY,
    op          TEXT NOT NULL,
    seq         BIGINT NOT NULL,
    value       INTEGER,
    writer_id   UUID NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
`

func runInit(ctx context.Context, pool *pgxpool.Pool) error {
	log.Println("Initializing schema...")
	_, err := pool.Exec(ctx, initSQL)
	if err != nil {
		return fmt.Errorf("exec schema: %w", err)
	}
	log.Println("Schema initialized.")
	return nil
}

// runSeed bulk-inserts rows using concurrent COPY workers.
// If totalRows > 0 it takes precedence; otherwise targetBytes is used.
func runSeed(ctx context.Context, pool *pgxpool.Pool, writerID uuid.UUID, totalRows int, targetBytes int64, batchSize, largeTextPct, workers int) error {
	sizeMode := totalRows <= 0 && targetBytes > 0
	if workers < 1 {
		workers = 1
	}
	if sizeMode {
		log.Printf("seeding ~%s of data (batch=%d, workers=%d)", formatSize(targetBytes), batchSize, workers)
	} else {
		log.Printf("seeding %d rows (batch=%d, workers=%d)", totalRows, batchSize, workers)
	}
	start := time.Now()

	// Shared counters across workers.
	var totalInserted atomic.Int64
	var totalBytes atomic.Int64
	// nextSeq is the next seq to claim; workers atomically grab ranges.
	var nextSeq atomic.Int64
	nextSeq.Store(1)

	// For row-count mode, compute per-worker share.
	// For size mode, workers run until totalBytes reaches the target.

	var wg sync.WaitGroup
	errCh := make(chan error, workers)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			conn, err := pool.Acquire(ctx)
			if err != nil {
				errCh <- fmt.Errorf("worker %d acquire conn: %w", workerID, err)
				return
			}
			defer conn.Release()

			// Per-worker reusable state.
			var buf bytes.Buffer
			buf.Grow(batchSize * 150)
			itoa := make([]byte, 0, 20)
			// Pre-build the static middle of each row: \t<uuid>\tinsert\t<payload>\t
			mid := "\t" + writerID.String() + "\tinsert\t" + staticPayload + "\t"
			// Pre-build the tail for NULL large_text: \N\t
			nullTail := `\N` + "\t"

			for {
				if ctx.Err() != nil {
					return
				}
				if sizeMode {
					if totalBytes.Load() >= targetBytes {
						return
					}
				} else {
					if totalInserted.Load() >= int64(totalRows) {
						return
					}
				}

				// Claim a range of seq values.
				seqStart := nextSeq.Add(int64(batchSize)) - int64(batchSize)

				n := batchSize
				if !sizeMode {
					remaining := int64(totalRows) - (seqStart - 1)
					if remaining <= 0 {
						return
					}
					if int64(n) > remaining {
						n = int(remaining)
					}
				}

				buf.Reset()
				var batchBytes int64
				for i := 0; i < n; i++ {
					seq := int64(seqStart) + int64(i)
					itoa = strconv.AppendInt(itoa[:0], seq, 10)
					buf.Write(itoa)       // seq
					buf.WriteString(mid)  // \t<uuid>\tinsert\t<payload>\t
					if largeTextPct > 0 && rand.Intn(100) < largeTextPct {
						buf.WriteString(staticLargeText)
						buf.WriteByte('\t')
						batchBytes += int64(len(staticLargeText))
					} else {
						buf.WriteString(nullTail) // \N\t
					}
					buf.Write(itoa) // value (= seq)
					buf.WriteByte('\n')
					batchBytes += int64(48 + len(staticPayload))
				}

				_, err := conn.Conn().PgConn().CopyFrom(ctx,
					&buf,
					"COPY bench_events (seq, writer_id, op_type, payload, large_text, value) FROM STDIN",
				)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					errCh <- fmt.Errorf("worker %d copy at seq %d: %w", workerID, seqStart, err)
					return
				}

				totalInserted.Add(int64(n))
				totalBytes.Add(batchBytes)
			}
		}(w)
	}

	// Progress reporter.
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				ins := totalInserted.Load()
				bw := totalBytes.Load()
				elapsed := time.Since(start).Seconds()
				if sizeMode {
					log.Printf("seed progress: %d rows, %s / %s (%.0f rows/s)",
						ins, formatSize(bw), formatSize(targetBytes), float64(ins)/elapsed)
				} else {
					log.Printf("seed progress: %d/%d (%.0f rows/s)",
						ins, totalRows, float64(ins)/elapsed)
				}
			}
		}
	}()

	wg.Wait()
	close(done)
	close(errCh)

	// Check for worker errors.
	for err := range errCh {
		if err != nil {
			return err
		}
	}

	seedElapsed := time.Since(start)
	ins := totalInserted.Load()
	bw := totalBytes.Load()
	log.Printf("seed insert complete: %d rows, %s in %s (%.0f rows/s)",
		ins, formatSize(bw), seedElapsed, float64(ins)/seedElapsed.Seconds())

	elapsed := time.Since(start)
	log.Printf("seed complete: %d rows, %s in %s (%.0f rows/s)",
		ins, formatSize(bw), elapsed, float64(ins)/elapsed.Seconds())
	return nil
}

// runStream writes at a sustained rate with a configurable mix of insert/update/delete.
func runStream(ctx context.Context, pool *pgxpool.Pool, writerID uuid.UUID, targetRate int, dur time.Duration, insertPct, updatePct, batchSize, largeTextPct int) error {
	deletePct := 100 - insertPct - updatePct
	if deletePct < 0 {
		deletePct = 0
	}
	log.Printf("streaming at %d rows/s for %s (insert=%d%% update=%d%% delete=%d%% toast=%d%%)",
		targetRate, dur, insertPct, updatePct, deletePct, largeTextPct)

	ctx, cancel := context.WithTimeout(ctx, dur)
	defer cancel()

	// Find the current max seq to continue from.
	var maxSeq int64
	if err := pool.QueryRow(ctx, "SELECT COALESCE(MAX(seq), 0) FROM bench_events").Scan(&maxSeq); err != nil {
		return fmt.Errorf("get max seq: %w", err)
	}
	nextSeq := maxSeq + 1

	// Track inserted seqs that are eligible for update/delete.
	// We use a simple ring buffer of recent seq values.
	liveSeqs := make([]int64, 0, 100000)

	var opsInsert, opsUpdate, opsDelete atomic.Int64
	start := time.Now()

	// Stats ticker.
	statsTicker := time.NewTicker(10 * time.Second)
	defer statsTicker.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-statsTicker.C:
				elapsed := time.Since(start).Seconds()
				ins := opsInsert.Load()
				upd := opsUpdate.Load()
				del := opsDelete.Load()
				total := ins + upd + del
				log.Printf("stream stats: total=%d (%.0f/s) insert=%d update=%d delete=%d",
					total, float64(total)/elapsed, ins, upd, del)
			}
		}
	}()

	// Rate limiting: send batches at intervals.
	batchInterval := time.Duration(float64(time.Second) * float64(batchSize) / float64(targetRate))
	if batchInterval < time.Millisecond {
		batchInterval = time.Millisecond
	}
	ticker := time.NewTicker(batchInterval)
	defer ticker.Stop()

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()

	for {
		select {
		case <-ctx.Done():
			elapsed := time.Since(start)
			ins := opsInsert.Load()
			upd := opsUpdate.Load()
			del := opsDelete.Load()
			total := ins + upd + del
			log.Printf("stream complete: %d ops in %s (%.0f ops/s) insert=%d update=%d delete=%d",
				total, elapsed, float64(total)/elapsed.Seconds(), ins, upd, del)
			return nil
		case <-ticker.C:
		}

		// Execute a batch of mixed operations.
		tx, err := conn.Begin(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("begin tx: %w", err)
		}

		for i := 0; i < batchSize; i++ {
			roll := rand.Intn(100)
			switch {
			case roll < insertPct:
				// INSERT
				seq := nextSeq
				nextSeq++
				payload := makePayload(int(seq))
				var largeText *string
				if largeTextPct > 0 && rand.Intn(100) < largeTextPct {
					s := makeLargeText()
					largeText = &s
				}
				now := time.Now()
				_, err := tx.Exec(ctx,
					"INSERT INTO bench_events (seq, writer_id, op_type, payload, large_text, value, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
					seq, writerID, "insert", payload, largeText, seq, now, now)
				if err != nil {
					tx.Rollback(ctx)
					if ctx.Err() != nil {
						return nil
					}
					return fmt.Errorf("insert seq=%d: %w", seq, err)
				}
				_, err = tx.Exec(ctx,
					"INSERT INTO bench_operations (op, seq, value, writer_id) VALUES ($1,$2,$3,$4)",
					"insert", seq, seq, writerID)
				if err != nil {
					tx.Rollback(ctx)
					if ctx.Err() != nil {
						return nil
					}
					return fmt.Errorf("log insert: %w", err)
				}
				liveSeqs = append(liveSeqs, seq)
				opsInsert.Add(1)

			case roll < insertPct+updatePct && len(liveSeqs) > 0:
				// UPDATE
				idx := rand.Intn(len(liveSeqs))
				seq := liveSeqs[idx]
				newVal := rand.Intn(1000000)
				now := time.Now()
				_, err := tx.Exec(ctx,
					"UPDATE bench_events SET value = $1, updated_at = $2 WHERE seq = $3 AND writer_id = $4",
					newVal, now, seq, writerID)
				if err != nil {
					tx.Rollback(ctx)
					if ctx.Err() != nil {
						return nil
					}
					return fmt.Errorf("update seq=%d: %w", seq, err)
				}
				_, err = tx.Exec(ctx,
					"INSERT INTO bench_operations (op, seq, value, writer_id) VALUES ($1,$2,$3,$4)",
					"update", seq, newVal, writerID)
				if err != nil {
					tx.Rollback(ctx)
					if ctx.Err() != nil {
						return nil
					}
					return fmt.Errorf("log update: %w", err)
				}
				opsUpdate.Add(1)

			default:
				if len(liveSeqs) > 0 {
					// DELETE
					idx := rand.Intn(len(liveSeqs))
					seq := liveSeqs[idx]
					// Remove from liveSeqs.
					liveSeqs[idx] = liveSeqs[len(liveSeqs)-1]
					liveSeqs = liveSeqs[:len(liveSeqs)-1]

					_, err := tx.Exec(ctx,
						"DELETE FROM bench_events WHERE seq = $1 AND writer_id = $2",
						seq, writerID)
					if err != nil {
						tx.Rollback(ctx)
						if ctx.Err() != nil {
							return nil
						}
						return fmt.Errorf("delete seq=%d: %w", seq, err)
					}
					_, err = tx.Exec(ctx,
						"INSERT INTO bench_operations (op, seq, value, writer_id) VALUES ($1,$2,$3,$4)",
						"delete", seq, nil, writerID)
					if err != nil {
						tx.Rollback(ctx)
						if ctx.Err() != nil {
							return nil
						}
						return fmt.Errorf("log delete: %w", err)
					}
					opsDelete.Add(1)
				} else {
					// No rows to delete yet, do an insert instead.
					seq := nextSeq
					nextSeq++
					payload := makePayload(int(seq))
					now := time.Now()
					_, err := tx.Exec(ctx,
						"INSERT INTO bench_events (seq, writer_id, op_type, payload, large_text, value, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
						seq, writerID, "insert", payload, nil, seq, now, now)
					if err != nil {
						tx.Rollback(ctx)
						if ctx.Err() != nil {
							return nil
						}
						return fmt.Errorf("insert seq=%d: %w", seq, err)
					}
					_, err = tx.Exec(ctx,
						"INSERT INTO bench_operations (op, seq, value, writer_id) VALUES ($1,$2,$3,$4)",
						"insert", seq, seq, writerID)
					if err != nil {
						tx.Rollback(ctx)
						return fmt.Errorf("log insert: %w", err)
					}
					liveSeqs = append(liveSeqs, seq)
					opsInsert.Add(1)
				}
			}
		}

		if err := tx.Commit(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("commit: %w", err)
		}
	}
}

// logOperationsBatch logs a range of insert operations for seed mode.
func logOperationsBatch(ctx context.Context, conn *pgx.Conn, writerID uuid.UUID, op string, startSeq, count int) error {
	rows := make([][]any, count)
	for i := 0; i < count; i++ {
		seq := startSeq + i
		rows[i] = []any{op, int64(seq), seq, writerID}
	}
	_, err := conn.CopyFrom(ctx,
		pgx.Identifier{"bench_operations"},
		[]string{"op", "seq", "value", "writer_id"},
		pgx.CopyFromRows(rows),
	)
	return err
}

// staticPayload is a fixed JSONB value used for seeding. Avoids per-row
// formatting overhead while still exercising the JSONB code path.
var staticPayload = `{"bench":true,"region":"us-east-1","tags":["benchmark"]}`

func makePayload(seq int) string {
	return staticPayload
}

// parseSize parses a human-readable size string like "10GB", "500MB", "1TB".
func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	multipliers := []struct {
		suffix string
		mult   int64
	}{
		{"TB", 1024 * 1024 * 1024 * 1024},
		{"GB", 1024 * 1024 * 1024},
		{"MB", 1024 * 1024},
		{"KB", 1024},
		{"B", 1},
	}
	for _, m := range multipliers {
		if strings.HasSuffix(s, m.suffix) {
			numStr := strings.TrimSuffix(s, m.suffix)
			var n float64
			if _, err := fmt.Sscanf(numStr, "%f", &n); err != nil {
				return 0, fmt.Errorf("invalid number %q", numStr)
			}
			return int64(n * float64(m.mult)), nil
		}
	}
	// Try plain number as bytes.
	var n int64
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		return 0, fmt.Errorf("unrecognized size format %q (use e.g. 10GB, 500MB)", s)
	}
	return n, nil
}

func formatSize(b int64) string {
	switch {
	case b >= 1024*1024*1024:
		return fmt.Sprintf("%.1fGB", float64(b)/(1024*1024*1024))
	case b >= 1024*1024:
		return fmt.Sprintf("%.1fMB", float64(b)/(1024*1024))
	case b >= 1024:
		return fmt.Sprintf("%.1fKB", float64(b)/1024)
	default:
		return fmt.Sprintf("%dB", b)
	}
}

// staticLargeText is a pre-generated 8KB string to trigger TOAST storage.
var staticLargeText = strings.Repeat("abcdefghijklmnopqrstuvwxyz", 320)[:8192]

func makeLargeText() string {
	return staticLargeText
}
