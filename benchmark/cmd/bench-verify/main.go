package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
)

const chunkWidth int64 = 100000

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		cancel()
	}()

	cfg := loadConfig()
	log.Printf("bench-verify: namespace=%s table=%s", cfg.Namespace, cfg.Table)

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("connect to postgres: %v", err)
	}
	defer pool.Close()

	// Step 1: Read events WAL table first (append-only, no equality deletes).
	// This tests basic DuckDB+Iceberg connectivity before trying the materialized table.
	log.Println("step 1: loading events WAL via DuckDB...")
	walMetadataLoc := getMetadataLocation(cfg, cfg.Table+"_events")
	walEvents := duckdbScanWALEvents(cfg, walMetadataLoc)
	walState := replayEventsWAL(walEvents)
	log.Printf("  WAL replay: %d events -> %d live rows", len(walEvents), len(walState))

	// Step 2: Read materialized table via DuckDB (handles MoR + equality deletes).
	log.Println("step 2: reading materialized table via DuckDB...")
	matMetadataLoc := getMetadataLocation(cfg, cfg.Table)
	actual := duckdbScanTable(cfg, matMetadataLoc, "seq", "value")
	log.Printf("  materialized table: %d live rows", len(actual))

	// Step 3: Flush completeness check. — compare WAL replay against PG source.
	var opsCount int64
	if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM bench_operations").Scan(&opsCount); err != nil {
		log.Fatalf("count operations: %v", err)
	}
	if opsCount > 0 {
		log.Printf("step 3: checking flush completeness against PG operations log (%d entries)...", opsCount)
		pgState := loadPGExpectedState(ctx, pool)
		flushMissing := 0
		for seq := range pgState {
			if _, ok := walState[seq]; !ok {
				flushMissing++
			}
		}
		if flushMissing > 0 {
			log.Printf("  FLUSH GAP: %d rows in PG but not in events WAL (pg=%d wal=%d)",
				flushMissing, len(pgState), len(walState))
		} else {
			log.Printf("  flush complete: all %d PG rows present in events WAL", len(pgState))
		}
	} else {
		log.Println("step 3: seed-only mode (no operations log), skipping flush check")
	}

	// Step 4: Materialization check — compare materialized table against WAL replay.
	log.Println("step 4: verifying materialized table against events WAL...")
	result := verify(walState, actual)
	printResult(result)

	if !result.Pass {
		os.Exit(1)
	}
}

// --- DuckDB ---

// duckdbSQL runs a SQL query via the DuckDB CLI and returns stdout.
func duckdbSQL(cfg config, sql string) string {
	// Build S3 + Iceberg setup preamble.
	setup := fmt.Sprintf(`
INSTALL iceberg;
LOAD iceberg;
INSTALL httpfs;
LOAD httpfs;
SET s3_region='%s';
SET s3_access_key_id='%s';
SET s3_secret_access_key='%s';
SET s3_endpoint='%s';
SET s3_url_style='path';
SET s3_use_ssl=false;
`, cfg.S3Region, cfg.S3AccessKey, cfg.S3SecretKey, strings.TrimPrefix(cfg.S3Endpoint, "http://"))

	fullSQL := setup + sql
	cmd := exec.Command("duckdb", "-csv", "-noheader")
	cmd.Stdin = strings.NewReader(fullSQL)
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Fatalf("duckdb error: %v\nSQL: %s\nOutput: %s", err, fullSQL, string(out))
	}
	return string(out)
}

// duckdbScanTable reads all (seq, value) rows from an Iceberg table.
func duckdbScanTable(cfg config, metadataLocation string, seqCol, valCol string) map[int64]int64 {
	sql := fmt.Sprintf("SELECT %s, %s FROM iceberg_scan('%s');\n", seqCol, valCol, metadataLocation)
	out := duckdbSQL(cfg, sql)
	return parseCSVPairs(out)
}

// duckdbScanWALEvents reads all events from the events WAL Iceberg table.
func duckdbScanWALEvents(cfg config, metadataLocation string) []walEvent {
	sql := fmt.Sprintf("SELECT _op, _seq, id, seq, value FROM iceberg_scan('%s') ORDER BY _seq;\n", metadataLocation)
	out := duckdbSQL(cfg, sql)

	var events []walEvent
	r := csv.NewReader(strings.NewReader(out))
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("parse WAL CSV: %v", err)
		}
		if len(record) < 5 {
			continue
		}
		events = append(events, walEvent{
			op:     record[0],
			seq:    parseInt64(record[1]),
			id:     parseInt64(record[2]),
			rowSeq: parseInt64(record[3]),
			value:  parseInt64(record[4]),
		})
	}
	log.Printf("  loaded %d events", len(events))
	return events
}

func parseCSVPairs(csvData string) map[int64]int64 {
	result := make(map[int64]int64)
	r := csv.NewReader(strings.NewReader(csvData))
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("parse CSV: %v", err)
		}
		if len(record) < 2 {
			continue
		}
		seq := parseInt64(record[0])
		value := parseInt64(record[1])
		result[seq] = value
	}
	return result
}

func parseInt64(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

// --- Iceberg catalog (REST API) ---

// getMetadataLocation fetches the metadata-location for a table from the REST catalog.
func getMetadataLocation(cfg config, table string) string {
	url := fmt.Sprintf("%s/v1/namespaces/%s/tables/%s",
		strings.TrimRight(cfg.CatalogURI, "/"), cfg.Namespace, table)
	resp, err := http.Get(url)
	if err != nil {
		log.Fatalf("catalog GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		log.Fatalf("catalog error %d for %s: %s", resp.StatusCode, table, string(body))
	}

	var tm struct {
		MetadataLocation string `json:"metadata-location"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tm); err != nil {
		log.Fatalf("decode table metadata for %s: %v", table, err)
	}
	if tm.MetadataLocation == "" {
		log.Fatalf("no metadata-location for table %s.%s", cfg.Namespace, table)
	}
	return tm.MetadataLocation
}

// --- WAL replay ---

type walEvent struct {
	op     string
	seq    int64
	id     int64
	rowSeq int64
	value  int64
}

func replayEventsWAL(events []walEvent) map[int64]int64 {
	state := make(map[int64]int64)
	idToSeq := make(map[int64]int64)
	for _, ev := range events {
		switch ev.op {
		case "I":
			state[ev.rowSeq] = ev.value
			idToSeq[ev.id] = ev.rowSeq
		case "U":
			if _, ok := state[ev.rowSeq]; ok {
				state[ev.rowSeq] = ev.value
				idToSeq[ev.id] = ev.rowSeq
			}
		case "D":
			if seq, ok := idToSeq[ev.id]; ok {
				delete(state, seq)
				delete(idToSeq, ev.id)
			}
		}
	}
	return state
}

// --- PG expected state ---

func loadPGExpectedState(ctx context.Context, pool *pgxpool.Pool) map[int64]int64 {
	var seqMin, seqMax int64
	if err := pool.QueryRow(ctx, "SELECT COALESCE(MIN(seq),0), COALESCE(MAX(seq),0) FROM bench_events").Scan(&seqMin, &seqMax); err != nil {
		log.Fatalf("get seq range: %v", err)
	}
	pgState := make(map[int64]int64)
	for lo := (seqMin / chunkWidth) * chunkWidth; lo <= seqMax; lo += chunkWidth {
		rows, err := pool.Query(ctx,
			"SELECT op, seq, value FROM bench_operations WHERE seq >= $1 AND seq < $2 ORDER BY id ASC", lo, lo+chunkWidth)
		if err != nil {
			log.Fatalf("query operations [%d, %d): %v", lo, lo+chunkWidth, err)
		}
		for rows.Next() {
			var op string
			var seq int64
			var value *int64
			if err := rows.Scan(&op, &seq, &value); err != nil {
				log.Fatalf("scan: %v", err)
			}
			switch op {
			case "insert":
				if value != nil {
					pgState[seq] = *value
				}
			case "update":
				if _, ok := pgState[seq]; ok && value != nil {
					pgState[seq] = *value
				}
			case "delete":
				delete(pgState, seq)
			}
		}
		rows.Close()
	}
	return pgState
}

// --- Verification ---

type verifyResult struct {
	Pass          bool
	TotalExpected int
	TotalActual   int
	Missing       []int64
	Extra         []int64
	WrongValue    []int64
}

func verify(expected, actual map[int64]int64) verifyResult {
	r := verifyResult{
		TotalExpected: len(expected),
		TotalActual:   len(actual),
	}
	for seq := range expected {
		if _, ok := actual[seq]; !ok {
			r.Missing = append(r.Missing, seq)
		}
	}
	for seq := range actual {
		if _, ok := expected[seq]; !ok {
			r.Extra = append(r.Extra, seq)
		}
	}
	for seq, expVal := range expected {
		if actVal, ok := actual[seq]; ok && expVal != actVal {
			r.WrongValue = append(r.WrongValue, seq)
		}
	}
	r.Pass = len(r.Missing) == 0 && len(r.Extra) == 0 && len(r.WrongValue) == 0
	return r
}

func printResult(r verifyResult) {
	log.Println("========== VERIFICATION RESULT ==========")
	if r.Pass {
		log.Printf("PASS: all %d expected rows found with correct values", r.TotalExpected)
	} else {
		log.Println("FAIL")
	}
	log.Printf("  expected rows:    %d", r.TotalExpected)
	log.Printf("  actual rows:      %d", r.TotalActual)
	log.Printf("  missing rows:     %d", len(r.Missing))
	log.Printf("  extra rows:       %d", len(r.Extra))
	log.Printf("  wrong value rows: %d", len(r.WrongValue))

	if len(r.Missing) > 0 {
		n := 10
		if n > len(r.Missing) {
			n = len(r.Missing)
		}
		log.Printf("  missing sample (first %d): %v", n, r.Missing[:n])
	}
	if len(r.Extra) > 0 {
		n := 10
		if n > len(r.Extra) {
			n = len(r.Extra)
		}
		log.Printf("  extra sample (first %d): %v", n, r.Extra[:n])
	}
	if len(r.WrongValue) > 0 {
		n := 10
		if n > len(r.WrongValue) {
			n = len(r.WrongValue)
		}
		log.Printf("  wrong value sample (first %d): %v", n, r.WrongValue[:n])
	}
	log.Println("==========================================")
}

// --- Config ---

type config struct {
	DatabaseURL string
	S3Endpoint  string
	S3AccessKey string
	S3SecretKey string
	S3Region    string
	CatalogURI  string
	Warehouse   string
	Namespace   string
	Table       string
}

func loadConfig() config {
	return config{
		DatabaseURL: requireEnv("DATABASE_URL"),
		S3Endpoint:  os.Getenv("S3_ENDPOINT"),
		S3AccessKey: os.Getenv("S3_ACCESS_KEY"),
		S3SecretKey: os.Getenv("S3_SECRET_KEY"),
		S3Region:    envOr("S3_REGION", "us-east-1"),
		CatalogURI:  requireEnv("CATALOG_URI"),
		Warehouse:   requireEnv("WAREHOUSE"),
		Namespace:   requireEnv("NAMESPACE"),
		Table:       requireEnv("TABLE"),
	}
}

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("required env var %s is not set", key)
	}
	return v
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// Unused import guards.
var _ = sort.Slice
var _ = fmt.Sprintf
