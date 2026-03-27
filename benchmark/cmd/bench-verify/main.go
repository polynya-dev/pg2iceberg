package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/linkedin/goavro/v2"
	pq "github.com/parquet-go/parquet-go"
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

	s3Client := newS3Client(cfg)

	// Build HTTP client for catalog calls (plain or SigV4-signed).
	catalogClient := &http.Client{}
	if cfg.CatalogAuth == "sigv4" {
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.S3Region))
		if err != nil {
			log.Fatalf("load aws config: %v", err)
		}
		catalogClient = &http.Client{
			Transport: &sigV4Transport{
				inner:   http.DefaultTransport,
				creds:   awsCfg.Credentials,
				signer:  v4.NewSigner(),
				region:  cfg.S3Region,
				service: "glue",
			},
		}
	}

	// Step 1: Discover Iceberg data files grouped by partition.
	log.Println("step 1: reading iceberg table metadata...")
	partitionFiles, deleteSet, err := loadIcebergFileIndex(ctx, cfg, s3Client, catalogClient)
	if err != nil {
		log.Fatalf("load iceberg file index: %v", err)
	}
	totalFiles := 0
	for _, files := range partitionFiles {
		totalFiles += len(files)
	}
	log.Printf("  found %d partitions, %d data files, %d deletes",
		len(partitionFiles), totalFiles, len(deleteSet))

	// Step 2: Determine verification mode (ops log vs seed-only).
	var opsCount int64
	if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM bench_operations").Scan(&opsCount); err != nil {
		log.Fatalf("count operations: %v", err)
	}
	useOpsLog := opsCount > 0
	if useOpsLog {
		log.Printf("step 2: using operations log (%d entries)", opsCount)
	} else {
		log.Println("step 2: seed-only mode (no operations log)")
	}

	// Step 3: Build complete set of partition boundaries from both PG and Iceberg.
	var seqMin, seqMax int64
	if err := pool.QueryRow(ctx, "SELECT COALESCE(MIN(seq),0), COALESCE(MAX(seq),0) FROM bench_events").Scan(&seqMin, &seqMax); err != nil {
		log.Fatalf("get seq range: %v", err)
	}

	icebergPartKeys := sortedPartitionKeys(partitionFiles)
	allPartitions := mergePartitionRanges(icebergPartKeys, seqMin, seqMax, chunkWidth)
	log.Printf("step 3: verifying %d partitions chunk-by-chunk (seq range [%d, %d])...",
		len(allPartitions), seqMin, seqMax)

	// Step 4: Process each partition.
	result := verifyResult{
		MissingSample: 10,
		ExtraSample:   10,
		WrongSample:   10,
	}

	for i, partKey := range allPartitions {
		lo := partKey
		hi := partKey + chunkWidth

		// Load expected state for this chunk from PG.
		var expected map[int64]int64
		if useOpsLog {
			expected, err = loadExpectedChunkFromOpsLog(ctx, pool, lo, hi)
		} else {
			expected, err = loadExpectedChunkFromTable(ctx, pool, lo, hi)
		}
		if err != nil {
			log.Fatalf("load expected chunk [%d, %d): %v", lo, hi, err)
		}

		// Load actual state for this chunk from Iceberg.
		partPathPrefix := fmt.Sprintf("seq_truncate=%d/", lo)
		files := partitionFiles[partPathPrefix]
		actual, err := loadIcebergChunk(ctx, s3Client, cfg, files, deleteSet)
		if err != nil {
			log.Fatalf("load iceberg chunk [%d, %d): %v", lo, hi, err)
		}

		// Compare.
		cr := verifyChunk(expected, actual)
		result.TotalExpected += cr.TotalExpected
		result.TotalActual += cr.TotalActual
		result.Missing = append(result.Missing, cr.Missing...)
		result.Extra = append(result.Extra, cr.Extra...)
		result.WrongValue = append(result.WrongValue, cr.WrongValue...)

		if len(cr.Missing) > 0 || len(cr.Extra) > 0 || len(cr.WrongValue) > 0 {
			log.Printf("  partition %d/%d [%d, %d): expected=%d actual=%d missing=%d extra=%d wrong=%d",
				i+1, len(allPartitions), lo, hi,
				cr.TotalExpected, cr.TotalActual,
				len(cr.Missing), len(cr.Extra), len(cr.WrongValue))
		}

		if (i+1)%10 == 0 || i+1 == len(allPartitions) {
			log.Printf("  progress: %d/%d partitions verified, %d rows checked",
				i+1, len(allPartitions), result.TotalExpected)
		}
	}

	result.Pass = len(result.Missing) == 0 && len(result.Extra) == 0 && len(result.WrongValue) == 0
	printResult(result)

	if !result.Pass {
		os.Exit(1)
	}
}

// --- Config ---

type config struct {
	DatabaseURL string
	S3Endpoint  string
	S3AccessKey string
	S3SecretKey string
	S3Region    string
	CatalogURI  string
	CatalogAuth string // "" or "sigv4"
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
		CatalogAuth: os.Getenv("CATALOG_AUTH"),
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

// --- Partition-aware Iceberg loading ---

// loadIcebergFileIndex reads the Iceberg catalog and returns data files grouped by
// partition path prefix, plus a set of deleted IDs from equality delete files.
func loadIcebergFileIndex(ctx context.Context, cfg config, s3Client *s3.Client, httpClient *http.Client) (map[string][]string, map[int64]struct{}, error) {
	manifestListURI, err := getManifestListURI(cfg, httpClient)
	if err != nil {
		return nil, nil, fmt.Errorf("get manifest list: %w", err)
	}
	if manifestListURI == "" {
		return nil, nil, fmt.Errorf("no snapshots found for table %s.%s", cfg.Namespace, cfg.Table)
	}

	mlKey := s3KeyFromURI(manifestListURI)
	mlData, err := s3Download(ctx, s3Client, cfg.Warehouse, mlKey)
	if err != nil {
		return nil, nil, fmt.Errorf("download manifest list: %w", err)
	}

	manifestInfos, err := readManifestList(mlData)
	if err != nil {
		return nil, nil, fmt.Errorf("parse manifest list: %w", err)
	}

	var dataFilePaths []string
	var deleteFilePaths []string

	for _, mfi := range manifestInfos {
		mKey := s3KeyFromURI(mfi.Path)
		mData, err := s3Download(ctx, s3Client, cfg.Warehouse, mKey)
		if err != nil {
			log.Printf("  warning: failed to download manifest %s: %v", mfi.Path, err)
			continue
		}
		entries, err := readManifest(mData)
		if err != nil {
			log.Printf("  warning: failed to parse manifest %s: %v", mfi.Path, err)
			continue
		}
		for _, e := range entries {
			if e.Status == 2 { // deleted
				continue
			}
			switch e.Content {
			case 0: // data
				dataFilePaths = append(dataFilePaths, e.FilePath)
			case 2: // equality delete
				deleteFilePaths = append(deleteFilePaths, e.FilePath)
			}
		}
	}

	// Build delete set.
	deleteSet := make(map[int64]struct{})
	for _, path := range deleteFilePaths {
		key := s3KeyFromURI(path)
		data, err := s3Download(ctx, s3Client, cfg.Warehouse, key)
		if err != nil {
			log.Printf("  warning: failed to download delete file %s: %v", path, err)
			continue
		}
		delRows, err := readParquetRows(data)
		if err != nil {
			log.Printf("  warning: failed to read delete file %s: %v", path, err)
			continue
		}
		for _, row := range delRows {
			if id, ok := row["id"]; ok {
				deleteSet[toInt64(id)] = struct{}{}
			}
		}
	}
	log.Printf("  delete set: %d entries", len(deleteSet))

	// Group data files by partition path.
	// File paths look like: .../data/seq_truncate=0/uuid-data-0.parquet
	partitionFiles := make(map[string][]string)
	for _, fp := range dataFilePaths {
		partKey := extractPartitionPrefix(fp)
		partitionFiles[partKey] = append(partitionFiles[partKey], fp)
	}

	return partitionFiles, deleteSet, nil
}

// extractPartitionPrefix extracts the partition directory from a data file path.
// Example: ".../data/seq_truncate=0/uuid-data-0.parquet" → "seq_truncate=0/"
// For unpartitioned files: "" (empty string).
func extractPartitionPrefix(filePath string) string {
	idx := strings.Index(filePath, "/data/")
	if idx < 0 {
		return ""
	}
	afterData := filePath[idx+len("/data/"):]
	lastSlash := strings.LastIndex(afterData, "/")
	if lastSlash < 0 {
		return ""
	}
	return afterData[:lastSlash+1]
}

// sortedPartitionKeys returns partition keys sorted by their numeric value.
func sortedPartitionKeys(partitionFiles map[string][]string) []int64 {
	var keys []int64
	for k := range partitionFiles {
		val := parsePartitionValue(k)
		keys = append(keys, val)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// parsePartitionValue extracts the numeric value from "seq_truncate=N/".
func parsePartitionValue(partKey string) int64 {
	parts := strings.SplitN(partKey, "=", 2)
	if len(parts) != 2 {
		return 0
	}
	valStr := strings.TrimSuffix(parts[1], "/")
	return toInt64(valStr)
}

// mergePartitionRanges returns a sorted, deduplicated list of partition boundaries
// covering both the Iceberg partitions and the PG seq range.
func mergePartitionRanges(icebergPartitions []int64, seqMin, seqMax, width int64) []int64 {
	seen := make(map[int64]struct{})
	for _, p := range icebergPartitions {
		seen[p] = struct{}{}
	}
	for lo := (seqMin / width) * width; lo <= seqMax; lo += width {
		seen[lo] = struct{}{}
	}
	result := make([]int64, 0, len(seen))
	for k := range seen {
		result = append(result, k)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

// --- Chunked PG loading ---

func loadExpectedChunkFromTable(ctx context.Context, pool *pgxpool.Pool, lo, hi int64) (map[int64]int64, error) {
	rows, err := pool.Query(ctx,
		"SELECT seq, value FROM bench_events WHERE seq >= $1 AND seq < $2", lo, hi)
	if err != nil {
		return nil, fmt.Errorf("query bench_events: %w", err)
	}
	defer rows.Close()

	state := make(map[int64]int64)
	for rows.Next() {
		var seq, value int64
		if err := rows.Scan(&seq, &value); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		state[seq] = value
	}
	return state, rows.Err()
}

func loadExpectedChunkFromOpsLog(ctx context.Context, pool *pgxpool.Pool, lo, hi int64) (map[int64]int64, error) {
	rows, err := pool.Query(ctx,
		"SELECT op, seq, value FROM bench_operations WHERE seq >= $1 AND seq < $2 ORDER BY id ASC", lo, hi)
	if err != nil {
		return nil, fmt.Errorf("query operations: %w", err)
	}
	defer rows.Close()

	state := make(map[int64]int64)
	for rows.Next() {
		var op string
		var seq int64
		var value *int64
		if err := rows.Scan(&op, &seq, &value); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		switch op {
		case "insert":
			if value != nil {
				state[seq] = *value
			}
		case "update":
			if _, ok := state[seq]; ok && value != nil {
				state[seq] = *value
			}
		case "delete":
			delete(state, seq)
		}
	}
	return state, rows.Err()
}

// --- Chunked Iceberg loading ---

func loadIcebergChunk(ctx context.Context, s3Client *s3.Client, cfg config, files []string, deleteSet map[int64]struct{}) (map[int64]int64, error) {
	result := make(map[int64]int64)
	for _, path := range files {
		key := s3KeyFromURI(path)
		data, err := s3Download(ctx, s3Client, cfg.Warehouse, key)
		if err != nil {
			log.Printf("  warning: failed to download data file %s: %v", path, err)
			continue
		}
		rows, err := readParquetRows(data)
		if err != nil {
			log.Printf("  warning: failed to read data file %s: %v", path, err)
			continue
		}
		for _, row := range rows {
			id := toInt64(row["id"])
			if _, deleted := deleteSet[id]; deleted {
				continue
			}
			seq := toInt64(row["seq"])
			value := toInt64(row["value"])
			result[seq] = value
		}
	}
	return result, nil
}

// --- Verification ---

type chunkResult struct {
	TotalExpected int
	TotalActual   int
	Missing       []int64
	Extra         []int64
	WrongValue    []int64
}

func verifyChunk(expected, actual map[int64]int64) chunkResult {
	r := chunkResult{
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
		if actVal, ok := actual[seq]; ok {
			if expVal != actVal {
				r.WrongValue = append(r.WrongValue, seq)
			}
		}
	}
	return r
}

type verifyResult struct {
	Pass          bool
	TotalExpected int
	TotalActual   int
	Missing       []int64
	Extra         []int64
	WrongValue    []int64
	MissingSample int
	ExtraSample   int
	WrongSample   int
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
		n := r.MissingSample
		if n > len(r.Missing) {
			n = len(r.Missing)
		}
		log.Printf("  missing sample (first %d): %v", n, r.Missing[:n])
	}
	if len(r.Extra) > 0 {
		n := r.ExtraSample
		if n > len(r.Extra) {
			n = len(r.Extra)
		}
		log.Printf("  extra sample (first %d): %v", n, r.Extra[:n])
	}
	if len(r.WrongValue) > 0 {
		n := r.WrongSample
		if n > len(r.WrongValue) {
			n = len(r.WrongValue)
		}
		log.Printf("  wrong value sample (first %d): %v", n, r.WrongValue[:n])
	}
	log.Println("==========================================")
}

// --- S3 helpers ---

func newS3Client(cfg config) *s3.Client {
	opts := s3.Options{
		Region: cfg.S3Region,
	}
	if cfg.S3AccessKey != "" {
		opts.Credentials = credentials.NewStaticCredentialsProvider(cfg.S3AccessKey, cfg.S3SecretKey, "")
	}
	if cfg.S3Endpoint != "" {
		opts.BaseEndpoint = &cfg.S3Endpoint
		opts.UsePathStyle = true
	}
	return s3.New(opts)
}

func s3Download(ctx context.Context, client *s3.Client, warehouse, key string) ([]byte, error) {
	u, _ := url.Parse(warehouse)
	bucket := u.Host
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

func s3KeyFromURI(uri string) string {
	u, err := url.Parse(uri)
	if err != nil {
		return uri
	}
	if len(u.Path) > 1 {
		return u.Path[1:]
	}
	return uri
}

// --- Iceberg catalog helpers ---

func getManifestListURI(cfg config, httpClient *http.Client) (string, error) {
	url := fmt.Sprintf("%s/v1/namespaces/%s/tables/%s", strings.TrimRight(cfg.CatalogURI, "/"), cfg.Namespace, cfg.Table)
	resp, err := httpClient.Get(url)
	if err != nil {
		return "", fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return "", fmt.Errorf("table not found: %s.%s", cfg.Namespace, cfg.Table)
	}
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("catalog error %d: %s", resp.StatusCode, string(body))
	}

	var tm struct {
		Metadata struct {
			CurrentSnapshotID int64 `json:"current-snapshot-id"`
			Snapshots         []struct {
				SnapshotID   int64  `json:"snapshot-id"`
				ManifestList string `json:"manifest-list"`
			} `json:"snapshots"`
		} `json:"metadata"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tm); err != nil {
		return "", fmt.Errorf("decode: %w", err)
	}

	for _, snap := range tm.Metadata.Snapshots {
		if snap.SnapshotID == tm.Metadata.CurrentSnapshotID {
			return snap.ManifestList, nil
		}
	}
	return "", nil
}

// --- Avro manifest reading ---

type manifestInfo struct {
	Path    string
	Content int
}

type manifestEntry struct {
	Status   int
	Content  int
	FilePath string
}

func readManifestList(data []byte) ([]manifestInfo, error) {
	reader, err := goavro.NewOCFReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	var infos []manifestInfo
	for reader.Scan() {
		record, err := reader.Read()
		if err != nil {
			return nil, err
		}
		m, ok := record.(map[string]any)
		if !ok {
			continue
		}
		info := manifestInfo{
			Path: avroGetString(m, "manifest_path"),
		}
		if c, ok := m["content"]; ok {
			info.Content = avroToInt(c)
		}
		infos = append(infos, info)
	}
	return infos, nil
}

func readManifest(data []byte) ([]manifestEntry, error) {
	reader, err := goavro.NewOCFReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	var entries []manifestEntry
	for reader.Scan() {
		record, err := reader.Read()
		if err != nil {
			return nil, err
		}
		m, ok := record.(map[string]any)
		if !ok {
			continue
		}

		e := manifestEntry{
			Status: avroToInt(m["status"]),
		}

		if df, ok := m["data_file"].(map[string]any); ok {
			e.Content = avroToInt(df["content"])
			e.FilePath = avroGetString(df, "file_path")
		}

		entries = append(entries, e)
	}
	return entries, nil
}

func avroGetString(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func avroToInt(v any) int {
	switch x := v.(type) {
	case int:
		return x
	case int32:
		return int(x)
	case int64:
		return int(x)
	default:
		return 0
	}
}

// --- Parquet reading ---

func readParquetRows(data []byte) ([]map[string]any, error) {
	reader := pq.NewReader(bytes.NewReader(data))

	colNames := make([]string, len(reader.Schema().Fields()))
	for i, f := range reader.Schema().Fields() {
		colNames[i] = f.Name()
	}

	var rows []map[string]any
	rowBuf := make([]pq.Row, 1)
	for {
		n, _ := reader.ReadRows(rowBuf)
		if n == 0 {
			break
		}

		row := make(map[string]any, len(colNames))
		for _, v := range rowBuf[0] {
			colIdx := v.Column()
			if colIdx >= 0 && colIdx < len(colNames) {
				if v.IsNull() {
					row[colNames[colIdx]] = nil
				} else {
					row[colNames[colIdx]] = parquetValueToGo(v)
				}
			}
		}
		rows = append(rows, row)
	}

	return rows, nil
}

func parquetValueToGo(v pq.Value) any {
	switch v.Kind() {
	case pq.Boolean:
		return v.Boolean()
	case pq.Int32:
		return int64(v.Int32())
	case pq.Int64:
		return v.Int64()
	case pq.Float:
		return float64(v.Float())
	case pq.Double:
		return v.Double()
	case pq.ByteArray, pq.FixedLenByteArray:
		return string(v.ByteArray())
	default:
		return v.String()
	}
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int32:
		return int64(x)
	case int:
		return int64(x)
	case float64:
		return int64(x)
	case string:
		var n int64
		fmt.Sscanf(x, "%d", &n)
		return n
	default:
		return 0
	}
}

// --- SigV4 HTTP transport ---

type sigV4Transport struct {
	inner   http.RoundTripper
	creds   aws.CredentialsProvider
	signer  *v4.Signer
	region  string
	service string
}

func (t *sigV4Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	var body []byte
	if req.Body != nil {
		var err error
		body, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, fmt.Errorf("read body for signing: %w", err)
		}
		req.Body = io.NopCloser(bytes.NewReader(body))
	}

	hash := sha256.Sum256(body)
	payloadHash := fmt.Sprintf("%x", hash)

	creds, err := t.creds.Retrieve(req.Context())
	if err != nil {
		return nil, fmt.Errorf("retrieve aws credentials: %w", err)
	}

	if err := t.signer.SignHTTP(req.Context(), creds, req, payloadHash, t.service, t.region, time.Now()); err != nil {
		return nil, fmt.Errorf("sigv4 sign: %w", err)
	}

	return t.inner.RoundTrip(req)
}
