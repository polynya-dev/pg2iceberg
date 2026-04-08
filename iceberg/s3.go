package iceberg

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pg2iceberg/pg2iceberg/pipeline"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

var s3tracer = otel.Tracer("pg2iceberg/s3")

// s3SpanName builds a descriptive span name from the S3 key.
// e.g. "s3.Upload rideshare.db/orders/data/abc.parquet" → "s3.Upload orders data"
//      "rideshare.db/orders_events/metadata/snap-123.avro" → "s3.Upload orders_events metadata"
// Handles partitioned keys: "ns.db/table/data/_ts_day=2026-04-08/abc.parquet" → "s3.Upload table data"
func s3SpanName(op, key string) string {
	// Find the ".db/" marker to locate the table name reliably.
	dbIdx := strings.Index(key, ".db/")
	if dbIdx >= 0 {
		after := key[dbIdx+4:] // everything after ".db/"
		parts := strings.SplitN(after, "/", 3)
		if len(parts) >= 2 {
			table := parts[0]
			kind := parts[1] // "data" or "metadata"
			return op + " " + table + " " + kind
		}
	}
	return op + " " + path.Base(key)
}

// ObjectStorage abstracts file upload and download operations.
type ObjectStorage interface {
	Upload(ctx context.Context, key string, data []byte) (string, error)
	Download(ctx context.Context, key string) ([]byte, error)
	// DownloadRange reads a byte range from an object. offset is the start byte,
	// length is the number of bytes to read.
	DownloadRange(ctx context.Context, key string, offset, length int64) ([]byte, error)
	// StatObject returns the size of an object in bytes.
	StatObject(ctx context.Context, key string) (int64, error)
	// URIForKey returns the full URI for a key without uploading.
	// Enables building manifests that reference files before they're uploaded.
	URIForKey(key string) string
}

// S3Client wraps the S3 SDK for uploading and downloading files.
type S3Client struct {
	client *s3.Client
	bucket string
}

func NewS3Client(endpoint, accessKey, secretKey, region, warehouse string) (*S3Client, error) {
	u, err := url.Parse(warehouse)
	if err != nil {
		return nil, fmt.Errorf("parse warehouse URI: %w", err)
	}
	bucket := u.Host

	client := s3.New(s3.Options{
		Region:       region,
		Credentials:  credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		BaseEndpoint: &endpoint,
		UsePathStyle: true,
	})

	return &S3Client{client: client, bucket: bucket}, nil
}

// NewIAMS3Client creates an S3 client using the default AWS credential chain
// (environment variables, IAM role, etc.) — no explicit keys needed.
func NewIAMS3Client(ctx context.Context, region, warehouse string) (*S3Client, error) {
	u, err := url.Parse(warehouse)
	if err != nil {
		return nil, fmt.Errorf("parse warehouse URI: %w", err)
	}
	bucket := u.Host

	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg)
	return &S3Client{client: client, bucket: bucket}, nil
}

// Upload writes data to an S3 key and returns the full s3:// URI.
func (c *S3Client) Upload(ctx context.Context, key string, data []byte) (string, error) {
	ctx, span := s3tracer.Start(ctx, s3SpanName("s3.Upload", key), trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(
		attribute.String("s3.key", key),
		attribute.Int("s3.size_bytes", len(data)),
		semconv.PeerService("s3"),
	))
	defer span.End()

	start := time.Now()
	_, err := c.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &c.bucket,
		Key:    &key,
		Body:   bytes.NewReader(data),
	})
	pipeline.S3OperationDurationSeconds.WithLabelValues("upload").Observe(time.Since(start).Seconds())
	if err != nil {
		pipeline.S3ErrorsTotal.WithLabelValues("upload").Inc()
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", fmt.Errorf("upload %s: %w", key, err)
	}
	pipeline.S3BytesUploadedTotal.Add(float64(len(data)))
	return c.URIForKey(key), nil
}

func (c *S3Client) URIForKey(key string) string {
	return fmt.Sprintf("s3://%s/%s", c.bucket, key)
}

// Download reads an S3 object.
func (c *S3Client) Download(ctx context.Context, key string) ([]byte, error) {
	ctx, span := s3tracer.Start(ctx, s3SpanName("s3.Download", key), trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(
		attribute.String("s3.key", key),
		semconv.PeerService("s3"),
	))
	defer span.End()

	start := time.Now()
	out, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &c.bucket,
		Key:    aws.String(key),
	})
	pipeline.S3OperationDurationSeconds.WithLabelValues("download").Observe(time.Since(start).Seconds())
	if err != nil {
		pipeline.S3ErrorsTotal.WithLabelValues("download").Inc()
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("download %s: %w", key, err)
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err == nil {
		pipeline.S3BytesDownloadedTotal.Add(float64(len(data)))
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return data, err
}

// DownloadRange reads a byte range from an S3 object.
func (c *S3Client) DownloadRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	ctx, span := s3tracer.Start(ctx, s3SpanName("s3.DownloadRange", key), trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(
		attribute.String("s3.key", key),
		attribute.Int64("s3.offset", offset),
		attribute.Int64("s3.length", length),
		semconv.PeerService("s3"),
	))
	defer span.End()

	rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	start := time.Now()
	out, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &c.bucket,
		Key:    aws.String(key),
		Range:  &rangeHeader,
	})
	pipeline.S3OperationDurationSeconds.WithLabelValues("download_range").Observe(time.Since(start).Seconds())
	if err != nil {
		pipeline.S3ErrorsTotal.WithLabelValues("download_range").Inc()
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("download range %s: %w", key, err)
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err == nil {
		pipeline.S3BytesDownloadedTotal.Add(float64(len(data)))
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return data, err
}

// StatObject returns the size of an S3 object in bytes.
func (c *S3Client) StatObject(ctx context.Context, key string) (int64, error) {
	ctx, span := s3tracer.Start(ctx, s3SpanName("s3.StatObject", key), trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(
		attribute.String("s3.key", key),
		semconv.PeerService("s3"),
	))
	defer span.End()

	start := time.Now()
	out, err := c.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &c.bucket,
		Key:    aws.String(key),
	})
	pipeline.S3OperationDurationSeconds.WithLabelValues("head").Observe(time.Since(start).Seconds())
	if err != nil {
		pipeline.S3ErrorsTotal.WithLabelValues("head").Inc()
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, fmt.Errorf("head %s: %w", key, err)
	}
	return *out.ContentLength, nil
}

// S3ReaderAt implements io.ReaderAt using S3 range reads.
// Used by pq.OpenFile to read only the parquet footer and needed column chunks.
type S3ReaderAt struct {
	Ctx context.Context
	S3  ObjectStorage
	Key string
}

func (r *S3ReaderAt) ReadAt(p []byte, off int64) (int, error) {
	data, err := r.S3.DownloadRange(r.Ctx, r.Key, off, int64(len(p)))
	if err != nil {
		return 0, err
	}
	n := copy(p, data)
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// KeyFromURI extracts the S3 key from an s3://bucket/key URI.
func KeyFromURI(uri string) (string, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", err
	}
	// u.Path starts with /
	if len(u.Path) > 1 {
		return u.Path[1:], nil
	}
	return "", fmt.Errorf("empty key in URI: %s", uri)
}
