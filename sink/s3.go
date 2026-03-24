package sink

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

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

// Upload writes data to an S3 key and returns the full s3:// URI.
func (c *S3Client) Upload(ctx context.Context, key string, data []byte) (string, error) {
	_, err := c.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &c.bucket,
		Key:    &key,
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return "", fmt.Errorf("upload %s: %w", key, err)
	}
	return fmt.Sprintf("s3://%s/%s", c.bucket, key), nil
}

// Download reads an S3 object.
func (c *S3Client) Download(ctx context.Context, key string) ([]byte, error) {
	out, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &c.bucket,
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("download %s: %w", key, err)
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
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
