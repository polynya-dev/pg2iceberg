package sink

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
)

// SigV4Transport is an http.RoundTripper that signs requests with AWS SigV4.
type SigV4Transport struct {
	inner   http.RoundTripper
	creds   aws.CredentialsProvider
	signer  *v4.Signer
	region  string
	service string // "glue" for AWS Glue Iceberg REST endpoint
}

// NewSigV4Transport creates a SigV4-signing HTTP transport.
// It loads AWS credentials from the default chain (env vars, IAM role, etc.).
func NewSigV4Transport(region string) (*SigV4Transport, error) {
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(region),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	return &SigV4Transport{
		inner:   http.DefaultTransport,
		creds:   cfg.Credentials,
		signer:  v4.NewSigner(),
		region:  region,
		service: "glue",
	}, nil
}

func (t *SigV4Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Read and buffer the body for signing.
	var body []byte
	if req.Body != nil {
		var err error
		body, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, fmt.Errorf("read request body for signing: %w", err)
		}
		req.Body = io.NopCloser(bytes.NewReader(body))
	}

	hash := sha256.Sum256(body)
	payloadHash := fmt.Sprintf("%x", hash)

	creds, err := t.creds.Retrieve(req.Context())
	if err != nil {
		return nil, fmt.Errorf("retrieve aws credentials: %w", err)
	}

	err = t.signer.SignHTTP(req.Context(), creds, req, payloadHash, t.service, t.region, time.Now())
	if err != nil {
		return nil, fmt.Errorf("sigv4 sign: %w", err)
	}

	return t.inner.RoundTrip(req)
}
