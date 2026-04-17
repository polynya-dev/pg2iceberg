package iceberg

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// OAuth2Transport is an http.RoundTripper that obtains and caches
// OAuth2 client_credentials tokens from a Polaris-compatible token endpoint.
type OAuth2Transport struct {
	inner        http.RoundTripper
	tokenURL     string
	clientID     string
	clientSecret string

	mu      sync.RWMutex
	token   string
	expires time.Time
}

func NewOAuth2Transport(tokenURL, clientID, clientSecret string) *OAuth2Transport {
	return &OAuth2Transport{
		inner:        http.DefaultTransport,
		tokenURL:     tokenURL,
		clientID:     clientID,
		clientSecret: clientSecret,
	}
}

func (t *OAuth2Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	token, err := t.getToken()
	if err != nil {
		return nil, fmt.Errorf("oauth2 token: %w", err)
	}
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer "+token)
	return t.inner.RoundTrip(req)
}

func (t *OAuth2Transport) getToken() (string, error) {
	t.mu.RLock()
	if t.token != "" && time.Now().Before(t.expires) {
		defer t.mu.RUnlock()
		return t.token, nil
	}
	t.mu.RUnlock()

	t.mu.Lock()
	defer t.mu.Unlock()

	// Double-check after acquiring write lock.
	if t.token != "" && time.Now().Before(t.expires) {
		return t.token, nil
	}

	data := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {t.clientID},
		"client_secret": {t.clientSecret},
		"scope":         {"PRINCIPAL_ROLE:ALL"},
	}

	u, err := url.Parse(t.tokenURL)
	if err != nil {
		return "", fmt.Errorf("parse token URL: %w", err)
	}

	resp, err := t.inner.RoundTrip(&http.Request{
		Method: "POST",
		URL:    u,
		Header: http.Header{
			"Content-Type": {"application/x-www-form-urlencoded"},
		},
		Body: io.NopCloser(strings.NewReader(data.Encode())),
	})
	if err != nil {
		return "", fmt.Errorf("token request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("token endpoint returned %d", resp.StatusCode)
	}

	var result struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
		TokenType   string `json:"token_type"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode token response: %w", err)
	}

	t.token = result.AccessToken
	// Refresh 5 minutes before expiry.
	ttl := time.Duration(result.ExpiresIn) * time.Second
	if ttl > 5*time.Minute {
		ttl -= 5 * time.Minute
	}
	t.expires = time.Now().Add(ttl)

	return t.token, nil
}
