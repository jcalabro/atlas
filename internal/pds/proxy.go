package pds

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/indigo/api/bsky"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/jcalabro/atlas/internal/pds/metrics"
)

// appviewBackend represents a single appview backend with health status.
type appviewBackend struct {
	url     string
	healthy atomic.Bool
}

// appviewProxy manages a pool of appview backends with health checking.
type appviewProxy struct {
	log      *slog.Logger
	backends []*appviewBackend
	client   *http.Client

	healthCheckInterval time.Duration
}

// newAppviewProxy creates a new appview proxy with the given URLs.
// If urls is empty, the proxy will be disabled (nil returned).
func newAppviewProxy(log *slog.Logger, urls []string) *appviewProxy {
	if len(urls) == 0 {
		return nil
	}

	backends := make([]*appviewBackend, len(urls))
	for i, u := range urls {
		backends[i] = &appviewBackend{url: u}
		backends[i].healthy.Store(true) // assume healthy initially
	}

	// use a custom transport so we can close idle connections
	transport := &http.Transport{
		MaxIdleConns:        10,
		IdleConnTimeout:     30 * time.Second,
		DisableCompression:  true,
		DisableKeepAlives:   false,
		MaxIdleConnsPerHost: 2,
	}

	return &appviewProxy{
		log:                 log.With("component", "appview-proxy"),
		backends:            backends,
		client:              &http.Client{Timeout: 15 * time.Second, Transport: transport},
		healthCheckInterval: 15 * time.Second,
	}
}

// CloseIdleConnections closes any idle connections in the proxy's HTTP client.
// This should be called when the proxy is no longer needed.
func (p *appviewProxy) CloseIdleConnections() {
	if p == nil || p.client == nil {
		return
	}
	if transport, ok := p.client.Transport.(*http.Transport); ok {
		transport.CloseIdleConnections()
	}
}

// Start begins the health check goroutines for all backends.
// Returns when ctx is cancelled.
func (p *appviewProxy) Start(ctx context.Context) {
	if p == nil {
		return
	}

	var wg sync.WaitGroup
	defer wg.Wait()

	for _, backend := range p.backends {
		b := backend
		wg.Go(func() {
			p.runHealthCheck(ctx, b)
		})
	}
}

func (p *appviewProxy) runHealthCheck(ctx context.Context, backend *appviewBackend) {
	ticker := time.NewTicker(p.healthCheckInterval)
	defer ticker.Stop()

	// do an initial check immediately
	p.checkHealth(ctx, backend)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.checkHealth(ctx, backend)
		}
	}
}

func (p *appviewProxy) checkHealth(ctx context.Context, backend *appviewBackend) {
	healthURL := fmt.Sprintf("%s/xrpc/_health", backend.url)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
	if err != nil {
		p.log.Warn("failed to create health check request", "url", backend.url, "err", err)
		backend.healthy.Store(false)
		return
	}

	resp, err := p.client.Do(req)
	if err != nil {
		p.log.Warn("health check failed", "url", backend.url, "err", err)
		backend.healthy.Store(false)
		return
	}
	defer resp.Body.Close() // nolint:errcheck

	// drain the body to allow connection reuse
	io.Copy(io.Discard, resp.Body) // nolint:errcheck

	wasHealthy := backend.healthy.Load()
	isHealthy := resp.StatusCode == http.StatusOK

	if wasHealthy != isHealthy {
		if isHealthy {
			p.log.Info("appview became healthy", "url", backend.url)
		} else {
			p.log.Warn("appview became unhealthy", "url", backend.url, "status", resp.StatusCode)
		}
	}

	backend.healthy.Store(isHealthy)
}

// getHealthyBackend returns the first healthy backend URL.
// If no backends are healthy, returns the first backend anyway to avoid blocking all requests.
// Backends are checked in order.
func (p *appviewProxy) getHealthyBackend() (string, error) {
	if p == nil || len(p.backends) == 0 {
		return "", fmt.Errorf("no appview backends configured")
	}

	for _, backend := range p.backends {
		if backend.healthy.Load() {
			return backend.url, nil
		}
	}

	// fall back to first backend if none are healthy
	p.log.Warn("no healthy appview backends, falling back to first backend", "url", p.backends[0].url)
	return p.backends[0].url, nil
}

// proxy forwards an HTTP request to a healthy appview backend.
func (p *appviewProxy) proxy(w http.ResponseWriter, r *http.Request) error {
	return p.proxyWithAuth(w, r, "")
}

// proxyWithAuth forwards an HTTP request to a healthy appview backend with an optional
// service auth token. If serviceAuthToken is non-empty, it replaces the Authorization header.
func (p *appviewProxy) proxyWithAuth(w http.ResponseWriter, r *http.Request, serviceAuthToken string) error {
	start := time.Now()

	backendURL, err := p.getHealthyBackend()
	if err != nil {
		metrics.ProxyErrors.WithLabelValues("no_backend").Inc()
		return err
	}

	// parse the backend URL
	target, err := url.Parse(backendURL)
	if err != nil {
		metrics.ProxyErrors.WithLabelValues("invalid_url").Inc()
		return fmt.Errorf("invalid backend URL: %w", err)
	}

	// build the proxied request URL
	proxyURL := *r.URL
	proxyURL.Scheme = target.Scheme
	proxyURL.Host = target.Host

	// create the proxy request
	var body io.Reader
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		body = r.Body
	}

	proxyReq, err := http.NewRequestWithContext(r.Context(), r.Method, proxyURL.String(), body)
	if err != nil {
		metrics.ProxyErrors.WithLabelValues("request_creation").Inc()
		return fmt.Errorf("failed to create proxy request: %w", err)
	}

	// copy headers from the original request
	for key, values := range r.Header {
		for _, value := range values {
			proxyReq.Header.Add(key, value)
		}
	}

	// handle Authorization header: either replace with service auth token or remove entirely
	// we never want to forward the user's PDS access token to upstream services
	if serviceAuthToken != "" {
		proxyReq.Header.Set("Authorization", "Bearer "+serviceAuthToken)
	} else {
		proxyReq.Header.Del("Authorization")
	}

	// forward the request
	resp, err := p.client.Do(proxyReq)
	if err != nil {
		metrics.ProxyErrors.WithLabelValues("upstream").Inc()
		return fmt.Errorf("proxy request failed: %w", err)
	}
	defer resp.Body.Close() // nolint:errcheck

	// record proxy metrics
	duration := time.Since(start).Seconds()
	metrics.ProxyDuration.WithLabelValues(r.Method).Observe(duration)
	metrics.ProxyRequests.WithLabelValues(r.Method, strconv.Itoa(resp.StatusCode)).Inc()

	// copy response headers, but skip CORS headers since we handle those ourselves
	for key, values := range resp.Header {
		if strings.HasPrefix(strings.ToLower(key), "access-control-") {
			continue
		}
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	// write the status code
	w.WriteHeader(resp.StatusCode)

	// copy the response body
	if _, err := io.Copy(w, resp.Body); err != nil {
		// can't return error here as we've already started writing the response
		p.log.Error("failed to copy proxy response body", "err", err)
	}

	return nil
}

func (s *server) handleProxy(w http.ResponseWriter, r *http.Request) {
	if s.appviewProxy == nil {
		s.notFound(w, fmt.Errorf("no appview configured for proxying"))
		return
	}

	// extract the lexicon method from the path (e.g., /xrpc/app.bsky.notification.listNotifications)
	path := r.URL.Path
	parts := strings.Split(path, "/")
	if len(parts) != 3 || parts[1] != "xrpc" {
		s.badRequest(w, fmt.Errorf("invalid xrpc path"))
		return
	}
	lxm := parts[2]

	// get the service DID from the atproto-proxy header
	proxyHeader := r.Header.Get("atproto-proxy")
	if proxyHeader == "" {
		// no proxy header, just forward without auth
		if err := s.appviewProxy.proxy(w, r); err != nil {
			s.log.Error("proxy error", "err", err, "path", r.URL.Path)
			s.internalErr(w, fmt.Errorf("proxy error: %w", err))
		}
		return
	}

	// parse the proxy header (format: did:web:api.bsky.app#bsky_appview)
	hashIdx := strings.LastIndex(proxyHeader, "#")
	if hashIdx == -1 {
		s.badRequest(w, fmt.Errorf("invalid atproto-proxy header format"))
		return
	}
	serviceDID := proxyHeader[:hashIdx]

	// try to authenticate the user (optional auth for proxy requests)
	actor := s.tryGetAuthenticatedActor(r)

	var serviceAuthToken string
	if actor != nil {
		// create service auth token for the target service
		token, err := createServiceAuthToken(actor, serviceDID, lxm)
		if err != nil {
			s.log.Error("failed to create service auth token", "err", err, "did", actor.Did)
			s.internalErr(w, fmt.Errorf("authentication error"))
			return
		}
		serviceAuthToken = token
	}

	// proxy the request with the service auth token (or empty to strip auth header)
	if err := s.appviewProxy.proxyWithAuth(w, r, serviceAuthToken); err != nil {
		s.log.Error("proxy error", "err", err, "path", r.URL.Path)
		s.internalErr(w, fmt.Errorf("proxy error: %w", err))
	}
}

// getRecordResponse is the response from com.atproto.repo.getRecord
type getRecordResponse struct {
	URI   string                      `json:"uri"`
	CID   *string                     `json:"cid,omitempty"`
	Value *lexutil.LexiconTypeDecoder `json:"value"`
}

// getFeedGenerator fetches a feed generator record from the appview and returns the feed generator DID.
func (p *appviewProxy) getFeedGenerator(ctx context.Context, repo, collection, rkey string) (string, error) {
	backendURL, err := p.getHealthyBackend()
	if err != nil {
		return "", err
	}

	u, err := url.Parse(backendURL)
	if err != nil {
		return "", fmt.Errorf("invalid backend URL: %w", err)
	}
	u.Path = "/xrpc/com.atproto.repo.getRecord"
	q := u.Query()
	q.Set("repo", repo)
	q.Set("collection", collection)
	q.Set("rkey", rkey)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close() // nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body) // nolint:errcheck
		return "", fmt.Errorf("getRecord failed with status %d: %s", resp.StatusCode, string(body))
	}

	var record getRecordResponse
	if err := json.NewDecoder(resp.Body).Decode(&record); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}

	if record.Value == nil {
		return "", fmt.Errorf("record value is nil")
	}

	feedGen, ok := record.Value.Val.(*bsky.FeedGenerator)
	if !ok {
		return "", fmt.Errorf("record is not a feed generator")
	}

	return feedGen.Did, nil
}
