package pds

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
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

	return &appviewProxy{
		log:                 log.With("component", "appview-proxy"),
		backends:            backends,
		client:              &http.Client{Timeout: 15 * time.Second},
		healthCheckInterval: 15 * time.Second,
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
	backendURL, err := p.getHealthyBackend()
	if err != nil {
		return err
	}

	// parse the backend URL
	target, err := url.Parse(backendURL)
	if err != nil {
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
		return fmt.Errorf("failed to create proxy request: %w", err)
	}

	// copy headers from the original request
	for key, values := range r.Header {
		for _, value := range values {
			proxyReq.Header.Add(key, value)
		}
	}

	// forward the request
	resp, err := p.client.Do(proxyReq)
	if err != nil {
		return fmt.Errorf("proxy request failed: %w", err)
	}
	defer resp.Body.Close() // nolint:errcheck

	// copy response headers
	for key, values := range resp.Header {
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

	if err := s.appviewProxy.proxy(w, r); err != nil {
		s.log.Error("proxy error", "err", err, "path", r.URL.Path)
		s.internalErr(w, fmt.Errorf("proxy error: %w", err))
	}
}
