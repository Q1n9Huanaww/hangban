package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ProxyRotator struct {
	proxies []*url.URL
	idx     uint64
}

func NewProxyRotator(raw []string) (*ProxyRotator, error) {
	r := &ProxyRotator{}
	for _, item := range raw {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		u, err := url.Parse(item)
		if err != nil {
			return nil, fmt.Errorf("invalid proxy %q: %w", item, err)
		}
		r.proxies = append(r.proxies, u)
	}
	return r, nil
}

func (r *ProxyRotator) Next() *url.URL {
	if r == nil || len(r.proxies) == 0 {
		return nil
	}
	i := atomic.AddUint64(&r.idx, 1)
	return r.proxies[(i-1)%uint64(len(r.proxies))]
}

type Signer interface {
	Apply(req *http.Request) error
}

type NoopSigner struct{}

func (NoopSigner) Apply(_ *http.Request) error { return nil }

type DedupeCache struct {
	mu    sync.RWMutex
	items map[string]int64
	ttl   time.Duration
}

func NewDedupeCache(ttl time.Duration) *DedupeCache {
	return &DedupeCache{
		items: make(map[string]int64, 10000),
		ttl:   ttl,
	}
}

func (d *DedupeCache) SeenOrAdd(fd FlightData) bool {
	key := fmt.Sprintf("%s:%d", strings.ToLower(fd.ICAO24), fd.Timestamp)
	now := time.Now().Unix()
	cutoff := time.Now().Add(-d.ttl).Unix()

	d.mu.Lock()
	defer d.mu.Unlock()
	for k, ts := range d.items {
		if ts < cutoff {
			delete(d.items, k)
		}
	}
	if _, exists := d.items[key]; exists {
		return true
	}
	d.items[key] = now
	return false
}

type Collector struct {
	cfg      Config
	client   *http.Client
	rotator  *ProxyRotator
	signer   Signer
	dedupe   *DedupeCache
	log      *slog.Logger
	lastData atomic.Pointer[[]FlightData]
}

func NewCollector(cfg Config, log *slog.Logger) (*Collector, error) {
	rotator, err := NewProxyRotator(cfg.Proxies)
	if err != nil {
		return nil, err
	}

	tr := &http.Transport{
		Proxy: func(_ *http.Request) (*url.URL, error) {
			return rotator.Next(), nil
		},
		MaxIdleConns:        256,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     60 * time.Second,
		TLSHandshakeTimeout: 8 * time.Second,
		ForceAttemptHTTP2:   true,
		DialContext: (&net.Dialer{
			Timeout:   8 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}

	c := &Collector{
		cfg:     cfg,
		client:  &http.Client{Transport: tr, Timeout: cfg.RequestTimeout},
		rotator: rotator,
		signer:  NoopSigner{},
		dedupe:  NewDedupeCache(cfg.CacheTTL),
		log:     log,
	}

	empty := []FlightData{}
	c.lastData.Store(&empty)
	return c, nil
}

func (c *Collector) Run(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.ScrapeInterval)
	defer ticker.Stop()

	c.scrapeOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.scrapeOnce(ctx)
		}
	}
}

func (c *Collector) GetSnapshot() []FlightData {
	ptr := c.lastData.Load()
	if ptr == nil {
		return nil
	}
	src := *ptr
	out := make([]FlightData, len(src))
	copy(out, src)
	return out
}

func (c *Collector) scrapeOnce(ctx context.Context) {
	type result struct {
		data []FlightData
		err  error
	}

	jobs := make(chan string)
	results := make(chan result, len(c.cfg.Sources)*maxInt(len(c.cfg.Regions), 1))
	var wg sync.WaitGroup

	workers := c.cfg.MaxConcurrentJob
	if workers < 1 {
		workers = 1
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for endpoint := range jobs {
				data, err := c.fetchEndpoint(ctx, endpoint)
				results <- result{data: data, err: err}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, source := range c.cfg.Sources {
			if len(c.cfg.Regions) == 0 {
				jobs <- source
				continue
			}
			for _, r := range c.cfg.Regions {
				u, err := url.Parse(source)
				if err != nil {
					continue
				}
				q := u.Query()
				q.Set("minLat", fmt.Sprintf("%.6f", r.MinLat))
				q.Set("maxLat", fmt.Sprintf("%.6f", r.MaxLat))
				q.Set("minLon", fmt.Sprintf("%.6f", r.MinLon))
				q.Set("maxLon", fmt.Sprintf("%.6f", r.MaxLon))
				q.Set("region", r.Name)
				u.RawQuery = q.Encode()
				jobs <- u.String()
			}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	var merged []FlightData
	var okCount int
	for r := range results {
		if r.err != nil {
			c.log.Warn("scrape endpoint failed", "error", r.err)
			continue
		}
		okCount++
		merged = append(merged, r.data...)
	}

	filtered := c.filterFlights(merged)
	c.lastData.Store(&filtered)
	c.log.Info("scrape cycle done", "sources_ok", okCount, "raw", len(merged), "filtered", len(filtered))
}

func (c *Collector) fetchEndpoint(ctx context.Context, endpoint string) ([]FlightData, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	applyBrowserLikeHeaders(req)
	if err := c.signer.Apply(req); err != nil {
		return nil, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("http status %d", resp.StatusCode)
	}
	return parseFlightPayload(resp)
}

func (c *Collector) filterFlights(in []FlightData) []FlightData {
	out := make([]FlightData, 0, len(in))
	for _, fd := range in {
		if !isFlightValid(fd) {
			continue
		}
		if c.dedupe.SeenOrAdd(fd) {
			continue
		}
		out = append(out, fd)
	}
	return out
}

func applyBrowserLikeHeaders(req *http.Request) {
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Pragma", "no-cache")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
	req.Header.Set("Sec-CH-UA", `"Chromium";v="125", "Not.A/Brand";v="24", "Google Chrome";v="125"`)
	req.Header.Set("Sec-CH-UA-Mobile", "?0")
	req.Header.Set("Sec-CH-UA-Platform", `"Windows"`)
	req.Header.Set("Sec-Fetch-Dest", "empty")
	req.Header.Set("Sec-Fetch-Mode", "cors")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
}

func parseFlightPayload(resp *http.Response) ([]FlightData, error) {
	var envelope map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return nil, err
	}

	raw, ok := envelope["ac"]
	if !ok {
		raw, ok = envelope["aircraft"]
	}
	if !ok {
		return nil, errors.New("payload missing ac/aircraft field")
	}

	arr, ok := raw.([]any)
	if !ok {
		return nil, errors.New("ac/aircraft is not an array")
	}

	out := make([]FlightData, 0, len(arr))
	for _, item := range arr {
		obj, ok := item.(map[string]any)
		if !ok {
			continue
		}
		fd := FlightData{
			ICAO24:    asString(obj["hex"], obj["icao24"]),
			Callsign:  strings.TrimSpace(asString(obj["flight"], obj["call"], obj["callsign"])),
			Alt:       asInt(obj["alt_baro"], obj["altitude"], obj["alt"]),
			Spd:       asFloat(obj["gs"], obj["spd"], obj["speed"]),
			Lat:       asFloat(obj["lat"]),
			Lon:       asFloat(obj["lon"], obj["lng"]),
			Squawk:    asString(obj["squawk"]),
			Timestamp: asInt64(obj["seen_pos"], obj["timestamp"], obj["ts"]),
			Source:    asString(obj["source"], resp.Request.URL.Host),
		}
		if fd.Timestamp < 1_000_000_000 {
			fd.Timestamp = time.Now().Unix()
		}
		out = append(out, fd)
	}
	return out, nil
}

func isFlightValid(fd FlightData) bool {
	if len(fd.ICAO24) < 6 || len(fd.ICAO24) > 8 {
		return false
	}
	if fd.Lat < -90 || fd.Lat > 90 || fd.Lon < -180 || fd.Lon > 180 {
		return false
	}
	if fd.Alt < -1200 || fd.Alt > 65000 {
		return false
	}
	if math.IsNaN(fd.Spd) || fd.Spd < 0 || fd.Spd > 1500 {
		return false
	}
	return true
}

func asString(values ...any) string {
	for _, v := range values {
		switch x := v.(type) {
		case string:
			if strings.TrimSpace(x) != "" {
				return x
			}
		case float64:
			return strconv.FormatFloat(x, 'f', -1, 64)
		case int64:
			return strconv.FormatInt(x, 10)
		case int:
			return strconv.Itoa(x)
		}
	}
	return ""
}

func asInt(values ...any) int {
	return int(asInt64(values...))
}

func asInt64(values ...any) int64 {
	for _, v := range values {
		switch x := v.(type) {
		case float64:
			return int64(x)
		case int64:
			return x
		case int:
			return int64(x)
		case string:
			if x == "" {
				continue
			}
			i, err := strconv.ParseInt(x, 10, 64)
			if err == nil {
				return i
			}
			f, err := strconv.ParseFloat(x, 64)
			if err == nil {
				return int64(f)
			}
		}
	}
	return 0
}

func asFloat(values ...any) float64 {
	for _, v := range values {
		switch x := v.(type) {
		case float64:
			return x
		case int64:
			return float64(x)
		case int:
			return float64(x)
		case string:
			if x == "" {
				continue
			}
			f, err := strconv.ParseFloat(x, 64)
			if err == nil {
				return f
			}
		}
	}
	return 0
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

