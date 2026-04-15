package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

func main() {
	cfg := loadConfig()
	logger := newLogger()

	collector, err := NewCollector(cfg, logger)
	if err != nil {
		logger.Error("collector init failed", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go collector.Run(ctx)

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           buildRouter(cfg, collector, logger),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("http server started", "addr", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		logger.Info("shutdown signal received", "signal", sig.String())
	case err := <-errCh:
		logger.Error("server failed", "error", err)
	}

	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
	}
	logger.Info("service stopped")
}

func buildRouter(cfg Config, collector *Collector, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	limiter := NewRateLimiter(cfg.RatePerSecond, cfg.RateBurst, 5*time.Minute)

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "ts": time.Now().Unix()})
	})

	mux.HandleFunc("/flights", func(w http.ResponseWriter, r *http.Request) {
		if err := validateRapidAPI(r, cfg); err != nil {
			writeJSON(w, http.StatusUnauthorized, map[string]any{"error": err.Error()})
			return
		}
		key := r.Header.Get("X-RapidAPI-Key") + "|" + clientIP(r)
		if !limiter.Allow(key) {
			writeJSON(w, http.StatusTooManyRequests, map[string]any{"error": "rate limited"})
			return
		}

		snapshot := collector.GetSnapshot()
		resp := APIResponse{
			Count: len(snapshot),
			Data:  snapshot,
		}
		writeJSON(w, http.StatusOK, resp)
	})

	return loggingMiddleware(log, mux)
}

func validateRapidAPI(r *http.Request, cfg Config) error {
	if cfg.RapidAPIKey == "" {
		return nil
	}
	key := r.Header.Get("X-RapidAPI-Key")
	host := r.Header.Get("X-RapidAPI-Host")
	if key == "" || key != cfg.RapidAPIKey {
		return errors.New("invalid rapidapi key")
	}
	if cfg.RapidAPIHost != "" && host != cfg.RapidAPIHost {
		return errors.New("invalid rapidapi host")
	}
	return nil
}

func loadConfig() Config {
	return Config{
		ListenAddr:       getEnv("LISTEN_ADDR", ":8080"),
		ScrapeInterval:   getEnvDuration("SCRAPE_INTERVAL", 2*time.Second),
		Sources:          splitList(getEnv("SOURCES", "https://example.com/adsb.json")),
		Regions:          parseRegions(getEnv("REGIONS", "")),
		Proxies:          splitList(getEnv("PROXIES", "")),
		RapidAPIKey:      getEnv("RAPIDAPI_KEY", ""),
		RapidAPIHost:     getEnv("RAPIDAPI_HOST", ""),
		RatePerSecond:    getEnvFloat("RATE_PER_SECOND", 10),
		RateBurst:        getEnvInt("RATE_BURST", 20),
		CacheTTL:         getEnvDuration("CACHE_TTL", 30*time.Second),
		RequestTimeout:   getEnvDuration("REQUEST_TIMEOUT", 8*time.Second),
		ShutdownTimeout:  getEnvDuration("SHUTDOWN_TIMEOUT", 10*time.Second),
		MaxConcurrentJob: getEnvInt("MAX_CONCURRENT_JOB", 32),
	}
}

func newLogger() *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	return slog.New(handler)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func loggingMiddleware(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Info("request", "method", r.Method, "path", r.URL.Path, "latency_ms", time.Since(start).Milliseconds())
	})
}

type tokenBucket struct {
	mu      sync.Mutex
	tokens  float64
	lastRef time.Time
}

type RateLimiter struct {
	mu       sync.Mutex
	rps      float64
	burst    float64
	ttl      time.Duration
	buckets  map[string]*tokenBucket
	lastSeen map[string]time.Time
}

func NewRateLimiter(rps float64, burst int, ttl time.Duration) *RateLimiter {
	return &RateLimiter{
		rps:      rps,
		burst:    float64(maxInt(1, burst)),
		ttl:      ttl,
		buckets:  make(map[string]*tokenBucket),
		lastSeen: make(map[string]time.Time),
	}
}

func (l *RateLimiter) Allow(key string) bool {
	now := time.Now()

	l.mu.Lock()
	for k, seen := range l.lastSeen {
		if now.Sub(seen) > l.ttl {
			delete(l.lastSeen, k)
			delete(l.buckets, k)
		}
	}
	b, ok := l.buckets[key]
	if !ok {
		b = &tokenBucket{tokens: l.burst, lastRef: now}
		l.buckets[key] = b
	}
	l.lastSeen[key] = now
	l.mu.Unlock()

	b.mu.Lock()
	defer b.mu.Unlock()
	elapsed := now.Sub(b.lastRef).Seconds()
	b.tokens += elapsed * l.rps
	if b.tokens > l.burst {
		b.tokens = l.burst
	}
	b.lastRef = now
	if b.tokens < 1 {
		return false
	}
	b.tokens -= 1
	return true
}

func parseRegions(raw string) []Region {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	// region format: name:minLat,maxLat,minLon,maxLon;name2:...
	parts := strings.Split(raw, ";")
	out := make([]Region, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		kv := strings.SplitN(p, ":", 2)
		if len(kv) != 2 {
			continue
		}
		name := strings.TrimSpace(kv[0])
		nums := strings.Split(kv[1], ",")
		if len(nums) != 4 {
			continue
		}
		minLat, err1 := strconv.ParseFloat(strings.TrimSpace(nums[0]), 64)
		maxLat, err2 := strconv.ParseFloat(strings.TrimSpace(nums[1]), 64)
		minLon, err3 := strconv.ParseFloat(strings.TrimSpace(nums[2]), 64)
		maxLon, err4 := strconv.ParseFloat(strings.TrimSpace(nums[3]), 64)
		if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
			continue
		}
		out = append(out, Region{Name: name, MinLat: minLat, MaxLat: maxLat, MinLon: minLon, MaxLon: maxLon})
	}
	return out
}

func splitList(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func getEnv(key, def string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	return v
}

func getEnvInt(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func getEnvFloat(key string, def float64) float64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return f
}

func getEnvDuration(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func clientIP(r *http.Request) string {
	if xff := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); xff != "" {
		return strings.Split(xff, ",")[0]
	}
	host := r.RemoteAddr
	if i := strings.LastIndex(host, ":"); i > 0 {
		return host[:i]
	}
	return host
}
