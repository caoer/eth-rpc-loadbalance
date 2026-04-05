package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type VirtualEndpoint struct {
	URL      string
	Outbound *Outbound
	healthy  atomic.Bool
	cooldown atomic.Int64 // unix timestamp
}

type Balancer struct {
	vEndpoints    []*VirtualEndpoint
	directClient  *http.Client
	config        *Config
	counter       atomic.Uint64
	methodCircuit *methodCircuitBreaker
}

// Per-method circuit breaker tracks method+endpoint failures.
type circuitKey struct {
	url    string
	method string
}

type methodCircuitBreaker struct {
	mu      sync.RWMutex
	tripped map[circuitKey]int64 // expiry unix timestamp
}

func newMethodCircuitBreaker() *methodCircuitBreaker {
	return &methodCircuitBreaker{
		tripped: make(map[circuitKey]int64),
	}
}

func (cb *methodCircuitBreaker) trip(url, method string, duration time.Duration) {
	key := circuitKey{url, method}
	cb.mu.Lock()
	cb.tripped[key] = time.Now().Add(duration).Unix()
	cb.mu.Unlock()
	slog.Info("circuit break", "url", url, "method", method, "duration", duration)
}

func (cb *methodCircuitBreaker) isOpen(url, method string) bool {
	cb.mu.RLock()
	expiry, ok := cb.tripped[circuitKey{url, method}]
	cb.mu.RUnlock()
	return ok && time.Now().Unix() < expiry
}

func (cb *methodCircuitBreaker) cleanup() {
	cb.mu.Lock()
	now := time.Now().Unix()
	for k, expiry := range cb.tripped {
		if now >= expiry {
			delete(cb.tripped, k)
		}
	}
	cb.mu.Unlock()
}

func (cb *methodCircuitBreaker) active() []circuitKey {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	now := time.Now().Unix()
	var keys []circuitKey
	for k, expiry := range cb.tripped {
		if now < expiry {
			keys = append(keys, k)
		}
	}
	return keys
}

// Cacheable RPC methods whose responses may have stale IDs from upstream caches.
var cacheableRPCMethods = map[string]bool{
	"eth_chainId":        true,
	"net_version":        true,
	"web3_clientVersion": true,
}

// fixResponseID replaces the "id" in respBody with the "id" from reqBody.
func fixResponseID(reqBody, respBody []byte) []byte {
	var req struct {
		ID json.RawMessage `json:"id"`
	}
	if json.Unmarshal(reqBody, &req) != nil || req.ID == nil {
		return respBody
	}
	var resp map[string]json.RawMessage
	if json.Unmarshal(respBody, &resp) != nil {
		return respBody
	}
	if bytes.Equal(resp["id"], req.ID) {
		return respBody
	}
	resp["id"] = req.ID
	fixed, err := json.Marshal(resp)
	if err != nil {
		return respBody
	}
	return fixed
}

func NewBalancer(cfg *Config) (*Balancer, error) {
	direct := NewDirectOutbound()
	outbounds := []*Outbound{direct}

	for _, pc := range cfg.Proxies {
		ob, err := NewSSOutbound(pc)
		if err != nil {
			return nil, fmt.Errorf("create proxy %s: %w", pc.Name, err)
		}
		outbounds = append(outbounds, ob)
	}

	var veps []*VirtualEndpoint
	for _, ec := range cfg.Endpoints {
		for _, ob := range outbounds {
			ve := &VirtualEndpoint{
				URL:      ec.URL,
				Outbound: ob,
			}
			ve.healthy.Store(true)
			veps = append(veps, ve)
		}
	}

	slog.Info("balancer ready",
		"virtual_endpoints", len(veps),
		"endpoints", len(cfg.Endpoints),
		"outbounds", len(outbounds),
	)

	return &Balancer{
		vEndpoints:    veps,
		directClient:  direct.Client,
		config:        cfg,
		methodCircuit: newMethodCircuitBreaker(),
	}, nil
}

// uniqueURLs returns the number of distinct backend URLs.
func (b *Balancer) uniqueURLs() int {
	seen := make(map[string]bool)
	for _, ve := range b.vEndpoints {
		seen[ve.URL] = true
	}
	return len(seen)
}

// pick selects a virtual endpoint, excluding URLs already tried for this request.
func (b *Balancer) pick(method string, triedURLs map[string]bool) *VirtualEndpoint {
	now := time.Now().Unix()
	n := len(b.vEndpoints)
	start := int(b.counter.Add(1) % uint64(n))

	// Best: healthy, not cooled down, not circuit-broken, not tried
	for i := 0; i < n; i++ {
		ve := b.vEndpoints[(start+i)%n]
		if triedURLs[ve.URL] {
			continue
		}
		if !ve.healthy.Load() {
			continue
		}
		if ve.cooldown.Load() > now {
			continue
		}
		if method != "" && b.methodCircuit.isOpen(ve.URL, method) {
			continue
		}
		return ve
	}
	// Fallback 1: not tried, not circuit-broken (allow unhealthy/cooldown)
	var candidates []*VirtualEndpoint
	for i := 0; i < n; i++ {
		ve := b.vEndpoints[(start+i)%n]
		if triedURLs[ve.URL] {
			continue
		}
		if method != "" && b.methodCircuit.isOpen(ve.URL, method) {
			continue
		}
		candidates = append(candidates, ve)
	}
	if len(candidates) > 0 {
		return candidates[rand.Intn(len(candidates))]
	}
	// Fallback 2: not tried (allow everything)
	for i := 0; i < n; i++ {
		ve := b.vEndpoints[(start+i)%n]
		if !triedURLs[ve.URL] {
			candidates = append(candidates, ve)
		}
	}
	if len(candidates) > 0 {
		return candidates[rand.Intn(len(candidates))]
	}
	// All URLs tried, pick any healthy one
	for i := 0; i < n; i++ {
		ve := b.vEndpoints[(start+i)%n]
		if ve.healthy.Load() && ve.cooldown.Load() <= now {
			return ve
		}
	}
	return b.vEndpoints[rand.Intn(n)]
}

func (b *Balancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body failed", http.StatusBadRequest)
		return
	}

	method := parseRPCMethod(body)

	// Retry at least once per unique endpoint URL so every backend gets a chance
	maxRetries := b.uniqueURLs()
	if cfg := b.config.MaxRetries; cfg > maxRetries {
		maxRetries = cfg
	}
	var lastErr error
	var wasRateLimited bool
	var lastRespBody []byte
	triedURLs := make(map[string]bool)

	for attempt := 0; attempt < maxRetries; attempt++ {
		ve := b.pick(method, triedURLs)
		triedURLs[ve.URL] = true

		start := time.Now()
		resp, err := b.forward(ve, body, r)
		elapsed := time.Since(start)

		if err != nil {
			lastErr = err
			slog.Warn("request failed",
				"method", method, "url", ve.URL, "outbound", ve.Outbound.Name,
				"error", err, "ms", elapsed.Milliseconds(),
			)
			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			cooldown := b.config.GetRateLimitCooldown()
			ve.cooldown.Store(time.Now().Add(cooldown).Unix())
			resp.Body.Close()
			wasRateLimited = true
			slog.Warn("rate limited",
				"method", method, "url", ve.URL, "outbound", ve.Outbound.Name,
				"cooldown", cooldown,
			)
			continue
		}

		// Read response body to check for JSON-RPC errors
		respBody, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode == http.StatusOK {
			if rpcErr, retriable := checkRPCError(respBody); rpcErr != nil {
				// Circuit-break and retry on retriable errors or method not found
				if retriable || rpcErr.Code == -32601 {
					if method != "" {
						b.methodCircuit.trip(ve.URL, method, b.config.GetCircuitBreakDuration())
					}
					slog.Warn("rpc error (retrying)",
						"method", method, "url", ve.URL, "outbound", ve.Outbound.Name,
						"code", rpcErr.Code, "message", rpcErr.Message,
						"ms", elapsed.Milliseconds(),
					)
					lastRespBody = respBody
					continue
				}
			}
		}

		// Fix response ID for cacheable methods (upstream may cache full response including ID)
		if cacheableRPCMethods[method] {
			respBody = fixResponseID(body, respBody)
		}

		slog.Debug("request ok",
			"method", method, "url", ve.URL, "outbound", ve.Outbound.Name,
			"status", resp.StatusCode, "ms", elapsed.Milliseconds(),
		)

		for k, vv := range resp.Header {
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		w.Write(respBody)
		return
	}

	// All retries exhausted — return the best available response
	if lastRespBody != nil {
		if cacheableRPCMethods[method] {
			lastRespBody = fixResponseID(body, lastRespBody)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(lastRespBody)
	} else if lastErr != nil {
		http.Error(w, fmt.Sprintf("all endpoints failed: %v", lastErr), http.StatusBadGateway)
	} else if wasRateLimited {
		http.Error(w, "all endpoints rate limited", http.StatusTooManyRequests)
	} else {
		http.Error(w, "no available endpoints", http.StatusServiceUnavailable)
	}
}

// parseRPCMethod extracts the "method" field from a JSON-RPC request body.
// Returns empty string for batch requests (JSON arrays) or parse failures.
func parseRPCMethod(body []byte) string {
	// Quick check: skip batch requests (arrays)
	for _, c := range body {
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			continue
		}
		if c == '[' {
			return ""
		}
		break
	}
	var req struct {
		Method string `json:"method"`
	}
	if json.Unmarshal(body, &req) != nil {
		return ""
	}
	return req.Method
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// checkRPCError parses the response body for a JSON-RPC error.
// Returns the error and whether it's retriable on a different endpoint.
func checkRPCError(body []byte) (*rpcError, bool) {
	var resp struct {
		Error *rpcError `json:"error"`
	}
	if json.Unmarshal(body, &resp) != nil || resp.Error == nil {
		return nil, false
	}
	e := resp.Error
	// Server errors: -32000 to -32099 (node-specific, worth retrying)
	if e.Code <= -32000 && e.Code >= -32099 {
		return e, true
	}
	// Internal error
	if e.Code == -32603 {
		return e, true
	}
	// Non-retriable: parse error (-32700), invalid request (-32600),
	// method not found (-32601), invalid params (-32602)
	return e, false
}

func (b *Balancer) forward(ve *VirtualEndpoint, body []byte, orig *http.Request) (*http.Response, error) {
	req, err := http.NewRequestWithContext(orig.Context(), "POST", ve.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return ve.Outbound.Client.Do(req)
}

func (b *Balancer) HandleHealth(w http.ResponseWriter, r *http.Request) {
	type epStatus struct {
		URL      string `json:"url"`
		Outbound string `json:"outbound"`
		Healthy  bool   `json:"healthy"`
		Cooldown int64  `json:"cooldown_until,omitempty"`
	}

	now := time.Now().Unix()
	var eps []epStatus
	healthy, total := 0, 0
	for _, ve := range b.vEndpoints {
		total++
		h := ve.healthy.Load() && ve.cooldown.Load() <= now
		if h {
			healthy++
		}
		eps = append(eps, epStatus{
			URL:      ve.URL,
			Outbound: ve.Outbound.Name,
			Healthy:  ve.healthy.Load(),
			Cooldown: ve.cooldown.Load(),
		})
	}

	type cbStatus struct {
		URL    string `json:"url"`
		Method string `json:"method"`
	}
	var circuits []cbStatus
	for _, k := range b.methodCircuit.active() {
		circuits = append(circuits, cbStatus{URL: k.url, Method: k.method})
	}

	status := http.StatusOK
	if healthy == 0 {
		status = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{
		"healthy":        healthy,
		"total":          total,
		"endpoints":      eps,
		"circuit_breaks": circuits,
	})
}

func (b *Balancer) StartHealthCheck() {
	interval := b.config.GetHealthCheckInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	b.runHealthChecks()
	for range ticker.C {
		b.runHealthChecks()
	}
}

func (b *Balancer) runHealthChecks() {
	b.methodCircuit.cleanup()

	const healthCheckID = 99999
	checkBody := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":%d}`, healthCheckID))

	seen := make(map[string]bool)
	for _, ve := range b.vEndpoints {
		if seen[ve.URL] {
			continue
		}
		seen[ve.URL] = true

		healthy := false
		reason := ""

		req, err := http.NewRequest("POST", ve.URL, bytes.NewReader(checkBody))
		if err != nil {
			reason = err.Error()
		} else {
			req.Header.Set("Content-Type", "application/json")
			resp, err := b.directClient.Do(req)
			if err != nil {
				reason = err.Error()
			} else {
				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					reason = err.Error()
				} else if resp.StatusCode != 200 {
					reason = fmt.Sprintf("status %d", resp.StatusCode)
				} else {
					var rpcResp struct {
						ID json.RawMessage `json:"id"`
					}
					if json.Unmarshal(body, &rpcResp) != nil {
						reason = "invalid json response"
					} else {
						var respID int
						if json.Unmarshal(rpcResp.ID, &respID) != nil || respID != healthCheckID {
							reason = fmt.Sprintf("id mismatch: sent %d, got %s", healthCheckID, string(rpcResp.ID))
						} else {
							healthy = true
						}
					}
				}
			}
		}

		for _, ve2 := range b.vEndpoints {
			if ve2.URL == ve.URL {
				ve2.healthy.Store(healthy)
			}
		}

		if healthy {
			slog.Debug("health ok", "url", ve.URL)
		} else {
			slog.Warn("health failed", "url", ve.URL, "reason", reason)
		}
	}
}
