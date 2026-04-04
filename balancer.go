package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
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
	vEndpoints   []*VirtualEndpoint
	directClient *http.Client
	config       *Config
	counter      atomic.Uint64
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
		vEndpoints:   veps,
		directClient: direct.Client,
		config:       cfg,
	}, nil
}

func (b *Balancer) pick() *VirtualEndpoint {
	now := time.Now().Unix()
	n := len(b.vEndpoints)
	start := int(b.counter.Add(1) % uint64(n))

	for i := 0; i < n; i++ {
		ve := b.vEndpoints[(start+i)%n]
		if !ve.healthy.Load() {
			continue
		}
		if ve.cooldown.Load() > now {
			continue
		}
		return ve
	}
	return b.vEndpoints[rand.Intn(n)]
}

func (b *Balancer) pickRandom(exclude *VirtualEndpoint) *VirtualEndpoint {
	now := time.Now().Unix()
	var candidates []*VirtualEndpoint
	for _, ve := range b.vEndpoints {
		if ve == exclude {
			continue
		}
		if !ve.healthy.Load() {
			continue
		}
		if ve.cooldown.Load() > now {
			continue
		}
		candidates = append(candidates, ve)
	}
	if len(candidates) == 0 {
		return b.vEndpoints[rand.Intn(len(b.vEndpoints))]
	}
	return candidates[rand.Intn(len(candidates))]
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

	maxRetries := b.config.MaxRetries
	var lastErr error
	var wasRateLimited bool

	var lastVE *VirtualEndpoint
	for attempt := 0; attempt < maxRetries; attempt++ {
		var ve *VirtualEndpoint
		if attempt == 0 {
			ve = b.pick()
		} else {
			ve = b.pickRandom(lastVE)
		}
		lastVE = ve

		start := time.Now()
		resp, err := b.forward(ve, body, r)
		elapsed := time.Since(start)

		if err != nil {
			lastErr = err
			slog.Warn("request failed",
				"url", ve.URL, "outbound", ve.Outbound.Name,
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
				"url", ve.URL, "outbound", ve.Outbound.Name,
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
			if rpcErr, retriable := checkRPCError(respBody); retriable {
				slog.Warn("rpc error (retrying)",
					"url", ve.URL, "outbound", ve.Outbound.Name,
					"code", rpcErr.Code, "message", rpcErr.Message,
					"ms", elapsed.Milliseconds(),
				)
				continue
			}
		}

		slog.Debug("request ok",
			"url", ve.URL, "outbound", ve.Outbound.Name,
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

	if lastErr != nil {
		http.Error(w, fmt.Sprintf("all endpoints failed: %v", lastErr), http.StatusBadGateway)
	} else if wasRateLimited {
		http.Error(w, "all endpoints rate limited", http.StatusTooManyRequests)
	} else {
		http.Error(w, "no available endpoints", http.StatusServiceUnavailable)
	}
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

	status := http.StatusOK
	if healthy == 0 {
		status = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{
		"healthy":   healthy,
		"total":     total,
		"endpoints": eps,
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
	checkBody := []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`)

	seen := make(map[string]bool)
	for _, ve := range b.vEndpoints {
		if seen[ve.URL] {
			continue
		}
		seen[ve.URL] = true

		req, err := http.NewRequest("POST", ve.URL, bytes.NewReader(checkBody))
		if err != nil {
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := b.directClient.Do(req)
		healthy := err == nil && resp != nil && resp.StatusCode == 200
		if resp != nil {
			resp.Body.Close()
		}

		for _, ve2 := range b.vEndpoints {
			if ve2.URL == ve.URL {
				ve2.healthy.Store(healthy)
			}
		}

		if healthy {
			slog.Debug("health ok", "url", ve.URL)
		} else {
			slog.Warn("health failed", "url", ve.URL, "error", err)
		}
	}
}
