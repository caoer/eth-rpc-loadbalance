package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type Config struct {
	Listen              string           `json:"listen"`
	Endpoints           []EndpointConfig `json:"endpoints"`
	ProxyDefaults       ProxyDefaults    `json:"proxy_defaults"`
	Proxies             []ProxyConfig    `json:"proxies"`
	HealthCheckInterval string           `json:"health_check_interval"`
	RateLimitCooldown   string           `json:"rate_limit_cooldown"`
	MaxRetries           int              `json:"max_retries"`
	CircuitBreakDuration string           `json:"circuit_break_duration"`
}

type EndpointConfig struct {
	URL    string `json:"url"`
	Weight int    `json:"weight"`
}

type ProxyDefaults struct {
	Method   string `json:"method"`
	Password string `json:"password"`
}

type ProxyConfig struct {
	Name     string `json:"name"`
	Server   string `json:"server"`
	Port     uint16 `json:"port"`
	Method   string `json:"method"`
	Password string `json:"password"`
}

func (c *Config) GetHealthCheckInterval() time.Duration {
	d, err := time.ParseDuration(c.HealthCheckInterval)
	if err != nil {
		return 30 * time.Second
	}
	return d
}

func (c *Config) GetRateLimitCooldown() time.Duration {
	d, err := time.ParseDuration(c.RateLimitCooldown)
	if err != nil {
		return 60 * time.Second
	}
	return d
}

func (c *Config) GetCircuitBreakDuration() time.Duration {
	d, err := time.ParseDuration(c.CircuitBreakDuration)
	if err != nil {
		return 60 * time.Second
	}
	return d
}

func LoadConfig(source string) (*Config, error) {
	var data []byte
	var err error

	if strings.HasPrefix(source, "http://") || strings.HasPrefix(source, "https://") {
		resp, err := http.Get(source)
		if err != nil {
			return nil, fmt.Errorf("fetch config: %w", err)
		}
		defer resp.Body.Close()
		data, err = io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("read config response: %w", err)
		}
	} else {
		data, err = os.ReadFile(source)
		if err != nil {
			return nil, fmt.Errorf("read config file: %w", err)
		}
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	if cfg.Listen == "" {
		cfg.Listen = ":8545"
	}
	if cfg.HealthCheckInterval == "" {
		cfg.HealthCheckInterval = "30s"
	}
	if cfg.RateLimitCooldown == "" {
		cfg.RateLimitCooldown = "60s"
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}
	if cfg.CircuitBreakDuration == "" {
		cfg.CircuitBreakDuration = "60s"
	}
	for i := range cfg.Endpoints {
		if cfg.Endpoints[i].Weight == 0 {
			cfg.Endpoints[i].Weight = 1
		}
	}

	// Apply proxy defaults
	for i := range cfg.Proxies {
		if cfg.Proxies[i].Method == "" {
			cfg.Proxies[i].Method = cfg.ProxyDefaults.Method
		}
		if cfg.Proxies[i].Password == "" {
			cfg.Proxies[i].Password = cfg.ProxyDefaults.Password
		}
	}

	return &cfg, nil
}
