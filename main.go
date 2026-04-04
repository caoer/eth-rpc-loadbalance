package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	configPath := flag.String("config", "config.json", "config file path or URL")
	ssPassword := flag.String("ss-password", "", "override shadowsocks password/PSK (also reads SS_PASSWORD env)")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, error")
	flag.Parse()

	setupLogging(*logLevel)

	cfg, err := LoadConfig(*configPath)
	if err != nil {
		slog.Error("load config", "error", err)
		os.Exit(1)
	}

	// Override password from env/flag
	if pw := os.Getenv("SS_PASSWORD"); pw != "" {
		cfg.ProxyDefaults.Password = pw
	}
	if *ssPassword != "" {
		cfg.ProxyDefaults.Password = *ssPassword
	}

	lb, err := NewBalancer(cfg)
	if err != nil {
		slog.Error("init balancer", "error", err)
		os.Exit(1)
	}

	go lb.StartHealthCheck()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", lb.HandleHealth)
	mux.HandleFunc("/", lb.ServeHTTP)

	server := &http.Server{
		Addr:    cfg.Listen,
		Handler: mux,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		slog.Info("shutting down")
		server.Close()
	}()

	slog.Info("starting",
		"addr", cfg.Listen,
		"endpoints", len(cfg.Endpoints),
		"outbounds", len(cfg.Proxies)+1,
	)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}

func setupLogging(level string) {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l})))
}
