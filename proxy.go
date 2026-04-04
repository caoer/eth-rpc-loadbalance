package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"

	shadowsocks "github.com/sagernet/sing-shadowsocks"
	"github.com/sagernet/sing-shadowsocks/shadowaead"
	"github.com/sagernet/sing-shadowsocks/shadowaead_2022"
	M "github.com/sagernet/sing/common/metadata"
)

type Outbound struct {
	Name   string
	Client *http.Client
}

func NewDirectOutbound() *Outbound {
	return &Outbound{
		Name: "direct",
		Client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func NewSSOutbound(cfg ProxyConfig) (*Outbound, error) {
	method, err := newSSMethod(cfg.Method, cfg.Password)
	if err != nil {
		return nil, fmt.Errorf("create ss method %q: %w", cfg.Method, err)
	}

	dialer := &ssDialer{
		server: fmt.Sprintf("%s:%d", cfg.Server, cfg.Port),
		method: method,
	}

	transport := &http.Transport{
		DialContext:         dialer.DialContext,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
	}

	name := cfg.Name
	if name == "" {
		name = fmt.Sprintf("ss-%s:%d", cfg.Server, cfg.Port)
	}

	return &Outbound{
		Name: name,
		Client: &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
		},
	}, nil
}

func newSSMethod(method, password string) (shadowsocks.Method, error) {
	if strings.HasPrefix(method, "2022-") {
		return shadowaead_2022.NewWithPassword(method, password, nil)
	}
	return shadowaead.New(method, nil, password)
}

type ssDialer struct {
	server string
	method shadowsocks.Method
}

func (d *ssDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	var nd net.Dialer
	conn, err := nd.DialContext(ctx, "tcp", d.server)
	if err != nil {
		return nil, fmt.Errorf("dial ss server %s: %w", d.server, err)
	}

	dest, err := parseSocksaddr(addr)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return d.method.DialEarlyConn(conn, dest), nil
}

func parseSocksaddr(addr string) (M.Socksaddr, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return M.Socksaddr{}, fmt.Errorf("parse addr %q: %w", addr, err)
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return M.Socksaddr{}, fmt.Errorf("parse port %q: %w", portStr, err)
	}

	if ip, err := netip.ParseAddr(host); err == nil {
		return M.Socksaddr{Addr: ip, Port: uint16(port)}, nil
	}
	return M.Socksaddr{Fqdn: host, Port: uint16(port)}, nil
}
