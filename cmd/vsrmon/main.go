// Command vsrmon serves a live dashboard of VSR replicas polled from their vsrd status endpoints.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/derinil/vsr/internal/vsrmon"
)

func main() {
	opts, err := parseOptions(os.Args[1:], os.Getenv)
	if err != nil {
		fmt.Fprintln(os.Stderr, "vsrmon:", err)
		os.Exit(2)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(opts, logger); err != nil {
		logger.Error("vsrmon", "err", err)
		os.Exit(1)
	}
}

type options struct {
	nodes      []string
	spares     []string
	statusPort int
	listen     string
	interval   time.Duration
	timeout    time.Duration
}

func parseOptions(args []string, getenv func(string) string) (options, error) {
	fs := flag.NewFlagSet("vsrmon", flag.ExitOnError)
	nodes := fs.String("nodes", getenv("VSR_MON_NODES"), "comma separated node base URLs or host:port (env VSR_MON_NODES)")
	listen := fs.String("listen", envOr(getenv, "VSR_MON_LISTEN", ":8080"), "dashboard listen address (env VSR_MON_LISTEN)")
	interval := fs.Duration("interval", 500*time.Millisecond, "poll interval for all nodes")
	timeout := fs.Duration("timeout", 300*time.Millisecond, "timeout for polling one node")
	spares := fs.String("spares", getenv("VSR_MON_SPARES"), "comma separated replica addresses offered for adding to the configuration (env VSR_MON_SPARES)")
	statusPort := fs.Int("status-port", envInt(getenv, "VSR_MON_STATUS_PORT"), "status port of replicas, used to poll members added by reconfiguration; 0 disables (env VSR_MON_STATUS_PORT)")
	fs.Parse(args)

	urls := normalizeNodes(splitList(*nodes))
	switch {
	case len(urls) == 0:
		return options{}, errors.New("-nodes (VSR_MON_NODES) is required")
	case *interval <= 0:
		return options{}, errors.New("-interval must be positive")
	case *timeout <= 0:
		return options{}, errors.New("-timeout must be positive")
	case *statusPort < 0 || *statusPort > 65535:
		return options{}, errors.New("-status-port must be a port number")
	}
	return options{nodes: urls, spares: splitList(*spares), statusPort: *statusPort, listen: *listen, interval: *interval, timeout: *timeout}, nil
}

func run(o options, logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	m := vsrmon.New(vsrmon.Options{
		Nodes:     o.nodes,
		Spares:    o.spares,
		StatusURL: statusURL(o.statusPort),
		Interval:  o.interval,
		Timeout:   o.timeout,
		Logger:    logger,
	})
	return vsrmon.Serve(ctx, m, o.listen, logger)
}

func statusURL(port int) func(string) string {
	if port == 0 {
		return nil
	}
	return func(addr string) string {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return ""
		}
		return "http://" + net.JoinHostPort(host, strconv.Itoa(port))
	}
}

func envInt(getenv func(string) string, key string) int {
	v, _ := strconv.Atoi(getenv(key))
	return v
}

func normalizeNodes(items []string) []string {
	out := make([]string, 0, len(items))
	for _, s := range items {
		if !strings.Contains(s, "://") {
			s = "http://" + s
		}
		out = append(out, strings.TrimSuffix(s, "/"))
	}
	return out
}

func envOr(getenv func(string) string, key, fallback string) string {
	if v := getenv(key); v != "" {
		return v
	}
	return fallback
}

func splitList(s string) []string {
	var out []string
	for item := range strings.SplitSeq(s, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}
