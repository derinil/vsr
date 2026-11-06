// Command vsrmon serves a live dashboard of VSR replicas polled from their vsrd status endpoints.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const shutdownTimeout = 5 * time.Second

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
	nodes    []string
	listen   string
	interval time.Duration
	timeout  time.Duration
}

func parseOptions(args []string, getenv func(string) string) (options, error) {
	fs := flag.NewFlagSet("vsrmon", flag.ExitOnError)
	nodes := fs.String("nodes", getenv("VSR_MON_NODES"), "comma separated node base URLs or host:port (env VSR_MON_NODES)")
	listen := fs.String("listen", envOr(getenv, "VSR_MON_LISTEN", ":8080"), "dashboard listen address (env VSR_MON_LISTEN)")
	interval := fs.Duration("interval", 500*time.Millisecond, "poll interval for all nodes")
	timeout := fs.Duration("timeout", 300*time.Millisecond, "timeout for polling one node")
	fs.Parse(args)

	urls := normalizeNodes(splitList(*nodes))
	switch {
	case len(urls) == 0:
		return options{}, errors.New("-nodes (VSR_MON_NODES) is required")
	case *interval <= 0:
		return options{}, errors.New("-interval must be positive")
	case *timeout <= 0:
		return options{}, errors.New("-timeout must be positive")
	}
	return options{nodes: urls, listen: *listen, interval: *interval, timeout: *timeout}, nil
}

func run(o options, logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	m := newMonitor(o.nodes, o.interval, o.timeout, logger)
	handler, err := newHandler(m)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Addr:              o.listen,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}

	go m.run(ctx)
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	logger.Info("vsrmon listening", "addr", o.listen, "nodes", len(o.nodes))

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
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
