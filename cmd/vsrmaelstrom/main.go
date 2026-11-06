package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/derinil/vsr/vsr"
	"github.com/derinil/vsr/vsrclient"
	"github.com/derinil/vsr/vsrtcp"
)

const defaultClusterList = "127.0.0.1:10188,127.0.0.1:10189,127.0.0.1:10190"

type config struct {
	cluster        []string
	timeout        time.Duration
	requestTimeout time.Duration
}

func main() {
	cfg, err := loadConfig(os.Args[1:], os.Getenv)
	if err != nil {
		slog.Error("invalid configuration", "err", err)
		os.Exit(2)
	}
	p, err := newPool(vsrclient.Options{
		Config: cfg.cluster,
		NewTransport: func(h vsr.Handler) vsr.Transport {
			return vsrtcp.New(h, vsrtcp.Options{})
		},
		RequestTimeout: cfg.requestTimeout,
	})
	if err != nil {
		slog.Error("create client", "err", err)
		os.Exit(1)
	}
	n := newNode(os.Stdout, cfg.timeout, p.invoke)
	err = run(os.Stdin, n)
	p.close()
	if err != nil {
		slog.Error("read stdin", "err", err)
		os.Exit(1)
	}
}

func loadConfig(args []string, getenv func(string) string) (config, error) {
	fs := flag.NewFlagSet("vsrmaelstrom", flag.ContinueOnError)
	cluster := fs.String("cluster", envOr(getenv, "VSR_CLUSTER", defaultClusterList),
		"comma separated replica addresses in canonical configuration order")
	timeout := fs.Duration("timeout", time.Second, "per-operation deadline")
	requestTimeout := fs.Duration("request-timeout", 200*time.Millisecond, "vsrclient request timeout")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	addrs := splitList(*cluster)
	if len(addrs) == 0 {
		return config{}, errors.New("empty cluster")
	}
	return config{cluster: addrs, timeout: *timeout, requestTimeout: *requestTimeout}, nil
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

func run(in io.Reader, n *node) error {
	r := bufio.NewReader(in)
	for {
		line, err := r.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) > 0 {
			n.handle(line)
		}
		if err != nil {
			n.wait()
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("read: %w", err)
		}
	}
}
