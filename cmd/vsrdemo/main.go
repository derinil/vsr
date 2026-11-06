// Command vsrdemo runs a local SQLite cluster with one OS process per
// replica, spare replicas ready to join it, many concurrent clients driving
// load, a chaos agent injecting crashes, pauses, partitions and lossy links,
// and the monitor dashboard.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/derinil/vsr"
	"github.com/derinil/vsr/internal/vsrmon"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == nodeCommand {
		if err := runNode(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "vsrdemo node:", err)
			os.Exit(1)
		}
		return
	}
	o, err := parseOptions(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "vsrdemo:", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(ctx, o, logger); err != nil {
		logger.Error("vsrdemo", "err", err)
		os.Exit(1)
	}
}

type options struct {
	replicas   []string
	spares     []string
	status     []string
	listen     string
	clients    int
	keys       int
	timeout    time.Duration
	chaos      bool
	chaosEvery time.Duration
	report     time.Duration
}

func parseOptions(args []string) (options, error) {
	fs := flag.NewFlagSet("vsrdemo", flag.ExitOnError)
	n := fs.Int("replicas", 5, "number of replicas in the initial configuration")
	spares := fs.Int("spares", 2, "number of extra replicas waiting to join through a reconfiguration")
	port := fs.Int("port", vsr.DefaultPort, "first replica port; replica i, spares last, listens on port+i")
	statusPort := fs.Int("status-port", 18080, "first status port; replica i serves GET /status on status-port+i")
	listen := fs.String("listen", ":9999", "dashboard listen address")
	clients := fs.Int("clients", 64, "number of concurrent clients issuing queries back to back")
	keys := fs.Int("keys", 1000, "number of distinct shared keys queried")
	timeout := fs.Duration("timeout", 5*time.Second, "time limit for one query")
	chaos := fs.Bool("chaos", true, "inject crashes, pauses, partitions and lossy links")
	chaosEvery := fs.Duration("chaos-interval", 4*time.Second, "mean delay between injected faults")
	report := fs.Duration("report", 5*time.Second, "interval between load and per-node CPU and memory reports")
	fs.Parse(args)

	switch {
	case *n < 1:
		return options{}, errors.New("-replicas must be positive")
	case *spares < 0:
		return options{}, errors.New("-spares must not be negative")
	case *clients < 1:
		return options{}, errors.New("-clients must be positive")
	case *keys < 1:
		return options{}, errors.New("-keys must be positive")
	case *timeout <= 0:
		return options{}, errors.New("-timeout must be positive")
	case *chaosEvery <= 0:
		return options{}, errors.New("-chaos-interval must be positive")
	case *report <= 0:
		return options{}, errors.New("-report must be positive")
	}
	o := options{
		listen:     *listen,
		clients:    *clients,
		keys:       *keys,
		timeout:    *timeout,
		chaos:      *chaos,
		chaosEvery: *chaosEvery,
		report:     *report,
	}
	for i := range *n + *spares {
		addr := fmt.Sprintf("127.0.0.1:%d", *port+i)
		if i < *n {
			o.replicas = append(o.replicas, addr)
		} else {
			o.spares = append(o.spares, addr)
		}
		o.status = append(o.status, fmt.Sprintf("127.0.0.1:%d", *statusPort+i))
	}
	return o, nil
}

func run(ctx context.Context, o options, logger *slog.Logger) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "vsrdemo-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var wg sync.WaitGroup
	defer wg.Wait()

	c := newCluster(exe, dir, o, logger)
	for _, n := range c.nodes {
		wg.Go(func() { n.supervise(ctx) })
	}

	urls := make(map[string]string, len(c.nodes))
	for _, n := range c.nodes {
		urls[n.addr] = "http://" + n.http
	}
	nodes := make([]string, len(o.replicas))
	for i, addr := range o.replicas {
		nodes[i] = urls[addr]
	}
	mon := vsrmon.New(vsrmon.Options{
		Nodes:     nodes,
		Spares:    o.spares,
		StatusURL: func(addr string) string { return urls[addr] },
		Interval:  500 * time.Millisecond,
		Timeout:   300 * time.Millisecond,
		Logger:    logger,
	})
	wg.Go(func() {
		if err := vsrmon.Serve(ctx, mon, o.listen, logger); err != nil {
			cancel(err)
		}
	})

	stats := &loadStats{}
	for i := range o.clients {
		wg.Go(func() { worker(ctx, i, slices.Clone(o.replicas), o, stats, logger) })
	}
	wg.Go(func() { report(ctx, c, stats, o.report, logger) })
	if o.chaos {
		wg.Go(func() { chaos(ctx, c, o.chaosEvery, logger) })
	}

	<-ctx.Done()
	if err := context.Cause(ctx); !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
