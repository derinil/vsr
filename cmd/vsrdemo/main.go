// Command vsrdemo runs a local key-value cluster, a client issuing random
// queries every few seconds, and the monitor dashboard in one process.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/derinil/vsr"
	"github.com/derinil/vsr/internal/kvservice"
	"github.com/derinil/vsr/internal/vsrmon"
)

func main() {
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
	replicas []string
	status   []string
	listen   string
	minWait  time.Duration
	maxWait  time.Duration
	keys     int
	timeout  time.Duration
}

func parseOptions(args []string) (options, error) {
	fs := flag.NewFlagSet("vsrdemo", flag.ExitOnError)
	n := fs.Int("replicas", 5, "number of replicas")
	port := fs.Int("port", vsr.DefaultPort, "first replica port; replica i listens on port+i")
	statusPort := fs.Int("status-port", 18080, "first status port; replica i serves GET /status on status-port+i")
	listen := fs.String("listen", ":8080", "dashboard listen address")
	minWait := fs.Duration("min-interval", time.Second, "minimum delay between queries")
	maxWait := fs.Duration("max-interval", 3*time.Second, "maximum delay between queries")
	keys := fs.Int("keys", 5, "number of distinct keys queried")
	timeout := fs.Duration("timeout", 5*time.Second, "time limit for one query")
	fs.Parse(args)

	switch {
	case *n < 1:
		return options{}, errors.New("-replicas must be positive")
	case *minWait <= 0 || *maxWait < *minWait:
		return options{}, errors.New("need 0 < -min-interval <= -max-interval")
	case *keys < 1:
		return options{}, errors.New("-keys must be positive")
	case *timeout <= 0:
		return options{}, errors.New("-timeout must be positive")
	}
	o := options{listen: *listen, minWait: *minWait, maxWait: *maxWait, keys: *keys, timeout: *timeout}
	for i := range *n {
		o.replicas = append(o.replicas, fmt.Sprintf("127.0.0.1:%d", *port+i))
		o.status = append(o.status, fmt.Sprintf("127.0.0.1:%d", *statusPort+i))
	}
	return o, nil
}

func run(ctx context.Context, o options, logger *slog.Logger) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var wg sync.WaitGroup
	defer wg.Wait()
	fail := func(err error) {
		if err != nil {
			cancel(err)
		}
	}

	urls := make([]string, len(o.status))
	for i, addr := range o.replicas {
		r, err := vsr.NewReplica(vsr.ReplicaOptions{
			Addr:     addr,
			Listen:   addr,
			HTTPAddr: o.status[i],
			Config:   o.replicas,
			Service:  kvservice.New(),
			Logger:   logger,
		})
		if err != nil {
			cancel(err)
			return err
		}
		wg.Go(func() { fail(r.Run(ctx)) })
		urls[i] = "http://" + o.status[i]
	}

	cl, err := vsr.NewClient(vsr.ClientOptions{Config: o.replicas})
	if err != nil {
		cancel(err)
		return err
	}
	defer cl.Close()

	mon := vsrmon.New(urls, 500*time.Millisecond, 300*time.Millisecond, logger)
	wg.Go(func() { fail(vsrmon.Serve(ctx, mon, o.listen, logger)) })
	wg.Go(func() { query(ctx, cl, o, logger) })

	<-ctx.Done()
	if err := context.Cause(ctx); !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func query(ctx context.Context, cl *vsr.Client, o options, logger *slog.Logger) {
	timer := time.NewTimer(wait(o))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		op, err := randomOp(o.keys)
		if err != nil {
			logger.Error("encode query", "err", err)
			return
		}
		qctx, cancel := context.WithTimeout(ctx, o.timeout)
		start := time.Now()
		b, err := cl.Invoke(qctx, op)
		cancel()
		latency := time.Since(start).Round(time.Microsecond)
		switch r, derr := kvservice.DecodeResult(b); {
		case err != nil:
			logger.Warn("query failed", "op", string(op), "err", err, "latency", latency)
		case derr != nil:
			logger.Warn("query result malformed", "op", string(op), "err", derr)
		default:
			logger.Info("query", "op", string(op), "code", r.Code, "value", string(r.Value), "error", r.Error, "latency", latency)
		}
		timer.Reset(wait(o))
	}
}

func wait(o options) time.Duration {
	return o.minWait + rand.N(o.maxWait-o.minWait+1)
}

func randomOp(keys int) ([]byte, error) {
	key := fmt.Sprintf("k%d", rand.N(keys))
	switch p := rand.N(10); {
	case p < 4:
		return kvservice.Read(key)
	case p < 8:
		return kvservice.Write(key, rand.N(100))
	default:
		return kvservice.CAS(key, rand.N(100), rand.N(100))
	}
}
