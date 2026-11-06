// Command vsrd runs a VR replica serving kvservice over TCP.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/derinil/vsr"
	"github.com/derinil/vsr/internal/kvservice"
)

func main() {
	opts, err := parseOptions(os.Args[1:], os.Getenv)
	if err != nil {
		fmt.Fprintln(os.Stderr, "vsrd:", err)
		os.Exit(2)
	}
	opts.Logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(opts); err != nil {
		opts.Logger.Error("vsrd", "err", err)
		os.Exit(1)
	}
}

func parseOptions(args []string, getenv func(string) string) (vsr.ReplicaOptions, error) {
	fs := flag.NewFlagSet("vsrd", flag.ExitOnError)
	addr := fs.String("addr", getenv("VSR_ADDR"), "this replica's address as it appears in -config (env VSR_ADDR)")
	listen := fs.String("listen", envOr(getenv, "VSR_LISTEN", fmt.Sprintf(":%d", vsr.DefaultPort)), "listen address (env VSR_LISTEN)")
	httpAddr := fs.String("http", getenv("VSR_HTTP"), "status HTTP listen address serving GET /status, empty disables (env VSR_HTTP)")
	config := fs.String("config", getenv("VSR_CONFIG"), "comma separated replica addresses in canonical order (env VSR_CONFIG)")
	data := fs.String("data", getenv("VSR_DATA"), "directory for state.json, empty disables persistence (env VSR_DATA)")
	recovering := fs.Bool("recover", false, "start with the recovery protocol")
	join := fs.Bool("join", envBool(getenv, "VSR_JOIN"), "start as a replica joining the cluster through a reconfiguration (env VSR_JOIN)")
	tick := fs.Duration("tick", 10*time.Millisecond, "logical clock tick")
	heartbeat := fs.Duration("heartbeat", 100*time.Millisecond, "primary heartbeat interval")
	viewChange := fs.Duration("view-change-timeout", 500*time.Millisecond, "backup timeout on primary silence, jittered up to 2x")
	resend := fs.Duration("resend", 200*time.Millisecond, "resend interval for view change and recovery messages")
	checkpointInterval := fs.Int("checkpoint-interval", envInt(getenv, "VSR_CHECKPOINT_INTERVAL"), "ops between checkpoints, 0 selects 1000, negative disables (env VSR_CHECKPOINT_INTERVAL)")
	checkpointRetain := fs.Int("checkpoint-retain", envInt(getenv, "VSR_CHECKPOINT_RETAIN"), "committed ops kept before log GC, 0 uses the checkpoint interval (env VSR_CHECKPOINT_RETAIN)")
	pageSize := fs.Int("page-size", envInt(getenv, "VSR_PAGE_SIZE"), "Merkle page size for checkpoint transfer, 0 selects 4096 (env VSR_PAGE_SIZE)")
	fs.Parse(args)

	cfg := splitList(*config)
	switch {
	case *addr == "":
		return vsr.ReplicaOptions{}, errors.New("-addr (VSR_ADDR) is required")
	case len(cfg) == 0:
		return vsr.ReplicaOptions{}, errors.New("-config (VSR_CONFIG) is required")
	}
	return vsr.ReplicaOptions{
		Addr:               *addr,
		Listen:             *listen,
		HTTPAddr:           *httpAddr,
		Config:             cfg,
		Service:            kvservice.New(),
		DataDir:            *data,
		Recovering:         *recovering,
		Join:               *join,
		Tick:               *tick,
		Heartbeat:          *heartbeat,
		ViewChangeTimeout:  *viewChange,
		Resend:             *resend,
		CheckpointInterval: *checkpointInterval,
		CheckpointRetain:   *checkpointRetain,
		PageSize:           *pageSize,
	}, nil
}

func run(o vsr.ReplicaOptions) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	s, err := vsr.NewReplica(o)
	if err != nil {
		return err
	}
	return s.Run(ctx)
}

func envOr(getenv func(string) string, key, fallback string) string {
	if v := getenv(key); v != "" {
		return v
	}
	return fallback
}

func envBool(getenv func(string) string, key string) bool {
	v, _ := strconv.ParseBool(getenv(key))
	return v
}

func envInt(getenv func(string) string, key string) int {
	v, _ := strconv.Atoi(getenv(key))
	return v
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
