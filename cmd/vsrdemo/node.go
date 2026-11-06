package main

import (
	"bufio"
	"context"
	"flag"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/derinil/vsr"
	"github.com/derinil/vsr/internal/kvservice"
	"github.com/derinil/vsr/internal/vsrtcp"
	"github.com/derinil/vsr/message"
)

const nodeCommand = "node"

func runNode(args []string) error {
	fs := flag.NewFlagSet("vsrdemo node", flag.ExitOnError)
	addr := fs.String("addr", "", "replica address")
	httpAddr := fs.String("http", "", "status listen address")
	config := fs.String("config", "", "comma separated initial configuration")
	data := fs.String("data", "", "data directory")
	join := fs.Bool("join", false, "join through a reconfiguration")
	fs.Parse(args)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	f := &faults{}
	go func() {
		f.control(os.Stdin)
		cancel()
	}()
	r, err := vsr.NewReplica(vsr.ReplicaOptions{
		Addr:         *addr,
		Listen:       *addr,
		HTTPAddr:     *httpAddr,
		Config:       strings.Split(*config, ","),
		Service:      kvservice.New(),
		DataDir:      *data,
		Join:         *join,
		Logger:       slog.New(slog.NewTextHandler(os.Stderr, nil)),
		NewTransport: f.wrap,
	})
	if err != nil {
		return err
	}
	return r.Run(ctx)
}

// faults drops and delays outbound messages as instructed on the control stream:
// "block <addr>..." drops everything sent to those replicas, and
// "net <loss> <delay>" drops a loss fraction of all messages and delays the rest
// by delay/2 up to 3*delay/2.
type faults struct {
	*vsrtcp.Transport

	mu      sync.RWMutex
	blocked map[string]bool
	loss    float64
	delay   time.Duration
}

func (f *faults) wrap(h vsr.Handler) vsr.Transport {
	f.Transport = vsrtcp.New(h, vsrtcp.Options{})
	return f
}

func (f *faults) control(r io.Reader) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 0 {
			continue
		}
		f.mu.Lock()
		switch fields[0] {
		case "block":
			f.blocked = make(map[string]bool, len(fields)-1)
			for _, addr := range fields[1:] {
				f.blocked[addr] = true
			}
		case "net":
			if len(fields) == 3 {
				f.loss, _ = strconv.ParseFloat(fields[1], 64)
				f.delay, _ = time.ParseDuration(fields[2])
			}
		}
		f.mu.Unlock()
	}
}

func (f *faults) fate(to string) (time.Duration, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.blocked[to] || rand.Float64() < f.loss {
		return 0, false
	}
	if f.delay <= 0 {
		return 0, true
	}
	return f.delay/2 + rand.N(f.delay), true
}

func (f *faults) Send(to string, m message.Message) {
	if d, ok := f.fate(to); ok {
		after(d, func() { f.Transport.Send(to, m) })
	}
}

func (f *faults) Reply(client uint64, m message.Message) {
	if d, ok := f.fate(""); ok {
		after(d, func() { f.Transport.Reply(client, m) })
	}
}

func after(d time.Duration, fn func()) {
	if d == 0 {
		fn()
		return
	}
	time.AfterFunc(d, fn)
}
