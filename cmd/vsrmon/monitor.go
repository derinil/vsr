package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/derinil/vsr/internal/vsrstatus"
)

const maxStatusBytes = 1 << 20

type monitor struct {
	nodes    []string
	interval time.Duration
	timeout  time.Duration
	client   *http.Client
	logger   *slog.Logger
	latest   atomic.Pointer[[]byte]
	broker   broker
}

func newMonitor(nodes []string, interval, timeout time.Duration, logger *slog.Logger) *monitor {
	return &monitor{
		nodes:    nodes,
		interval: interval,
		timeout:  timeout,
		client:   &http.Client{},
		logger:   logger,
	}
}

func (m *monitor) run(ctx context.Context) {
	t := time.NewTicker(m.interval)
	defer t.Stop()
	for {
		m.poll(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (m *monitor) poll(ctx context.Context) {
	targets := make([]vsrstatus.Target, len(m.nodes))
	var wg sync.WaitGroup
	for i, base := range m.nodes {
		wg.Go(func() { targets[i] = m.pollNode(ctx, base) })
	}
	wg.Wait()
	if ctx.Err() != nil {
		return
	}
	data, err := json.Marshal(vsrstatus.Cluster{Time: time.Now(), Targets: targets})
	if err != nil {
		m.logger.Error("encode cluster", "err", err)
		return
	}
	m.publish(data)
}

func (m *monitor) pollNode(ctx context.Context, base string) vsrstatus.Target {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	start := time.Now()
	node, err := m.fetchStatus(ctx, base)
	t := vsrstatus.Target{URL: base, LatencyMs: float64(time.Since(start).Microseconds()) / 1000}
	if err != nil {
		t.Error = err.Error()
		return t
	}
	t.Reachable = true
	t.Node = &node
	return t
}

func (m *monitor) fetchStatus(ctx context.Context, base string) (vsrstatus.Node, error) {
	var n vsrstatus.Node
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/status", nil)
	if err != nil {
		return n, err
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return n, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return n, fmt.Errorf("status %s", resp.Status)
	}
	err = json.NewDecoder(io.LimitReader(resp.Body, maxStatusBytes)).Decode(&n)
	return n, err
}

func (m *monitor) publish(data []byte) {
	m.latest.Store(&data)
	m.broker.publish(data)
}
