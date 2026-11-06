// Package vsrmon polls replica status endpoints and serves a live dashboard.
package vsrmon

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/derinil/vsr/internal/vsrstatus"
)

const (
	maxStatusBytes            = 1 << 20
	defaultReconfigureTimeout = 30 * time.Second
)

// Options configures a Monitor.
type Options struct {
	// Nodes are the status base URLs polled from the start.
	Nodes []string
	// Spares are replica addresses offered for adding to the configuration.
	Spares []string
	// StatusURL maps a replica address to its status base URL, or "" if
	// unknown. Members found in a polled configuration are then polled too,
	// and new members are checked before a reconfiguration. Nil disables both.
	StatusURL func(addr string) string
	// Interval is the poll period.
	Interval time.Duration
	// Timeout bounds polling one node.
	Timeout time.Duration
	// ReconfigureTimeout bounds a reconfiguration; zero selects 30s.
	ReconfigureTimeout time.Duration
	// Logger receives poll and reconfiguration logs.
	Logger *slog.Logger
}

// Monitor polls replicas and publishes the aggregated cluster status.
type Monitor struct {
	opts   Options
	client *http.Client
	logger *slog.Logger
	latest atomic.Pointer[[]byte]
	broker broker

	reconfiguring atomic.Bool

	mu           sync.Mutex
	nodes        []string
	config       []string
	epoch        uint64
	reconfigured time.Time
}

// New returns a Monitor.
func New(o Options) *Monitor {
	if o.ReconfigureTimeout <= 0 {
		o.ReconfigureTimeout = defaultReconfigureTimeout
	}
	return &Monitor{
		opts:   o,
		client: &http.Client{},
		logger: o.Logger,
		nodes:  slices.Clone(o.Nodes),
	}
}

// Run polls until ctx is done.
func (m *Monitor) Run(ctx context.Context) {
	t := time.NewTicker(m.opts.Interval)
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

func (m *Monitor) poll(ctx context.Context) {
	start := time.Now()
	m.mu.Lock()
	nodes := slices.Clone(m.nodes)
	m.mu.Unlock()
	targets := make([]vsrstatus.Target, len(nodes))
	var wg sync.WaitGroup
	for i, base := range nodes {
		wg.Go(func() { targets[i] = m.pollNode(ctx, base) })
	}
	wg.Wait()
	if ctx.Err() != nil {
		return
	}
	config, epoch := currentConfig(targets)
	m.mu.Lock()
	if epoch >= m.epoch || start.After(m.reconfigured) {
		m.config, m.epoch = config, epoch
	}
	config = m.config
	m.discover(targets)
	m.mu.Unlock()
	spares := slices.DeleteFunc(slices.Clone(m.opts.Spares), func(addr string) bool { return slices.Contains(config, addr) })
	data, err := json.Marshal(vsrstatus.Cluster{Time: time.Now(), Targets: targets, Spares: spares})
	if err != nil {
		m.logger.Error("encode cluster", "err", err)
		return
	}
	m.publish(data)
}

func (m *Monitor) pollNode(ctx context.Context, base string) vsrstatus.Target {
	ctx, cancel := context.WithTimeout(ctx, m.opts.Timeout)
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

func (m *Monitor) fetchStatus(ctx context.Context, base string) (vsrstatus.Node, error) {
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

func currentConfig(targets []vsrstatus.Target) ([]string, uint64) {
	var best *vsrstatus.Node
	for _, t := range targets {
		n := t.Node
		if !t.Reachable || n == nil || n.Status == "shutdown" || len(n.Config) == 0 {
			continue
		}
		if best == nil || n.Epoch > best.Epoch {
			best = n
		}
	}
	if best == nil {
		return nil, 0
	}
	return slices.Clone(best.Config), best.Epoch
}

func (m *Monitor) discover(targets []vsrstatus.Target) {
	if m.opts.StatusURL == nil {
		return
	}
	for _, t := range targets {
		if t.Node == nil {
			continue
		}
		for _, addr := range t.Node.Config {
			if url := m.opts.StatusURL(addr); url != "" && !slices.Contains(m.nodes, url) {
				m.nodes = append(m.nodes, url)
				m.logger.Info("polling new member", "addr", addr, "url", url)
			}
		}
	}
}

func (m *Monitor) publish(data []byte) {
	m.latest.Store(&data)
	m.broker.publish(data)
}
