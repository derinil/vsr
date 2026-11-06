package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/derinil/vsr"
	"github.com/derinil/vsr/internal/kvservice"
)

type loadStats struct {
	mu         sync.Mutex
	latencies  []time.Duration
	failed     int
	violations int
	total      totals
}

type totals struct {
	ops, failed, violations int
}

func (s *loadStats) record(d time.Duration, ok, violation bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ok {
		s.latencies = append(s.latencies, d)
	} else {
		s.failed++
	}
	if violation {
		s.violations++
	}
}

func (s *loadStats) take() (lat []time.Duration, failed, violations int, total totals) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lat, failed, violations = s.latencies, s.failed, s.violations
	s.total.ops += len(lat)
	s.total.failed += failed
	s.total.violations += violations
	s.latencies, s.failed, s.violations = make([]time.Duration, 0, len(lat)), 0, 0
	return lat, failed, violations, s.total
}

// register checks one client's private key: its writes carry increasing
// values, so linearizable reads never go backwards, never precede the last
// acknowledged write and never exceed the last attempted one.
type register struct {
	key       string
	acked     int
	attempted int
	seen      int
}

func (r *register) check(v int) error {
	if v < r.acked || v < r.seen || v > r.attempted {
		return fmt.Errorf("read %s=%d, want within [%d, %d]", r.key, v, max(r.acked, r.seen), r.attempted)
	}
	r.seen = v
	return nil
}

func worker(ctx context.Context, id int, config []string, o options, stats *loadStats, logger *slog.Logger) {
	cl, err := vsr.NewClient(vsr.ClientOptions{Config: config})
	if err != nil {
		logger.Error("client", "err", err)
		return
	}
	defer cl.Close()
	reg := &register{key: "c" + strconv.Itoa(id)}
	for ctx.Err() == nil {
		op, verify, err := nextOp(reg, o.keys)
		if err != nil {
			logger.Error("encode query", "err", err)
			return
		}
		qctx, cancel := context.WithTimeout(ctx, o.timeout)
		start := time.Now()
		b, err := cl.Invoke(qctx, op)
		cancel()
		latency := time.Since(start)
		if ctx.Err() != nil {
			return
		}
		r, derr := kvservice.DecodeResult(b)
		ok := err == nil && derr == nil && r.Code != kvservice.CodeMalformed
		var violation error
		if ok && verify != nil {
			violation = verify(r)
		}
		if violation != nil {
			logger.Error("consistency violation", "client", id, "err", violation)
		}
		stats.record(latency, ok, violation != nil)
	}
}

func nextOp(reg *register, keys int) ([]byte, func(kvservice.Result) error, error) {
	switch p := rand.N(10); {
	case p < 2:
		reg.attempted++
		v := reg.attempted
		op, err := kvservice.Write(reg.key, v)
		return op, func(kvservice.Result) error {
			reg.acked = v
			return nil
		}, err
	case p < 4:
		op, err := kvservice.Read(reg.key)
		return op, func(r kvservice.Result) error {
			if r.Code == kvservice.CodeNotFound {
				return reg.check(0)
			}
			v, err := strconv.Atoi(string(r.Value))
			if err != nil {
				return fmt.Errorf("read %s: %w", reg.key, err)
			}
			return reg.check(v)
		}, err
	default:
		op, err := randomOp(keys)
		return op, nil, err
	}
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

type usage struct {
	samples int
	cpuSum  float64
	cpuMax  float64
	memMax  uint64
}

func report(ctx context.Context, c *cluster, stats *loadStats, every time.Duration, logger *slog.Logger) {
	peaks := make(map[string]*usage, len(c.nodes))
	for _, n := range c.nodes {
		peaks[n.addr] = &usage{}
	}
	start, last := time.Now(), time.Now()
	for sleep(ctx, every) {
		now := time.Now()
		lat, failed, violations, _ := stats.take()
		slices.Sort(lat)
		logger.Info("load",
			"ops/s", fmt.Sprintf("%.0f", float64(len(lat))/now.Sub(last).Seconds()),
			"failed", failed,
			"p50", percentile(lat, 0.50),
			"p99", percentile(lat, 0.99),
			"max", percentile(lat, 1),
			"violations", violations,
		)
		last = now
		s := c.survey(ctx)
		for _, n := range c.nodes {
			st := s.status[n.addr]
			if st == nil {
				logger.Info("node", "addr", n.addr, "status", "down")
				continue
			}
			u := peaks[n.addr]
			u.samples++
			u.cpuSum += st.Process.CPUPercent
			u.cpuMax = max(u.cpuMax, st.Process.CPUPercent)
			u.memMax = max(u.memMax, st.Process.MemBytes)
			logger.Info("node",
				"addr", n.addr,
				"status", st.Status,
				"primary", st.Primary,
				"cpu", fmt.Sprintf("%.0f%%", st.Process.CPUPercent),
				"mem", mib(st.Process.MemBytes),
				"heap", mib(st.Process.HeapBytes),
				"goroutines", st.Process.Goroutines,
				"commit", st.CommitNum,
			)
		}
	}
	_, _, _, t := stats.take()
	elapsed := time.Since(start)
	logger.Info("summary",
		"duration", elapsed.Round(time.Second),
		"ops", t.ops,
		"ops/s", fmt.Sprintf("%.0f", float64(t.ops)/elapsed.Seconds()),
		"failed", t.failed,
		"violations", t.violations,
	)
	for _, n := range c.nodes {
		u := peaks[n.addr]
		if u.samples == 0 {
			continue
		}
		logger.Info("summary node",
			"addr", n.addr,
			"cpu_avg", fmt.Sprintf("%.0f%%", u.cpuSum/float64(u.samples)),
			"cpu_max", fmt.Sprintf("%.0f%%", u.cpuMax),
			"mem_max", mib(u.memMax),
		)
	}
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[int(p*float64(len(sorted)-1))].Round(time.Microsecond)
}

func mib(b uint64) string {
	return fmt.Sprintf("%.1fMiB", float64(b)/(1<<20))
}
