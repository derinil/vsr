package vsr

import (
	"fmt"
	"slices"
	"testing"
)

const (
	randomClients      = 3
	randomOpsPerClient = 20
	randomSteps        = 5000
	randomFinalTicks   = 5000
	randomDropRate     = 0.1
)

func TestRandomizedSimulation(t *testing.T) {
	seeds := 300
	if testing.Short() {
		seeds = 30
	}
	for _, n := range []int{3, 5} {
		for seed := range seeds {
			t.Run(fmt.Sprintf("replicas=%d/seed=%d", n, seed+1), func(t *testing.T) {
				t.Parallel()
				runRandom(t, n, int64(seed+1), false)
			})
		}
	}
}

func TestRandomizedReconfiguration(t *testing.T) {
	seeds := 100
	if testing.Short() {
		seeds = 20
	}
	for seed := range seeds {
		t.Run(fmt.Sprintf("seed=%d", seed+1), func(t *testing.T) {
			t.Parallel()
			runRandom(t, 3, int64(seed+1), true)
		})
	}
}

func runRandom(t *testing.T, n int, seed int64, reconfigure bool) {
	c := newCluster(t, n, seed)
	for range randomClients {
		c.addClient(randomOpsPerClient)
	}
	var admin *simClient
	var pool []string
	if reconfigure {
		admin = c.addClient(0)
		for i := range n + 2 {
			pool = append(pool, fmt.Sprintf("r%d", i))
		}
	}
	c.dropRate = randomDropRate
	for range randomSteps {
		c.chaos(admin, pool)
		if c.rng.Float64() < 0.2 {
			c.tickAll()
		} else if len(c.queue) > 0 {
			c.deliverRandom()
		}
		c.check()
	}

	c.dropRate = 0
	c.heal()
	for range randomFinalTicks {
		if c.converged() {
			c.check()
			return
		}
		c.restartOneMember()
		c.tickAll()
		c.settle()
		c.check()
	}
	t.Fatalf("seed %d: no convergence after healing: %s", seed, c.describe())
}

func (c *cluster) chaos(admin *simClient, pool []string) {
	if c.rng.Float64() < 0.002 && (admin == nil || c.quiescent()) {
		c.crashRandomly()
	}
	for i := range c.replicas {
		if c.replicas[i] == nil && c.rng.Float64() < 0.02 {
			c.restart(i)
		}
	}
	if c.rng.Float64() < 0.005 {
		c.partitionRandomly()
	}
	if c.rng.Float64() < 0.01 {
		c.heal()
	}
	if admin != nil && c.replied(admin) && c.quiescent() && c.rng.Float64() < 0.002 {
		if next, ok := c.randomConfig(pool); ok {
			c.reconfigure(admin, next)
		}
	}
}

func (c *cluster) quiescent() bool {
	for _, addr := range c.config {
		r := c.replicas[c.addrIndex[addr]]
		if r == nil || r.Status() != StatusNormal || r.Epoch() != c.epoch {
			return false
		}
	}
	return true
}

func (c *cluster) restartOneMember() {
	var top uint64
	for _, r := range c.replicas {
		if r != nil {
			top = max(top, r.Epoch())
		}
	}
	for _, r := range c.replicas {
		if r != nil && r.Status() == StatusRecovering && r.Epoch() == top {
			return
		}
	}
	for i, r := range c.replicas {
		if r == nil {
			c.restart(i)
			return
		}
	}
}

func (c *cluster) randomConfig(pool []string) ([]string, bool) {
	var avail []string
	for _, addr := range pool {
		if c.available(addr) {
			avail = append(avail, addr)
		}
	}
	if len(avail) < 3 {
		return nil, false
	}
	for range 10 {
		size := 3 + 2*c.rng.IntN(2)
		if size > len(avail) {
			size = 3
		}
		perm := c.rng.Perm(len(avail))
		next := make([]string, size)
		for i := range next {
			next[i] = avail[perm[i]]
		}
		if !slices.Equal(next, c.config) {
			return next, true
		}
	}
	return nil, false
}

func (c *cluster) available(addr string) bool {
	i, ok := c.addrIndex[addr]
	if !ok || slices.Contains(c.config, addr) {
		return true
	}
	r := c.replicas[i]
	return r != nil && r.Status() == StatusShutdown
}

func (c *cluster) crashRandomly() {
	faults := (len(c.config) - 1) / 2
	down := 0
	for _, addr := range c.config {
		if r := c.replicas[c.addrIndex[addr]]; r == nil || r.Status() == StatusRecovering {
			down++
		}
	}
	if down >= faults {
		return
	}
	i := c.rng.IntN(len(c.replicas))
	if r := c.replicas[i]; r != nil && r.Status() == StatusNormal && slices.Contains(c.config, c.addrs[i]) {
		c.crash(i)
	}
}

func (c *cluster) partitionRandomly() {
	g := make([]int, len(c.replicas))
	for i := range g {
		g[i] = c.rng.IntN(2)
	}
	c.group = g
}

func (c *cluster) converged() bool {
	for _, cl := range c.clients {
		if !c.replied(cl) {
			return false
		}
	}
	if len(c.history) != randomClients*randomOpsPerClient {
		return false
	}
	commit := uint64(0)
	for i, addr := range c.config {
		r := c.replicas[c.addrIndex[addr]]
		if r == nil || r.Status() != StatusNormal || r.Epoch() != c.epoch {
			return false
		}
		if i == 0 {
			commit = r.CommitNum()
		} else if r.CommitNum() != commit {
			return false
		}
		if !slices.Equal(c.services[c.addrIndex[addr]].ops, c.ops()) {
			return false
		}
	}
	return true
}
