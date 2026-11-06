package vsr

import (
	"fmt"
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
				runRandom(t, n, int64(seed+1))
			})
		}
	}
}

func runRandom(t *testing.T, n int, seed int64) {
	c := newCluster(t, n, seed)
	for range randomClients {
		c.addClient(randomOpsPerClient)
	}
	c.dropRate = randomDropRate
	faults := (n - 1) / 2
	for range randomSteps {
		c.chaos(faults)
		if c.rng.Float64() < 0.2 {
			c.tickAll()
		} else if len(c.queue) > 0 {
			c.deliverRandom()
		}
		c.check()
	}

	c.dropRate = 0
	c.heal()
	for i := range n {
		if c.replicas[i] == nil {
			c.restart(i)
		}
	}
	for range randomFinalTicks {
		if c.converged() {
			c.check()
			return
		}
		c.tickAll()
		c.settle()
		c.check()
	}
	t.Fatalf("seed %d: no convergence after healing: %s", seed, c.describe())
}

func (c *cluster) chaos(faults int) {
	if c.rng.Float64() < 0.002 {
		c.crashRandomly(faults)
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
}

func (c *cluster) crashRandomly(faults int) {
	down := 0
	for _, r := range c.replicas {
		if r == nil || r.Status() == StatusRecovering {
			down++
		}
	}
	if down >= faults {
		return
	}
	i := c.rng.IntN(len(c.replicas))
	if r := c.replicas[i]; r != nil && r.Status() == StatusNormal {
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
	issued := 0
	for _, cl := range c.clients {
		if !c.replied(cl) {
			return false
		}
		issued += randomOpsPerClient
	}
	for _, r := range c.replicas {
		if r == nil || r.Status() != StatusNormal || r.CommitNum() != uint64(len(c.history)) {
			return false
		}
	}
	return len(c.history) == issued
}
