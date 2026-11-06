package main

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

type chaosAgent struct {
	c       *cluster
	logger  *slog.Logger
	wg      sync.WaitGroup
	netBusy atomic.Bool
}

// chaos injects a random fault roughly every interval. Crashes and pauses never
// take down more than f members at once, since a crashed replica loses its log
// and VR recovery needs f+1 intact replicas; network faults are unrestricted.
func chaos(ctx context.Context, c *cluster, every time.Duration, logger *slog.Logger) {
	a := &chaosAgent{c: c, logger: logger}
	defer a.wg.Wait()
	for sleep(ctx, every/2+rand.N(every)) {
		s := c.survey(ctx)
		if len(s.config) == 0 {
			continue
		}
		var healthy []string
		for _, addr := range s.config {
			if s.healthy(c, addr) {
				healthy = append(healthy, addr)
			}
		}
		budget := (len(s.config)-1)/2 - (len(s.config) - len(healthy))
		primaryOK := slices.Contains(healthy, s.primary)
		switch p := rand.N(10); {
		case p < 3 && budget > 0:
			rand.Shuffle(len(healthy), func(i, j int) { healthy[i], healthy[j] = healthy[j], healthy[i] })
			a.crash(healthy[:1+rand.N(budget)])
		case p < 5 && budget > 0 && primaryOK:
			a.crash([]string{s.primary})
		case p < 6 && budget > 0 && primaryOK:
			a.pause(ctx, s.primary)
		case p < 9 && a.netBusy.CompareAndSwap(false, true):
			a.partition(ctx, s.primary)
		case a.netBusy.CompareAndSwap(false, true):
			a.flaky(ctx)
		}
	}
}

func (a *chaosAgent) crash(addrs []string) {
	d := time.Second + rand.N(5*time.Second)
	a.logger.Warn("chaos: crash", "nodes", addrs, "for", d)
	for _, addr := range addrs {
		a.c.byAddr[addr].crash(d)
	}
}

func (a *chaosAgent) pause(ctx context.Context, addr string) {
	d := time.Second + rand.N(3*time.Second)
	n := a.c.byAddr[addr]
	a.logger.Warn("chaos: pause primary", "node", addr, "for", d)
	n.pause(true)
	a.hold(ctx, d, func() {
		n.pause(false)
		a.logger.Warn("chaos: resume", "node", addr)
	})
}

func (a *chaosAgent) partition(ctx context.Context, primary string) {
	addrs := make([]string, len(a.c.nodes))
	for i, n := range a.c.nodes {
		addrs[i] = n.addr
	}
	rand.Shuffle(len(addrs), func(i, j int) { addrs[i], addrs[j] = addrs[j], addrs[i] })
	cut := 1 + rand.N(len(addrs)/2)
	if i := slices.Index(addrs, primary); i >= cut && rand.N(2) == 0 {
		addrs[0], addrs[i] = addrs[i], addrs[0]
	}
	minority, majority := addrs[:cut], addrs[cut:]
	oneWay := rand.N(4) == 0
	d := 2*time.Second + rand.N(5*time.Second)
	a.logger.Warn("chaos: partition", "minority", minority, "majority", majority, "one_way", oneWay, "for", d)
	for _, addr := range minority {
		a.c.byAddr[addr].setBlock(majority)
	}
	if !oneWay {
		for _, addr := range majority {
			a.c.byAddr[addr].setBlock(minority)
		}
	}
	a.hold(ctx, d, func() {
		for _, n := range a.c.nodes {
			n.setBlock(nil)
		}
		a.netBusy.Store(false)
		a.logger.Warn("chaos: partition healed")
	})
}

func (a *chaosAgent) flaky(ctx context.Context) {
	nodes := slices.Clone(a.c.nodes)
	rand.Shuffle(len(nodes), func(i, j int) { nodes[i], nodes[j] = nodes[j], nodes[i] })
	nodes = nodes[:1+rand.N(len(nodes))]
	loss := 0.05 + 0.25*rand.Float64()
	delay := rand.N(80 * time.Millisecond)
	d := 3*time.Second + rand.N(5*time.Second)
	addrs := make([]string, len(nodes))
	for i, n := range nodes {
		addrs[i] = n.addr
		n.setNet(loss, delay)
	}
	a.logger.Warn("chaos: flaky network", "nodes", addrs, "loss", loss, "delay", delay, "for", d)
	a.hold(ctx, d, func() {
		for _, n := range nodes {
			n.setNet(0, 0)
		}
		a.netBusy.Store(false)
		a.logger.Warn("chaos: network restored", "nodes", addrs)
	})
}

func (a *chaosAgent) hold(ctx context.Context, d time.Duration, undo func()) {
	a.wg.Go(func() {
		sleep(ctx, d)
		undo()
	})
}
