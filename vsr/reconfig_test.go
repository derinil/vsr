package vsr

import (
	"slices"
	"testing"
)

func isMsg[T Message](e envelope) bool {
	_, ok := e.msg.(T)
	return ok
}

func (c *cluster) ops() []string {
	ops := make([]string, len(c.history))
	for i, h := range c.history {
		ops[i] = h.op
	}
	return ops
}

func (c *cluster) settledIn(config []string) bool {
	for _, addr := range config {
		r := c.replicas[c.addrIndex[addr]]
		if r == nil || r.Status() != StatusNormal || r.Epoch() != c.epoch {
			return false
		}
	}
	return true
}

func (c *cluster) shutDown(addrs ...string) bool {
	for _, addr := range addrs {
		if r := c.replicas[c.addrIndex[addr]]; r == nil || r.Status() != StatusShutdown {
			return false
		}
	}
	return true
}

func (c *cluster) agree(config []string) bool {
	for _, addr := range config {
		if !slices.Equal(c.services[c.addrIndex[addr]].ops, c.ops()) {
			return false
		}
	}
	return true
}

func TestReconfigGrowKeepsState(t *testing.T) {
	c := newCluster(t, 3, 1)
	cl := c.addClient(0)
	admin := c.addClient(0)
	for range 5 {
		c.submit(cl)
		c.settle()
	}
	next := []string{"r0", "r1", "r2", "r3", "r4"}
	c.reconfigure(admin, next)
	ok := c.waitFor(func() bool { return c.replied(admin) && c.settledIn(next) }, 1000)
	c.check()
	if !ok {
		t.Fatalf("reconfiguration did not complete: %s", c.describe())
	}
	c.submit(cl)
	c.settle()
	c.run(defaultHeartbeatTicks + 2)
	c.check()
	if cl.completed != 6 || cl.epoch != 1 {
		t.Fatalf("client completed %d in epoch %d, want 6 in epoch 1", cl.completed, cl.epoch)
	}
	if !c.replicas[0].IsPrimary() || c.replicas[0].View() != 0 {
		t.Fatalf("config[0] is not the new primary: %s", c.describe())
	}
	if !c.agree(next) || len(c.ops()) != 6 {
		t.Fatalf("new group executed %v, history %v", c.services[3].ops, c.ops())
	}
}

func TestReconfigReplaceAll(t *testing.T) {
	c := newCluster(t, 3, 2)
	cl := c.addClient(0)
	admin := c.addClient(0)
	c.submit(cl)
	c.settle()
	old := []string{"r0", "r1", "r2"}
	next := []string{"n0", "n1", "n2"}
	c.reconfigure(admin, next)
	ok := c.waitFor(func() bool {
		return c.replied(admin) && c.settledIn(next) && c.shutDown(old...)
	}, 1000)
	c.check()
	if !ok {
		t.Fatalf("replacement did not complete: %s", c.describe())
	}
	c.submit(cl)
	c.settle()
	c.run(defaultHeartbeatTicks + 2)
	c.check()
	if cl.completed != 2 || cl.epoch != 1 {
		t.Fatalf("client completed %d in epoch %d, want 2 in epoch 1", cl.completed, cl.epoch)
	}
	if !c.agree(next) {
		t.Fatalf("new group executed %v, history %v", c.services[3].ops, c.ops())
	}
}

func TestReconfigShrinkRetainsMembers(t *testing.T) {
	c := newCluster(t, 5, 3)
	cl := c.addClient(0)
	admin := c.addClient(0)
	c.submit(cl)
	c.settle()
	next := []string{"r0", "r1", "r2"}
	c.reconfigure(admin, next)
	ok := c.waitFor(func() bool {
		return c.replied(admin) && c.settledIn(next) && c.shutDown("r3", "r4")
	}, 1000)
	c.check()
	if !ok {
		t.Fatalf("shrink did not complete: %s", c.describe())
	}
	c.submit(cl)
	c.settle()
	c.run(defaultHeartbeatTicks + 2)
	c.check()
	if cl.completed != 2 || !c.agree(next) {
		t.Fatalf("client completed %d, history %v", cl.completed, c.ops())
	}
}

func TestReconfigPrimaryCrashBeforeCommit(t *testing.T) {
	c := newCluster(t, 3, 4)
	cl := c.addClient(0)
	admin := c.addClient(0)
	c.submit(cl)
	c.settle()
	c.filter = isMsg[PrepareOK]
	next := []string{"r1", "r2", "r3"}
	c.reconfigure(admin, next)
	c.settle()
	if log := c.replicas[0].Log(); len(log) != 2 || log[1].Kind != EntryReconfig {
		t.Fatalf("reconfiguration not prepared at primary: %v", log)
	}
	c.crash(0)
	c.filter = func(e envelope) bool { return isMsg[PrepareOK](e) && e.msg.(PrepareOK).View == 1 }
	ok := c.waitForReplicas(func() bool {
		return c.replicas[1].IsPrimary() && c.replicas[1].View() == 1 && c.replicas[2].Status() == StatusNormal
	}, 500, 1, 2)
	if !ok {
		t.Fatalf("no view change: %s", c.describe())
	}
	before := c.replicas[1].OpNum()
	c.submit(cl)
	c.settle()
	if c.replicas[1].OpNum() != before || !cl.pending {
		t.Fatalf("new primary accepted a request while reconfiguration was pending")
	}
	c.filter = nil
	ok = c.waitFor(func() bool {
		return c.replied(admin) && c.settledIn(next) && cl.completed == 2
	}, 1000)
	c.check()
	if !ok {
		t.Fatalf("reconfiguration did not complete: %s", c.describe())
	}
	c.run(defaultHeartbeatTicks + 2)
	c.check()
	if !c.agree(next) {
		t.Fatalf("new group executed %v, history %v", c.services[1].ops, c.ops())
	}
}

func TestReconfigMemberMissesCommit(t *testing.T) {
	c := newCluster(t, 3, 5)
	cl := c.addClient(0)
	admin := c.addClient(0)
	c.submit(cl)
	c.settle()
	c.filter = func(e envelope) bool { return e.to == 1 && isMsg[Commit](e) }
	next := []string{"r0", "r1", "r3"}
	c.reconfigure(admin, next)
	c.settle()
	c.filter = nil
	ok := c.waitFor(func() bool {
		return c.replied(admin) && c.settledIn(next)
	}, 1000)
	c.check()
	if !ok || c.replicas[1].Epoch() != 1 {
		t.Fatalf("member did not learn the new epoch: %s", c.describe())
	}
	c.submit(cl)
	c.settle()
	c.run(defaultHeartbeatTicks + 2)
	c.check()
	if cl.completed != 2 || !c.agree(next) {
		t.Fatalf("history %v", c.ops())
	}
}

func TestReconfigLaggingMemberCatchesUp(t *testing.T) {
	c := newCluster(t, 3, 6)
	cl := c.addClient(0)
	admin := c.addClient(0)
	c.submit(cl)
	c.settle()
	c.filter = func(e envelope) bool { return e.to == 1 && isMsg[Prepare](e) }
	next := []string{"r0", "r1", "r3"}
	c.reconfigure(admin, next)
	c.settle()
	c.filter = nil
	ok := c.waitFor(func() bool {
		return c.replied(admin) && c.settledIn(next)
	}, 1000)
	c.check()
	if !ok {
		t.Fatalf("lagging member did not catch up: %s", c.describe())
	}
	c.run(defaultHeartbeatTicks + 2)
	c.check()
	if !c.agree(next) {
		t.Fatalf("history %v", c.ops())
	}
}

func TestReconfigRestartInNewEpoch(t *testing.T) {
	c := newCluster(t, 3, 7)
	cl := c.addClient(0)
	admin := c.addClient(0)
	c.submit(cl)
	c.settle()
	next := []string{"r0", "r1", "r2", "r3"}
	c.reconfigure(admin, next)
	ok := c.waitFor(func() bool { return c.replied(admin) && c.settledIn(next) }, 1000)
	c.check()
	if !ok {
		t.Fatalf("reconfiguration did not complete: %s", c.describe())
	}
	c.crash(2)
	c.restart(2)
	ok = c.waitFor(func() bool {
		return c.settledIn(next) && c.replicas[2].CommitNum() == c.replicas[0].CommitNum()
	}, 1000)
	c.check()
	if !ok || c.replicas[2].Epoch() != 1 {
		t.Fatalf("restarted member did not recover in new epoch: %s", c.describe())
	}
	c.submit(cl)
	c.settle()
	c.run(defaultHeartbeatTicks + 2)
	c.check()
	if !c.agree(next) {
		t.Fatalf("history %v", c.ops())
	}
}

func TestReconfigRestartWithOldEpoch(t *testing.T) {
	c := newCluster(t, 3, 8)
	cl := c.addClient(0)
	admin := c.addClient(0)
	c.submit(cl)
	c.settle()
	c.crash(2)
	next := []string{"r0", "r2", "r3"}
	c.reconfigure(admin, next)
	ok := c.waitFor(func() bool { return c.replied(admin) && c.settledIn([]string{"r0", "r3"}) }, 1000)
	c.check()
	if !ok {
		t.Fatalf("reconfiguration did not complete: %s", c.describe())
	}
	c.restart(2)
	ok = c.waitFor(func() bool { return c.settledIn(next) }, 2000)
	c.check()
	if !ok || c.replicas[2].Epoch() != 1 {
		t.Fatalf("old-epoch restart did not reach new epoch: %s", c.describe())
	}
	c.submit(cl)
	c.settle()
	c.run(defaultHeartbeatTicks + 2)
	c.check()
	if !c.agree(next) {
		t.Fatalf("history %v", c.ops())
	}
}

func TestReconfigRejected(t *testing.T) {
	tests := []struct {
		name   string
		epoch  uint64
		config []string
	}{
		{"fewer than three replicas", 0, []string{"r0", "r1"}},
		{"duplicate address", 0, []string{"r0", "r1", "r1"}},
		{"unchanged configuration", 0, []string{"r0", "r1", "r2"}},
		{"future epoch", 1, []string{"r0", "r1", "r3"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newCluster(t, 3, 1)
			cl := c.addClient(0)
			c.enqueue(envelope{from: -1, to: 0, msg: Reconfiguration{Epoch: tt.epoch, ClientID: cl.id, RequestNum: 1, Config: tt.config}})
			c.settle()
			c.check()
			if r := c.replicas[0]; r.Epoch() != 0 || r.OpNum() != 0 {
				t.Fatalf("rejected reconfiguration changed state: %s", c.describe())
			}
		})
	}
}

func TestReconfigWhilePendingRejected(t *testing.T) {
	c := newCluster(t, 3, 9)
	cl := c.addClient(0)
	admin := c.addClient(0)
	c.submit(cl)
	c.settle()
	c.filter = isMsg[PrepareOK]
	c.reconfigure(admin, []string{"r0", "r1", "r3"})
	c.settle()
	c.enqueue(envelope{from: -1, to: 0, msg: Reconfiguration{Epoch: 0, ClientID: 9, RequestNum: 1, Config: []string{"r0", "r2", "r3"}}})
	c.settle()
	c.check()
	reconfigs := 0
	for _, e := range c.replicas[0].Log() {
		if e.Kind == EntryReconfig {
			reconfigs++
		}
	}
	if reconfigs != 1 {
		t.Fatalf("log holds %d reconfigurations, want 1", reconfigs)
	}
	c.filter = nil
	ok := c.waitFor(func() bool { return c.replied(admin) && c.settledIn([]string{"r0", "r1", "r3"}) }, 1000)
	c.check()
	if !ok {
		t.Fatalf("reconfiguration did not complete: %s", c.describe())
	}
}

func TestRequestsDuringReconfigDroppedThenRetried(t *testing.T) {
	c := newCluster(t, 3, 10)
	cl := c.addClient(0)
	admin := c.addClient(0)
	c.submit(cl)
	c.settle()
	c.filter = isMsg[PrepareOK]
	next := []string{"r0", "r1", "r3"}
	c.reconfigure(admin, next)
	c.settle()
	before := c.replicas[0].OpNum()
	c.submit(cl)
	c.settle()
	if c.replicas[0].OpNum() != before || cl.completed != 1 {
		t.Fatalf("request accepted while reconfiguration pending")
	}
	c.filter = nil
	ok := c.waitFor(func() bool { return c.replied(admin) && c.replied(cl) }, 1000)
	c.check()
	if !ok || cl.epoch != 1 || cl.completed != 2 {
		t.Fatalf("retried request not served in new epoch: %s", c.describe())
	}
	c.run(defaultHeartbeatTicks + 2)
	c.check()
	if !c.agree(next) {
		t.Fatalf("history %v", c.ops())
	}
}
