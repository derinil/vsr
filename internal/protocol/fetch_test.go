package protocol

import (
	"slices"
	"testing"

	"github.com/derinil/vsr/message"
)

func distinctPages(state []byte, pageSize int) int {
	seen := make(map[digest]bool)
	for _, h := range buildMerkle(state, pageSize).levels[0] {
		seen[h] = true
	}
	return len(seen)
}

func (c *cluster) servicesEqual(i, j int) bool {
	return slices.Equal(c.services[i].ops, c.services[j].ops)
}

func TestCheckpointGCBoundsLog(t *testing.T) {
	tests := []struct {
		name     string
		interval int
	}{
		{"interval 3", 3},
		{"interval 4", 4},
		{"interval 5", 5},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cp := checkpointing{interval: tc.interval, retain: tc.interval, pageSize: 16}
			c := newClusterWith(t, 3, 1, cp)
			cl := c.addClient(0)
			for range 10 * tc.interval {
				c.submit(cl)
				c.settle()
				c.check()
				for i, r := range c.replicas {
					inflight := int(r.OpNum() - r.CommitNum())
					if n := len(r.Log()); n > cp.retain+cp.interval+inflight {
						t.Fatalf("replica %d log %d entries, bound %d (base %d)", i, n, cp.retain+cp.interval+inflight, r.LogBase())
					}
				}
			}
			if c.replicas[0].LogBase() == 0 {
				t.Fatalf("no log GC after %d ops", 10*tc.interval)
			}
		})
	}
}

func TestCheckpointLaggingBackupCatchesUp(t *testing.T) {
	c := newClusterWith(t, 3, 2, checkpointing{interval: 4, retain: 4, pageSize: 16})
	cl := c.addClient(0)
	c.isolate(2)
	for range 30 {
		c.submit(cl)
		c.settle()
	}
	c.heal()
	ok := c.waitFor(func() bool {
		return c.replicas[2].Status() == StatusNormal && c.replicas[2].CommitNum() == 30
	}, 1000)
	c.check()
	if !ok {
		t.Fatalf("lagging backup did not catch up: %s", c.describe())
	}
	if c.replicas[2].LogBase() == 0 {
		t.Fatalf("lagging backup caught up without a checkpoint")
	}
	if !c.servicesEqual(2, 0) {
		t.Fatalf("lagging backup executed %v, primary executed %v", c.services[2].ops, c.services[0].ops)
	}
}

func TestCheckpointRestartPageTransfer(t *testing.T) {
	tests := []struct {
		name       string
		checkpoint bool
	}{
		{"restores disk checkpoint", true},
		{"no disk checkpoint", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cp := checkpointing{interval: 4, retain: 4, pageSize: 16}
			c := newClusterWith(t, 3, 3, cp)
			cl := c.addClient(0)
			for range 20 {
				c.submit(cl)
				c.settle()
			}
			c.crash(2)
			for range 8 {
				c.submit(cl)
				c.settle()
			}
			if !tc.checkpoint {
				c.saved[2].checkpoint = nil
			}
			c.pagesTo = make(map[int]int)
			c.restart(2)
			ok := c.waitFor(func() bool {
				return c.replicas[2].Status() == StatusNormal && c.replicas[2].CommitNum() == 28
			}, 1000)
			c.check()
			if !ok {
				t.Fatalf("restarted replica did not recover: %s", c.describe())
			}
			if !c.servicesEqual(2, 0) {
				t.Fatalf("restarted replica executed %v", c.services[2].ops)
			}
			latest := c.replicas[0].Checkpoint()
			if latest == nil || latest.OpNum != 28 {
				t.Fatalf("primary latest checkpoint %v, want 28", latest)
			}
			distinct := distinctPages(latest.State, cp.pageSize)
			fetched := c.pagesTo[2]
			if tc.checkpoint && (fetched == 0 || fetched >= distinct) {
				t.Fatalf("restart fetched %d of %d distinct pages, want a strict subset", fetched, distinct)
			}
			if !tc.checkpoint && fetched != distinct {
				t.Fatalf("full fetch transferred %d pages, want %d", fetched, distinct)
			}
		})
	}
}

func TestCheckpointNewPrimaryBehindFetchesBeforeServing(t *testing.T) {
	c := newClusterWith(t, 3, 4, checkpointing{interval: 4, retain: 4, pageSize: 16})
	cl := c.addClient(0)
	c.isolate(1)
	for range 20 {
		c.submit(cl)
		c.settle()
	}
	c.crash(0)
	c.heal()
	ok := c.waitFor(func() bool {
		r := c.replicas[1]
		return r.IsPrimary() && r.View() == 1 && r.CommitNum() == 20
	}, 1000)
	c.check()
	if !ok {
		t.Fatalf("behind primary did not catch up: %s", c.describe())
	}
	if c.replicas[1].LogBase() == 0 {
		t.Fatalf("new primary served without a checkpoint")
	}
	c.submit(cl)
	c.settle()
	c.run(defaultHeartbeatTicks + 1)
	c.check()
	if cl.completed != 21 {
		t.Fatalf("request after view change completed %d times, want 21", cl.completed)
	}
	for _, i := range []int{1, 2} {
		if len(c.services[i].ops) != 21 {
			t.Fatalf("replica %d executed %d ops, want 21", i, len(c.services[i].ops))
		}
	}
}

func TestCheckpointSourceTakesNewerCheckpoint(t *testing.T) {
	c := newClusterWith(t, 3, 5, checkpointing{interval: 4, retain: 4, pageSize: 16})
	cl := c.addClient(0)
	c.isolate(2)
	for range 12 {
		c.submit(cl)
		c.settle()
	}
	c.heal()
	c.filter = func(e envelope) bool {
		return e.to == 2 && (isMsg[message.MerkleNodes](e) || isMsg[message.Pages](e) || isMsg[message.Prepare](e) || isMsg[message.Commit](e))
	}
	c.replicas[2].Step(message.Commit{CommitNum: 12})
	c.settle()
	first := c.replicas[0].Checkpoint().OpNum
	for range 16 {
		c.submit(cl)
		c.settle()
	}
	c.filter = nil
	ok := c.waitFor(func() bool {
		return c.replicas[2].Status() == StatusNormal && c.replicas[2].CommitNum() == 28
	}, 1000)
	c.check()
	if !ok {
		t.Fatalf("lagging backup did not converge: %s", c.describe())
	}
	if latest := c.replicas[2].Checkpoint(); latest == nil || latest.OpNum <= first {
		t.Fatalf("backup holds checkpoint %v, want one newer than %d", latest, first)
	}
	if !c.servicesEqual(2, 0) {
		t.Fatalf("backup executed %v", c.services[2].ops)
	}
}

func TestCheckpointJoinerAfterGC(t *testing.T) {
	c := newClusterWith(t, 3, 6, checkpointing{interval: 4, retain: 4, pageSize: 16})
	cl := c.addClient(0)
	admin := c.addClient(0)
	for range 6 {
		c.submit(cl)
		c.settle()
	}
	const epochStart = 7
	c.filter = func(e envelope) bool { return e.to == 3 }
	c.reconfigure(admin, []string{"r0", "r1", "r3"})
	ok := c.waitFor(func() bool { return c.replied(admin) && c.settledIn([]string{"r0", "r1"}) }, 1000)
	c.check()
	if !ok {
		t.Fatalf("reconfiguration did not complete: %s", c.describe())
	}
	for range 10 {
		c.submit(cl)
		c.settle()
	}
	if base := c.replicas[0].LogBase(); base <= epochStart {
		t.Fatalf("primary base %d not past epoch start %d", base, epochStart)
	}
	c.filter = nil
	ok = c.waitFor(func() bool {
		return c.settledIn([]string{"r0", "r1", "r3"}) && c.replicas[3].CommitNum() == c.replicas[0].CommitNum()
	}, 2000)
	c.check()
	if !ok {
		t.Fatalf("joiner did not catch up: %s", c.describe())
	}
	if c.replicas[3].LogBase() == 0 {
		t.Fatalf("joiner caught up without a checkpoint")
	}
	if !c.agree([]string{"r0", "r1", "r3"}) || len(c.ops()) != 16 {
		t.Fatalf("history %v, joiner executed %v", c.ops(), c.services[3].ops)
	}
}

func TestCheckpointRestoreAtStartup(t *testing.T) {
	c := newClusterWith(t, 3, 7, checkpointing{interval: 4, retain: 4, pageSize: 16})
	cl := c.addClient(0)
	for range 8 {
		c.submit(cl)
		c.settle()
	}
	c.run(defaultHeartbeatTicks + 1)
	c.crash(1)
	cp := c.saved[1].checkpoint
	if cp == nil || cp.OpNum != 8 {
		t.Fatalf("crashed replica checkpoint %v, want op 8", cp)
	}
	c.restart(1)
	r := c.replicas[1]
	if r.Status() != StatusRecovering || r.OpNum() != 8 || r.CommitNum() != 8 || r.LogBase() != 8 {
		t.Fatalf("restored replica: %s", c.describe())
	}
	if !slices.Equal(c.services[1].ops, c.services[0].ops[:8]) {
		t.Fatalf("restored state %v", c.services[1].ops)
	}
}
