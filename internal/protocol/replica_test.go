package protocol

import (
	"errors"
	"fmt"
	"slices"
	"testing"
)

func TestNewReplicaValidation(t *testing.T) {
	svc := &recorder{}
	out := simOutbox{}
	config := []string{"a", "b", "c"}
	tests := []struct {
		name string
		o    Options
	}{
		{"empty config", Options{Addr: "a", Service: svc, Outbox: out}},
		{"address not in config", Options{Addr: "x", Config: config, Service: svc, Outbox: out}},
		{"epoch without recovering or join", Options{Addr: "a", Config: config, Epoch: 1, Service: svc, Outbox: out}},
		{"duplicate address", Options{Addr: "a", Config: []string{"a", "b", "a"}, Service: svc, Outbox: out}},
		{"nil service", Options{Addr: "a", Config: config, Outbox: out}},
		{"nil outbox", Options{Addr: "a", Config: config, Service: svc}},
		{"negative ticks", Options{Addr: "a", Config: config, Service: svc, Outbox: out, ResendTicks: -1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewReplica(tt.o); !errors.Is(err, ErrInvalidOptions) {
				t.Fatalf("err = %v, want ErrInvalidOptions", err)
			}
		})
	}

	r, err := NewReplica(Options{Addr: "b", Config: config, Service: svc, Outbox: out})
	if err != nil {
		t.Fatal(err)
	}
	if r.Index() != 1 || r.Status() != StatusNormal || r.View() != 0 || r.OpNum() != 0 {
		t.Fatalf("unexpected initial state: index=%d status=%v view=%d op=%d", r.Index(), r.Status(), r.View(), r.OpNum())
	}
}

func TestNormalCommit(t *testing.T) {
	for _, n := range []int{1, 3, 5} {
		t.Run(fmt.Sprintf("replicas=%d", n), func(t *testing.T) {
			c := newCluster(t, n, 1)
			cl := c.addClient(0)
			const ops = 10
			for k := 1; k <= ops; k++ {
				c.submit(cl)
				c.settle()
				if cl.completed != k {
					t.Fatalf("after op %d: %d replies", k, cl.completed)
				}
			}
			c.run(defaultHeartbeatTicks + 1)
			c.check()
			for i := range n {
				if got := c.replicas[i].CommitNum(); got != ops {
					t.Fatalf("replica %d commit = %d, want %d", i, got, ops)
				}
				if !slices.Equal(c.services[i].ops, c.services[0].ops) {
					t.Fatalf("replica %d executed %v, replica 0 executed %v", i, c.services[i].ops, c.services[0].ops)
				}
			}
		})
	}
}

func TestDuplicateRequestReturnsCachedReply(t *testing.T) {
	c := newCluster(t, 3, 1)
	cl := c.addClient(0)
	c.submit(cl)
	c.settle()
	p := c.leader()
	before := len(c.replies)
	c.enqueue(envelope{from: -1, to: p, msg: Request{ClientID: 1, RequestNum: 1, Op: []byte(opKey(1, 1))}})
	c.settle()
	c.check()
	if got := len(c.replies) - before; got != 1 {
		t.Fatalf("duplicate produced %d replies, want 1", got)
	}
	if got := len(c.services[p].ops); got != 1 {
		t.Fatalf("duplicate re-executed: %d executions", got)
	}
}

func TestOldRequestDropped(t *testing.T) {
	c := newCluster(t, 3, 1)
	cl := c.addClient(0)
	c.submit(cl)
	c.settle()
	c.submit(cl)
	c.settle()
	p := c.leader()
	before := len(c.replies)
	c.enqueue(envelope{from: -1, to: p, msg: Request{ClientID: 1, RequestNum: 1, Op: []byte(opKey(1, 1))}})
	c.settle()
	c.check()
	if len(c.replies) != before {
		t.Fatalf("old request produced a reply")
	}
	if got := len(c.services[p].ops); got != 2 {
		t.Fatalf("old request executed: %d executions", got)
	}
}

func TestBackupIgnoresRequests(t *testing.T) {
	c := newCluster(t, 3, 1)
	c.enqueue(envelope{from: -1, to: 1, msg: Request{ClientID: 1, RequestNum: 1, Op: []byte(opKey(1, 1))}})
	c.settle()
	c.check()
	if len(c.replies) != 0 {
		t.Fatalf("backup replied to a request")
	}
	for i := range 3 {
		if c.replicas[i].OpNum() != 0 || len(c.services[i].ops) != 0 {
			t.Fatalf("replica %d changed state after a backup request", i)
		}
	}
}

func TestBackupsExecuteAfterCommitHeartbeat(t *testing.T) {
	c := newCluster(t, 3, 1)
	cl := c.addClient(0)
	c.submit(cl)
	c.settle()
	if len(c.services[1].ops) != 0 || len(c.services[2].ops) != 0 {
		t.Fatalf("backups executed before receiving a commit point")
	}
	c.run(defaultHeartbeatTicks + 1)
	c.check()
	for _, i := range []int{1, 2} {
		if !slices.Equal(c.services[i].ops, []string{opKey(1, 1)}) {
			t.Fatalf("replica %d executed %v", i, c.services[i].ops)
		}
	}
}

func TestPrimaryCrashViewChange(t *testing.T) {
	c := newCluster(t, 3, 1)
	cl := c.addClient(0)
	for range 3 {
		c.submit(cl)
		c.settle()
	}
	committed := slices.Clone(c.services[0].ops)
	c.crash(0)
	ok := c.waitFor(func() bool {
		return c.replicas[1].IsPrimary() && c.replicas[1].View() == 1 && c.replicas[2].Status() == StatusNormal && c.replicas[2].View() == 1
	}, 500)
	if !ok {
		t.Fatalf("no view change: %s", c.describe())
	}
	c.submit(cl)
	c.settle()
	c.run(defaultHeartbeatTicks + 1)
	c.check()
	if cl.completed != 4 {
		t.Fatalf("new request not replied: completed %d", cl.completed)
	}
	for _, i := range []int{1, 2} {
		if !slices.Equal(c.services[i].ops[:len(committed)], committed) || len(c.services[i].ops) != 4 {
			t.Fatalf("replica %d executed %v, want committed prefix %v plus one", i, c.services[i].ops, committed)
		}
	}
}

func TestNewPrimaryWithBehindLogPicksLongestLog(t *testing.T) {
	c := newCluster(t, 3, 1)
	cl := c.addClient(0)
	c.filter = func(e envelope) bool { return e.from == 0 && e.to == 1 }
	c.submit(cl)
	c.settle()
	c.filter = nil
	if cl.completed != 1 {
		t.Fatalf("first request not committed by primary and backup 2")
	}
	c.crash(0)
	ok := c.waitFor(func() bool {
		return c.replicas[1].IsPrimary() && c.replicas[1].View() == 1 && c.replicas[2].Status() == StatusNormal
	}, 500)
	if !ok {
		t.Fatalf("no view change: %s", c.describe())
	}
	if got := c.replicas[1].OpNum(); got != 1 {
		t.Fatalf("new primary log length %d, want 1 taken from replica 2", got)
	}
	c.submit(cl)
	c.settle()
	c.run(defaultHeartbeatTicks + 1)
	c.check()
	want := []string{opKey(1, 1), opKey(1, 2)}
	for _, i := range []int{1, 2} {
		if !slices.Equal(c.services[i].ops, want) {
			t.Fatalf("replica %d executed %v, want %v", i, c.services[i].ops, want)
		}
	}
}

func TestLostPrepareRecoveredByResend(t *testing.T) {
	c := newCluster(t, 3, 1)
	cl := c.addClient(0)
	c.filter = func(e envelope) bool { return e.from == 0 && e.to == 1 }
	c.submit(cl)
	c.settle()
	c.filter = nil
	if len(c.services[1].ops) != 0 {
		t.Fatalf("replica 1 executed despite lost prepare")
	}
	c.run(defaultHeartbeatTicks + 2)
	c.check()
	if !slices.Equal(c.services[1].ops, []string{opKey(1, 1)}) {
		t.Fatalf("replica 1 executed %v after resend", c.services[1].ops)
	}
}

func TestLaggingBackupCatchesUpViaStateTransfer(t *testing.T) {
	c := newCluster(t, 3, 1)
	cl := c.addClient(0)
	c.isolate(2)
	for range 20 {
		c.submit(cl)
		c.settle()
	}
	c.heal()
	c.run(defaultHeartbeatTicks + 2)
	c.check()
	if got := c.replicas[2].CommitNum(); got != 20 {
		t.Fatalf("lagging backup commit = %d, want 20", got)
	}
	if !slices.Equal(c.services[2].ops, c.services[0].ops) {
		t.Fatalf("lagging backup executed %v", c.services[2].ops)
	}
}

func TestPartitionedReplicaRejoinsViaHigherView(t *testing.T) {
	c := newCluster(t, 5, 1)
	cl := c.addClient(0)
	c.submit(cl)
	c.settle()
	c.crash(0)
	c.isolate(4)
	majority := []int{1, 2, 3}
	ok := c.waitForReplicas(func() bool {
		for _, i := range majority {
			if c.replicas[i].Status() != StatusNormal || c.replicas[i].View() != 1 {
				return false
			}
		}
		return true
	}, 500, majority...)
	if !ok {
		t.Fatalf("view change did not complete: %s", c.describe())
	}
	if r := c.replicas[4]; r.Status() != StatusNormal || r.View() != 0 {
		t.Fatalf("isolated replica changed state: %s", c.describe())
	}
	c.submit(cl)
	c.settle()
	c.heal()
	c.run(defaultHeartbeatTicks + 2)
	c.check()
	if r := c.replicas[4]; r.View() != 1 || r.Status() != StatusNormal || r.CommitNum() != 2 {
		t.Fatalf("rejoined replica: %s", c.describe())
	}
	want := []string{opKey(1, 1), opKey(1, 2)}
	if !slices.Equal(c.services[4].ops, want) {
		t.Fatalf("rejoined replica executed %v, want %v", c.services[4].ops, want)
	}
}

func TestRecoverBackup(t *testing.T) {
	c := newCluster(t, 3, 1)
	cl := c.addClient(0)
	for range 5 {
		c.submit(cl)
		c.settle()
	}
	c.crash(2)
	for range 5 {
		c.submit(cl)
		c.settle()
	}
	c.restart(2)
	ok := c.waitFor(func() bool {
		return c.replicas[2].Status() == StatusNormal && c.replicas[2].CommitNum() == 10
	}, 200)
	if !ok {
		t.Fatalf("backup did not recover: %s", c.describe())
	}
	c.submit(cl)
	c.settle()
	c.run(defaultHeartbeatTicks + 1)
	c.check()
	if !slices.Equal(c.services[2].ops, c.services[0].ops) || len(c.services[2].ops) != 11 {
		t.Fatalf("recovered backup executed %v, primary executed %v", c.services[2].ops, c.services[0].ops)
	}
}

func TestRecoverCrashedPrimary(t *testing.T) {
	c := newCluster(t, 3, 1)
	cl := c.addClient(0)
	for range 3 {
		c.submit(cl)
		c.settle()
	}
	c.crash(0)
	c.restart(0)
	ok := c.waitFor(func() bool {
		r := c.replicas[0]
		return r.Status() == StatusNormal && r.View() == 1 && r.CommitNum() == 3
	}, 1000)
	if !ok {
		t.Fatalf("crashed primary did not recover: %s", c.describe())
	}
	c.submit(cl)
	c.settle()
	c.run(defaultHeartbeatTicks + 1)
	c.check()
	if !slices.Equal(c.services[0].ops, c.services[1].ops) || len(c.services[0].ops) != 4 {
		t.Fatalf("recovered primary executed %v, replica 1 executed %v", c.services[0].ops, c.services[1].ops)
	}
}

func TestRecoveringReplicaIgnoresProtocolMessages(t *testing.T) {
	c := newCluster(t, 3, 1)
	c.restart(1)
	r := c.replicas[1]
	e := Entry{Kind: EntryOp, ClientID: 1, RequestNum: 1, Op: []byte(opKey(1, 1))}
	r.Step(Prepare{View: 0, OpNum: 1, Entry: e})
	r.Step(StartView{View: 1, OpNum: 1, Log: []Entry{e}})
	r.Step(Commit{View: 0, CommitNum: 1})
	if r.Status() != StatusRecovering || r.OpNum() != 0 {
		t.Fatalf("recovering replica accepted protocol message: status=%v op=%d", r.Status(), r.OpNum())
	}
}

func TestDuplicateDoViewChangeCountsDistinctReplicas(t *testing.T) {
	c := newCluster(t, 5, 1)
	r := c.replicas[1]
	r.Step(StartViewChange{Epoch: 0, View: 1, Replica: 2})
	dvc := DoViewChange{View: 1, Replica: 2}
	r.Step(dvc)
	r.Step(dvc)
	if r.Status() != StatusViewChange {
		t.Fatalf("duplicate DoViewChange completed the view change")
	}
	r.Step(DoViewChange{View: 1, Replica: 3})
	if r.Status() != StatusNormal || r.View() != 1 || !r.IsPrimary() {
		t.Fatalf("view change did not complete: %s", c.describe())
	}
	starts := 0
	for _, e := range c.queue {
		if _, ok := e.msg.(StartView); ok {
			starts++
		}
	}
	if starts != 4 {
		t.Fatalf("sent %d StartView messages, want 4", starts)
	}
}

func TestDuplicateStartViewIgnored(t *testing.T) {
	c := newCluster(t, 3, 1)
	r := c.replicas[2]
	e := Entry{Kind: EntryOp, ClientID: 1, RequestNum: 1, Op: []byte(opKey(1, 1))}
	r.Step(StartView{View: 1, OpNum: 1, Log: []Entry{e}})
	r.Step(StartView{View: 1, OpNum: 2, Log: []Entry{e, e}})
	if r.Status() != StatusNormal || r.View() != 1 || r.OpNum() != 1 {
		t.Fatalf("duplicate StartView replaced the log: status=%v view=%d op=%d", r.Status(), r.View(), r.OpNum())
	}
}

func TestMessagesFromOtherEpochDropped(t *testing.T) {
	c := newCluster(t, 3, 1)
	r := c.replicas[1]
	e := Entry{Kind: EntryOp, ClientID: 1, RequestNum: 1, Op: []byte(opKey(1, 1))}
	r.Step(Prepare{Epoch: 1, View: 0, OpNum: 1, Entry: e})
	r.Step(StartViewChange{Epoch: 1, View: 1, Replica: 2})
	r.Step(Commit{Epoch: 2, View: 0, CommitNum: 1})
	if r.OpNum() != 0 || r.Status() != StatusNormal || r.View() != 0 {
		t.Fatalf("message from another epoch changed state")
	}
}

func TestLaggingReplicaDoesNotLoseExecutedOp(t *testing.T) {
	c := newCluster(t, 3, 1)
	cl := c.addClient(0)
	c.filter = func(e envelope) bool { return e.from == 0 && e.to == 2 }
	c.submit(cl)
	c.settle()
	if cl.completed != 1 {
		t.Fatalf("op A not committed by primary and backup 1")
	}
	c.filter = func(e envelope) bool {
		_, startView := e.msg.(StartView)
		_, getState := e.msg.(GetState)
		return (startView && e.to == 2) || (getState && e.from == 2)
	}
	c.isolate(0)
	if !c.waitForReplicas(func() bool { return c.replicas[1].IsPrimary() && c.replicas[1].View() == 1 }, 500, 1, 2) {
		t.Fatalf("view 1 did not start: %s", c.describe())
	}
	c.submit(cl)
	c.settle()
	c.group = []int{0, 1, 0}
	ok := c.waitForReplicas(func() bool {
		return c.replicas[2].IsPrimary() && c.replicas[2].View() == 2 && c.replicas[0].Status() == StatusNormal && c.replicas[0].View() == 2
	}, 500, 2)
	if !ok {
		t.Fatalf("view 2 did not start: %s", c.describe())
	}
	c.check()
	if !slices.Equal(c.services[0].ops, []string{opKey(1, 1)}) || !slices.Equal(c.services[2].ops, []string{opKey(1, 1)}) {
		t.Fatalf("view 2 lost executed op: r0=%v r2=%v", c.services[0].ops, c.services[2].ops)
	}
	c.filter = nil
	c.heal()
	ok = c.waitFor(func() bool {
		for _, r := range c.replicas {
			if r.Status() != StatusNormal || r.CommitNum() != 2 {
				return false
			}
		}
		return cl.completed == 2
	}, 1000)
	c.check()
	want := []string{opKey(1, 1), opKey(1, 2)}
	if !ok {
		t.Fatalf("did not converge: %s", c.describe())
	}
	for i := range c.replicas {
		if !slices.Equal(c.services[i].ops, want) {
			t.Fatalf("replica %d executed %v, want %v", i, c.services[i].ops, want)
		}
	}
}
