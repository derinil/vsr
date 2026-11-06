package vsr

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/derinil/vsr/internal/kvservice"
	"github.com/derinil/vsr/internal/protocol"
)

const (
	testTick           = 5 * time.Millisecond
	testHeartbeat      = 20 * time.Millisecond
	testViewChange     = 100 * time.Millisecond
	testResend         = 40 * time.Millisecond
	testRequestTimeout = 50 * time.Millisecond
	testWait           = 10 * time.Second
)

type node struct {
	addr     string
	httpAddr string
	dir      string
	config   []string
	join     bool
	svc      *kvservice.Store
	srv      *Replica
	cancel   context.CancelFunc
	done     chan error
}

type cluster struct {
	t                  *testing.T
	addrs              []string
	nodes              []*node
	checkpointInterval int
}

func newCluster(t *testing.T, addrs []string) *cluster {
	t.Helper()
	c := &cluster{t: t, addrs: addrs, nodes: make([]*node, len(addrs))}
	for i, addr := range addrs {
		c.nodes[i] = &node{addr: addr, dir: t.TempDir()}
	}
	t.Cleanup(func() {
		for i := range c.nodes {
			c.stop(i)
		}
	})
	return c
}

func startCluster(t *testing.T, n int) *cluster {
	return startClusterWith(t, n, 0)
}

func startClusterWith(t *testing.T, n, checkpointInterval int) *cluster {
	t.Helper()
	addrs := freeAddrs(t, n)
	c := newCluster(t, addrs)
	c.checkpointInterval = checkpointInterval
	for i := range n {
		c.add(i, addrs, false)
	}
	for i := range n {
		c.run(i)
	}
	return c
}

func (c *cluster) add(i int, config []string, join bool) {
	c.nodes[i].config, c.nodes[i].join = config, join
	c.create(i)
}

func (c *cluster) create(i int) *Replica {
	n := c.nodes[i]
	n.svc = kvservice.New()
	s, err := NewReplica(ReplicaOptions{
		Addr:               n.addr,
		Listen:             n.addr,
		HTTPAddr:           n.httpAddr,
		Config:             n.config,
		Join:               n.join,
		Service:            n.svc,
		DataDir:            n.dir,
		Tick:               testTick,
		Heartbeat:          testHeartbeat,
		ViewChangeTimeout:  testViewChange,
		Resend:             testResend,
		CheckpointInterval: c.checkpointInterval,
		Logger:             slog.New(slog.DiscardHandler),
	})
	if err != nil {
		c.t.Fatalf("New(replica %d): %v", i, err)
	}
	n.srv = s
	return s
}

func (c *cluster) run(i int) {
	n := c.nodes[i]
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	srv := n.srv
	go func() { done <- srv.Run(ctx) }()
	n.cancel, n.done = cancel, done
}

func (c *cluster) stop(i int) {
	n := c.nodes[i]
	if n.cancel == nil {
		return
	}
	n.cancel()
	if err := <-n.done; err != nil {
		c.t.Errorf("Run(replica %d): %v", i, err)
	}
	n.cancel, n.done = nil, nil
}

func (c *cluster) running(i int) bool {
	return c.nodes[i].cancel != nil
}

func (c *cluster) node(addr string) *node {
	return c.nodes[slices.Index(c.addrs, addr)]
}

func (c *cluster) primary() int {
	for i, n := range c.nodes {
		if c.running(i) && n.srv.State().Primary {
			return i
		}
	}
	return -1
}

func (c *cluster) client(config []string) *Client {
	cl, err := NewClient(ClientOptions{
		Config:         config,
		RequestTimeout: testRequestTimeout,
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(func() { cl.Close() })
	return cl
}

func (c *cluster) converged(config []string) bool {
	var ref State
	primary := false
	for i, addr := range config {
		n := c.node(addr)
		if n.cancel == nil {
			return false
		}
		st := n.srv.State()
		if st.Status != protocol.StatusNormal {
			return false
		}
		if i == 0 {
			ref = st
		}
		if st.Epoch != ref.Epoch || st.CommitNum != ref.CommitNum {
			return false
		}
		primary = primary || st.Primary
	}
	return primary
}

func freeAddrs(t *testing.T, n int) []string {
	t.Helper()
	lns := make([]net.Listener, n)
	addrs := make([]string, n)
	for i := range n {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns[i] = ln
		addrs[i] = ln.Addr().String()
	}
	for _, ln := range lns {
		ln.Close()
	}
	return addrs
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(testWait)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(testTick)
	}
}

func writeOp(key, value any) []byte {
	op, err := kvservice.Write(key, value)
	if err != nil {
		panic(err)
	}
	return op
}

func readOp(key any) []byte {
	op, err := kvservice.Read(key)
	if err != nil {
		panic(err)
	}
	return op
}

func casOp(key, from, to any) []byte {
	op, err := kvservice.CAS(key, from, to)
	if err != nil {
		panic(err)
	}
	return op
}

func invoke(cl *Client, op []byte) (kvservice.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	out, err := cl.Invoke(ctx, op)
	if err != nil {
		return kvservice.Result{}, err
	}
	return kvservice.DecodeResult(out)
}

func expect(cl *Client, op []byte, code int, value string) error {
	r, err := invoke(cl, op)
	if err != nil {
		return err
	}
	if r.Code != code || string(r.Value) != value {
		return fmt.Errorf("got code %d value %s, want code %d value %q", r.Code, r.Value, code, value)
	}
	return nil
}

func writeAll(t *testing.T, cl *Client, prefix string, n int) {
	t.Helper()
	for i := range n {
		if err := expect(cl, writeOp(fmt.Sprintf("%s-%d", prefix, i), i), 0, ""); err != nil {
			t.Fatal(err)
		}
	}
}

func readAll(t *testing.T, cl *Client, prefix string, n int) {
	t.Helper()
	for i := range n {
		if err := expect(cl, readOp(fmt.Sprintf("%s-%d", prefix, i)), 0, strconv.Itoa(i)); err != nil {
			t.Fatal(err)
		}
	}
}

func reconfigure(t *testing.T, cl *Client, config []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	if err := cl.Reconfigure(ctx, config); err != nil {
		t.Fatalf("Reconfigure(%v): %v", config, err)
	}
	if cl.Epoch() != 1 || !slices.Equal(cl.Config(), config) {
		t.Fatalf("client at epoch %d config %v after Reconfigure", cl.Epoch(), cl.Config())
	}
}

type persistedState struct {
	Epoch     uint64   `json:"epoch"`
	Config    []string `json:"config"`
	OldConfig []string `json:"old_config"`
}

func readCheckpointFile(dir string) ([]byte, protocol.Checkpoint, error) {
	b, err := os.ReadFile(filepath.Join(dir, "checkpoint.bin"))
	if err != nil {
		return nil, protocol.Checkpoint{}, err
	}
	if len(b) < sha256.Size {
		return nil, protocol.Checkpoint{}, errors.New("short checkpoint file")
	}
	c, err := protocol.DecodeCheckpoint(b[sha256.Size:])
	return b, c, err
}

func readPersistedState(t *testing.T, dir string) persistedState {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var st persistedState
	if err := json.Unmarshal(b, &st); err != nil {
		t.Fatal(err)
	}
	return st
}

func TestKVOperations(t *testing.T) {
	tests := []struct {
		name  string
		nodes int
	}{
		{"single node", 1},
		{"three nodes", 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := startCluster(t, tt.nodes)
			cl := c.client(c.addrs)
			steps := []struct {
				name  string
				op    []byte
				code  int
				value string
			}{
				{"write", writeOp("k", "v1"), 0, ""},
				{"read", readOp("k"), 0, `"v1"`},
				{"cas", casOp("k", "v1", "v2"), 0, ""},
				{"cas precondition", casOp("k", "v1", "v3"), kvservice.CodePreconditionFailed, ""},
				{"read after cas", readOp("k"), 0, `"v2"`},
				{"read missing", readOp("missing"), kvservice.CodeNotFound, ""},
			}
			for _, st := range steps {
				t.Run(st.name, func(t *testing.T) {
					if err := expect(cl, st.op, st.code, st.value); err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	}
}

func workload(cl *Client, key string, ops int) error {
	for i := range ops {
		if err := expect(cl, writeOp(key, i), 0, ""); err != nil {
			return fmt.Errorf("write %s=%d: %w", key, i, err)
		}
		if err := expect(cl, readOp(key), 0, strconv.Itoa(i)); err != nil {
			return fmt.Errorf("read %s after %d: %w", key, i, err)
		}
	}
	return nil
}

func increment(cl *Client, key string) error {
	for {
		r, err := invoke(cl, readOp(key))
		if err != nil {
			return err
		}
		cur, err := strconv.Atoi(string(r.Value))
		if err != nil {
			return fmt.Errorf("counter value %s: %w", r.Value, err)
		}
		r, err = invoke(cl, casOp(key, cur, cur+1))
		if err != nil {
			return err
		}
		switch r.Code {
		case 0:
			return nil
		case kvservice.CodePreconditionFailed:
		default:
			return fmt.Errorf("cas %s: code %d %s", key, r.Code, r.Error)
		}
	}
}

func TestConcurrentClients(t *testing.T) {
	const clients, ops = 8, 50
	c := startCluster(t, 3)
	cls := make([]*Client, clients)
	for i := range cls {
		cls[i] = c.client(c.addrs)
	}
	if err := expect(cls[0], writeOp("counter", 0), 0, ""); err != nil {
		t.Fatal(err)
	}

	errs := make(chan error, clients)
	var wg sync.WaitGroup
	for i, cl := range cls {
		wg.Go(func() {
			if err := workload(cl, fmt.Sprintf("key-%d", i), ops); err != nil {
				errs <- err
				return
			}
			for range ops {
				if err := increment(cl, "counter"); err != nil {
					errs <- err
					return
				}
			}
			errs <- nil
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}

	if err := expect(cls[0], readOp("counter"), 0, strconv.Itoa(clients*ops)); err != nil {
		t.Error(err)
	}
	for i := range clients {
		if err := expect(cls[0], readOp(fmt.Sprintf("key-%d", i)), 0, strconv.Itoa(ops-1)); err != nil {
			t.Errorf("key-%d: %v", i, err)
		}
	}
	eventually(t, "replicas to converge", func() bool { return c.converged(c.addrs) })
}

func TestStateReportsRoles(t *testing.T) {
	const writes = 3
	c := startCluster(t, 3)
	cl := c.client(c.addrs)
	for i, key := range []string{"a", "b", "c"} {
		if err := expect(cl, writeOp(key, i), 0, ""); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, "all replicas to commit", func() bool {
		for _, n := range c.nodes {
			if n.srv.State().CommitNum != writes {
				return false
			}
		}
		return true
	})
	for i, n := range c.nodes {
		want := State{
			Status:    protocol.StatusNormal,
			View:      0,
			OpNum:     writes,
			CommitNum: writes,
			Primary:   i == 0,
		}
		if got := n.srv.State(); got != want {
			t.Errorf("replica %d state = %+v, want %+v", i, got, want)
		}
	}
}

func TestFailoverAndRestart(t *testing.T) {
	c := startCluster(t, 3)
	cl := c.client(c.addrs)
	if err := expect(cl, writeOp("a", 1), 0, ""); err != nil {
		t.Fatal(err)
	}

	var victim int
	var oldView uint64
	t.Run("failover", func(t *testing.T) {
		victim = c.primary()
		oldView = c.nodes[victim].srv.State().View
		c.stop(victim)

		eventually(t, "new primary", func() bool {
			p := c.primary()
			return p >= 0 && c.nodes[p].srv.State().View > oldView
		})
		if err := expect(cl, writeOp("b", 2), 0, ""); err != nil {
			t.Fatal(err)
		}
		if err := expect(cl, readOp("a"), 0, "1"); err != nil {
			t.Fatal(err)
		}
		if err := expect(cl, readOp("b"), 0, "2"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("restart", func(t *testing.T) {
		restarted := c.create(victim)
		if st := restarted.State().Status; st != protocol.StatusRecovering {
			t.Fatalf("status before Run = %v, want %v", st, protocol.StatusRecovering)
		}
		c.run(victim)

		eventually(t, "restarted replica to recover", func() bool {
			p := c.primary()
			if p < 0 {
				return false
			}
			st, ps := restarted.State(), c.nodes[p].srv.State()
			return st.Status == protocol.StatusNormal && st.View == ps.View && st.CommitNum == ps.CommitNum && st.CommitNum > 0
		})
		if err := expect(cl, writeOp("c", 3), 0, ""); err != nil {
			t.Fatal(err)
		}
		if err := expect(cl, readOp("a"), 0, "1"); err != nil {
			t.Fatal(err)
		}
		eventually(t, "replicas to converge", func() bool { return c.converged(c.addrs) })
	})
}

func TestReconfigureGrow(t *testing.T) {
	addrs := freeAddrs(t, 5)
	old, next := addrs[:3], addrs
	c := newCluster(t, addrs)
	for i := range 3 {
		c.add(i, old, false)
	}
	for i := 3; i < 5; i++ {
		c.add(i, next, true)
	}
	for i := range 5 {
		c.run(i)
	}

	oldCl := c.client(old)
	writeAll(t, oldCl, "a", 5)
	reconfigure(t, c.client(old), next)
	eventually(t, "new configuration to converge", func() bool { return c.converged(next) })

	readAll(t, oldCl, "a", 5)
	newCl := c.client(next)
	readAll(t, newCl, "a", 5)
	writeAll(t, newCl, "b", 5)
	readAll(t, newCl, "b", 5)
	readAll(t, oldCl, "b", 5)
}

func TestReconfigureReplaceAll(t *testing.T) {
	addrs := freeAddrs(t, 6)
	old, next := addrs[:3], addrs[3:]
	c := newCluster(t, addrs)
	for i := range 3 {
		c.add(i, old, false)
	}
	for i := 3; i < 6; i++ {
		c.add(i, next, true)
	}
	for i := range 6 {
		c.run(i)
	}

	oldCl := c.client(old)
	writeAll(t, oldCl, "a", 5)
	reconfigure(t, c.client(old), next)
	eventually(t, "replaced replicas to shut down", func() bool {
		for _, addr := range old {
			if c.node(addr).srv.State().Status != protocol.StatusShutdown {
				return false
			}
		}
		return true
	})
	eventually(t, "new configuration to converge", func() bool { return c.converged(next) })

	readAll(t, oldCl, "a", 5)
	newCl := c.client(next)
	readAll(t, newCl, "a", 5)
	writeAll(t, newCl, "b", 5)
	readAll(t, newCl, "b", 5)
}

func TestReconfigurePersistsAndRecovers(t *testing.T) {
	addrs := freeAddrs(t, 4)
	old, next := addrs[:3], addrs
	c := newCluster(t, addrs)
	for i := range 3 {
		c.add(i, old, false)
	}
	c.add(3, next, true)
	for i := range 4 {
		c.run(i)
	}

	writeAll(t, c.client(old), "a", 5)
	reconfigure(t, c.client(old), next)
	eventually(t, "new configuration to converge", func() bool { return c.converged(next) })

	const member = 1
	st := readPersistedState(t, c.nodes[member].dir)
	if st.Epoch != 1 || !slices.Equal(st.Config, next) || !slices.Equal(st.OldConfig, old) {
		t.Fatalf("persisted state = %+v, want epoch 1 config %v old %v", st, next, old)
	}

	c.stop(member)
	writeAll(t, c.client(next), "b", 5)
	restarted := c.create(member)
	if s := restarted.State(); s.Status != protocol.StatusRecovering || s.Epoch != 1 {
		t.Fatalf("state before Run = %+v, want recovering in epoch 1", s)
	}
	c.run(member)
	eventually(t, "restarted member to catch up", func() bool { return c.converged(next) })

	newCl := c.client(next)
	readAll(t, newCl, "a", 5)
	readAll(t, newCl, "b", 5)
}

func TestRestartedReplacedReplicaRedirectsClients(t *testing.T) {
	addrs := freeAddrs(t, 6)
	old, next := addrs[:3], addrs[3:]
	c := newCluster(t, addrs)
	for i := range 3 {
		c.add(i, old, false)
	}
	for i := 3; i < 6; i++ {
		c.add(i, next, true)
	}
	for i := range 6 {
		c.run(i)
	}

	writeAll(t, c.client(old), "a", 5)
	reconfigure(t, c.client(old), next)
	eventually(t, "replaced replicas to shut down", func() bool {
		for _, addr := range old {
			if c.node(addr).srv.State().Status != protocol.StatusShutdown {
				return false
			}
		}
		return true
	})
	eventually(t, "new configuration to converge", func() bool { return c.converged(next) })

	for i := range 3 {
		c.stop(i)
	}
	restarted := c.create(0)
	if s := restarted.State(); s.Status != protocol.StatusShutdown || s.Epoch != 1 {
		t.Fatalf("state before Run = %+v, want shutdown in epoch 1", s)
	}
	c.run(0)

	stale := c.client(old)
	readAll(t, stale, "a", 5)
	if !slices.Equal(stale.Config(), next) || stale.Epoch() != 1 {
		t.Fatalf("stale client at epoch %d config %v, want epoch 1 config %v", stale.Epoch(), stale.Config(), next)
	}
	writeAll(t, stale, "b", 5)
	readAll(t, c.client(next), "b", 5)
}

func TestJoinerRestartBeforeJoining(t *testing.T) {
	addrs := freeAddrs(t, 4)
	old, next := addrs[:3], addrs
	c := newCluster(t, addrs)
	for i := range 3 {
		c.add(i, old, false)
	}
	c.add(3, next, true)
	for i := range 4 {
		c.run(i)
	}

	writeAll(t, c.client(old), "a", 5)
	c.stop(3)
	c.create(3)
	if st := c.nodes[3].srv.State(); st.Status != protocol.StatusTransitioning || st.Epoch != 0 {
		t.Fatalf("joiner state before Run = %+v, want transitioning in epoch 0", st)
	}
	c.run(3)
	if _, err := os.Stat(filepath.Join(c.nodes[3].dir, "state.json")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("joiner wrote state before joining: %v", err)
	}

	reconfigure(t, c.client(old), next)
	eventually(t, "joiner to join", func() bool { return c.converged(next) })
	readAll(t, c.client(next), "a", 5)
	if st := readPersistedState(t, c.nodes[3].dir); st.Epoch != 1 || !slices.Equal(st.Config, next) {
		t.Fatalf("joiner persisted state = %+v", st)
	}
}

func TestRestartFromCheckpoint(t *testing.T) {
	const interval, writes, victim = 5, 12, 2
	tests := []struct {
		name     string
		corrupt  func([]byte) []byte
		restored bool
	}{
		{"intact", nil, true},
		{"truncated", func(b []byte) []byte { return b[:len(b)/2] }, false},
		{"flipped byte", func(b []byte) []byte { b[len(b)-1] ^= 0xff; return b }, false},
		{"empty", func([]byte) []byte { return nil }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := startClusterWith(t, 3, interval)
			cl := c.client(c.addrs)
			writeAll(t, cl, "a", writes)
			eventually(t, "checkpoints to persist on every replica", func() bool {
				for _, n := range c.nodes {
					if _, ckpt, err := readCheckpointFile(n.dir); err != nil || ckpt.OpNum < 2*interval {
						return false
					}
				}
				return true
			})

			c.stop(victim)
			path := filepath.Join(c.nodes[victim].dir, "checkpoint.bin")
			raw, ckpt, err := readCheckpointFile(c.nodes[victim].dir)
			if err != nil {
				t.Fatal(err)
			}
			if tt.corrupt != nil {
				if err := os.WriteFile(path, tt.corrupt(raw), 0o644); err != nil {
					t.Fatal(err)
				}
			}

			restarted := c.create(victim)
			st := restarted.State()
			if st.Status != protocol.StatusRecovering {
				t.Fatalf("status before Run = %v, want %v", st.Status, protocol.StatusRecovering)
			}
			var want []byte
			wantOp := uint64(0)
			if tt.restored {
				wantOp, want = ckpt.OpNum, ckpt.State
			} else {
				want = kvservice.New().Snapshot()
			}
			if st.CommitNum != wantOp || st.LogBase != wantOp {
				t.Fatalf("restored commit %d base %d, want %d", st.CommitNum, st.LogBase, wantOp)
			}
			if !bytes.Equal(c.nodes[victim].svc.Snapshot(), want) {
				t.Fatalf("restored service state does not match expected state")
			}
			c.run(victim)

			writeAll(t, cl, "b", 3)
			eventually(t, "restarted replica to converge", func() bool { return c.converged(c.addrs) })
			readAll(t, cl, "a", writes)
			readAll(t, cl, "b", 3)

			for i := range c.nodes {
				c.stop(i)
			}
			for i := 1; i < len(c.nodes); i++ {
				if !bytes.Equal(c.nodes[i].svc.Snapshot(), c.nodes[0].svc.Snapshot()) {
					t.Fatalf("replica %d service state differs from replica 0", i)
				}
			}
		})
	}
}

func TestNewValidation(t *testing.T) {
	addrs := freeAddrs(t, 3)
	tests := []struct {
		name   string
		mutate func(*ReplicaOptions)
	}{
		{"empty config", func(o *ReplicaOptions) { o.Config = nil }},
		{"address not in config", func(o *ReplicaOptions) { o.Addr = "127.0.0.1:1" }},
		{"duplicate address", func(o *ReplicaOptions) { o.Config = []string{addrs[0], addrs[0], addrs[1]} }},
		{"nil service", func(o *ReplicaOptions) { o.Service = nil }},
		{"negative checkpoint retain", func(o *ReplicaOptions) { o.CheckpointRetain = -1 }},
		{"negative page size", func(o *ReplicaOptions) { o.PageSize = -1 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := ReplicaOptions{
				Addr:    addrs[0],
				Listen:  addrs[0],
				Config:  addrs,
				Service: kvservice.New(),
				DataDir: t.TempDir(),
			}
			tt.mutate(&o)
			s, err := NewReplica(o)
			if !errors.Is(err, protocol.ErrInvalidOptions) {
				t.Fatalf("New() = %v, %v; want ErrInvalidOptions", s, err)
			}
		})
	}
}
