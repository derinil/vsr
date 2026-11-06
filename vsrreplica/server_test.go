package vsrreplica_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/derinil/vsr/kvservice"
	"github.com/derinil/vsr/vsr"
	"github.com/derinil/vsr/vsrclient"
	"github.com/derinil/vsr/vsrreplica"
	"github.com/derinil/vsr/vsrtcp"
)

const (
	testTick           = 5 * time.Millisecond
	testHeartbeat      = 20 * time.Millisecond
	testViewChange     = 100 * time.Millisecond
	testResend         = 40 * time.Millisecond
	testRequestTimeout = 50 * time.Millisecond
	testWait           = 10 * time.Second
)

type cluster struct {
	t       *testing.T
	config  []string
	dirs    []string
	servers []*vsrreplica.Server
	cancels []context.CancelFunc
	dones   []chan error
}

func startCluster(t *testing.T, n int) *cluster {
	t.Helper()
	c := &cluster{
		t:       t,
		config:  freeAddrs(t, n),
		dirs:    make([]string, n),
		servers: make([]*vsrreplica.Server, n),
		cancels: make([]context.CancelFunc, n),
		dones:   make([]chan error, n),
	}
	for i := range n {
		c.dirs[i] = t.TempDir()
		c.create(i)
	}
	for i := range n {
		c.run(i)
	}
	t.Cleanup(func() {
		for i := range n {
			c.stop(i)
		}
	})
	return c
}

func (c *cluster) options(i int) vsrreplica.Options {
	return vsrreplica.Options{
		Addr:              c.config[i],
		Listen:            c.config[i],
		Config:            c.config,
		Service:           kvservice.New(),
		DataDir:           c.dirs[i],
		Tick:              testTick,
		Heartbeat:         testHeartbeat,
		ViewChangeTimeout: testViewChange,
		Resend:            testResend,
		Logger:            slog.New(slog.DiscardHandler),
	}
}

func (c *cluster) create(i int) *vsrreplica.Server {
	s, err := vsrreplica.New(c.options(i))
	if err != nil {
		c.t.Fatalf("New(replica %d): %v", i, err)
	}
	c.servers[i] = s
	return s
}

func (c *cluster) run(i int) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.servers[i].Run(ctx) }()
	c.cancels[i], c.dones[i] = cancel, done
}

func (c *cluster) stop(i int) {
	if c.cancels[i] == nil {
		return
	}
	c.cancels[i]()
	if err := <-c.dones[i]; err != nil {
		c.t.Errorf("Run(replica %d): %v", i, err)
	}
	c.cancels[i], c.dones[i] = nil, nil
}

func (c *cluster) running(i int) bool {
	return c.cancels[i] != nil
}

func (c *cluster) primary() int {
	for i, s := range c.servers {
		if c.running(i) && s.State().Primary {
			return i
		}
	}
	return -1
}

func (c *cluster) client() *vsrclient.Client {
	cl, err := vsrclient.New(vsrclient.Options{
		Config: c.config,
		NewTransport: func(h vsr.Handler) vsr.Transport {
			return vsrtcp.New(h, vsrtcp.Options{})
		},
		RequestTimeout: testRequestTimeout,
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(func() { cl.Close() })
	return cl
}

func (c *cluster) converged() bool {
	p := c.primary()
	if p < 0 {
		return false
	}
	want := c.servers[p].State()
	for i, s := range c.servers {
		if c.running(i) && s.State().CommitNum != want.CommitNum {
			return false
		}
	}
	return true
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

func invoke(cl *vsrclient.Client, op []byte) (kvservice.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	out, err := cl.Invoke(ctx, op)
	if err != nil {
		return kvservice.Result{}, err
	}
	return kvservice.DecodeResult(out)
}

func expect(cl *vsrclient.Client, op []byte, code int, value string) error {
	r, err := invoke(cl, op)
	if err != nil {
		return err
	}
	if r.Code != code || string(r.Value) != value {
		return fmt.Errorf("got code %d value %s, want code %d value %q", r.Code, r.Value, code, value)
	}
	return nil
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
			cl := startCluster(t, tt.nodes).client()
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

func workload(cl *vsrclient.Client, key string, ops int) error {
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

func increment(cl *vsrclient.Client, key string) error {
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
	cls := make([]*vsrclient.Client, clients)
	for i := range cls {
		cls[i] = c.client()
	}
	if err := expect(cls[0], writeOp("counter", 0), 0, ""); err != nil {
		t.Fatal(err)
	}

	errs := make(chan error, clients)
	var wg sync.WaitGroup
	for i, cl := range cls {
		wg.Add(1)
		go func() {
			defer wg.Done()
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
		}()
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
	eventually(t, "replicas to converge", c.converged)
}

func TestStateReportsRoles(t *testing.T) {
	const writes = 3
	c := startCluster(t, 3)
	cl := c.client()
	for i, key := range []string{"a", "b", "c"} {
		if err := expect(cl, writeOp(key, i), 0, ""); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, "all replicas to commit", func() bool {
		for _, s := range c.servers {
			if s.State().CommitNum != writes {
				return false
			}
		}
		return true
	})
	for i, s := range c.servers {
		want := vsrreplica.State{
			Status:    vsr.StatusNormal,
			View:      0,
			OpNum:     writes,
			CommitNum: writes,
			Primary:   i == 0,
		}
		if got := s.State(); got != want {
			t.Errorf("replica %d state = %+v, want %+v", i, got, want)
		}
	}
}

func TestFailoverAndRestart(t *testing.T) {
	c := startCluster(t, 3)
	cl := c.client()
	if err := expect(cl, writeOp("a", 1), 0, ""); err != nil {
		t.Fatal(err)
	}

	var victim int
	var oldView uint64
	t.Run("failover", func(t *testing.T) {
		victim = c.primary()
		oldView = c.servers[victim].State().View
		c.stop(victim)

		eventually(t, "new primary", func() bool {
			p := c.primary()
			return p >= 0 && c.servers[p].State().View > oldView
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
		if st := restarted.State().Status; st != vsr.StatusRecovering {
			t.Fatalf("status before Run = %v, want %v", st, vsr.StatusRecovering)
		}
		c.run(victim)

		eventually(t, "restarted replica to recover", func() bool {
			p := c.primary()
			if p < 0 {
				return false
			}
			st, ps := restarted.State(), c.servers[p].State()
			return st.Status == vsr.StatusNormal && st.View == ps.View && st.CommitNum == ps.CommitNum && st.CommitNum > 0
		})
		if err := expect(cl, writeOp("c", 3), 0, ""); err != nil {
			t.Fatal(err)
		}
		if err := expect(cl, readOp("a"), 0, "1"); err != nil {
			t.Fatal(err)
		}
		eventually(t, "replicas to converge", c.converged)
	})
}

func TestNewValidation(t *testing.T) {
	addrs := freeAddrs(t, 3)
	tests := []struct {
		name   string
		mutate func(*vsrreplica.Options)
	}{
		{"empty config", func(o *vsrreplica.Options) { o.Config = nil }},
		{"address not in config", func(o *vsrreplica.Options) { o.Addr = "127.0.0.1:1" }},
		{"duplicate address", func(o *vsrreplica.Options) { o.Config = []string{addrs[0], addrs[0], addrs[1]} }},
		{"nil service", func(o *vsrreplica.Options) { o.Service = nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := vsrreplica.Options{
				Addr:    addrs[0],
				Listen:  addrs[0],
				Config:  addrs,
				Service: kvservice.New(),
				DataDir: t.TempDir(),
			}
			tt.mutate(&o)
			s, err := vsrreplica.New(o)
			if !errors.Is(err, vsr.ErrInvalidOptions) {
				t.Fatalf("New() = %v, %v; want ErrInvalidOptions", s, err)
			}
		})
	}
}
