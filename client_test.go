package vsr

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/derinil/vsr/internal/protocol"
)

const (
	testID      = 7
	testTimeout = 50 * time.Millisecond
)

var (
	testConfig = []string{"r0", "r1", "r2"}
	newConfig  = []string{"n0", "n1", "n2"}
)

type delivery struct {
	to string
	m  protocol.Message
}

type responder func(f *fakeNet, to string, m protocol.Message)

type fakeNet struct {
	mu      sync.Mutex
	handler Handler
	respond responder
	sent    []delivery
	closes  int
}

func (f *fakeNet) Send(to string, b []byte) {
	m, err := protocol.DecodeMessage(b)
	if err != nil {
		panic(err)
	}
	f.mu.Lock()
	f.sent = append(f.sent, delivery{to, m})
	f.mu.Unlock()
	if f.respond != nil {
		f.respond(f, to, m)
	}
}

func (f *fakeNet) Reply(uint64, []byte) {}

func (f *fakeNet) Listen(string) error { return nil }

func (f *fakeNet) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closes++
	return nil
}

func (f *fakeNet) deliver(m protocol.Message) { f.handler(encode(m)) }

func (f *fakeNet) messages() []delivery {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.sent)
}

func (f *fakeNet) closeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closes
}

func noopTransport(Handler) Transport { return &fakeNet{} }

func newClient(t *testing.T, respond responder, config []string) (*Client, *fakeNet) {
	t.Helper()
	var fake *fakeNet
	c, err := NewClient(ClientOptions{
		Config: config,
		NewTransport: func(h Handler) Transport {
			fake = &fakeNet{handler: h, respond: respond}
			return fake
		},
		ClientID:       testID,
		RequestTimeout: testTimeout,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c, fake
}

func clientRequest(m protocol.Message) (epoch, num uint64, ok bool) {
	switch m := m.(type) {
	case protocol.Request:
		return m.Epoch, m.RequestNum, true
	case protocol.Reconfiguration:
		return m.Epoch, m.RequestNum, true
	case protocol.CheckEpoch:
		return m.Epoch, m.RequestNum, true
	}
	return 0, 0, false
}

func replyTo(m protocol.Message, view uint64, result string) (protocol.Reply, bool) {
	epoch, num, ok := clientRequest(m)
	if !ok {
		return protocol.Reply{}, false
	}
	id, _ := protocol.ClientOf(m)
	return protocol.Reply{Epoch: epoch, View: view, ClientID: id, RequestNum: num, Result: []byte(result)}, true
}

func replyOn(addr string, view uint64, result string) responder {
	return func(f *fakeNet, to string, m protocol.Message) {
		if r, ok := replyTo(m, view, result); ok && to == addr {
			f.deliver(r)
		}
	}
}

func answerAll(f *fakeNet, _ string, m protocol.Message) {
	if r, ok := replyTo(m, 0, "ok"); ok {
		f.deliver(r)
	}
}

func wantRequest(t *testing.T, d delivery, to string, epoch, num uint64) {
	t.Helper()
	gotEpoch, gotNum, ok := clientRequest(d.m)
	if d.to != to || !ok || gotEpoch != epoch || gotNum != num {
		t.Fatalf("sent %#v to %s, want request epoch %d num %d to %s", d.m, d.to, epoch, num, to)
	}
}

func TestNewClient(t *testing.T) {
	tests := []struct {
		name string
		opts ClientOptions
	}{
		{"empty config", ClientOptions{NewTransport: noopTransport}},
		{"duplicate address", ClientOptions{Config: []string{"r0", "r1", "r0"}, NewTransport: noopTransport}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, err := NewClient(tc.opts)
			if err == nil || c != nil {
				t.Fatalf("NewClient() = %v, %v; want error", c, err)
			}
		})
	}
}

func TestRandomClientID(t *testing.T) {
	a, err := NewClient(ClientOptions{Config: testConfig, NewTransport: noopTransport})
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewClient(ClientOptions{Config: testConfig, NewTransport: noopTransport})
	if err != nil {
		t.Fatal(err)
	}
	if a.ID() == 0 || b.ID() == 0 || a.ID() == b.ID() {
		t.Fatalf("ids = %d, %d; want distinct nonzero", a.ID(), b.ID())
	}
}

func TestInvoke(t *testing.T) {
	tests := []struct {
		name      string
		respond   responder
		wantView  uint64
		wantEpoch uint64
		check     func(t *testing.T, sent []delivery)
	}{
		{
			name:    "reply from primary",
			respond: replyOn("r0", 0, "ok"),
			check: func(t *testing.T, sent []delivery) {
				if len(sent) != 1 {
					t.Fatalf("sent %d messages, want 1", len(sent))
				}
				wantRequest(t, sent[0], "r0", 0, 1)
			},
		},
		{
			name:     "unresponsive primary broadcasts and adopts higher view",
			respond:  replyOn("r1", 1, "ok"),
			wantView: 1,
			check: func(t *testing.T, sent []delivery) {
				if len(sent) != 4 {
					t.Fatalf("sent %d messages, want 4", len(sent))
				}
				for i, addr := range []string{"r0", "r0", "r1", "r2"} {
					wantRequest(t, sent[i], addr, 0, 1)
				}
			},
		},
		{
			name: "stale reply ignored",
			respond: func(f *fakeNet, to string, m protocol.Message) {
				r, ok := replyTo(m, 0, "ok")
				if !ok || to != "r0" {
					return
				}
				stale := r
				stale.RequestNum--
				stale.Result = []byte("stale")
				f.deliver(stale)
				f.deliver(r)
			},
			check: func(t *testing.T, sent []delivery) {
				if len(sent) != 1 {
					t.Fatalf("sent %d messages, want 1", len(sent))
				}
			},
		},
		{
			name: "reply for other client ignored",
			respond: func(f *fakeNet, to string, m protocol.Message) {
				r, ok := replyTo(m, 0, "ok")
				if !ok || to != "r0" {
					return
				}
				other := r
				other.ClientID++
				other.View = 2
				other.Result = []byte("other")
				f.deliver(other)
				f.deliver(r)
			},
			check: func(t *testing.T, sent []delivery) {
				if len(sent) != 1 {
					t.Fatalf("sent %d messages, want 1", len(sent))
				}
			},
		},
		{
			name: "new epoch redirects request to new primary",
			respond: func(f *fakeNet, to string, m protocol.Message) {
				switch to {
				case "r0":
					f.deliver(protocol.NewEpoch{Epoch: 1, Config: newConfig})
				case "n0":
					if r, ok := replyTo(m, 0, "ok"); ok {
						f.deliver(r)
					}
				}
			},
			wantEpoch: 1,
			check: func(t *testing.T, sent []delivery) {
				if len(sent) != 2 {
					t.Fatalf("sent %d messages, want 2", len(sent))
				}
				wantRequest(t, sent[0], "r0", 0, 1)
				wantRequest(t, sent[1], "n0", 1, 1)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, fake := newClient(t, tc.respond, testConfig)
			got, err := c.Invoke(context.Background(), []byte("op"))
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != "ok" {
				t.Fatalf("result = %q, want %q", got, "ok")
			}
			if c.View() != tc.wantView {
				t.Fatalf("view = %d, want %d", c.View(), tc.wantView)
			}
			if c.Epoch() != tc.wantEpoch {
				t.Fatalf("epoch = %d, want %d", c.Epoch(), tc.wantEpoch)
			}
			tc.check(t, fake.messages())
		})
	}
}

func TestViewChangeRoutesNextRequest(t *testing.T) {
	c, fake := newClient(t, replyOn("r1", 1, "ok"), testConfig)
	ctx := context.Background()
	for _, op := range []string{"a", "b"} {
		if _, err := c.Invoke(ctx, []byte(op)); err != nil {
			t.Fatal(err)
		}
	}
	sent := fake.messages()
	if len(sent) != 5 {
		t.Fatalf("sent %d messages, want 5", len(sent))
	}
	wantRequest(t, sent[4], "r1", 0, 2)
}

func TestInvokeContext(t *testing.T) {
	tests := []struct {
		name    string
		ctx     func() (context.Context, context.CancelFunc)
		wantErr error
	}{
		{
			name: "canceled",
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, cancel
			},
			wantErr: context.Canceled,
		},
		{
			name: "deadline",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), testTimeout/5)
			},
			wantErr: context.DeadlineExceeded,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			answer := false
			c, _ := newClient(t, func(f *fakeNet, to string, m protocol.Message) {
				if r, ok := replyTo(m, 0, "ok"); ok && answer && to == "r0" {
					f.deliver(r)
				}
			}, testConfig)
			ctx, cancel := tc.ctx()
			defer cancel()
			if _, err := c.Invoke(ctx, []byte("op")); !errors.Is(err, tc.wantErr) {
				t.Fatalf("Invoke() error = %v, want %v", err, tc.wantErr)
			}
			answer = true
			got, err := c.Invoke(context.Background(), []byte("op"))
			if err != nil || string(got) != "ok" {
				t.Fatalf("Invoke() after cancel = %q, %v; want ok", got, err)
			}
		})
	}
}

func TestReconfigure(t *testing.T) {
	c, fake := newClient(t, func(f *fakeNet, to string, m protocol.Message) {
		switch to {
		case "r1":
			if r, ok := replyTo(m, 1, "ok"); ok {
				f.deliver(r)
			}
		case "n0":
			if r, ok := replyTo(m, 0, "ok"); ok {
				f.deliver(r)
			}
		}
	}, testConfig)
	ctx := context.Background()
	if _, err := c.Invoke(ctx, []byte("op")); err != nil {
		t.Fatal(err)
	}
	if c.View() != 1 {
		t.Fatalf("view = %d, want 1", c.View())
	}
	if err := c.Reconfigure(ctx, newConfig); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(c.Config(), newConfig) {
		t.Fatalf("config = %v, want %v", c.Config(), newConfig)
	}
	if c.Epoch() != 1 || c.View() != 0 {
		t.Fatalf("epoch, view = %d, %d; want 1, 0", c.Epoch(), c.View())
	}

	sent := fake.messages()
	if len(sent) != 6 {
		t.Fatalf("sent %d messages, want 6", len(sent))
	}
	rc, ok := sent[4].m.(protocol.Reconfiguration)
	if sent[4].to != "r1" || !ok || rc.Epoch != 0 || rc.RequestNum != 2 || !slices.Equal(rc.Config, newConfig) {
		t.Fatalf("sent %#v to %s, want Reconfiguration epoch 0 num 2 to r1", sent[4].m, sent[4].to)
	}
	ce, ok := sent[5].m.(protocol.CheckEpoch)
	if sent[5].to != "n0" || !ok || ce.Epoch != 1 || ce.RequestNum != 3 {
		t.Fatalf("sent %#v to %s, want CheckEpoch epoch 1 num 3 to n0", sent[5].m, sent[5].to)
	}
}

func TestReconfigureKeepsNewerEpoch(t *testing.T) {
	newer := []string{"x0", "x1", "x2"}
	adopted := false
	c, _ := newClient(t, func(f *fakeNet, to string, m protocol.Message) {
		if _, ok := m.(protocol.Reconfiguration); ok && !adopted {
			adopted = true
			f.deliver(protocol.NewEpoch{Epoch: 2, View: 3, Config: newer})
		}
		answerAll(f, to, m)
	}, testConfig)
	if err := c.Reconfigure(context.Background(), newConfig); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(c.Config(), newer) || c.Epoch() != 2 || c.View() != 3 {
		t.Fatalf("config %v epoch %d view %d, want %v 2 3", c.Config(), c.Epoch(), c.View(), newer)
	}
}

func TestReconfigureValidation(t *testing.T) {
	tests := []struct {
		name   string
		config []string
	}{
		{"nil", nil},
		{"one replica", []string{"n0"}},
		{"two replicas", []string{"n0", "n1"}},
		{"duplicate address", []string{"n0", "n1", "n1"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, fake := newClient(t, nil, testConfig)
			if err := c.Reconfigure(context.Background(), tc.config); err == nil {
				t.Fatal("Reconfigure() error = nil, want error")
			}
			if n := len(fake.messages()); n != 0 {
				t.Fatalf("sent %d messages, want 0", n)
			}
			if !slices.Equal(c.Config(), testConfig) || c.Epoch() != 0 {
				t.Fatalf("state changed: config %v epoch %d", c.Config(), c.Epoch())
			}
		})
	}
}

func TestRequestNumbersIncrease(t *testing.T) {
	c, fake := newClient(t, answerAll, testConfig)
	ctx := context.Background()
	for range 2 {
		if _, err := c.Invoke(ctx, []byte("op")); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Reconfigure(ctx, newConfig); err != nil {
		t.Fatal(err)
	}
	var nums []uint64
	for _, d := range fake.messages() {
		_, num, _ := clientRequest(d.m)
		nums = append(nums, num)
	}
	if want := []uint64{1, 2, 3, 4}; !slices.Equal(nums, want) {
		t.Fatalf("request numbers = %v, want %v", nums, want)
	}
}

func TestCloseUnblocksInvoke(t *testing.T) {
	sent := make(chan struct{}, 1)
	c, fake := newClient(t, func(*fakeNet, string, protocol.Message) {
		select {
		case sent <- struct{}{}:
		default:
		}
	}, testConfig)
	errc := make(chan error, 1)
	go func() {
		_, err := c.Invoke(context.Background(), []byte("op"))
		errc <- err
	}()
	select {
	case <-sent:
	case <-time.After(5 * time.Second):
		t.Fatal("request was not sent")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errc:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Invoke() error = %v, want %v", err, ErrClosed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Invoke was not unblocked by Close")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if _, err := c.Invoke(context.Background(), []byte("op")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Invoke() after Close error = %v, want %v", err, ErrClosed)
	}
	if err := c.Reconfigure(context.Background(), newConfig); !errors.Is(err, ErrClosed) {
		t.Fatalf("Reconfigure() after Close error = %v, want %v", err, ErrClosed)
	}
	if n := fake.closeCount(); n != 1 {
		t.Fatalf("transport closed %d times, want 1", n)
	}
}
