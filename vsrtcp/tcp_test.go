package vsrtcp_test

import (
	"encoding/binary"
	"errors"
	"net"
	"reflect"
	"runtime"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/derinil/vsr/vsr"
	"github.com/derinil/vsr/vsrtcp"
)

const timeout = 5 * time.Second

var samples = []vsr.Message{
	vsr.Request{Epoch: 1, ClientID: 7, RequestNum: 3, Op: []byte("put k v")},
	vsr.Reconfiguration{Epoch: 1, ClientID: 7, RequestNum: 4, Config: []string{"a:1", "b:2", "c:3"}},
	vsr.CheckEpoch{Epoch: 2, ClientID: 7, RequestNum: 5},
	vsr.Reply{Epoch: 2, View: 3, ClientID: 7, RequestNum: 5, Result: []byte("ok")},
	vsr.NewEpoch{Epoch: 3, View: 4, Config: []string{"a:1"}},
	vsr.Prepare{Epoch: 1, View: 2, OpNum: 9, CommitNum: 8, Entry: vsr.Entry{Kind: vsr.EntryOp, ClientID: 7, RequestNum: 3, Op: []byte("x")}},
	vsr.PrepareOK{Epoch: 1, View: 2, OpNum: 9, Replica: 1},
	vsr.Commit{Epoch: 1, View: 2, CommitNum: 9},
	vsr.StartViewChange{Epoch: 1, View: 5, Replica: 2},
	vsr.DoViewChange{Epoch: 1, View: 5, LastNormalView: 4, OpNum: 9, CommitNum: 8, Log: []vsr.Entry{{Kind: vsr.EntryReconfig, Config: []string{"a:1"}}}, Replica: 0},
	vsr.StartView{Epoch: 1, View: 5, OpNum: 9, CommitNum: 8, Log: []vsr.Entry{{Kind: vsr.EntryCheckEpoch, ClientID: 1, RequestNum: 1}}},
	vsr.Recovery{Epoch: 1, Nonce: 99, Replica: 2},
	vsr.RecoveryResponse{Epoch: 1, View: 5, Nonce: 99, OpNum: 9, CommitNum: 8, Log: []vsr.Entry{{Kind: vsr.EntryOp, Op: []byte("y")}}, HasLog: true, Config: []string{"a:1", "b:2"}, OldConfig: []string{"a:1"}, Replica: 0},
	vsr.GetState{Epoch: 1, View: 5, OpNum: 8, Replica: 1},
	vsr.NewState{Epoch: 1, View: 5, OpNum: 9, CommitNum: 8, Log: []vsr.Entry{{Kind: vsr.EntryOp, ClientID: 2, RequestNum: 2, Op: []byte("z")}}},
	vsr.StartEpoch{Epoch: 2, OpNum: 9, OldConfig: []string{"a:1", "b:2"}, NewConfig: []string{"b:2", "c:3"}, Replica: 1},
	vsr.EpochStarted{Epoch: 2, Replica: 0},
}

func newTransport(t *testing.T, h vsr.Handler, o vsrtcp.Options) *vsrtcp.Transport {
	t.Helper()
	tr := vsrtcp.New(h, o)
	t.Cleanup(func() { tr.Close() })
	return tr
}

func listen(t *testing.T, tr *vsrtcp.Transport) string {
	t.Helper()
	if err := tr.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	return tr.Addr().String()
}

func inbox(n int) (vsr.Handler, chan vsr.Message) {
	ch := make(chan vsr.Message, n)
	return func(m vsr.Message) { ch <- m }, ch
}

func next(t *testing.T, ch <-chan vsr.Message) vsr.Message {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(timeout):
		t.Fatal("timed out waiting for message")
		return nil
	}
}

func expectNone(t *testing.T, ch <-chan vsr.Message) {
	t.Helper()
	select {
	case m := <-ch:
		t.Fatalf("unexpected message %#v", m)
	case <-time.After(200 * time.Millisecond):
	}
}

func unusedAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func frame(payload []byte) []byte {
	return append(binary.BigEndian.AppendUint32(nil, uint32(len(payload))), payload...)
}

func TestListen(t *testing.T) {
	tr := newTransport(t, func(vsr.Message) {}, vsrtcp.Options{})
	if tr.Addr() != nil {
		t.Fatal("Addr before Listen should be nil")
	}
	addr := listen(t, tr)
	if err := tr.Listen("127.0.0.1:0"); err == nil {
		t.Fatal("second Listen should fail")
	}
	if tr.Addr().String() != addr {
		t.Fatalf("Addr = %v, want %v", tr.Addr(), addr)
	}
}

func TestSendDeliversEveryMessageType(t *testing.T) {
	covered := make(map[vsr.MsgType]bool)
	for _, m := range samples {
		covered[m.Type()] = true
	}
	for mt := vsr.MsgRequest; mt <= vsr.MsgEpochStarted; mt++ {
		if !covered[mt] {
			t.Errorf("no sample for %v", mt)
		}
	}

	h, ch := inbox(len(samples))
	addr := listen(t, newTransport(t, h, vsrtcp.Options{}))
	sender := newTransport(t, func(vsr.Message) {}, vsrtcp.Options{})

	for _, m := range samples {
		t.Run(m.Type().String(), func(t *testing.T) {
			sender.Send(addr, m)
			if got := next(t, ch); !reflect.DeepEqual(got, m) {
				t.Fatalf("got %#v, want %#v", got, m)
			}
		})
	}
}

func TestReplyRoutesToClient(t *testing.T) {
	requests, reqCh := inbox(1)
	srv := newTransport(t, requests, vsrtcp.Options{})
	addr := listen(t, srv)

	replies, replyCh := inbox(2)
	client := newTransport(t, replies, vsrtcp.Options{})

	req := vsr.Request{Epoch: 1, ClientID: 42, RequestNum: 1, Op: []byte("get k")}
	client.Send(addr, req)
	if got := next(t, reqCh); !reflect.DeepEqual(got, req) {
		t.Fatalf("got %#v, want %#v", got, req)
	}

	rep := vsr.Reply{Epoch: 1, ClientID: 42, RequestNum: 1, Result: []byte("v")}

	t.Run("unknown client dropped", func(t *testing.T) {
		srv.Reply(43, rep)
		expectNone(t, replyCh)
	})

	t.Run("known client receives", func(t *testing.T) {
		srv.Reply(42, rep)
		if got := next(t, replyCh); !reflect.DeepEqual(got, rep) {
			t.Fatalf("got %#v, want %#v", got, rep)
		}
	})
}

func TestOrderedDelivery(t *testing.T) {
	const n = 10_000
	h, ch := inbox(n)
	addr := listen(t, newTransport(t, h, vsrtcp.Options{}))
	sender := newTransport(t, func(vsr.Message) {}, vsrtcp.Options{QueueSize: n})

	for i := range n {
		sender.Send(addr, vsr.PrepareOK{OpNum: uint64(i), Replica: 1})
	}
	for i := range n {
		m, ok := next(t, ch).(vsr.PrepareOK)
		if !ok || m.OpNum != uint64(i) {
			t.Fatalf("message %d: got %#v", i, m)
		}
	}
}

func TestReconnectAfterServerRestart(t *testing.T) {
	h1, ch1 := inbox(16)
	srv1 := newTransport(t, h1, vsrtcp.Options{})
	addr := listen(t, srv1)

	client := newTransport(t, func(vsr.Message) {}, vsrtcp.Options{Backoff: 10 * time.Millisecond})
	client.Send(addr, vsr.Commit{View: 1, CommitNum: 0})
	next(t, ch1)

	srv1.Close()

	h2, ch2 := inbox(1024)
	srv2 := newTransport(t, h2, vsrtcp.Options{})
	if err := srv2.Listen(addr); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(timeout)
	for i := uint64(1); time.Now().Before(deadline); i++ {
		client.Send(addr, vsr.Commit{View: 1, CommitNum: i})
		select {
		case m := <-ch2:
			if _, ok := m.(vsr.Commit); !ok {
				t.Fatalf("got %#v, want Commit", m)
			}
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatal("message not delivered after server restart")
}

func TestSendToUnreachableDoesNotBlock(t *testing.T) {
	addr := unusedAddr(t)
	tr := vsrtcp.New(func(vsr.Message) {}, vsrtcp.Options{DialTimeout: 100 * time.Millisecond})

	sent := make(chan struct{})
	go func() {
		defer close(sent)
		for i := range 10_000 {
			tr.Send(addr, vsr.Commit{CommitNum: uint64(i)})
		}
	}()
	select {
	case <-sent:
	case <-time.After(timeout):
		t.Fatal("Send blocked")
	}

	closed := make(chan error, 1)
	go func() { closed <- tr.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(timeout):
		t.Fatal("Close blocked")
	}
}

func TestInboundFrames(t *testing.T) {
	const maxFrame = 1024
	h, ch := inbox(16)
	addr := listen(t, newTransport(t, h, vsrtcp.Options{MaxFrameSize: maxFrame}))

	valid := vsr.AppendMessage(nil, vsr.Commit{CommitNum: 1})

	t.Run("valid frame delivered", func(t *testing.T) {
		c := dial(t, addr)
		if _, err := c.Write(frame(valid)); err != nil {
			t.Fatal(err)
		}
		if got := next(t, ch); !reflect.DeepEqual(got, vsr.Commit{CommitNum: 1}) {
			t.Fatalf("got %#v", got)
		}
	})

	rejected := []struct {
		name  string
		bytes []byte
	}{
		{"oversize header", binary.BigEndian.AppendUint32(nil, maxFrame+1)},
		{"empty frame", frame(nil)},
		{"unknown type", frame([]byte{0xff})},
		{"truncated message", frame([]byte{byte(vsr.MsgRequest), 1})},
		{"trailing bytes", frame(append(slices.Clone(valid), 0))},
	}
	for _, tc := range rejected {
		t.Run(tc.name, func(t *testing.T) {
			c := dial(t, addr)
			if _, err := c.Write(tc.bytes); err != nil {
				t.Fatal(err)
			}
			c.SetReadDeadline(time.Now().Add(timeout))
			_, err := c.Read(make([]byte, 1))
			var ne net.Error
			if err == nil || (errors.As(err, &ne) && ne.Timeout()) {
				t.Fatalf("connection not closed: %v", err)
			}
		})
	}
}

func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestCloseIsIdempotent(t *testing.T) {
	tr := vsrtcp.New(func(vsr.Message) {}, vsrtcp.Options{})
	addr := listen(t, tr)
	tr.Send(addr, vsr.Commit{})

	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
	tr.Send(addr, vsr.Commit{})
	tr.Reply(1, vsr.Reply{})
	if err := tr.Listen("127.0.0.1:0"); err == nil {
		t.Fatal("Listen after Close should fail")
	}
}

func BenchmarkSend(b *testing.B) {
	const window = 1 << 14
	var received atomic.Int64

	recv := vsrtcp.New(func(vsr.Message) { received.Add(1) }, vsrtcp.Options{})
	b.Cleanup(func() { recv.Close() })
	if err := recv.Listen("127.0.0.1:0"); err != nil {
		b.Fatal(err)
	}
	send := vsrtcp.New(func(vsr.Message) {}, vsrtcp.Options{QueueSize: 2 * window})
	b.Cleanup(func() { send.Close() })

	addr := recv.Addr().String()
	m := vsr.PrepareOK{Epoch: 1, View: 1, OpNum: 1, Replica: 1}

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		for int64(i)-received.Load() >= window {
			runtime.Gosched()
		}
		send.Send(addr, m)
	}
	for received.Load() < int64(b.N) {
		runtime.Gosched()
	}
}
