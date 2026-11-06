package vsrtcp_test

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"reflect"
	"runtime"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/derinil/vsr/internal/protocol"
	"github.com/derinil/vsr/internal/vsrstatus"
	"github.com/derinil/vsr/internal/vsrtcp"
)

const timeout = 5 * time.Second

var samples = []protocol.Message{
	protocol.Request{Epoch: 1, ClientID: 7, RequestNum: 3, Op: []byte("put k v")},
	protocol.Reconfiguration{Epoch: 1, ClientID: 7, RequestNum: 4, Config: []string{"a:1", "b:2", "c:3"}},
	protocol.CheckEpoch{Epoch: 2, ClientID: 7, RequestNum: 5},
	protocol.Reply{Epoch: 2, View: 3, ClientID: 7, RequestNum: 5, Result: []byte("ok")},
	protocol.NewEpoch{Epoch: 3, View: 4, Config: []string{"a:1"}},
	protocol.Prepare{Epoch: 1, View: 2, OpNum: 9, CommitNum: 8, Entry: protocol.Entry{Kind: protocol.EntryOp, ClientID: 7, RequestNum: 3, Op: []byte("x")}},
	protocol.PrepareOK{Epoch: 1, View: 2, OpNum: 9, Replica: 1},
	protocol.Commit{Epoch: 1, View: 2, CommitNum: 9},
	protocol.StartViewChange{Epoch: 1, View: 5, Replica: 2},
	protocol.DoViewChange{Epoch: 1, View: 5, LastNormalView: 4, OpNum: 9, CommitNum: 8, Log: []protocol.Entry{{Kind: protocol.EntryReconfig, Config: []string{"a:1"}}}, Replica: 0},
	protocol.StartView{Epoch: 1, View: 5, OpNum: 9, CommitNum: 8, Log: []protocol.Entry{{Kind: protocol.EntryCheckEpoch, ClientID: 1, RequestNum: 1}}},
	protocol.Recovery{Epoch: 1, Nonce: 99, Replica: 2},
	protocol.RecoveryResponse{Epoch: 1, View: 5, Nonce: 99, OpNum: 9, CommitNum: 8, Log: []protocol.Entry{{Kind: protocol.EntryOp, Op: []byte("y")}}, HasLog: true, Config: []string{"a:1", "b:2"}, OldConfig: []string{"a:1"}, Replica: 0},
	protocol.GetState{Epoch: 1, View: 5, OpNum: 8, Replica: 1},
	protocol.NewState{Epoch: 1, View: 5, OpNum: 9, CommitNum: 8, Log: []protocol.Entry{{Kind: protocol.EntryOp, ClientID: 2, RequestNum: 2, Op: []byte("z")}}},
	protocol.StartEpoch{Epoch: 2, OpNum: 9, OldConfig: []string{"a:1", "b:2"}, NewConfig: []string{"b:2", "c:3"}, From: "b:2"},
	protocol.EpochStarted{Epoch: 2, Replica: 0},
}

func newTransport(t *testing.T, h vsrtcp.Handler, o vsrtcp.Options) *vsrtcp.Transport {
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

func inbox(n int) (vsrtcp.Handler, chan protocol.Message) {
	ch := make(chan protocol.Message, n)
	return func(b []byte) error {
		m, err := protocol.DecodeMessage(b)
		if err == nil {
			ch <- m
		}
		return err
	}, ch
}

func discard([]byte) error { return nil }

func enc(m protocol.Message) []byte { return protocol.AppendMessage(nil, m) }

func next(t *testing.T, ch <-chan protocol.Message) protocol.Message {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(timeout):
		t.Fatal("timed out waiting for message")
		return nil
	}
}

func expectNone(t *testing.T, ch <-chan protocol.Message) {
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
	tr := newTransport(t, discard, vsrtcp.Options{})
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
	covered := make(map[protocol.MsgType]bool)
	for _, m := range samples {
		covered[m.Type()] = true
	}
	for mt := protocol.MsgRequest; mt <= protocol.MsgEpochStarted; mt++ {
		if !covered[mt] {
			t.Errorf("no sample for %v", mt)
		}
	}

	h, ch := inbox(len(samples))
	addr := listen(t, newTransport(t, h, vsrtcp.Options{}))
	sender := newTransport(t, discard, vsrtcp.Options{})

	for _, m := range samples {
		t.Run(m.Type().String(), func(t *testing.T) {
			sender.Send(addr, enc(m))
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

	req := protocol.Request{Epoch: 1, ClientID: 42, RequestNum: 1, Op: []byte("get k")}
	client.Send(addr, enc(req))
	if got := next(t, reqCh); !reflect.DeepEqual(got, req) {
		t.Fatalf("got %#v, want %#v", got, req)
	}

	rep := protocol.Reply{Epoch: 1, ClientID: 42, RequestNum: 1, Result: []byte("v")}

	t.Run("unknown client dropped", func(t *testing.T) {
		srv.Reply(43, enc(rep))
		expectNone(t, replyCh)
	})

	t.Run("known client receives", func(t *testing.T) {
		srv.Reply(42, enc(rep))
		if got := next(t, replyCh); !reflect.DeepEqual(got, rep) {
			t.Fatalf("got %#v, want %#v", got, rep)
		}
	})
}

func TestOrderedDelivery(t *testing.T) {
	const n = 10_000
	h, ch := inbox(n)
	addr := listen(t, newTransport(t, h, vsrtcp.Options{}))
	sender := newTransport(t, discard, vsrtcp.Options{QueueSize: n})

	for i := range n {
		sender.Send(addr, enc(protocol.PrepareOK{OpNum: uint64(i), Replica: 1}))
	}
	for i := range n {
		m, ok := next(t, ch).(protocol.PrepareOK)
		if !ok || m.OpNum != uint64(i) {
			t.Fatalf("message %d: got %#v", i, m)
		}
	}
}

func TestReconnectAfterServerRestart(t *testing.T) {
	h1, ch1 := inbox(16)
	srv1 := newTransport(t, h1, vsrtcp.Options{})
	addr := listen(t, srv1)

	client := newTransport(t, discard, vsrtcp.Options{Backoff: 10 * time.Millisecond})
	client.Send(addr, enc(protocol.Commit{View: 1, CommitNum: 0}))
	next(t, ch1)

	srv1.Close()

	h2, ch2 := inbox(1024)
	srv2 := newTransport(t, h2, vsrtcp.Options{})
	if err := srv2.Listen(addr); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(timeout)
	for i := uint64(1); time.Now().Before(deadline); i++ {
		client.Send(addr, enc(protocol.Commit{View: 1, CommitNum: i}))
		select {
		case m := <-ch2:
			if _, ok := m.(protocol.Commit); !ok {
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
	tr := vsrtcp.New(discard, vsrtcp.Options{DialTimeout: 100 * time.Millisecond})

	sent := make(chan struct{})
	go func() {
		defer close(sent)
		for i := range 10_000 {
			tr.Send(addr, enc(protocol.Commit{CommitNum: uint64(i)}))
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

func TestStalledPeerDoesNotWedgeWriter(t *testing.T) {
	const writeTimeout = 100 * time.Millisecond
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	client := newTransport(t, discard, vsrtcp.Options{WriteTimeout: writeTimeout, QueueSize: 4, Backoff: 10 * time.Millisecond})

	op := make([]byte, 1<<20)
	for i := range 256 {
		client.Send(addr, enc(protocol.Request{Epoch: 1, ClientID: 1, RequestNum: uint64(i), Op: op}))
	}

	ln.SetDeadline(time.Now().Add(timeout))
	stuck, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * writeTimeout)
	stuck.SetReadDeadline(time.Now().Add(timeout))
	if _, err := io.Copy(io.Discard, stuck); err != nil {
		t.Fatalf("stalled connection not closed by write deadline: %v", err)
	}
	stuck.Close()
	ln.Close()

	h, ch := inbox(256)
	srv := newTransport(t, h, vsrtcp.Options{})
	if err := srv.Listen(addr); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		client.Send(addr, enc(protocol.Commit{CommitNum: 1}))
		select {
		case m := <-ch:
			if _, ok := m.(protocol.Commit); ok {
				return
			}
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatal("message not delivered after peer recovered")
}

func TestInboundFrames(t *testing.T) {
	const maxFrame = 1024
	h, ch := inbox(16)
	addr := listen(t, newTransport(t, h, vsrtcp.Options{MaxFrameSize: maxFrame}))

	valid := protocol.AppendMessage(nil, protocol.Commit{CommitNum: 1})

	t.Run("valid frame delivered", func(t *testing.T) {
		c := dial(t, addr)
		if _, err := c.Write(frame(valid)); err != nil {
			t.Fatal(err)
		}
		if got := next(t, ch); !reflect.DeepEqual(got, protocol.Commit{CommitNum: 1}) {
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
		{"truncated message", frame([]byte{byte(protocol.MsgRequest), 1})},
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
	tr := vsrtcp.New(discard, vsrtcp.Options{})
	addr := listen(t, tr)
	tr.Send(addr, enc(protocol.Commit{}))

	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
	tr.Send(addr, enc(protocol.Commit{}))
	tr.Reply(1, enc(protocol.Reply{}))
	if err := tr.Listen("127.0.0.1:0"); err == nil {
		t.Fatal("Listen after Close should fail")
	}
}

func waitPeer(t *testing.T, tr *vsrtcp.Transport, done func(vsrstatus.Peer) bool) vsrstatus.Peer {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ps := tr.Peers()
		if len(ps) == 1 && done(ps[0]) {
			return ps[0]
		}
		if time.Now().After(deadline) {
			t.Fatalf("peer state not reached, peers = %+v", ps)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPeersEmpty(t *testing.T) {
	tr := newTransport(t, discard, vsrtcp.Options{})
	if ps := tr.Peers(); ps == nil || len(ps) != 0 {
		t.Fatalf("Peers() = %#v, want empty non-nil slice", ps)
	}
}

func TestPeersStats(t *testing.T) {
	tests := []struct {
		name        string
		reachable   bool
		queueSize   int
		sends       int
		wantSent    uint64
		wantDropped uint64
		wantQueued  int
	}{
		{"delivered", true, 16, 5, 5, 0, 0},
		{"unreachable queue fills", false, 4, 10, 0, 6, 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr := unusedAddr(t)
			if tt.reachable {
				h, _ := inbox(tt.sends)
				addr = listen(t, newTransport(t, h, vsrtcp.Options{}))
			}
			sender := newTransport(t, discard, vsrtcp.Options{QueueSize: tt.queueSize, Backoff: 10 * time.Millisecond})
			for i := range tt.sends {
				sender.Send(addr, enc(protocol.Commit{CommitNum: uint64(i)}))
			}

			p := waitPeer(t, sender, func(p vsrstatus.Peer) bool {
				return p.Addr == addr && p.Connected == tt.reachable && p.Sent == tt.wantSent &&
					p.Dropped == tt.wantDropped && p.Queued == tt.wantQueued && p.Dials > 0
			})
			if p.Since.IsZero() == tt.reachable {
				t.Fatalf("Since = %v with Connected = %v", p.Since, p.Connected)
			}
		})
	}
}

func TestPeersSortedByAddr(t *testing.T) {
	sender := newTransport(t, discard, vsrtcp.Options{})
	addrs := []string{unusedAddr(t), unusedAddr(t), unusedAddr(t)}
	for _, a := range addrs {
		sender.Send(a, enc(protocol.Commit{}))
	}
	want := slices.Clone(addrs)
	slices.Sort(want)

	var got []string
	for _, p := range sender.Peers() {
		got = append(got, p.Addr)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("Peers() addrs = %v, want %v", got, want)
	}
}

func TestPeersTrackReconnects(t *testing.T) {
	h1, ch1 := inbox(16)
	srv1 := newTransport(t, h1, vsrtcp.Options{})
	addr := listen(t, srv1)

	client := newTransport(t, discard, vsrtcp.Options{Backoff: 10 * time.Millisecond})
	client.Send(addr, enc(protocol.Commit{}))
	next(t, ch1)
	waitPeer(t, client, func(p vsrstatus.Peer) bool { return p.Connected })

	srv1.Close()
	down := waitPeer(t, client, func(p vsrstatus.Peer) bool { return !p.Connected })
	if !down.Since.IsZero() {
		t.Fatalf("Since = %v while disconnected", down.Since)
	}

	srv2 := newTransport(t, discard, vsrtcp.Options{})
	if err := srv2.Listen(addr); err != nil {
		t.Fatal(err)
	}
	up := waitPeer(t, client, func(p vsrstatus.Peer) bool { return p.Connected && p.Dials >= 2 })
	if up.Since.IsZero() {
		t.Fatal("Since is zero while connected")
	}
}

func BenchmarkSend(b *testing.B) {
	const window = 1 << 14
	var received atomic.Int64

	recv := vsrtcp.New(func(b []byte) error {
		_, err := protocol.DecodeMessage(b)
		received.Add(1)
		return err
	}, vsrtcp.Options{})
	b.Cleanup(func() { recv.Close() })
	if err := recv.Listen("127.0.0.1:0"); err != nil {
		b.Fatal(err)
	}
	send := vsrtcp.New(discard, vsrtcp.Options{QueueSize: 2 * window})
	b.Cleanup(func() { send.Close() })

	addr := recv.Addr().String()
	m := protocol.PrepareOK{Epoch: 1, View: 1, OpNum: 1, Replica: 1}

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		for int64(i)-received.Load() >= window {
			runtime.Gosched()
		}
		send.Send(addr, enc(m))
	}
	for received.Load() < int64(b.N) {
		runtime.Gosched()
	}
}
