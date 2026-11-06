package vsr

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"reflect"
	"slices"
	"testing"

	"github.com/derinil/vsr/internal/kvservice"
	"github.com/derinil/vsr/internal/protocol"
	"github.com/derinil/vsr/internal/vsrstatus"
)

type nopTransport struct{}

func (nopTransport) Send(string, protocol.Message)  {}
func (nopTransport) Reply(uint64, protocol.Message) {}
func (nopTransport) Listen(string) error            { return nil }
func (nopTransport) Close() error                   { return nil }

type peersTransport struct {
	nopTransport
	peers []vsrstatus.Peer
}

func (t peersTransport) Peers() []vsrstatus.Peer { return t.peers }

func newStub(t *testing.T, tr protocol.Transport, o ReplicaOptions) (*Replica, protocol.Handler) {
	t.Helper()
	var deliver protocol.Handler
	o.Addr, o.Listen, o.Config = "127.0.0.1:1", "127.0.0.1:1", []string{"127.0.0.1:1"}
	o.Service = kvservice.New()
	o.Logger = slog.New(slog.DiscardHandler)
	o.newTransport = func(h protocol.Handler) protocol.Transport {
		deliver = h
		return tr
	}
	s, err := NewReplica(o)
	if err != nil {
		t.Fatal(err)
	}
	return s, deliver
}

func fetchStatus(t *testing.T, addr string) vsrstatus.Node {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /status = %s", resp.Status)
	}
	var n vsrstatus.Node
	if err := json.NewDecoder(resp.Body).Decode(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func allConnected(peers []vsrstatus.Peer) bool {
	for _, p := range peers {
		if !p.Connected {
			return false
		}
	}
	return len(peers) > 0
}

func startStatusCluster(t *testing.T, n, checkpointInterval int) *cluster {
	t.Helper()
	addrs := freeAddrs(t, n)
	httpAddrs := freeAddrs(t, n)
	c := newCluster(t, addrs)
	c.checkpointInterval = checkpointInterval
	for i := range n {
		c.nodes[i].httpAddr = httpAddrs[i]
		c.add(i, addrs, false)
	}
	for i := range n {
		c.run(i)
	}
	return c
}

func TestStatusCounters(t *testing.T) {
	tests := []struct {
		name         string
		inboxSize    int
		delivered    int
		wantReceived uint64
		wantDropped  uint64
	}{
		{"inbox holds one", 1, 10, 1, 9},
		{"inbox holds all", 16, 10, 10, 0},
		{"nothing delivered", 4, 0, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, deliver := newStub(t, nopTransport{}, ReplicaOptions{InboxSize: tt.inboxSize})
			for i := range tt.delivered {
				deliver(protocol.Commit{CommitNum: uint64(i)})
			}
			st := s.status()
			if st.Received != tt.wantReceived || st.InboxDropped != tt.wantDropped {
				t.Fatalf("received %d dropped %d, want %d and %d", st.Received, st.InboxDropped, tt.wantReceived, tt.wantDropped)
			}
		})
	}
}

func TestStatusPeers(t *testing.T) {
	tests := []struct {
		name string
		tr   protocol.Transport
		want []vsrstatus.Peer
	}{
		{"transport without peers", nopTransport{}, []vsrstatus.Peer{}},
		{
			"transport with peers",
			peersTransport{peers: []vsrstatus.Peer{{Addr: "127.0.0.1:2", Connected: true, Sent: 3}}},
			[]vsrstatus.Peer{{Addr: "127.0.0.1:2", Connected: true, Sent: 3}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newStub(t, tt.tr, ReplicaOptions{})
			if got := s.status().Peers; !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Peers = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestStatusEndpoint(t *testing.T) {
	c := startStatusCluster(t, 1, 0)
	base := "http://" + c.nodes[0].httpAddr
	eventually(t, "status endpoint to serve", func() bool {
		resp, err := http.Get(base + "/status")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})

	tests := []struct {
		name     string
		method   string
		path     string
		wantCode int
		wantCORS bool
	}{
		{"status", http.MethodGet, "/status", http.StatusOK, true},
		{"method not allowed", http.MethodPost, "/status", http.StatusMethodNotAllowed, false},
		{"unknown path", http.MethodGet, "/nope", http.StatusNotFound, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(tt.method, base+tt.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tt.wantCode {
				t.Fatalf("status = %s, want %d", resp.Status, tt.wantCode)
			}
			if got := resp.Header.Get("Access-Control-Allow-Origin") == "*"; got != tt.wantCORS {
				t.Fatalf("CORS header present = %v, want %v", got, tt.wantCORS)
			}
		})
	}
}

func TestStatusReportsCluster(t *testing.T) {
	const writes = 3
	c := startStatusCluster(t, 3, 0)
	writeAll(t, c.client(c.addrs), "a", writes)
	eventually(t, "replicas to converge", func() bool { return c.converged(c.addrs) })

	for i, addr := range c.addrs {
		t.Run(fmt.Sprintf("replica %d", i), func(t *testing.T) {
			eventually(t, "peers to connect", func() bool { return allConnected(c.nodes[i].srv.status().Peers) })
			got := fetchStatus(t, c.nodes[i].httpAddr)
			if got.Addr != addr || got.Status != "normal" || got.Primary != (i == 0) {
				t.Fatalf("identity = %s %s primary %v", got.Addr, got.Status, got.Primary)
			}
			if got.PrimaryAddr != c.addrs[0] || got.Epoch != 0 || got.View != 0 {
				t.Fatalf("primary %s epoch %d view %d", got.PrimaryAddr, got.Epoch, got.View)
			}
			if got.OpNum != writes || got.CommitNum != writes || got.LogBase != 0 || got.LogLen != writes {
				t.Fatalf("op %d commit %d base %d len %d", got.OpNum, got.CommitNum, got.LogBase, got.LogLen)
			}
			if !slices.Equal(got.Config, c.addrs) || len(got.OldConfig) != 0 {
				t.Fatalf("config %v old %v", got.Config, got.OldConfig)
			}
			if got.Received == 0 || got.Started.IsZero() || got.Now.Before(got.Started) {
				t.Fatalf("received %d started %v now %v", got.Received, got.Started, got.Now)
			}
			if !allConnected(got.Peers) {
				t.Fatalf("peers = %+v, want all connected", got.Peers)
			}
			for _, p := range got.Peers {
				if p.Addr == addr || !slices.Contains(c.addrs, p.Addr) {
					t.Fatalf("unexpected peer %s", p.Addr)
				}
			}
		})
	}
}

func TestStatusCheckpoint(t *testing.T) {
	const interval, writes = 5, 12
	c := startStatusCluster(t, 3, interval)
	writeAll(t, c.client(c.addrs), "a", writes)
	for i, n := range c.nodes {
		t.Run(fmt.Sprintf("replica %d", i), func(t *testing.T) {
			eventually(t, "status to report checkpoint persisted to disk", func() bool {
				st := n.srv.status()
				_, ckpt, err := readCheckpointFile(n.dir)
				return err == nil && st.CheckpointOp >= 2*interval &&
					st.CheckpointOp == ckpt.OpNum && st.CheckpointBytes == len(ckpt.State)+len(ckpt.Clients)
			})
		})
	}
}

func TestStatusAfterReconfigure(t *testing.T) {
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

	st := c.nodes[1].srv.status()
	if st.Epoch != 1 || !slices.Equal(st.Config, next) || !slices.Equal(st.OldConfig, old) {
		t.Fatalf("epoch %d config %v old %v, want epoch 1 config %v old %v", st.Epoch, st.Config, st.OldConfig, next, old)
	}
	if want := next[st.View%uint64(len(next))]; st.PrimaryAddr != want {
		t.Fatalf("PrimaryAddr = %s, want %s", st.PrimaryAddr, want)
	}
}

func TestStatusStopsWithRun(t *testing.T) {
	c := startStatusCluster(t, 1, 0)
	addr := c.nodes[0].httpAddr
	eventually(t, "status endpoint to serve", func() bool {
		resp, err := http.Get("http://" + addr + "/status")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})

	c.stop(0)
	if resp, err := http.Get("http://" + addr + "/status"); err == nil {
		resp.Body.Close()
		t.Fatal("status still served after Run returned")
	}
}

func TestRunFailsWhenStatusAddrInUse(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })

	s, _ := newStub(t, nopTransport{}, ReplicaOptions{HTTPAddr: ln.Addr().String()})
	if err := s.Run(context.Background()); err == nil {
		t.Fatal("Run succeeded with status address in use")
	}
}
