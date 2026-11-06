package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/derinil/vsr/internal/vsrstatus"
)

var healthyNode = vsrstatus.Node{Addr: "vsr1:8080", Status: "normal", View: 2, OpNum: 7, CommitNum: 6}

func healthyHandler(t *testing.T) http.HandlerFunc {
	t.Helper()
	body, err := json.Marshal(healthyNode)
	if err != nil {
		t.Fatal(err)
	}
	return func(w http.ResponseWriter, _ *http.Request) { w.Write(body) }
}

func testMonitor(nodes ...string) *monitor {
	return newMonitor(nodes, time.Second, 200*time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func startNode(t *testing.T, h http.HandlerFunc, down bool) string {
	t.Helper()
	srv := httptest.NewServer(h)
	if down {
		srv.Close()
	} else {
		t.Cleanup(srv.Close)
	}
	return srv.URL
}

func latestCluster(t *testing.T, m *monitor) vsrstatus.Cluster {
	t.Helper()
	data := m.latest.Load()
	if data == nil {
		t.Fatal("no snapshot published")
	}
	var c vsrstatus.Cluster
	if err := json.Unmarshal(*data, &c); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestPoll(t *testing.T) {
	tests := []struct {
		name          string
		handler       func(t *testing.T) http.HandlerFunc
		down          bool
		wantReachable bool
	}{
		{
			name:          "healthy",
			handler:       healthyHandler,
			wantReachable: true,
		},
		{
			name: "server error",
			handler: func(*testing.T) http.HandlerFunc {
				return func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "boom", http.StatusInternalServerError) }
			},
		},
		{
			name: "invalid json",
			handler: func(*testing.T) http.HandlerFunc {
				return func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("{")) }
			},
		},
		{
			name: "hangs past timeout",
			handler: func(*testing.T) http.HandlerFunc {
				return func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }
			},
		},
		{
			name:    "connection refused",
			handler: func(*testing.T) http.HandlerFunc { return nil },
			down:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var h http.HandlerFunc
			if tt.handler != nil {
				h = tt.handler(t)
			}
			base := startNode(t, h, tt.down)
			m := testMonitor(base)
			m.poll(t.Context())

			c := latestCluster(t, m)
			if len(c.Targets) != 1 {
				t.Fatalf("got %d targets, want 1", len(c.Targets))
			}
			got := c.Targets[0]
			if got.URL != base {
				t.Errorf("URL = %q, want %q", got.URL, base)
			}
			if got.Reachable != tt.wantReachable {
				t.Errorf("Reachable = %v, want %v", got.Reachable, tt.wantReachable)
			}
			if (got.Error != "") == tt.wantReachable {
				t.Errorf("Error = %q, want set only when unreachable", got.Error)
			}
			if (got.Node != nil) != tt.wantReachable {
				t.Fatalf("Node = %+v, want set only when reachable", got.Node)
			}
			if tt.wantReachable && got.Node.Status != healthyNode.Status {
				t.Errorf("Node.Status = %q, want %q", got.Node.Status, healthyNode.Status)
			}
		})
	}
}

func TestPollKeepsConfigOrder(t *testing.T) {
	up := startNode(t, healthyHandler(t), false)
	failing := startNode(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}, false)
	down := startNode(t, nil, true)
	m := testMonitor(up, failing, down)
	m.poll(t.Context())

	c := latestCluster(t, m)
	want := []struct {
		url       string
		reachable bool
	}{{up, true}, {failing, false}, {down, false}}
	if len(c.Targets) != len(want) {
		t.Fatalf("got %d targets, want %d", len(c.Targets), len(want))
	}
	for i, w := range want {
		if c.Targets[i].URL != w.url || c.Targets[i].Reachable != w.reachable {
			t.Errorf("target %d = {%s %v}, want {%s %v}", i, c.Targets[i].URL, c.Targets[i].Reachable, w.url, w.reachable)
		}
	}
}

func TestPollCanceledContextPublishesNothing(t *testing.T) {
	base := startNode(t, healthyHandler(t), false)
	m := testMonitor(base)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	m.poll(ctx)
	if m.latest.Load() != nil {
		t.Error("poll with canceled context published a snapshot")
	}
}
