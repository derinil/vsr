package vsrmon

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/derinil/vsr/internal/vsrstatus"
)

func nodeHandler(t *testing.T, n vsrstatus.Node) http.HandlerFunc {
	t.Helper()
	body, err := json.Marshal(n)
	if err != nil {
		t.Fatal(err)
	}
	return func(w http.ResponseWriter, _ *http.Request) { w.Write(body) }
}

func monitorWith(nodes []string, urls map[string]string, spares ...string) *Monitor {
	return New(Options{
		Nodes:     nodes,
		Spares:    spares,
		StatusURL: func(addr string) string { return urls[addr] },
		Interval:  time.Second,
		Timeout:   200 * time.Millisecond,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
}

func TestCheckConfig(t *testing.T) {
	current := []string{"a:1", "b:1", "c:1"}
	tests := []struct {
		name    string
		config  []string
		wantErr string
	}{
		{"valid", []string{"a:1", "b:1", "d:1"}, ""},
		{"too small", []string{"a:1", "b:1"}, "at least 3"},
		{"duplicate", []string{"a:1", "b:1", "a:1"}, "duplicate"},
		{"empty address", []string{"a:1", "b:1", ""}, "invalid"},
		{"padded address", []string{"a:1", "b:1", " d:1"}, "invalid"},
		{"unchanged", current, "unchanged"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkConfig(current, tt.config)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("checkConfig() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("checkConfig() = %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestServeReconfigureRejects(t *testing.T) {
	config := []string{"a:1", "b:1", "c:1"}
	member := startNode(t, nodeHandler(t, vsrstatus.Node{Addr: "a:1", Status: "normal", Config: config}), false)
	down := startNode(t, nil, true)
	shut := startNode(t, nodeHandler(t, vsrstatus.Node{Addr: "e:1", Status: "shutdown"}), false)
	urls := map[string]string{"a:1": member, "d:1": down, "e:1": shut}

	tests := []struct {
		name     string
		polled   bool
		busy     bool
		body     string
		wantCode int
		wantErr  string
	}{
		{name: "bad body", polled: true, body: `{`, wantCode: http.StatusBadRequest, wantErr: "invalid request body"},
		{name: "config unknown", body: `{"config":["a:1","b:1","d:1"]}`, wantCode: http.StatusServiceUnavailable, wantErr: "unknown"},
		{name: "too small", polled: true, body: `{"config":["a:1","b:1"]}`, wantCode: http.StatusBadRequest, wantErr: "at least 3"},
		{name: "unchanged", polled: true, body: `{"config":["a:1","b:1","c:1"]}`, wantCode: http.StatusBadRequest, wantErr: "unchanged"},
		{name: "new member unreachable", polled: true, body: `{"config":["a:1","b:1","d:1"]}`, wantCode: http.StatusBadRequest, wantErr: "not reachable"},
		{name: "new member shut down", polled: true, body: `{"config":["a:1","b:1","e:1"]}`, wantCode: http.StatusBadRequest, wantErr: "shut down"},
		{name: "busy", polled: true, busy: true, body: `{"config":["a:1","b:1","d:1"]}`, wantCode: http.StatusConflict, wantErr: "in progress"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := monitorWith([]string{member}, urls)
			if tt.polled {
				m.poll(t.Context())
			}
			m.reconfiguring.Store(tt.busy)
			h, err := newHandler(m)
			if err != nil {
				t.Fatal(err)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/reconfigure", strings.NewReader(tt.body)))
			var resp reconfigureResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("decode %q: %v", rec.Body.String(), err)
			}
			if rec.Code != tt.wantCode || !strings.Contains(resp.Error, tt.wantErr) {
				t.Fatalf("got %d %q, want %d containing %q", rec.Code, resp.Error, tt.wantCode, tt.wantErr)
			}
		})
	}
}

func TestPollDiscoversMembers(t *testing.T) {
	config := []string{"a:1", "b:1", "c:1"}
	a := startNode(t, nodeHandler(t, vsrstatus.Node{Addr: "a:1", Status: "normal", Epoch: 1, Config: config}), false)
	b := startNode(t, nodeHandler(t, vsrstatus.Node{Addr: "b:1", Status: "normal", Epoch: 1, Config: config}), false)
	m := monitorWith([]string{a}, map[string]string{"a:1": a, "b:1": b})

	m.poll(t.Context())
	m.poll(t.Context())

	c := latestCluster(t, m)
	urls := make([]string, len(c.Targets))
	for i, tg := range c.Targets {
		urls[i] = tg.URL
	}
	if want := []string{a, b}; !slices.Equal(urls, want) {
		t.Fatalf("polled %q, want %q", urls, want)
	}
}

func TestCurrentConfig(t *testing.T) {
	old := []string{"a:1", "b:1", "c:1"}
	cur := []string{"b:1", "c:1", "d:1"}
	targets := []vsrstatus.Target{
		{Reachable: true, Node: &vsrstatus.Node{Status: "shutdown", Epoch: 2, Config: []string{"x:1", "y:1", "z:1"}}},
		{Reachable: true, Node: &vsrstatus.Node{Status: "normal", Epoch: 0, Config: old}},
		{Reachable: true, Node: &vsrstatus.Node{Status: "normal", Epoch: 1, Config: cur}},
		{Reachable: false, Node: &vsrstatus.Node{Status: "normal", Epoch: 5, Config: old}},
	}
	if got, epoch := currentConfig(targets); !slices.Equal(got, cur) || epoch != 1 {
		t.Fatalf("currentConfig() = %q, %d, want %q, 1", got, epoch, cur)
	}
	if got, epoch := currentConfig(nil); got != nil || epoch != 0 {
		t.Fatalf("currentConfig(nil) = %q, %d, want nil, 0", got, epoch)
	}
}

func TestPollSparesExcludeMembers(t *testing.T) {
	a := startNode(t, nodeHandler(t, vsrstatus.Node{Addr: "a:1", Status: "normal", Config: []string{"a:1", "b:1", "s1:1"}}), false)
	m := monitorWith([]string{a}, nil, "s1:1", "s2:1")
	m.poll(t.Context())
	if got := latestCluster(t, m).Spares; !slices.Equal(got, []string{"s2:1"}) {
		t.Fatalf("spares = %q, want [s2:1]", got)
	}
}

func TestPollKeepsNewerConfig(t *testing.T) {
	old := []string{"a:1", "b:1", "c:1"}
	newer := []string{"b:1", "c:1", "d:1"}
	a := startNode(t, nodeHandler(t, vsrstatus.Node{Addr: "a:1", Status: "normal", Epoch: 0, Config: old}), false)
	m := monitorWith([]string{a}, nil)

	m.config, m.epoch, m.reconfigured = newer, 1, time.Now().Add(time.Hour)
	m.poll(t.Context())
	if !slices.Equal(m.config, newer) {
		t.Fatalf("poll started before reconfiguration replaced config with %q", m.config)
	}

	m.reconfigured = time.Now().Add(-time.Hour)
	m.poll(t.Context())
	if !slices.Equal(m.config, old) {
		t.Fatalf("poll after reconfiguration kept %q, want %q", m.config, old)
	}
}
