package vsrmon

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestServeCluster(t *testing.T) {
	tests := []struct {
		name     string
		snapshot string
		wantCode int
		wantBody string
	}{
		{name: "before first poll", wantCode: http.StatusServiceUnavailable},
		{name: "latest snapshot", snapshot: `{"targets":[]}`, wantCode: http.StatusOK, wantBody: `{"targets":[]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := testMonitor()
			if tt.snapshot != "" {
				m.publish([]byte(tt.snapshot))
			}
			h, err := newHandler(m)
			if err != nil {
				t.Fatal(err)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/cluster", nil))
			if rec.Code != tt.wantCode {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantCode)
			}
			if tt.wantCode == http.StatusOK {
				if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
					t.Errorf("Content-Type = %q, want application/json", ct)
				}
				if body := rec.Body.String(); body != tt.wantBody {
					t.Errorf("body = %q, want %q", body, tt.wantBody)
				}
			}
		})
	}
}

func TestServeIndex(t *testing.T) {
	want, err := webFS.ReadFile("web/index.html")
	if err != nil {
		t.Fatal(err)
	}
	h, err := newHandler(testMonitor())
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != string(want) {
		t.Errorf("GET / = %d, want 200 with embedded index.html", rec.Code)
	}
}

func TestServeStream(t *testing.T) {
	tests := []struct {
		name      string
		snapshots []string
		updates   []string
		want      []string
	}{
		{name: "snapshot sent on connect", snapshots: []string{"a"}, want: []string{"a"}},
		{name: "only latest snapshot sent", snapshots: []string{"a", "b"}, want: []string{"b"}},
		{name: "updates follow snapshot", snapshots: []string{"a"}, updates: []string{"b", "c"}, want: []string{"a", "b", "c"}},
		{name: "no snapshot waits for updates", updates: []string{"b"}, want: []string{"b"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := testMonitor()
			for _, s := range tt.snapshots {
				m.publish([]byte(s))
			}
			h, err := newHandler(m)
			if err != nil {
				t.Fatal(err)
			}
			srv := httptest.NewServer(h)
			defer srv.Close()

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/stream", nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
				t.Errorf("Content-Type = %q, want text/event-stream", ct)
			}
			if cc := resp.Header.Get("Cache-Control"); cc != "no-cache" {
				t.Errorf("Cache-Control = %q, want no-cache", cc)
			}

			r := bufio.NewReader(resp.Body)
			var got []string
			if len(tt.snapshots) > 0 {
				got = append(got, mustReadEvent(t, r))
			}
			for _, u := range tt.updates {
				m.publish([]byte(u))
				got = append(got, mustReadEvent(t, r))
			}
			if strings.Join(got, ",") != strings.Join(tt.want, ",") {
				t.Errorf("events = %q, want %q", got, tt.want)
			}

			cancel()
			srv.Close()
			if n := m.broker.count(); n != 0 {
				t.Errorf("%d subscribers left after disconnect, want 0", n)
			}
		})
	}
}

func mustReadEvent(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	var data []string
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("reading event: %v", err)
		}
		line = strings.TrimSuffix(line, "\n")
		switch {
		case line == "" && data != nil:
			return strings.Join(data, "\n")
		case strings.HasPrefix(line, "data: "):
			data = append(data, strings.TrimPrefix(line, "data: "))
		}
	}
}
