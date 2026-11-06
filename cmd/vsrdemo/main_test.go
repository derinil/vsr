package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derinil/vsr/internal/kvservice"
	"github.com/derinil/vsr/internal/vsrstatus"
)

func TestParseOptions(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		want    options
		wantErr bool
	}{
		{
			name: "defaults",
			want: options{
				replicas: []string{"127.0.0.1:10188", "127.0.0.1:10189", "127.0.0.1:10190", "127.0.0.1:10191", "127.0.0.1:10192"},
				status:   []string{"127.0.0.1:18080", "127.0.0.1:18081", "127.0.0.1:18082", "127.0.0.1:18083", "127.0.0.1:18084"},
				listen:   ":9999",
				minWait:  time.Second,
				maxWait:  3 * time.Second,
				keys:     5,
				timeout:  5 * time.Second,
			},
		},
		{
			name: "custom",
			args: []string{"-replicas", "3", "-port", "2000", "-status-port", "3000", "-listen", ":9", "-min-interval", "2s", "-max-interval", "2s", "-keys", "1", "-timeout", "1s"},
			want: options{
				replicas: []string{"127.0.0.1:2000", "127.0.0.1:2001", "127.0.0.1:2002"},
				status:   []string{"127.0.0.1:3000", "127.0.0.1:3001", "127.0.0.1:3002"},
				listen:   ":9",
				minWait:  2 * time.Second,
				maxWait:  2 * time.Second,
				keys:     1,
				timeout:  time.Second,
			},
		},
		{name: "no replicas", args: []string{"-replicas", "0"}, wantErr: true},
		{name: "max below min", args: []string{"-min-interval", "2s", "-max-interval", "1s"}, wantErr: true},
		{name: "zero min", args: []string{"-min-interval", "0"}, wantErr: true},
		{name: "no keys", args: []string{"-keys", "0"}, wantErr: true},
		{name: "zero timeout", args: []string{"-timeout", "0"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseOptions(tt.args)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseOptions() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseOptions() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestWaitWithinBounds(t *testing.T) {
	o := options{minWait: time.Second, maxWait: 3 * time.Second}
	for range 1000 {
		if d := wait(o); d < o.minWait || d > o.maxWait {
			t.Fatalf("wait() = %v, want within [%v, %v]", d, o.minWait, o.maxWait)
		}
	}
}

func TestRandomOpExecutes(t *testing.T) {
	s := kvservice.New()
	for range 200 {
		op, err := randomOp(3)
		if err != nil {
			t.Fatal(err)
		}
		r, err := kvservice.DecodeResult(s.Execute(op))
		if err != nil {
			t.Fatal(err)
		}
		if r.Code == kvservice.CodeMalformed {
			t.Fatalf("op %s malformed: %s", op, r.Error)
		}
	}
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func freeAddrs(t *testing.T, n int) []string {
	t.Helper()
	addrs := make([]string, n)
	for i := range addrs {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addrs[i] = ln.Addr().String()
		ln.Close()
	}
	return addrs
}

func TestRun(t *testing.T) {
	const n = 5
	addrs := freeAddrs(t, 2*n+1)
	o := options{
		replicas: addrs[:n],
		status:   addrs[n : 2*n],
		listen:   addrs[2*n],
		minWait:  20 * time.Millisecond,
		maxWait:  50 * time.Millisecond,
		keys:     3,
		timeout:  5 * time.Second,
	}
	var logs syncBuffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))

	ctx, stop := context.WithCancel(t.Context())
	defer stop()
	done := make(chan error, 1)
	go func() { done <- run(ctx, o, logger) }()

	deadline := time.Now().Add(10 * time.Second)
	for !clusterHealthy(o.listen, n) || strings.Count(logs.String(), "msg=query ") < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("cluster not serving queries; logs:\n%s", logs.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	if strings.Contains(logs.String(), "query failed") {
		t.Errorf("unexpected failed query; logs:\n%s", logs.String())
	}

	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return after cancel")
	}
}

func clusterHealthy(listen string, n int) bool {
	resp, err := http.Get("http://" + listen + "/api/cluster")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	var c vsrstatus.Cluster
	if json.NewDecoder(resp.Body).Decode(&c) != nil || len(c.Targets) != n {
		return false
	}
	for _, tg := range c.Targets {
		if !tg.Reachable {
			return false
		}
	}
	return true
}
