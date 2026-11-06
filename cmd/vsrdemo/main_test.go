package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derinil/vsr/internal/sqlservice"
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
				replicas:   []string{"127.0.0.1:10188", "127.0.0.1:10189", "127.0.0.1:10190", "127.0.0.1:10191", "127.0.0.1:10192"},
				spares:     []string{"127.0.0.1:10193", "127.0.0.1:10194"},
				status:     []string{"127.0.0.1:18080", "127.0.0.1:18081", "127.0.0.1:18082", "127.0.0.1:18083", "127.0.0.1:18084", "127.0.0.1:18085", "127.0.0.1:18086"},
				listen:     ":9999",
				clients:    64,
				keys:       1000,
				timeout:    5 * time.Second,
				chaos:      true,
				chaosEvery: 4 * time.Second,
				report:     5 * time.Second,
			},
		},
		{
			name: "custom",
			args: []string{"-replicas", "3", "-spares", "0", "-port", "2000", "-status-port", "3000", "-listen", ":9", "-clients", "2", "-keys", "1", "-timeout", "1s", "-chaos=false", "-chaos-interval", "1s", "-report", "2s"},
			want: options{
				replicas:   []string{"127.0.0.1:2000", "127.0.0.1:2001", "127.0.0.1:2002"},
				status:     []string{"127.0.0.1:3000", "127.0.0.1:3001", "127.0.0.1:3002"},
				listen:     ":9",
				clients:    2,
				keys:       1,
				timeout:    time.Second,
				chaosEvery: time.Second,
				report:     2 * time.Second,
			},
		},
		{name: "no replicas", args: []string{"-replicas", "0"}, wantErr: true},
		{name: "negative spares", args: []string{"-spares", "-1"}, wantErr: true},
		{name: "no clients", args: []string{"-clients", "0"}, wantErr: true},
		{name: "zero chaos interval", args: []string{"-chaos-interval", "0"}, wantErr: true},
		{name: "zero report", args: []string{"-report", "0"}, wantErr: true},
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

func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == nodeCommand {
		if err := runNode(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestRegister(t *testing.T) {
	r := &register{key: "c0", acked: 2, attempted: 4}
	for _, tc := range []struct {
		v  int
		ok bool
	}{{1, false}, {2, true}, {4, true}, {3, false}, {5, false}} {
		if err := r.check(tc.v); (err == nil) != tc.ok {
			t.Errorf("check(%d) = %v, want ok %t", tc.v, err, tc.ok)
		}
	}
}

func newService(t *testing.T) *sqlservice.Store {
	t.Helper()
	s, err := sqlservice.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	op, _ := sqlservice.Encode(sqlservice.Query(schemaSQL))
	s.Execute(op)
	return s
}

func TestRandomOpVerifiesOwnKey(t *testing.T) {
	s := newService(t)
	reg := &register{key: "c0"}
	for range 500 {
		op, verify, err := nextOp(reg, 3)
		if err != nil {
			t.Fatal(err)
		}
		r, err := sqlservice.DecodeResult(s.Execute(op))
		if err != nil {
			t.Fatal(err)
		}
		if r.Error != "" {
			t.Fatalf("op %s: %s", op, r.Error)
		}
		if verify != nil {
			if err := verify(r); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestRandomOpExecutes(t *testing.T) {
	s := newService(t)
	for range 200 {
		op, err := randomOp(3)
		if err != nil {
			t.Fatal(err)
		}
		r, err := sqlservice.DecodeResult(s.Execute(op))
		if err != nil {
			t.Fatal(err)
		}
		if r.Error != "" {
			t.Fatalf("op %s: %s", op, r.Error)
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
	const n, spares = 5, 2
	addrs := freeAddrs(t, 2*(n+spares)+1)
	o := options{
		replicas:   addrs[:n],
		spares:     addrs[n : n+spares],
		status:     addrs[n+spares : 2*(n+spares)],
		listen:     addrs[2*(n+spares)],
		clients:    8,
		keys:       3,
		timeout:    5 * time.Second,
		chaosEvery: time.Second,
		report:     200 * time.Millisecond,
	}
	var logs syncBuffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))

	ctx, stop := context.WithCancel(t.Context())
	defer stop()
	done := make(chan error, 1)
	go func() { done <- run(ctx, o, logger) }()

	eventually(t, &logs, o.listen, "cluster serving queries", func(c vsrstatus.Cluster) bool {
		return allReachable(c, n) && minCommit(c) > 10 && strings.Contains(logs.String(), "msg=node ") && strings.Contains(logs.String(), "cpu=")
	})

	config := append(slices.Clone(o.replicas[2:]), o.spares...)
	body, _ := json.Marshal(map[string][]string{"config": config})
	resp, err := http.Post("http://"+o.listen+"/api/reconfigure", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Epoch uint64 `json:"epoch"`
		Error string `json:"error"`
	}
	json.NewDecoder(resp.Body).Decode(&got)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || got.Epoch != 1 {
		t.Fatalf("reconfigure = %d %+v, want 200 epoch 1; logs:\n%s", resp.StatusCode, got, logs.String())
	}

	start := time.Now()
	resp, err = http.Post("http://"+o.listen+"/api/reconfigure", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest || time.Since(start) > time.Second {
		t.Fatalf("repeated reconfigure = %d after %v, want fast 400", resp.StatusCode, time.Since(start))
	}

	c, _ := fetchCluster(o.listen)
	before := minCommit(c)
	eventually(t, &logs, o.listen, "new members polled and serving", func(c vsrstatus.Cluster) bool {
		return allReachable(c, n+spares) && membersNormal(c, config) && minCommit(c) > before+10
	})

	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return after cancel")
	}
	out := logs.String()
	if !strings.Contains(out, "msg=summary ") || !strings.Contains(out, "violations=0") || strings.Contains(out, "consistency violation") {
		t.Errorf("missing clean summary; logs:\n%s", out)
	}
}

func TestRunChaos(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos run is slow")
	}
	const n = 5
	addrs := freeAddrs(t, 2*n+1)
	o := options{
		replicas:   addrs[:n],
		status:     addrs[n : 2*n],
		listen:     addrs[2*n],
		clients:    16,
		keys:       10,
		timeout:    2 * time.Second,
		chaos:      true,
		chaosEvery: 300 * time.Millisecond,
		report:     time.Second,
	}
	var logs syncBuffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	ctx, stop := context.WithCancel(t.Context())
	defer stop()
	time.AfterFunc(8*time.Second, stop)
	if err := run(ctx, o, logger); err != nil {
		t.Fatal(err)
	}
	out := logs.String()
	if !strings.Contains(out, "chaos: ") {
		t.Errorf("no faults injected; logs:\n%s", out)
	}
	if strings.Contains(out, "consistency violation") || !strings.Contains(out, "violations=0") {
		t.Errorf("consistency violated; logs:\n%s", out)
	}
}

func minCommit(c vsrstatus.Cluster) uint64 {
	var lo uint64
	for i, tg := range c.Targets {
		if tg.Node == nil {
			return 0
		}
		if i == 0 || tg.Node.CommitNum < lo {
			lo = tg.Node.CommitNum
		}
	}
	return lo
}

func eventually(t *testing.T, logs *syncBuffer, listen, what string, ok func(vsrstatus.Cluster) bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if c, err := fetchCluster(listen); err == nil && ok(c) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s; logs:\n%s", what, logs.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func fetchCluster(listen string) (vsrstatus.Cluster, error) {
	var c vsrstatus.Cluster
	resp, err := http.Get("http://" + listen + "/api/cluster")
	if err != nil {
		return c, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&c)
	return c, err
}

func allReachable(c vsrstatus.Cluster, n int) bool {
	if len(c.Targets) != n {
		return false
	}
	for _, tg := range c.Targets {
		if !tg.Reachable {
			return false
		}
	}
	return true
}

func membersNormal(c vsrstatus.Cluster, config []string) bool {
	normal := 0
	for _, tg := range c.Targets {
		if tg.Node != nil && tg.Node.Epoch == 1 && tg.Node.Status == "normal" && slices.Contains(config, tg.Node.Addr) {
			normal++
		}
	}
	return normal == len(config)
}
