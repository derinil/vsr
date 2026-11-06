package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derinil/vsr/kvservice"
)

func requireJSON(t *testing.T, got any, want string) {
	t.Helper()
	gb, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal got: %v", err)
	}
	var g, w any
	if err := json.Unmarshal(gb, &g); err != nil {
		t.Fatalf("unmarshal got: %v", err)
	}
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("unmarshal want: %v", err)
	}
	if !reflect.DeepEqual(g, w) {
		t.Errorf("got %s, want %s", gb, want)
	}
}

func TestRespond(t *testing.T) {
	deadline := context.DeadlineExceeded
	tests := []struct {
		name   string
		typ    string
		result string
		err    error
		want   string
	}{
		{"read ok", "read", `{"value":"abc"}`, nil, `{"type":"read_ok","value":"abc"}`},
		{"read ok number", "read", `{"value":5}`, nil, `{"type":"read_ok","value":5}`},
		{"write ok", "write", `{}`, nil, `{"type":"write_ok"}`},
		{"cas ok", "cas", `{}`, nil, `{"type":"cas_ok"}`},
		{"read key missing", "read", `{"code":20,"error":"key does not exist"}`, nil,
			`{"type":"error","code":20,"text":"key does not exist"}`},
		{"cas key missing", "cas", `{"code":20,"error":"key does not exist"}`, nil,
			`{"type":"error","code":20,"text":"key does not exist"}`},
		{"cas precondition failed", "cas", `{"code":22,"error":"expected 1, had 2"}`, nil,
			`{"type":"error","code":22,"text":"expected 1, had 2"}`},
		{"malformed op", "write", `{"code":12,"error":"unknown kind"}`, nil,
			`{"type":"error","code":12,"text":"unknown kind"}`},
		{"deadline", "write", "", deadline, `{"type":"error","code":0,"text":"context deadline exceeded"}`},
		{"other error", "read", "", errors.New("no quorum"), `{"type":"error","code":0,"text":"no quorum"}`},
		{"garbage result", "read", "garbage", nil, `{"type":"error","code":13,"text":"decode result: invalid character 'g' looking for beginning of value"}`},
		{"empty result", "write", "", nil, `{"type":"error","code":13,"text":"decode result: unexpected end of JSON input"}`},
		{"unknown result code", "read", `{"code":99,"error":"boom"}`, nil, `{"type":"error","code":13,"text":"unexpected result code 99: boom"}`},
		{"unknown type", "txn", `{}`, nil, `{"type":"error","code":10,"text":"unsupported type \"txn\""}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := respond(tc.typ, []byte(tc.result), tc.err)
			requireJSON(t, got, tc.want)
		})
	}
}

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		env     map[string]string
		want    config
		wantErr bool
	}{
		{
			name: "defaults",
			want: config{
				cluster:        []string{"127.0.0.1:10188", "127.0.0.1:10189", "127.0.0.1:10190"},
				timeout:        time.Second,
				requestTimeout: 200 * time.Millisecond,
			},
		},
		{
			name: "env cluster",
			env:  map[string]string{"VSR_CLUSTER": " a:1 , b:2,,c:3 "},
			want: config{
				cluster:        []string{"a:1", "b:2", "c:3"},
				timeout:        time.Second,
				requestTimeout: 200 * time.Millisecond,
			},
		},
		{
			name: "flags override env",
			args: []string{"-cluster", "x:1,y:2,z:3", "-timeout", "2s", "-request-timeout", "50ms"},
			env:  map[string]string{"VSR_CLUSTER": "a:1"},
			want: config{
				cluster:        []string{"x:1", "y:2", "z:3"},
				timeout:        2 * time.Second,
				requestTimeout: 50 * time.Millisecond,
			},
		},
		{name: "empty cluster", args: []string{"-cluster", " , "}, wantErr: true},
		{name: "bad flag", args: []string{"-timeout", "soon"}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := loadConfig(tc.args, func(k string) string { return tc.env[k] })
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr && !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

type fakeCluster struct {
	mu    sync.Mutex
	store *kvservice.Store
}

func (f *fakeCluster) invoke(_ context.Context, op []byte) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.store.Execute(op), nil
}

func line(t *testing.T, src, dest, body string) []byte {
	t.Helper()
	return fmt.Appendf(nil, `{"src":%q,"dest":%q,"body":%s}`, src, dest, body)
}

func TestNodeLinesAgainstStore(t *testing.T) {
	var out bytes.Buffer
	n := newNode(&out, time.Second, (&fakeCluster{store: kvservice.New()}).invoke)

	steps := []struct {
		name string
		in   []byte
		want string
	}{
		{"init", line(t, "c0", "n1", `{"type":"init","msg_id":1,"node_id":"n1","node_ids":["n1","n2"]}`),
			`{"type":"init_ok","in_reply_to":1,"msg_id":1}`},
		{"read missing", line(t, "c1", "n1", `{"type":"read","msg_id":2,"key":1}`),
			`{"type":"error","code":20,"text":"key does not exist","in_reply_to":2,"msg_id":2}`},
		{"write", line(t, "c1", "n1", `{"type":"write","msg_id":3,"key":1,"value":"x"}`),
			`{"type":"write_ok","in_reply_to":3,"msg_id":3}`},
		{"read", line(t, "c2", "n1", `{"type":"read","msg_id":4,"key":1}`),
			`{"type":"read_ok","value":"x","in_reply_to":4,"msg_id":4}`},
		{"cas", line(t, "c2", "n1", `{"type":"cas","msg_id":5,"key":1,"from":"x","to":"y"}`),
			`{"type":"cas_ok","in_reply_to":5,"msg_id":5}`},
		{"cas stale", line(t, "c1", "n1", `{"type":"cas","msg_id":6,"key":1,"from":"x","to":"z"}`),
			`{"type":"error","code":22,"text":"expected \"x\", had \"y\"","in_reply_to":6,"msg_id":6}`},
		{"read updated", line(t, "c1", "n1", `{"type":"read","msg_id":7,"key":1}`),
			`{"type":"read_ok","value":"y","in_reply_to":7,"msg_id":7}`},
		{"unknown type", line(t, "c1", "n1", `{"type":"txn","msg_id":8}`),
			`{"type":"error","code":10,"text":"unsupported type \"txn\"","in_reply_to":8,"msg_id":8}`},
	}
	for _, s := range steps {
		t.Run(s.name, func(t *testing.T) {
			before := out.Len()
			n.handle(s.in)
			n.wait()
			reply := strings.TrimSuffix(out.String()[before:], "\n")
			var env envelope[map[string]any]
			if err := json.Unmarshal([]byte(reply), &env); err != nil {
				t.Fatalf("decode reply %q: %v", reply, err)
			}
			if env.Dest == "" || env.Src != "n1" {
				t.Errorf("src/dest = %q/%q, want n1/...", env.Src, env.Dest)
			}
			requireJSON(t, env.Body, s.want)
		})
	}
}

func TestRunWaitsForInFlightOnEOF(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var out bytes.Buffer
	n := newNode(&out, time.Second, func(context.Context, []byte) ([]byte, error) {
		close(started)
		<-release
		return []byte(`{}`), nil
	})
	in := strings.NewReader(string(line(t, "c1", "n1", `{"type":"write","msg_id":1,"key":1,"value":2}`)) + "\n")
	done := make(chan error, 1)
	go func() { done <- run(in, n) }()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never invoked")
	}
	select {
	case err := <-done:
		t.Fatalf("run returned before in-flight handler finished: %v", err)
	default:
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(out.String(), `"type":"write_ok"`) {
		t.Errorf("missing write_ok reply: %q", out.String())
	}
}
