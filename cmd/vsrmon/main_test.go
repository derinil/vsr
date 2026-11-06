package main

import (
	"reflect"
	"slices"
	"testing"
	"time"
)

func TestParseOptions(t *testing.T) {
	env := func(vars map[string]string) func(string) string {
		return func(key string) string { return vars[key] }
	}
	defaults := func(nodes ...string) options {
		return options{nodes: nodes, listen: ":8080", interval: 500 * time.Millisecond, timeout: 300 * time.Millisecond}
	}
	tests := []struct {
		name    string
		args    []string
		env     map[string]string
		want    options
		wantErr bool
	}{
		{
			name: "env nodes with defaults",
			env:  map[string]string{"VSR_MON_NODES": "vsr1:8080,vsr2:8080"},
			want: defaults("http://vsr1:8080", "http://vsr2:8080"),
		},
		{
			name: "env listen",
			env:  map[string]string{"VSR_MON_NODES": "a:1", "VSR_MON_LISTEN": ":9000"},
			want: options{nodes: []string{"http://a:1"}, listen: ":9000", interval: 500 * time.Millisecond, timeout: 300 * time.Millisecond},
		},
		{
			name: "flags override env",
			args: []string{"-nodes", "https://a:1/", "-listen", ":7000", "-interval", "1s", "-timeout", "2s"},
			env:  map[string]string{"VSR_MON_NODES": "b:2", "VSR_MON_LISTEN": ":6000"},
			want: options{nodes: []string{"https://a:1"}, listen: ":7000", interval: time.Second, timeout: 2 * time.Second},
		},
		{
			name: "spares and status port",
			env:  map[string]string{"VSR_MON_NODES": "a:1", "VSR_MON_SPARES": "s1:2, s2:2", "VSR_MON_STATUS_PORT": "8080"},
			want: options{nodes: []string{"http://a:1"}, spares: []string{"s1:2", "s2:2"}, statusPort: 8080, listen: ":8080", interval: 500 * time.Millisecond, timeout: 300 * time.Millisecond},
		},
		{name: "bad status port", args: []string{"-nodes", "a:1", "-status-port", "70000"}, wantErr: true},
		{name: "missing nodes", wantErr: true},
		{name: "blank nodes", args: []string{"-nodes", " , "}, wantErr: true},
		{name: "zero interval", args: []string{"-nodes", "a:1", "-interval", "0"}, wantErr: true},
		{name: "negative timeout", args: []string{"-nodes", "a:1", "-timeout", "-1s"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseOptions(tt.args, env(tt.env))
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseOptions() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseOptions() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestNormalizeNodes(t *testing.T) {
	tests := []struct {
		name  string
		items []string
		want  []string
	}{
		{name: "empty", items: nil, want: nil},
		{name: "host port gets http", items: []string{"vsr1:8080"}, want: []string{"http://vsr1:8080"}},
		{name: "scheme kept", items: []string{"https://a:1"}, want: []string{"https://a:1"}},
		{name: "trailing slash trimmed", items: []string{"http://a:1/"}, want: []string{"http://a:1"}},
		{name: "mixed", items: []string{"a:1", "http://b:2/", "https://c"}, want: []string{"http://a:1", "http://b:2", "https://c"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeNodes(tt.items); !slices.Equal(got, tt.want) {
				t.Errorf("normalizeNodes(%q) = %q, want %q", tt.items, got, tt.want)
			}
		})
	}
}

func TestStatusURL(t *testing.T) {
	if statusURL(0) != nil {
		t.Fatal("statusURL(0) should be nil")
	}
	f := statusURL(8080)
	tests := map[string]string{"vsr6:10188": "http://vsr6:8080", "[::1]:1": "http://[::1]:8080", "bad": ""}
	for addr, want := range tests {
		if got := f(addr); got != want {
			t.Errorf("statusURL(8080)(%q) = %q, want %q", addr, got, want)
		}
	}
}
