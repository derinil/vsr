package main

import (
	"maps"
	"slices"
	"testing"
)

func TestParseOptions(t *testing.T) {
	base := map[string]string{"VSR_ADDR": "a:1", "VSR_CONFIG": "a:1, b:2"}
	withEnv := func(kv ...string) map[string]string {
		env := maps.Clone(base)
		for pair := range slices.Chunk(kv, 2) {
			env[pair[0]] = pair[1]
		}
		return env
	}
	tests := []struct {
		name     string
		args     []string
		env      map[string]string
		wantErr  bool
		wantHTTP string
	}{
		{"status disabled by default", nil, base, false, ""},
		{"status from env", nil, withEnv("VSR_HTTP", ":8080"), false, ":8080"},
		{"status flag overrides env", []string{"-http", ":9090"}, withEnv("VSR_HTTP", ":8080"), false, ":9090"},
		{"status flag without env", []string{"-http", ":9090"}, base, false, ":9090"},
		{"missing addr", nil, map[string]string{"VSR_CONFIG": "a:1"}, true, ""},
		{"missing config", nil, map[string]string{"VSR_ADDR": "a:1"}, true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := parseOptions(tt.args, func(k string) string { return tt.env[k] })
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if opts.HTTPAddr != tt.wantHTTP {
				t.Errorf("HTTPAddr = %q, want %q", opts.HTTPAddr, tt.wantHTTP)
			}
			if opts.Addr != "a:1" || !slices.Equal(opts.Config, []string{"a:1", "b:2"}) {
				t.Errorf("Addr = %q, Config = %v", opts.Addr, opts.Config)
			}
		})
	}
}
