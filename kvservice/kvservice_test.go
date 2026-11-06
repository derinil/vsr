package kvservice

import (
	"testing"
)

type step struct {
	op   string
	want string
}

func TestExecute(t *testing.T) {
	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "read missing",
			steps: []step{
				{`{"k":"read","key":1}`, `{"code":20,"error":"key does not exist"}`},
			},
		},
		{
			name: "write then read",
			steps: []step{
				{`{"k":"write","key":"a","value":1}`, `{}`},
				{`{"k":"read","key":"a"}`, `{"value":1}`},
			},
		},
		{
			name: "overwrite",
			steps: []step{
				{`{"k":"write","key":"a","value":1}`, `{}`},
				{`{"k":"write","key":"a","value":2}`, `{}`},
				{`{"k":"read","key":"a"}`, `{"value":2}`},
			},
		},
		{
			name: "cas success",
			steps: []step{
				{`{"k":"write","key":"a","value":1}`, `{}`},
				{`{"k":"cas","key":"a","from":1,"to":2}`, `{}`},
				{`{"k":"read","key":"a"}`, `{"value":2}`},
			},
		},
		{
			name: "cas mismatch",
			steps: []step{
				{`{"k":"write","key":"a","value":2}`, `{}`},
				{`{"k":"cas","key":"a","from":1,"to":3}`, `{"code":22,"error":"expected 1, had 2"}`},
				{`{"k":"read","key":"a"}`, `{"value":2}`},
			},
		},
		{
			name: "cas missing",
			steps: []step{
				{`{"k":"cas","key":"a","from":1,"to":2}`, `{"code":20,"error":"key does not exist"}`},
				{`{"k":"read","key":"a"}`, `{"code":20,"error":"key does not exist"}`},
			},
		},
		{
			name: "string and int keys are distinct",
			steps: []step{
				{`{"k":"write","key":1,"value":"int"}`, `{}`},
				{`{"k":"write","key":"1","value":"str"}`, `{}`},
				{`{"k":"read","key":1}`, `{"value":"int"}`},
				{`{"k":"read","key":"1"}`, `{"value":"str"}`},
			},
		},
		{
			name: "1 and 1.0 are equal keys",
			steps: []step{
				{`{"k":"write","key":1.0,"value":5}`, `{}`},
				{`{"k":"read","key":1}`, `{"value":5}`},
			},
		},
		{
			name: "1 and 1.0 are equal values",
			steps: []step{
				{`{"k":"write","key":"a","value":1.0}`, `{}`},
				{`{"k":"read","key":"a"}`, `{"value":1}`},
				{`{"k":"cas","key":"a","from":1,"to":2}`, `{}`},
				{`{"k":"read","key":"a"}`, `{"value":2}`},
			},
		},
		{
			name: "cas from 5.0 matches stored 5",
			steps: []step{
				{`{"k":"write","key":"a","value":5}`, `{}`},
				{`{"k":"cas","key":"a","from":5.0,"to":6}`, `{}`},
				{`{"k":"read","key":"a"}`, `{"value":6}`},
			},
		},
		{
			name: "object values ignore key order and whitespace",
			steps: []step{
				{`{"k":"write","key":"a","value":{"b": 1, "a": [1, 2]}}`, `{}`},
				{`{"k":"read","key":"a"}`, `{"value":{"a":[1,2],"b":1}}`},
				{`{"k":"cas","key":"a","from":{"a":[1,2.0],"b":1},"to":"x"}`, `{}`},
				{`{"k":"read","key":"a"}`, `{"value":"x"}`},
			},
		},
		{
			name: "null key and value",
			steps: []step{
				{`{"k":"write","key":null,"value":null}`, `{}`},
				{`{"k":"read","key":null}`, `{"value":null}`},
			},
		},
		{
			name: "keys are independent",
			steps: []step{
				{`{"k":"write","key":"a","value":1}`, `{}`},
				{`{"k":"write","key":"b","value":2}`, `{}`},
				{`{"k":"read","key":"a"}`, `{"value":1}`},
				{`{"k":"read","key":"b"}`, `{"value":2}`},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			for i, st := range tt.steps {
				if got := s.Execute([]byte(st.op)); string(got) != st.want {
					t.Fatalf("step %d %s: got %s, want %s", i, st.op, got, st.want)
				}
			}
		})
	}
}

func TestExecuteMalformed(t *testing.T) {
	tests := []struct {
		name string
		op   string
	}{
		{"empty input", ``},
		{"truncated json", `{"k":"read"`},
		{"not an object", `[]`},
		{"null op", `null`},
		{"kind wrong type", `{"k":1,"key":1}`},
		{"unknown kind", `{"k":"delete","key":1}`},
		{"missing kind", `{"key":1}`},
		{"missing key", `{"k":"read"}`},
		{"write missing key", `{"k":"write","value":1}`},
		{"write missing value", `{"k":"write","key":1}`},
		{"cas missing key", `{"k":"cas","from":1,"to":2}`},
		{"cas missing from", `{"k":"cas","key":1,"to":2}`},
		{"cas missing to", `{"k":"cas","key":1,"from":1}`},
		{"invalid key json", `{"k":"read","key":{"a"}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeResult(New().Execute([]byte(tt.op)))
			if err != nil {
				t.Fatal(err)
			}
			if got.Code != CodeMalformed {
				t.Fatalf("got code %d, want %d (error %q)", got.Code, CodeMalformed, got.Error)
			}
		})
	}
}

func TestEncoders(t *testing.T) {
	s := New()
	steps := []struct {
		op   func() ([]byte, error)
		want string
	}{
		{func() ([]byte, error) { return Read("k") }, `{"code":20,"error":"key does not exist"}`},
		{func() ([]byte, error) { return Write("k", 1) }, `{}`},
		{func() ([]byte, error) { return Read("k") }, `{"value":1}`},
		{func() ([]byte, error) { return CAS("k", 1, 2) }, `{}`},
		{func() ([]byte, error) { return CAS("k", 1, 3) }, `{"code":22,"error":"expected 1, had 2"}`},
		{func() ([]byte, error) { return Read("k") }, `{"value":2}`},
	}
	for i, st := range steps {
		op, err := st.op()
		if err != nil {
			t.Fatalf("step %d: encode: %v", i, err)
		}
		if got := s.Execute(op); string(got) != st.want {
			t.Fatalf("step %d: got %s, want %s", i, got, st.want)
		}
	}
}

func TestEncodeUnsupportedValue(t *testing.T) {
	if _, err := Write("k", make(chan int)); err == nil {
		t.Fatal("expected error encoding chan value")
	}
}

func BenchmarkExecute(b *testing.B) {
	s := New()
	write := []byte(`{"k":"write","key":"key-1","value":42}`)
	read := []byte(`{"k":"read","key":"key-1"}`)
	cas := []byte(`{"k":"cas","key":"key-1","from":42,"to":43}`)
	b.ReportAllocs()
	for b.Loop() {
		s.Execute(write)
		s.Execute(read)
		s.Execute(cas)
	}
}
