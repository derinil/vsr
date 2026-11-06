package sqlservice

import (
	"bytes"
	"strings"
	"testing"

	"github.com/derinil/vsr/internal/protocol"
)

var _ protocol.Snapshotter = (*Store)(nil)

func newStore(t *testing.T) *Store {
	t.Helper()
	s, err := New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

const schema = `[{"sql":"CREATE TABLE kv (k TEXT PRIMARY KEY, v)"}]`

func TestExecute(t *testing.T) {
	tests := []struct {
		name  string
		steps [][2]string
	}{
		{
			name: "insert then select",
			steps: [][2]string{
				{`[{"sql":"INSERT INTO kv VALUES (?, ?), (?, ?)","args":["a",1,"b",2.5]}]`, `{"results":[{"changes":2}]}`},
				{`[{"sql":"SELECT k, v, typeof(v) FROM kv ORDER BY k"}]`, `{"results":[{"columns":["k","v","typeof(v)"],"rows":[["a",1,"integer"],["b",2.5,"real"]]}]}`},
			},
		},
		{
			name: "several statements in one op",
			steps: [][2]string{
				{`[{"sql":"INSERT INTO kv VALUES ('a', 1)"},{"sql":"UPDATE kv SET v = v + 1"},{"sql":"SELECT v FROM kv"}]`, `{"results":[{"changes":1},{"changes":1},{"columns":["v"],"rows":[[2]]}]}`},
			},
		},
		{
			name: "empty select",
			steps: [][2]string{
				{`[{"sql":"SELECT v FROM kv"}]`, `{"results":[{"columns":["v"]}]}`},
			},
		},
		{
			name: "failed op rolls back",
			steps: [][2]string{
				{`[{"sql":"INSERT INTO kv VALUES ('a', 1)"},{"sql":"INSERT INTO kv VALUES ('a', 2)"}]`, `{"error":"statement 1: constraint failed: UNIQUE constraint failed: kv.k (1555)"}`},
				{`[{"sql":"SELECT count(*) FROM kv"}]`, `{"results":[{"columns":["count(*)"],"rows":[[0]]}]}`},
			},
		},
		{
			name: "read-only pragma",
			steps: [][2]string{
				{`[{"sql":"SELECT name FROM pragma_table_info('kv')"}]`, `{"results":[{"columns":["name"],"rows":[["k"],["v"]]}]}`},
			},
		},
		{
			name:  "malformed",
			steps: [][2]string{{`{}`, `{"error":"json: cannot unmarshal object into Go value of type []sqlservice.Stmt"}`}},
		},
		{
			name:  "no statements",
			steps: [][2]string{{`[]`, `{"error":"no statements"}`}},
		},
		{
			name:  "unsupported arg",
			steps: [][2]string{{`[{"sql":"SELECT ?","args":[[1]]}]`, `{"error":"statement 0 arg 0: unsupported type []interface {}"}`}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newStore(t)
			s.Execute([]byte(schema))
			for _, st := range tc.steps {
				if got := string(s.Execute([]byte(st[0]))); got != st[1] {
					t.Fatalf("Execute(%s) = %s, want %s", st[0], got, st[1])
				}
			}
		})
	}
}

func TestRejectsNondeterminism(t *testing.T) {
	for _, q := range []string{
		"BEGIN",
		"COMMIT",
		"SAVEPOINT x",
		"SELECT random()",
		"SELECT randomblob(4)",
		"SELECT datetime('now')",
		"SELECT CURRENT_TIMESTAMP",
		"SELECT strftime('%s', 'now')",
		"ATTACH 'x.db' AS x",
		"CREATE TEMP TABLE t (a)",
		"PRAGMA user_version = 1",
		"PRAGMA foreign_keys = OFF",
		"SELECT load_extension('x')",
		"VACUUM INTO 'x.db'",
	} {
		s := newStore(t)
		before := s.Snapshot()
		op, _ := Encode(Query(q))
		r, err := DecodeResult(s.Execute(op))
		if err != nil {
			t.Fatal(err)
		}
		if r.Error == "" {
			t.Errorf("%s succeeded: %+v", q, r.Results)
		}
		if !bytes.Equal(s.Snapshot(), before) {
			t.Errorf("%s changed the database", q)
		}
	}
}

func TestStillUsableAfterRejection(t *testing.T) {
	s := newStore(t)
	s.Execute([]byte(`[{"sql":"BEGIN"}]`))
	s.Execute([]byte(schema))
	if got := string(s.Execute([]byte(`[{"sql":"INSERT INTO kv VALUES (1, 1)"}]`))); got != `{"results":[{"changes":1}]}` {
		t.Fatal(got)
	}
}

func run(t *testing.T, s *Store, ops []string) []string {
	t.Helper()
	var out []string
	for _, op := range ops {
		out = append(out, string(s.Execute([]byte(op))))
	}
	return out
}

func TestSnapshotDeterministic(t *testing.T) {
	ops := []string{schema}
	for i := range 300 {
		ops = append(ops, `[{"sql":"INSERT INTO kv VALUES (?, ?) ON CONFLICT DO UPDATE SET v = excluded.v","args":["k`+string(rune('a'+i%26))+`",`+strings.Repeat("7", i%15+1)+`]}]`)
		if i%7 == 0 {
			ops = append(ops, `[{"sql":"DELETE FROM kv WHERE v > 777"}]`)
		}
	}
	a, b := newStore(t), newStore(t)
	run(t, a, ops[:150])
	run(t, b, ops[:150])
	snap := a.Snapshot()
	if !bytes.Equal(snap, b.Snapshot()) {
		t.Fatal("equal histories produced different snapshots")
	}
	c := newStore(t)
	if err := c.Restore(snap); err != nil {
		t.Fatal(err)
	}
	ra, rc := run(t, a, ops[150:]), run(t, c, ops[150:])
	if strings.Join(ra, "\n") != strings.Join(rc, "\n") {
		t.Fatal("restored store diverged")
	}
	if !bytes.Equal(a.Snapshot(), c.Snapshot()) {
		t.Fatal("restored store produced a different snapshot")
	}
}

func TestRestoreEmpty(t *testing.T) {
	a := newStore(t)
	snap := a.Snapshot()
	b := newStore(t)
	b.Execute([]byte(schema))
	if err := b.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b.Snapshot(), snap) {
		t.Fatal("restore of empty snapshot kept data")
	}
}

func TestRestoreCorrupt(t *testing.T) {
	s := newStore(t)
	s.Execute([]byte(schema))
	before := s.Snapshot()
	for _, state := range [][]byte{nil, []byte("garbage"), before[:len(before)/2], bytes.Repeat([]byte{0xff}, len(before))} {
		if err := s.Restore(state); err == nil {
			t.Errorf("Restore(%d bytes) succeeded", len(state))
		}
		if !bytes.Equal(s.Snapshot(), before) {
			t.Fatal("failed restore changed the database")
		}
	}
}
