package protocol

import (
	"slices"
	"testing"
)

func opEntries(from, to uint64) []Entry {
	var es []Entry
	for op := from; op <= to; op++ {
		es = append(es, Entry{RequestNum: op})
	}
	return es
}

func TestAdoptLog(t *testing.T) {
	tests := []struct {
		name      string
		base      uint64
		commit    uint64
		logLen    uint64
		adoptBase uint64
		adopt     []Entry
		wantBase  uint64
		wantOps   []uint64
		wantFetch bool
	}{
		{"peer trimmed below own base", 4, 4, 2, 2, opEntries(3, 6), 4, []uint64{5, 6}, false},
		{"peer base equal to own", 4, 4, 2, 4, opEntries(5, 6), 4, []uint64{5, 6}, false},
		{"keeps own committed prefix", 0, 3, 4, 2, opEntries(3, 5), 0, []uint64{1, 2, 3, 4, 5}, false},
		{"base past commit starts fetch", 0, 1, 3, 4, opEntries(5, 6), 4, []uint64{5, 6}, true},
		{"fetch when base is past local log", 2, 2, 0, 6, opEntries(7, 7), 6, []uint64{7}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := &outboxLog{}
			r, err := NewReplica(Options{Addr: "r0", Config: []string{"r0", "r1", "r2"}, Service: &recorder{}, Outbox: out})
			if err != nil {
				t.Fatal(err)
			}
			r.base, r.commitNum = tc.base, tc.commit
			r.log = opEntries(tc.base+1, tc.base+tc.logLen)
			r.adoptLog(tc.adoptBase, tc.adopt, "r1")
			var ops []uint64
			for _, e := range r.log {
				ops = append(ops, e.RequestNum)
			}
			if r.base != tc.wantBase || !slices.Equal(ops, tc.wantOps) {
				t.Fatalf("base %d log %v, want base %d log %v", r.base, ops, tc.wantBase, tc.wantOps)
			}
			if fetching := r.fetch != nil; fetching != tc.wantFetch {
				t.Fatalf("fetching = %v, want %v", fetching, tc.wantFetch)
			}
			if tc.wantFetch {
				if len(out.sent) != 1 || out.sent[0].Type() != MsgGetCheckpoint {
					t.Fatalf("sent %v, want one GetCheckpoint", out.sent)
				}
			} else if len(out.sent) != 0 {
				t.Fatalf("sent %v without fetch", out.sent)
			}
		})
	}
}
