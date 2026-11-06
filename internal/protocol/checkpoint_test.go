package protocol

import (
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"slices"
	"testing"
)

func TestCheckpointRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		c    Checkpoint
	}{
		{"zero", Checkpoint{}},
		{"state only", Checkpoint{OpNum: 10, State: []byte("state")}},
		{"full", Checkpoint{OpNum: 1 << 40, State: testData(100), Clients: []byte{1, 2, 3}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			enc := AppendCheckpoint([]byte{0xAA}, tc.c)
			if enc[0] != 0xAA {
				t.Fatalf("prefix clobbered: %x", enc[0])
			}
			got, err := DecodeCheckpoint(enc[1:])
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.c) {
				t.Fatalf("got %#v, want %#v", got, tc.c)
			}
		})
	}
}

func TestDecodeCheckpointMalformed(t *testing.T) {
	huge := binary.AppendUvarint(nil, math.MaxUint64)
	valid := AppendCheckpoint(nil, Checkpoint{OpNum: 1, State: []byte("s"), Clients: []byte("c")})
	tests := []struct {
		name string
		in   []byte
	}{
		{"empty", nil},
		{"op only", binary.AppendUvarint(nil, 1)},
		{"huge state length", append(binary.AppendUvarint(nil, 1), huge...)},
		{"huge clients length", append(binary.AppendUvarint(nil, 1), append([]byte{0}, huge...)...)},
		{"trailing byte", append(append([]byte{}, valid...), 0)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DecodeCheckpoint(tc.in); !errors.Is(err, ErrMalformed) {
				t.Fatalf("err = %v, want ErrMalformed", err)
			}
		})
	}
}

func TestDecodeCheckpointTruncated(t *testing.T) {
	enc := AppendCheckpoint(nil, Checkpoint{OpNum: 300, State: testData(20), Clients: []byte{7, 8}})
	for i := range enc {
		if _, err := DecodeCheckpoint(enc[:i]); !errors.Is(err, ErrMalformed) {
			t.Fatalf("prefix %d/%d: err = %v", i, len(enc), err)
		}
	}
}

func TestClientsRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		clients map[uint64]clientRecord
	}{
		{"empty", map[uint64]clientRecord{}},
		{"one", map[uint64]clientRecord{7: {req: 3, result: []byte("ok")}}},
		{"nil result", map[uint64]clientRecord{1: {req: 1}}},
		{"many", map[uint64]clientRecord{
			math.MaxUint64: {req: 9, result: []byte{0, 255}},
			0:              {req: 1},
			5:              {req: 2, result: []byte("five")},
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decodeClients(encodeClients(tc.clients))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.clients) {
				t.Fatalf("got %#v, want %#v", got, tc.clients)
			}
		})
	}
}

func TestEncodeClientsSorted(t *testing.T) {
	clients := map[uint64]clientRecord{
		9: {req: 1},
		2: {req: 3, result: []byte{4}},
	}
	want := []byte{2, 2, 3, 1, 4, 9, 1, 0}
	if got := encodeClients(clients); !reflect.DeepEqual(got, want) {
		t.Fatalf("encodeClients = %v, want %v", got, want)
	}
}

func TestDecodeClientsMalformed(t *testing.T) {
	huge := binary.AppendUvarint(nil, math.MaxUint64)
	tests := []struct {
		name string
		in   []byte
	}{
		{"nil", nil},
		{"huge count", huge},
		{"truncated record", []byte{1, 2}},
		{"huge result length", append([]byte{1, 1, 1}, huge...)},
		{"unsorted ids", []byte{2, 9, 1, 0, 2, 1, 0}},
		{"duplicate ids", []byte{2, 2, 1, 0, 2, 1, 0}},
		{"trailing byte", []byte{0, 0}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeClients(tc.in); !errors.Is(err, ErrMalformed) {
				t.Fatalf("err = %v, want ErrMalformed", err)
			}
		})
	}
}

func TestCheckpointTree(t *testing.T) {
	tests := []struct {
		name  string
		state []byte
	}{
		{"empty", nil},
		{"partial page", testData(3)},
		{"seven pages", testData(7 * testPageSize)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := Checkpoint{OpNum: 4, State: tc.state}
			cp := checkpoint{Checkpoint: c, tree: buildMerkle(c.State, testPageSize)}
			if cp.tree.size != len(tc.state) || cp.tree.root() != buildMerkle(cp.State, testPageSize).root() {
				t.Fatal("tree does not describe checkpoint state")
			}
		})
	}
}

type plainService struct{}

func (plainService) Execute(op []byte) []byte { return op }

func TestCheckpointOptions(t *testing.T) {
	cp := &Checkpoint{OpNum: 4, State: (&recorder{}).Snapshot(), Clients: encodeClients(nil)}
	badClients := &Checkpoint{OpNum: 4, Clients: []byte{0xFF}}
	tests := []struct {
		name    string
		service Service
		o       Options
		invalid bool
	}{
		{"negative interval", &recorder{}, Options{CheckpointInterval: -1}, true},
		{"negative retain", &recorder{}, Options{CheckpointInterval: 4, CheckpointRetain: -1}, true},
		{"negative page size", &recorder{}, Options{CheckpointInterval: 4, PageSize: -1}, true},
		{"interval needs snapshotter", plainService{}, Options{CheckpointInterval: 4}, true},
		{"checkpoint needs recovering", &recorder{}, Options{Checkpoint: cp}, true},
		{"checkpoint needs snapshotter", plainService{}, Options{Checkpoint: cp, Recovering: true}, true},
		{"malformed checkpoint clients", &recorder{}, Options{Checkpoint: badClients, Recovering: true}, true},
		{"interval with snapshotter", &recorder{}, Options{CheckpointInterval: 4, CheckpointRetain: 2, PageSize: 16}, false},
		{"no checkpoints with plain service", plainService{}, Options{}, false},
		{"restored checkpoint", &recorder{}, Options{Checkpoint: cp, Recovering: true}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o := tc.o
			o.Addr, o.Config, o.Service, o.Outbox = "r0", []string{"r0", "r1", "r2"}, tc.service, &outboxLog{}
			r, err := NewReplica(o)
			if tc.invalid {
				if !errors.Is(err, ErrInvalidOptions) {
					t.Fatalf("err = %v, want ErrInvalidOptions", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if o.Checkpoint == nil {
				if r.LogBase() != 0 || r.Checkpoint() != nil {
					t.Fatalf("fresh replica: base %d checkpoint %v", r.LogBase(), r.Checkpoint())
				}
				return
			}
			if r.Status() != StatusRecovering || r.LogBase() != 4 || r.OpNum() != 4 || r.CommitNum() != 4 {
				t.Fatalf("restored replica: status %v base %d op %d commit %d", r.Status(), r.LogBase(), r.OpNum(), r.CommitNum())
			}
			if got := r.Checkpoint(); got == nil || got.OpNum != 4 {
				t.Fatalf("Checkpoint() = %v, want op 4", got)
			}
		})
	}
}

func TestCollectGarbage(t *testing.T) {
	tests := []struct {
		name     string
		retain   uint64
		base     uint64
		commit   uint64
		held     []uint64
		wantBase uint64
		wantHeld []uint64
	}{
		{"newest checkpoint past retain", 4, 0, 8, []uint64{4, 8}, 4, []uint64{4, 8}},
		{"skips checkpoints still within retain", 4, 0, 12, []uint64{4, 8, 12}, 8, []uint64{8, 12}},
		{"nothing past retain", 5, 0, 8, []uint64{4, 8}, 0, []uint64{8}},
		{"base kept until superseded", 4, 4, 9, []uint64{4, 8}, 4, []uint64{4, 8}},
		{"zero retain collects latest", 0, 0, 4, []uint64{4}, 4, []uint64{4}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &Replica{checkpointRetain: tc.retain, base: tc.base, commitNum: tc.commit}
			for op := tc.base + 1; op <= tc.commit; op++ {
				r.log = append(r.log, Entry{RequestNum: op})
			}
			for _, n := range tc.held {
				r.ckpts = append(r.ckpts, newCheckpoint(Checkpoint{OpNum: n}, testPageSize))
			}
			r.collectGarbage()
			if r.base != tc.wantBase || r.OpNum() != tc.commit {
				t.Fatalf("base %d op %d, want base %d op %d", r.base, r.OpNum(), tc.wantBase, tc.commit)
			}
			var held []uint64
			for _, cp := range r.ckpts {
				held = append(held, cp.OpNum)
			}
			if !slices.Equal(held, tc.wantHeld) {
				t.Fatalf("held checkpoints %v, want %v", held, tc.wantHeld)
			}
			if len(r.log) > 0 && r.log[0].RequestNum != tc.wantBase+1 {
				t.Fatalf("log starts at op %d, want %d", r.log[0].RequestNum, tc.wantBase+1)
			}
		})
	}
}
