package vsr

import (
	"encoding/binary"
	"errors"
	"math"
	"reflect"
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
