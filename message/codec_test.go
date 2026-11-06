package message_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/derinil/vsr/message"
)

func samples() []message.Message {
	config := []string{"10.0.0.1:10188", "10.0.0.2:10188", "10.0.0.3:10188"}
	entries := []message.Entry{
		{Kind: message.EntryOp, ClientID: 1, RequestNum: 2, Op: []byte("get k")},
		{Kind: message.EntryCheckEpoch, ClientID: math.MaxUint64, RequestNum: 4},
		{Kind: message.EntryReconfig, ClientID: 5, RequestNum: 6, Config: config[1:]},
	}
	return []message.Message{
		message.Request{Epoch: 3, ClientID: 1 << 40, RequestNum: 99, Op: []byte("put k v")},
		message.Reconfiguration{Epoch: 2, ClientID: 7, RequestNum: 8, Config: config},
		message.CheckEpoch{Epoch: 4, ClientID: math.MaxUint64, RequestNum: 10},
		message.Reply{Epoch: 1, View: 5, ClientID: 6, RequestNum: 7, Result: []byte{0, 255, 1}},
		message.NewEpoch{Epoch: 2, View: 3, Config: config[:1]},
		message.Prepare{Epoch: 1, View: 2, OpNum: 3, CommitNum: 2, Entry: entries[2]},
		message.PrepareOK{Epoch: 1, View: 2, OpNum: 3, Replica: 2},
		message.Commit{Epoch: 1, View: 2, CommitNum: 3},
		message.StartViewChange{Epoch: 1, View: 9, Replica: 1},
		message.DoViewChange{Epoch: 1, View: 9, LastNormalView: 4, OpNum: 7, CommitNum: 5, Log: entries, Replica: 3, LogBase: 2},
		message.StartView{Epoch: 1, View: 9, OpNum: 7, CommitNum: 5, Log: entries, LogBase: 4},
		message.Recovery{Epoch: 1, Nonce: 42, Replica: 0, CheckpointNum: 6},
		message.RecoveryResponse{Epoch: 1, View: 9, Nonce: 42, OpNum: 7, CommitNum: 5, Log: entries, HasLog: true, Config: config, OldConfig: config[:2], Replica: 2, LogBase: 3, EpochStart: 5},
		message.GetState{Epoch: 1, View: 9, OpNum: 7, Replica: 1},
		message.NewState{Epoch: 1, View: 9, OpNum: 7, CommitNum: 5, Log: entries[:1], LogBase: 6},
		message.StartEpoch{Epoch: 2, OpNum: 7, OldConfig: config[:2], NewConfig: config[1:], From: "b"},
		message.EpochStarted{Epoch: math.MaxUint64, Replica: math.MaxInt},
		message.GetCheckpoint{Epoch: 2, MinOpNum: 100, From: config[0]},
		message.CheckpointInfo{Epoch: 2, OpNum: 100, Size: 1 << 20, PageSize: 4096, Root: bytes.Repeat([]byte{0xAB}, 32), Clients: []byte{1, 2, 3}},
		message.GetMerkle{OpNum: 100, Level: 3, Indices: []uint64{0, 1, math.MaxUint64}, From: config[1]},
		message.MerkleNodes{OpNum: 100, Level: 3, Indices: []uint64{0, 1}, Hashes: bytes.Repeat([]byte{0xCD}, 64)},
		message.GetPages{OpNum: 100, Indices: []uint64{5}, From: config[2]},
		message.Pages{OpNum: 100, Indices: []uint64{5, 6}, Data: [][]byte{[]byte("page-a"), {0, 255}}},
		message.Request{},
		message.Prepare{Entry: message.Entry{Kind: message.EntryReconfig}},
		message.DoViewChange{Replica: 0},
		message.RecoveryResponse{},
		message.GetCheckpoint{},
		message.CheckpointInfo{},
		message.GetMerkle{},
		message.MerkleNodes{},
		message.GetPages{},
		message.Pages{},
	}
}

func varints(vs ...uint64) []byte {
	var b []byte
	for _, v := range vs {
		b = binary.AppendUvarint(b, v)
	}
	return b
}

func msg(t message.Type, body ...[]byte) []byte {
	b := []byte{byte(t)}
	for _, p := range body {
		b = append(b, p...)
	}
	return b
}

func TestRoundTrip(t *testing.T) {
	seen := make(map[message.Type]bool)
	for _, m := range samples() {
		seen[m.Type()] = true
		t.Run(m.Type().String(), func(t *testing.T) {
			enc := message.Append([]byte{0xAA}, m)
			if enc[0] != 0xAA {
				t.Fatalf("prefix clobbered: %x", enc[0])
			}
			got, err := message.Decode(enc[1:])
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, m) {
				t.Fatalf("got %#v, want %#v", got, m)
			}
		})
	}
	for mt := message.TypeRequest; mt <= message.TypePages; mt++ {
		if !seen[mt] {
			t.Errorf("no sample for %s", mt)
		}
	}
}

func TestDecodeMalformed(t *testing.T) {
	huge := varints(math.MaxUint64)
	tests := []struct {
		name string
		in   []byte
	}{
		{"empty", nil},
		{"unknown type zero", []byte{0}},
		{"unknown type after last", []byte{byte(message.TypePages) + 1}},
		{"unknown type 255", []byte{255}},
		{"request huge op length", msg(message.TypeRequest, varints(1, 2, 3), huge)},
		{"reconfiguration huge count", msg(message.TypeReconfiguration, varints(1, 2, 3), huge)},
		{"reconfiguration huge string length", msg(message.TypeReconfiguration, varints(1, 2, 3, 1), huge)},
		{"start view huge log count", msg(message.TypeStartView, varints(1, 2, 3, 4), huge)},
		{"replica overflows int", msg(message.TypeStartViewChange, varints(1, 2, 1<<63))},
		{"prepare invalid entry kind", msg(message.TypePrepare, varints(1, 2, 3, 4), []byte{9})},
		{"recovery response invalid bool", msg(message.TypeRecoveryResponse, varints(1, 2, 3, 4, 5, 0), []byte{2})},
		{"get checkpoint huge from length", msg(message.TypeGetCheckpoint, varints(1), huge)},
		{"checkpoint info page size overflows int", msg(message.TypeCheckpointInfo, varints(1, 2, 1<<63))},
		{"checkpoint info huge root length", msg(message.TypeCheckpointInfo, varints(1, 2, 3), huge)},
		{"get merkle huge index count", msg(message.TypeGetMerkle, varints(1, 2), huge)},
		{"merkle nodes huge hash length", msg(message.TypeMerkleNodes, varints(1, 2, 0), huge)},
		{"get pages huge index count", msg(message.TypeGetPages, varints(1), huge)},
		{"pages huge data count", msg(message.TypePages, varints(1, 0), huge)},
		{"pages huge data element length", msg(message.TypePages, varints(1, 1, 0), varints(1), huge)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := message.Decode(tc.in); !errors.Is(err, message.ErrMalformed) {
				t.Fatalf("err = %v, want ErrMalformed", err)
			}
		})
	}
}

func TestDecodeTruncated(t *testing.T) {
	for _, m := range samples() {
		enc := message.Append(nil, m)
		for i := range enc {
			if _, err := message.Decode(enc[:i]); !errors.Is(err, message.ErrMalformed) {
				t.Fatalf("%s prefix %d/%d: err = %v", m.Type(), i, len(enc), err)
			}
		}
	}
}

func TestDecodeTrailing(t *testing.T) {
	for _, m := range samples() {
		enc := append(message.Append(nil, m), 0)
		if _, err := message.Decode(enc); !errors.Is(err, message.ErrMalformed) {
			t.Fatalf("%s trailing byte: err = %v", m.Type(), err)
		}
	}
}

func TestAppendPanics(t *testing.T) {
	tests := []struct {
		name string
		m    message.Message
	}{
		{"nil", nil},
		{"pointer", &message.Request{}},
		{"negative replica", message.PrepareOK{Replica: -1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			message.Append(nil, tc.m)
		})
	}
}

func TestClientOf(t *testing.T) {
	tests := []struct {
		name string
		m    message.Message
		id   uint64
		ok   bool
	}{
		{"request", message.Request{ClientID: 5}, 5, true},
		{"reconfiguration", message.Reconfiguration{ClientID: 6}, 6, true},
		{"check epoch", message.CheckEpoch{ClientID: 7}, 7, true},
		{"reply", message.Reply{ClientID: 8}, 0, false},
		{"prepare", message.Prepare{Entry: message.Entry{ClientID: 9}}, 0, false},
		{"nil", nil, 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id, ok := message.ClientOf(tc.m)
			if id != tc.id || ok != tc.ok {
				t.Fatalf("ClientOf = (%d, %t), want (%d, %t)", id, ok, tc.id, tc.ok)
			}
		})
	}
}

func TestTypeString(t *testing.T) {
	tests := []struct {
		t    message.Type
		want string
	}{
		{message.TypeRequest, "Request"},
		{message.TypePrepareOK, "PrepareOK"},
		{message.TypeEpochStarted, "EpochStarted"},
		{message.TypeMerkleNodes, "MerkleNodes"},
		{message.TypePages, "Pages"},
		{0, "Type(0)"},
		{message.TypePages + 1, "Type(24)"},
		{255, "Type(255)"},
	}
	for _, tc := range tests {
		if got := tc.t.String(); got != tc.want {
			t.Errorf("Type(%d).String() = %q, want %q", uint8(tc.t), got, tc.want)
		}
	}
}

func FuzzDecode(f *testing.F) {
	for _, m := range samples() {
		f.Add(message.Append(nil, m))
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		m, err := message.Decode(b)
		if err != nil {
			if !errors.Is(err, message.ErrMalformed) {
				t.Fatalf("unexpected error: %v", err)
			}
			return
		}
		again, err := message.Decode(message.Append(nil, m))
		if err != nil {
			t.Fatalf("re-decode: %v", err)
		}
		if !reflect.DeepEqual(again, m) {
			t.Fatalf("round trip mismatch: %#v != %#v", again, m)
		}
	})
}
