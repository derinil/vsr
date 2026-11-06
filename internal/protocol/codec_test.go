package protocol_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/derinil/vsr/internal/protocol"
)

func samples() []protocol.Message {
	config := []string{"10.0.0.1:10188", "10.0.0.2:10188", "10.0.0.3:10188"}
	entries := []protocol.Entry{
		{Kind: protocol.EntryOp, ClientID: 1, RequestNum: 2, Op: []byte("get k")},
		{Kind: protocol.EntryCheckEpoch, ClientID: math.MaxUint64, RequestNum: 4},
		{Kind: protocol.EntryReconfig, ClientID: 5, RequestNum: 6, Config: config[1:]},
	}
	return []protocol.Message{
		protocol.Request{Epoch: 3, ClientID: 1 << 40, RequestNum: 99, Op: []byte("put k v")},
		protocol.Reconfiguration{Epoch: 2, ClientID: 7, RequestNum: 8, Config: config},
		protocol.CheckEpoch{Epoch: 4, ClientID: math.MaxUint64, RequestNum: 10},
		protocol.Reply{Epoch: 1, View: 5, ClientID: 6, RequestNum: 7, Result: []byte{0, 255, 1}},
		protocol.NewEpoch{Epoch: 2, View: 3, Config: config[:1]},
		protocol.Prepare{Epoch: 1, View: 2, OpNum: 3, CommitNum: 2, Entry: entries[2]},
		protocol.PrepareOK{Epoch: 1, View: 2, OpNum: 3, Replica: 2},
		protocol.Commit{Epoch: 1, View: 2, CommitNum: 3},
		protocol.StartViewChange{Epoch: 1, View: 9, Replica: 1},
		protocol.DoViewChange{Epoch: 1, View: 9, LastNormalView: 4, OpNum: 7, CommitNum: 5, Log: entries, Replica: 3, LogBase: 2},
		protocol.StartView{Epoch: 1, View: 9, OpNum: 7, CommitNum: 5, Log: entries, LogBase: 4},
		protocol.Recovery{Epoch: 1, Nonce: 42, Replica: 0, CheckpointNum: 6},
		protocol.RecoveryResponse{Epoch: 1, View: 9, Nonce: 42, OpNum: 7, CommitNum: 5, Log: entries, HasLog: true, Config: config, OldConfig: config[:2], Replica: 2, LogBase: 3, EpochStart: 5},
		protocol.GetState{Epoch: 1, View: 9, OpNum: 7, Replica: 1},
		protocol.NewState{Epoch: 1, View: 9, OpNum: 7, CommitNum: 5, Log: entries[:1], LogBase: 6},
		protocol.StartEpoch{Epoch: 2, OpNum: 7, OldConfig: config[:2], NewConfig: config[1:], From: "b"},
		protocol.EpochStarted{Epoch: math.MaxUint64, Replica: math.MaxInt},
		protocol.GetCheckpoint{Epoch: 2, MinOpNum: 100, From: config[0]},
		protocol.CheckpointInfo{Epoch: 2, OpNum: 100, Size: 1 << 20, PageSize: 4096, Root: bytes.Repeat([]byte{0xAB}, 32), Clients: []byte{1, 2, 3}},
		protocol.GetMerkle{OpNum: 100, Level: 3, Indices: []uint64{0, 1, math.MaxUint64}, From: config[1]},
		protocol.MerkleNodes{OpNum: 100, Level: 3, Indices: []uint64{0, 1}, Hashes: bytes.Repeat([]byte{0xCD}, 64)},
		protocol.GetPages{OpNum: 100, Indices: []uint64{5}, From: config[2]},
		protocol.Pages{OpNum: 100, Indices: []uint64{5, 6}, Data: [][]byte{[]byte("page-a"), {0, 255}}},
		protocol.Request{},
		protocol.Prepare{Entry: protocol.Entry{Kind: protocol.EntryReconfig}},
		protocol.DoViewChange{Replica: 0},
		protocol.RecoveryResponse{},
		protocol.GetCheckpoint{},
		protocol.CheckpointInfo{},
		protocol.GetMerkle{},
		protocol.MerkleNodes{},
		protocol.GetPages{},
		protocol.Pages{},
	}
}

func varints(vs ...uint64) []byte {
	var b []byte
	for _, v := range vs {
		b = binary.AppendUvarint(b, v)
	}
	return b
}

func msg(t protocol.MsgType, body ...[]byte) []byte {
	b := []byte{byte(t)}
	for _, p := range body {
		b = append(b, p...)
	}
	return b
}

func TestRoundTrip(t *testing.T) {
	seen := make(map[protocol.MsgType]bool)
	for _, m := range samples() {
		seen[m.Type()] = true
		t.Run(m.Type().String(), func(t *testing.T) {
			enc := protocol.AppendMessage([]byte{0xAA}, m)
			if enc[0] != 0xAA {
				t.Fatalf("prefix clobbered: %x", enc[0])
			}
			got, err := protocol.DecodeMessage(enc[1:])
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, m) {
				t.Fatalf("got %#v, want %#v", got, m)
			}
		})
	}
	for mt := protocol.MsgRequest; mt <= protocol.MsgPages; mt++ {
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
		{"unknown type after last", []byte{byte(protocol.MsgPages) + 1}},
		{"unknown type 255", []byte{255}},
		{"request huge op length", msg(protocol.MsgRequest, varints(1, 2, 3), huge)},
		{"reconfiguration huge count", msg(protocol.MsgReconfiguration, varints(1, 2, 3), huge)},
		{"reconfiguration huge string length", msg(protocol.MsgReconfiguration, varints(1, 2, 3, 1), huge)},
		{"start view huge log count", msg(protocol.MsgStartView, varints(1, 2, 3, 4), huge)},
		{"replica overflows int", msg(protocol.MsgStartViewChange, varints(1, 2, 1<<63))},
		{"prepare invalid entry kind", msg(protocol.MsgPrepare, varints(1, 2, 3, 4), []byte{9})},
		{"recovery response invalid bool", msg(protocol.MsgRecoveryResponse, varints(1, 2, 3, 4, 5, 0), []byte{2})},
		{"get checkpoint huge from length", msg(protocol.MsgGetCheckpoint, varints(1), huge)},
		{"checkpoint info page size overflows int", msg(protocol.MsgCheckpointInfo, varints(1, 2, 1<<63))},
		{"checkpoint info huge root length", msg(protocol.MsgCheckpointInfo, varints(1, 2, 3), huge)},
		{"get merkle huge index count", msg(protocol.MsgGetMerkle, varints(1, 2), huge)},
		{"merkle nodes huge hash length", msg(protocol.MsgMerkleNodes, varints(1, 2, 0), huge)},
		{"get pages huge index count", msg(protocol.MsgGetPages, varints(1), huge)},
		{"pages huge data count", msg(protocol.MsgPages, varints(1, 0), huge)},
		{"pages huge data element length", msg(protocol.MsgPages, varints(1, 1, 0), varints(1), huge)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := protocol.DecodeMessage(tc.in); !errors.Is(err, protocol.ErrMalformed) {
				t.Fatalf("err = %v, want ErrMalformed", err)
			}
		})
	}
}

func TestDecodeTruncated(t *testing.T) {
	for _, m := range samples() {
		enc := protocol.AppendMessage(nil, m)
		for i := range enc {
			if _, err := protocol.DecodeMessage(enc[:i]); !errors.Is(err, protocol.ErrMalformed) {
				t.Fatalf("%s prefix %d/%d: err = %v", m.Type(), i, len(enc), err)
			}
		}
	}
}

func TestDecodeTrailing(t *testing.T) {
	for _, m := range samples() {
		enc := append(protocol.AppendMessage(nil, m), 0)
		if _, err := protocol.DecodeMessage(enc); !errors.Is(err, protocol.ErrMalformed) {
			t.Fatalf("%s trailing byte: err = %v", m.Type(), err)
		}
	}
}

func TestAppendMessagePanics(t *testing.T) {
	tests := []struct {
		name string
		m    protocol.Message
	}{
		{"nil", nil},
		{"pointer", &protocol.Request{}},
		{"negative replica", protocol.PrepareOK{Replica: -1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			protocol.AppendMessage(nil, tc.m)
		})
	}
}

func TestClientOf(t *testing.T) {
	tests := []struct {
		name string
		m    protocol.Message
		id   uint64
		ok   bool
	}{
		{"request", protocol.Request{ClientID: 5}, 5, true},
		{"reconfiguration", protocol.Reconfiguration{ClientID: 6}, 6, true},
		{"check epoch", protocol.CheckEpoch{ClientID: 7}, 7, true},
		{"reply", protocol.Reply{ClientID: 8}, 0, false},
		{"prepare", protocol.Prepare{Entry: protocol.Entry{ClientID: 9}}, 0, false},
		{"nil", nil, 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id, ok := protocol.ClientOf(tc.m)
			if id != tc.id || ok != tc.ok {
				t.Fatalf("ClientOf = (%d, %t), want (%d, %t)", id, ok, tc.id, tc.ok)
			}
		})
	}
}

func TestPeekClient(t *testing.T) {
	for _, m := range samples() {
		t.Run(m.Type().String(), func(t *testing.T) {
			wantID, wantOK := protocol.ClientOf(m)
			if id, ok := protocol.PeekClient(protocol.AppendMessage(nil, m)); id != wantID || ok != wantOK {
				t.Fatalf("PeekClient = (%d, %t), want (%d, %t)", id, ok, wantID, wantOK)
			}
		})
	}
	big := protocol.Request{Epoch: 1 << 40, ClientID: 1<<63 + 5}
	if id, ok := protocol.PeekClient(protocol.AppendMessage(nil, big)); !ok || id != big.ClientID {
		t.Fatalf("PeekClient = (%d, %t), want (%d, true)", id, ok, big.ClientID)
	}
	for _, b := range [][]byte{nil, {byte(protocol.MsgRequest)}, {byte(protocol.MsgRequest), 1}, {byte(protocol.MsgRequest), 0x80}} {
		if _, ok := protocol.PeekClient(b); ok {
			t.Errorf("PeekClient(%v) ok, want not ok", b)
		}
	}
}

func TestMsgTypeString(t *testing.T) {
	tests := []struct {
		t    protocol.MsgType
		want string
	}{
		{protocol.MsgRequest, "Request"},
		{protocol.MsgPrepareOK, "PrepareOK"},
		{protocol.MsgEpochStarted, "EpochStarted"},
		{protocol.MsgMerkleNodes, "MerkleNodes"},
		{protocol.MsgPages, "Pages"},
		{0, "MsgType(0)"},
		{protocol.MsgPages + 1, "MsgType(24)"},
		{255, "MsgType(255)"},
	}
	for _, tc := range tests {
		if got := tc.t.String(); got != tc.want {
			t.Errorf("MsgType(%d).String() = %q, want %q", uint8(tc.t), got, tc.want)
		}
	}
}

func FuzzDecodeMessage(f *testing.F) {
	for _, m := range samples() {
		f.Add(protocol.AppendMessage(nil, m))
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		m, err := protocol.DecodeMessage(b)
		if err != nil {
			if !errors.Is(err, protocol.ErrMalformed) {
				t.Fatalf("unexpected error: %v", err)
			}
			return
		}
		again, err := protocol.DecodeMessage(protocol.AppendMessage(nil, m))
		if err != nil {
			t.Fatalf("re-decode: %v", err)
		}
		if !reflect.DeepEqual(again, m) {
			t.Fatalf("round trip mismatch: %#v != %#v", again, m)
		}
	})
}
