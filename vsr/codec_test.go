package vsr_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/derinil/vsr/vsr"
)

func samples() []vsr.Message {
	config := []string{"10.0.0.1:10188", "10.0.0.2:10188", "10.0.0.3:10188"}
	entries := []vsr.Entry{
		{Kind: vsr.EntryOp, ClientID: 1, RequestNum: 2, Op: []byte("get k")},
		{Kind: vsr.EntryCheckEpoch, ClientID: math.MaxUint64, RequestNum: 4},
		{Kind: vsr.EntryReconfig, ClientID: 5, RequestNum: 6, Config: config[1:]},
	}
	return []vsr.Message{
		vsr.Request{Epoch: 3, ClientID: 1 << 40, RequestNum: 99, Op: []byte("put k v")},
		vsr.Reconfiguration{Epoch: 2, ClientID: 7, RequestNum: 8, Config: config},
		vsr.CheckEpoch{Epoch: 4, ClientID: math.MaxUint64, RequestNum: 10},
		vsr.Reply{Epoch: 1, View: 5, ClientID: 6, RequestNum: 7, Result: []byte{0, 255, 1}},
		vsr.NewEpoch{Epoch: 2, View: 3, Config: config[:1]},
		vsr.Prepare{Epoch: 1, View: 2, OpNum: 3, CommitNum: 2, Entry: entries[2]},
		vsr.PrepareOK{Epoch: 1, View: 2, OpNum: 3, Replica: 2},
		vsr.Commit{Epoch: 1, View: 2, CommitNum: 3},
		vsr.StartViewChange{Epoch: 1, View: 9, Replica: 1},
		vsr.DoViewChange{Epoch: 1, View: 9, LastNormalView: 4, OpNum: 7, CommitNum: 5, Log: entries, Replica: 3, LogBase: 2},
		vsr.StartView{Epoch: 1, View: 9, OpNum: 7, CommitNum: 5, Log: entries, LogBase: 4},
		vsr.Recovery{Epoch: 1, Nonce: 42, Replica: 0, CheckpointNum: 6},
		vsr.RecoveryResponse{Epoch: 1, View: 9, Nonce: 42, OpNum: 7, CommitNum: 5, Log: entries, HasLog: true, Config: config, OldConfig: config[:2], Replica: 2, LogBase: 3, EpochStart: 5},
		vsr.GetState{Epoch: 1, View: 9, OpNum: 7, Replica: 1},
		vsr.NewState{Epoch: 1, View: 9, OpNum: 7, CommitNum: 5, Log: entries[:1], LogBase: 6},
		vsr.StartEpoch{Epoch: 2, OpNum: 7, OldConfig: config[:2], NewConfig: config[1:], From: "b"},
		vsr.EpochStarted{Epoch: math.MaxUint64, Replica: math.MaxInt},
		vsr.GetCheckpoint{Epoch: 2, MinOpNum: 100, From: config[0]},
		vsr.CheckpointInfo{Epoch: 2, OpNum: 100, Size: 1 << 20, PageSize: 4096, Root: bytes.Repeat([]byte{0xAB}, 32), Clients: []byte{1, 2, 3}},
		vsr.GetMerkle{OpNum: 100, Level: 3, Indices: []uint64{0, 1, math.MaxUint64}, From: config[1]},
		vsr.MerkleNodes{OpNum: 100, Level: 3, Indices: []uint64{0, 1}, Hashes: bytes.Repeat([]byte{0xCD}, 64)},
		vsr.GetPages{OpNum: 100, Indices: []uint64{5}, From: config[2]},
		vsr.Pages{OpNum: 100, Indices: []uint64{5, 6}, Data: [][]byte{[]byte("page-a"), {0, 255}}},
		vsr.Request{},
		vsr.Prepare{Entry: vsr.Entry{Kind: vsr.EntryReconfig}},
		vsr.DoViewChange{Replica: 0},
		vsr.RecoveryResponse{},
		vsr.GetCheckpoint{},
		vsr.CheckpointInfo{},
		vsr.GetMerkle{},
		vsr.MerkleNodes{},
		vsr.GetPages{},
		vsr.Pages{},
	}
}

func varints(vs ...uint64) []byte {
	var b []byte
	for _, v := range vs {
		b = binary.AppendUvarint(b, v)
	}
	return b
}

func msg(t vsr.MsgType, body ...[]byte) []byte {
	b := []byte{byte(t)}
	for _, p := range body {
		b = append(b, p...)
	}
	return b
}

func TestRoundTrip(t *testing.T) {
	seen := make(map[vsr.MsgType]bool)
	for _, m := range samples() {
		seen[m.Type()] = true
		t.Run(m.Type().String(), func(t *testing.T) {
			enc := vsr.AppendMessage([]byte{0xAA}, m)
			if enc[0] != 0xAA {
				t.Fatalf("prefix clobbered: %x", enc[0])
			}
			got, err := vsr.DecodeMessage(enc[1:])
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, m) {
				t.Fatalf("got %#v, want %#v", got, m)
			}
		})
	}
	for mt := vsr.MsgRequest; mt <= vsr.MsgPages; mt++ {
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
		{"unknown type after last", []byte{byte(vsr.MsgPages) + 1}},
		{"unknown type 255", []byte{255}},
		{"request huge op length", msg(vsr.MsgRequest, varints(1, 2, 3), huge)},
		{"reconfiguration huge count", msg(vsr.MsgReconfiguration, varints(1, 2, 3), huge)},
		{"reconfiguration huge string length", msg(vsr.MsgReconfiguration, varints(1, 2, 3, 1), huge)},
		{"start view huge log count", msg(vsr.MsgStartView, varints(1, 2, 3, 4), huge)},
		{"replica overflows int", msg(vsr.MsgStartViewChange, varints(1, 2, 1<<63))},
		{"prepare invalid entry kind", msg(vsr.MsgPrepare, varints(1, 2, 3, 4), []byte{9})},
		{"recovery response invalid bool", msg(vsr.MsgRecoveryResponse, varints(1, 2, 3, 4, 5, 0), []byte{2})},
		{"get checkpoint huge from length", msg(vsr.MsgGetCheckpoint, varints(1), huge)},
		{"checkpoint info page size overflows int", msg(vsr.MsgCheckpointInfo, varints(1, 2, 1<<63))},
		{"checkpoint info huge root length", msg(vsr.MsgCheckpointInfo, varints(1, 2, 3), huge)},
		{"get merkle huge index count", msg(vsr.MsgGetMerkle, varints(1, 2), huge)},
		{"merkle nodes huge hash length", msg(vsr.MsgMerkleNodes, varints(1, 2, 0), huge)},
		{"get pages huge index count", msg(vsr.MsgGetPages, varints(1), huge)},
		{"pages huge data count", msg(vsr.MsgPages, varints(1, 0), huge)},
		{"pages huge data element length", msg(vsr.MsgPages, varints(1, 1, 0), varints(1), huge)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := vsr.DecodeMessage(tc.in); !errors.Is(err, vsr.ErrMalformed) {
				t.Fatalf("err = %v, want ErrMalformed", err)
			}
		})
	}
}

func TestDecodeTruncated(t *testing.T) {
	for _, m := range samples() {
		enc := vsr.AppendMessage(nil, m)
		for i := range enc {
			if _, err := vsr.DecodeMessage(enc[:i]); !errors.Is(err, vsr.ErrMalformed) {
				t.Fatalf("%s prefix %d/%d: err = %v", m.Type(), i, len(enc), err)
			}
		}
	}
}

func TestDecodeTrailing(t *testing.T) {
	for _, m := range samples() {
		enc := append(vsr.AppendMessage(nil, m), 0)
		if _, err := vsr.DecodeMessage(enc); !errors.Is(err, vsr.ErrMalformed) {
			t.Fatalf("%s trailing byte: err = %v", m.Type(), err)
		}
	}
}

func TestAppendMessagePanics(t *testing.T) {
	tests := []struct {
		name string
		m    vsr.Message
	}{
		{"nil", nil},
		{"pointer", &vsr.Request{}},
		{"negative replica", vsr.PrepareOK{Replica: -1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			vsr.AppendMessage(nil, tc.m)
		})
	}
}

func TestClientOf(t *testing.T) {
	tests := []struct {
		name string
		m    vsr.Message
		id   uint64
		ok   bool
	}{
		{"request", vsr.Request{ClientID: 5}, 5, true},
		{"reconfiguration", vsr.Reconfiguration{ClientID: 6}, 6, true},
		{"check epoch", vsr.CheckEpoch{ClientID: 7}, 7, true},
		{"reply", vsr.Reply{ClientID: 8}, 0, false},
		{"prepare", vsr.Prepare{Entry: vsr.Entry{ClientID: 9}}, 0, false},
		{"nil", nil, 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id, ok := vsr.ClientOf(tc.m)
			if id != tc.id || ok != tc.ok {
				t.Fatalf("ClientOf = (%d, %t), want (%d, %t)", id, ok, tc.id, tc.ok)
			}
		})
	}
}

func TestMsgTypeString(t *testing.T) {
	tests := []struct {
		t    vsr.MsgType
		want string
	}{
		{vsr.MsgRequest, "Request"},
		{vsr.MsgPrepareOK, "PrepareOK"},
		{vsr.MsgEpochStarted, "EpochStarted"},
		{vsr.MsgMerkleNodes, "MerkleNodes"},
		{vsr.MsgPages, "Pages"},
		{0, "MsgType(0)"},
		{vsr.MsgPages + 1, "MsgType(24)"},
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
		f.Add(vsr.AppendMessage(nil, m))
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		m, err := vsr.DecodeMessage(b)
		if err != nil {
			if !errors.Is(err, vsr.ErrMalformed) {
				t.Fatalf("unexpected error: %v", err)
			}
			return
		}
		again, err := vsr.DecodeMessage(vsr.AppendMessage(nil, m))
		if err != nil {
			t.Fatalf("re-decode: %v", err)
		}
		if !reflect.DeepEqual(again, m) {
			t.Fatalf("round trip mismatch: %#v != %#v", again, m)
		}
	})
}
