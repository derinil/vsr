package vsr

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
)

// ErrMalformed is returned by DecodeMessage for invalid input.
var ErrMalformed = errors.New("vsr: malformed message")

// AppendMessage appends the binary encoding of m to dst.
func AppendMessage(dst []byte, m Message) []byte {
	e := encoder(dst)
	e.u8(byte(m.Type()))
	switch m := m.(type) {
	case Request:
		e.u64(m.Epoch)
		e.u64(m.ClientID)
		e.u64(m.RequestNum)
		e.bytes(m.Op)
	case Reconfiguration:
		e.u64(m.Epoch)
		e.u64(m.ClientID)
		e.u64(m.RequestNum)
		e.strings(m.Config)
	case CheckEpoch:
		e.u64(m.Epoch)
		e.u64(m.ClientID)
		e.u64(m.RequestNum)
	case Reply:
		e.u64(m.Epoch)
		e.u64(m.View)
		e.u64(m.ClientID)
		e.u64(m.RequestNum)
		e.bytes(m.Result)
	case NewEpoch:
		e.u64(m.Epoch)
		e.u64(m.View)
		e.strings(m.Config)
	case Prepare:
		e.u64(m.Epoch)
		e.u64(m.View)
		e.u64(m.OpNum)
		e.u64(m.CommitNum)
		e.entry(m.Entry)
	case PrepareOK:
		e.u64(m.Epoch)
		e.u64(m.View)
		e.u64(m.OpNum)
		e.int(m.Replica)
	case Commit:
		e.u64(m.Epoch)
		e.u64(m.View)
		e.u64(m.CommitNum)
	case StartViewChange:
		e.u64(m.Epoch)
		e.u64(m.View)
		e.int(m.Replica)
	case DoViewChange:
		e.u64(m.Epoch)
		e.u64(m.View)
		e.u64(m.LastNormalView)
		e.u64(m.OpNum)
		e.u64(m.CommitNum)
		e.entries(m.Log)
		e.int(m.Replica)
		e.u64(m.LogBase)
	case StartView:
		e.u64(m.Epoch)
		e.u64(m.View)
		e.u64(m.OpNum)
		e.u64(m.CommitNum)
		e.entries(m.Log)
		e.u64(m.LogBase)
	case Recovery:
		e.u64(m.Epoch)
		e.u64(m.Nonce)
		e.int(m.Replica)
		e.u64(m.CheckpointNum)
	case RecoveryResponse:
		e.u64(m.Epoch)
		e.u64(m.View)
		e.u64(m.Nonce)
		e.u64(m.OpNum)
		e.u64(m.CommitNum)
		e.entries(m.Log)
		e.boolean(m.HasLog)
		e.strings(m.Config)
		e.strings(m.OldConfig)
		e.int(m.Replica)
		e.u64(m.LogBase)
		e.u64(m.EpochStart)
	case GetState:
		e.u64(m.Epoch)
		e.u64(m.View)
		e.u64(m.OpNum)
		e.int(m.Replica)
	case NewState:
		e.u64(m.Epoch)
		e.u64(m.View)
		e.u64(m.OpNum)
		e.u64(m.CommitNum)
		e.entries(m.Log)
		e.u64(m.LogBase)
	case StartEpoch:
		e.u64(m.Epoch)
		e.u64(m.OpNum)
		e.strings(m.OldConfig)
		e.strings(m.NewConfig)
		e.int(m.Replica)
	case EpochStarted:
		e.u64(m.Epoch)
		e.int(m.Replica)
	case GetCheckpoint:
		e.u64(m.MinOpNum)
		e.str(m.From)
	case CheckpointInfo:
		e.u64(m.OpNum)
		e.u64(m.Size)
		e.int(m.PageSize)
		e.bytes(m.Root)
		e.bytes(m.Clients)
	case GetMerkle:
		e.u64(m.OpNum)
		e.int(m.Level)
		e.u64s(m.Indices)
		e.str(m.From)
	case MerkleNodes:
		e.u64(m.OpNum)
		e.int(m.Level)
		e.u64s(m.Indices)
		e.bytes(m.Hashes)
	case GetPages:
		e.u64(m.OpNum)
		e.u64s(m.Indices)
		e.str(m.From)
	case Pages:
		e.u64(m.OpNum)
		e.u64s(m.Indices)
		e.byteSlices(m.Data)
	default:
		panic(fmt.Sprintf("vsr: cannot encode %T", m))
	}
	return e
}

// DecodeMessage decodes a message encoded by AppendMessage.
func DecodeMessage(b []byte) (Message, error) {
	d := decoder{b: b}
	var m Message
	switch MsgType(d.u8()) {
	case MsgRequest:
		m = Request{Epoch: d.u64(), ClientID: d.u64(), RequestNum: d.u64(), Op: d.bytes()}
	case MsgReconfiguration:
		m = Reconfiguration{Epoch: d.u64(), ClientID: d.u64(), RequestNum: d.u64(), Config: d.strings()}
	case MsgCheckEpoch:
		m = CheckEpoch{Epoch: d.u64(), ClientID: d.u64(), RequestNum: d.u64()}
	case MsgReply:
		m = Reply{Epoch: d.u64(), View: d.u64(), ClientID: d.u64(), RequestNum: d.u64(), Result: d.bytes()}
	case MsgNewEpoch:
		m = NewEpoch{Epoch: d.u64(), View: d.u64(), Config: d.strings()}
	case MsgPrepare:
		m = Prepare{Epoch: d.u64(), View: d.u64(), OpNum: d.u64(), CommitNum: d.u64(), Entry: d.entry()}
	case MsgPrepareOK:
		m = PrepareOK{Epoch: d.u64(), View: d.u64(), OpNum: d.u64(), Replica: d.int()}
	case MsgCommit:
		m = Commit{Epoch: d.u64(), View: d.u64(), CommitNum: d.u64()}
	case MsgStartViewChange:
		m = StartViewChange{Epoch: d.u64(), View: d.u64(), Replica: d.int()}
	case MsgDoViewChange:
		m = DoViewChange{
			Epoch:          d.u64(),
			View:           d.u64(),
			LastNormalView: d.u64(),
			OpNum:          d.u64(),
			CommitNum:      d.u64(),
			Log:            d.entries(),
			Replica:        d.int(),
			LogBase:        d.u64(),
		}
	case MsgStartView:
		m = StartView{Epoch: d.u64(), View: d.u64(), OpNum: d.u64(), CommitNum: d.u64(), Log: d.entries(), LogBase: d.u64()}
	case MsgRecovery:
		m = Recovery{Epoch: d.u64(), Nonce: d.u64(), Replica: d.int(), CheckpointNum: d.u64()}
	case MsgRecoveryResponse:
		m = RecoveryResponse{
			Epoch:      d.u64(),
			View:       d.u64(),
			Nonce:      d.u64(),
			OpNum:      d.u64(),
			CommitNum:  d.u64(),
			Log:        d.entries(),
			HasLog:     d.boolean(),
			Config:     d.strings(),
			OldConfig:  d.strings(),
			Replica:    d.int(),
			LogBase:    d.u64(),
			EpochStart: d.u64(),
		}
	case MsgGetState:
		m = GetState{Epoch: d.u64(), View: d.u64(), OpNum: d.u64(), Replica: d.int()}
	case MsgNewState:
		m = NewState{Epoch: d.u64(), View: d.u64(), OpNum: d.u64(), CommitNum: d.u64(), Log: d.entries(), LogBase: d.u64()}
	case MsgStartEpoch:
		m = StartEpoch{Epoch: d.u64(), OpNum: d.u64(), OldConfig: d.strings(), NewConfig: d.strings(), Replica: d.int()}
	case MsgEpochStarted:
		m = EpochStarted{Epoch: d.u64(), Replica: d.int()}
	case MsgGetCheckpoint:
		m = GetCheckpoint{MinOpNum: d.u64(), From: d.str()}
	case MsgCheckpointInfo:
		m = CheckpointInfo{OpNum: d.u64(), Size: d.u64(), PageSize: d.int(), Root: d.bytes(), Clients: d.bytes()}
	case MsgGetMerkle:
		m = GetMerkle{OpNum: d.u64(), Level: d.int(), Indices: d.u64s(), From: d.str()}
	case MsgMerkleNodes:
		m = MerkleNodes{OpNum: d.u64(), Level: d.int(), Indices: d.u64s(), Hashes: d.bytes()}
	case MsgGetPages:
		m = GetPages{OpNum: d.u64(), Indices: d.u64s(), From: d.str()}
	case MsgPages:
		m = Pages{OpNum: d.u64(), Indices: d.u64s(), Data: d.byteSlices()}
	default:
		return nil, ErrMalformed
	}
	if d.err != nil || len(d.b) > 0 {
		return nil, ErrMalformed
	}
	return m, nil
}

type encoder []byte

func (e *encoder) u8(v byte) {
	*e = append(*e, v)
}

func (e *encoder) u64(v uint64) {
	*e = binary.AppendUvarint(*e, v)
}

func (e *encoder) int(v int) {
	if v < 0 {
		panic("vsr: negative int field")
	}
	e.u64(uint64(v))
}

func (e *encoder) boolean(v bool) {
	if v {
		e.u8(1)
	} else {
		e.u8(0)
	}
}

func (e *encoder) bytes(v []byte) {
	e.u64(uint64(len(v)))
	*e = append(*e, v...)
}

func (e *encoder) str(v string) {
	e.u64(uint64(len(v)))
	*e = append(*e, v...)
}

func (e *encoder) strings(v []string) {
	e.u64(uint64(len(v)))
	for _, s := range v {
		e.str(s)
	}
}

func (e *encoder) entry(v Entry) {
	e.u8(byte(v.Kind))
	e.u64(v.ClientID)
	e.u64(v.RequestNum)
	e.bytes(v.Op)
	e.strings(v.Config)
}

func (e *encoder) entries(v []Entry) {
	e.u64(uint64(len(v)))
	for _, x := range v {
		e.entry(x)
	}
}

func (e *encoder) u64s(v []uint64) {
	e.u64(uint64(len(v)))
	for _, x := range v {
		e.u64(x)
	}
}

func (e *encoder) byteSlices(v [][]byte) {
	e.u64(uint64(len(v)))
	for _, x := range v {
		e.bytes(x)
	}
}

type decoder struct {
	b   []byte
	err error
}

func (d *decoder) take(n int) []byte {
	if d.err != nil || n > len(d.b) {
		d.err = ErrMalformed
		return nil
	}
	v := d.b[:n]
	d.b = d.b[n:]
	return v
}

func (d *decoder) u8() byte {
	if b := d.take(1); b != nil {
		return b[0]
	}
	return 0
}

func (d *decoder) u64() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.err = ErrMalformed
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) int() int {
	v := d.u64()
	if v > math.MaxInt {
		d.err = ErrMalformed
		return 0
	}
	return int(v)
}

func (d *decoder) boolean() bool {
	switch d.u8() {
	case 0:
		return false
	case 1:
		return true
	}
	d.err = ErrMalformed
	return false
}

func (d *decoder) count() int {
	n := d.u64()
	if n > uint64(len(d.b)) {
		d.err = ErrMalformed
		return 0
	}
	return int(n)
}

func (d *decoder) bytes() []byte {
	n := d.count()
	if n == 0 {
		return nil
	}
	return slices.Clone(d.take(n))
}

func (d *decoder) str() string {
	return string(d.take(d.count()))
}

func (d *decoder) strings() []string {
	n := d.count()
	if n == 0 {
		return nil
	}
	ss := make([]string, n)
	for i := range ss {
		ss[i] = d.str()
	}
	return ss
}

func (d *decoder) kind() EntryKind {
	k := EntryKind(d.u8())
	if k > EntryCheckEpoch {
		d.err = ErrMalformed
	}
	return k
}

func (d *decoder) entry() Entry {
	return Entry{Kind: d.kind(), ClientID: d.u64(), RequestNum: d.u64(), Op: d.bytes(), Config: d.strings()}
}

func (d *decoder) entries() []Entry {
	n := d.count()
	if n == 0 {
		return nil
	}
	es := make([]Entry, n)
	for i := range es {
		es[i] = d.entry()
	}
	return es
}

func (d *decoder) u64s() []uint64 {
	n := d.count()
	if n == 0 {
		return nil
	}
	vs := make([]uint64, n)
	for i := range vs {
		vs[i] = d.u64()
	}
	return vs
}

func (d *decoder) byteSlices() [][]byte {
	n := d.count()
	if n == 0 {
		return nil
	}
	vs := make([][]byte, n)
	for i := range vs {
		vs[i] = d.bytes()
	}
	return vs
}
