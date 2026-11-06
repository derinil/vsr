package message

import (
	"errors"
	"fmt"

	"github.com/derinil/vsr/internal/wire"
)

// ErrMalformed is returned by Decode for invalid input.
var ErrMalformed = errors.New("vsr: malformed message")

// Append appends the binary encoding of m to dst.
func Append(dst []byte, m Message) []byte {
	e := wire.Encoder(dst)
	e.U8(byte(m.Type()))
	switch m := m.(type) {
	case Request:
		e.U64(m.Epoch)
		e.U64(m.ClientID)
		e.U64(m.RequestNum)
		e.Bytes(m.Op)
	case Reconfiguration:
		e.U64(m.Epoch)
		e.U64(m.ClientID)
		e.U64(m.RequestNum)
		e.Strings(m.Config)
	case CheckEpoch:
		e.U64(m.Epoch)
		e.U64(m.ClientID)
		e.U64(m.RequestNum)
	case Reply:
		e.U64(m.Epoch)
		e.U64(m.View)
		e.U64(m.ClientID)
		e.U64(m.RequestNum)
		e.Bytes(m.Result)
	case NewEpoch:
		e.U64(m.Epoch)
		e.U64(m.View)
		e.Strings(m.Config)
	case Prepare:
		e.U64(m.Epoch)
		e.U64(m.View)
		e.U64(m.OpNum)
		e.U64(m.CommitNum)
		putEntry(&e, m.Entry)
	case PrepareOK:
		e.U64(m.Epoch)
		e.U64(m.View)
		e.U64(m.OpNum)
		e.Int(m.Replica)
	case Commit:
		e.U64(m.Epoch)
		e.U64(m.View)
		e.U64(m.CommitNum)
	case StartViewChange:
		e.U64(m.Epoch)
		e.U64(m.View)
		e.Int(m.Replica)
	case DoViewChange:
		e.U64(m.Epoch)
		e.U64(m.View)
		e.U64(m.LastNormalView)
		e.U64(m.OpNum)
		e.U64(m.CommitNum)
		putEntries(&e, m.Log)
		e.Int(m.Replica)
		e.U64(m.LogBase)
	case StartView:
		e.U64(m.Epoch)
		e.U64(m.View)
		e.U64(m.OpNum)
		e.U64(m.CommitNum)
		putEntries(&e, m.Log)
		e.U64(m.LogBase)
	case Recovery:
		e.U64(m.Epoch)
		e.U64(m.Nonce)
		e.Int(m.Replica)
		e.U64(m.CheckpointNum)
	case RecoveryResponse:
		e.U64(m.Epoch)
		e.U64(m.View)
		e.U64(m.Nonce)
		e.U64(m.OpNum)
		e.U64(m.CommitNum)
		putEntries(&e, m.Log)
		e.Bool(m.HasLog)
		e.Strings(m.Config)
		e.Strings(m.OldConfig)
		e.Int(m.Replica)
		e.U64(m.LogBase)
		e.U64(m.EpochStart)
	case GetState:
		e.U64(m.Epoch)
		e.U64(m.View)
		e.U64(m.OpNum)
		e.Int(m.Replica)
	case NewState:
		e.U64(m.Epoch)
		e.U64(m.View)
		e.U64(m.OpNum)
		e.U64(m.CommitNum)
		putEntries(&e, m.Log)
		e.U64(m.LogBase)
	case StartEpoch:
		e.U64(m.Epoch)
		e.U64(m.OpNum)
		e.Strings(m.OldConfig)
		e.Strings(m.NewConfig)
		e.Str(m.From)
	case EpochStarted:
		e.U64(m.Epoch)
		e.Int(m.Replica)
	case GetCheckpoint:
		e.U64(m.Epoch)
		e.U64(m.MinOpNum)
		e.Str(m.From)
	case CheckpointInfo:
		e.U64(m.Epoch)
		e.U64(m.OpNum)
		e.U64(m.Size)
		e.Int(m.PageSize)
		e.Bytes(m.Root)
		e.Bytes(m.Clients)
	case GetMerkle:
		e.U64(m.OpNum)
		e.Int(m.Level)
		e.U64s(m.Indices)
		e.Str(m.From)
	case MerkleNodes:
		e.U64(m.OpNum)
		e.Int(m.Level)
		e.U64s(m.Indices)
		e.Bytes(m.Hashes)
	case GetPages:
		e.U64(m.OpNum)
		e.U64s(m.Indices)
		e.Str(m.From)
	case Pages:
		e.U64(m.OpNum)
		e.U64s(m.Indices)
		e.ByteSlices(m.Data)
	default:
		panic(fmt.Sprintf("vsr: cannot encode %T", m))
	}
	return e
}

// Decode decodes a message encoded by Append.
func Decode(b []byte) (Message, error) {
	d := wire.NewDecoder(b)
	var m Message
	switch Type(d.U8()) {
	case TypeRequest:
		m = Request{Epoch: d.U64(), ClientID: d.U64(), RequestNum: d.U64(), Op: d.Bytes()}
	case TypeReconfiguration:
		m = Reconfiguration{Epoch: d.U64(), ClientID: d.U64(), RequestNum: d.U64(), Config: d.Strings()}
	case TypeCheckEpoch:
		m = CheckEpoch{Epoch: d.U64(), ClientID: d.U64(), RequestNum: d.U64()}
	case TypeReply:
		m = Reply{Epoch: d.U64(), View: d.U64(), ClientID: d.U64(), RequestNum: d.U64(), Result: d.Bytes()}
	case TypeNewEpoch:
		m = NewEpoch{Epoch: d.U64(), View: d.U64(), Config: d.Strings()}
	case TypePrepare:
		m = Prepare{Epoch: d.U64(), View: d.U64(), OpNum: d.U64(), CommitNum: d.U64(), Entry: getEntry(&d)}
	case TypePrepareOK:
		m = PrepareOK{Epoch: d.U64(), View: d.U64(), OpNum: d.U64(), Replica: d.Int()}
	case TypeCommit:
		m = Commit{Epoch: d.U64(), View: d.U64(), CommitNum: d.U64()}
	case TypeStartViewChange:
		m = StartViewChange{Epoch: d.U64(), View: d.U64(), Replica: d.Int()}
	case TypeDoViewChange:
		m = DoViewChange{
			Epoch:          d.U64(),
			View:           d.U64(),
			LastNormalView: d.U64(),
			OpNum:          d.U64(),
			CommitNum:      d.U64(),
			Log:            getEntries(&d),
			Replica:        d.Int(),
			LogBase:        d.U64(),
		}
	case TypeStartView:
		m = StartView{Epoch: d.U64(), View: d.U64(), OpNum: d.U64(), CommitNum: d.U64(), Log: getEntries(&d), LogBase: d.U64()}
	case TypeRecovery:
		m = Recovery{Epoch: d.U64(), Nonce: d.U64(), Replica: d.Int(), CheckpointNum: d.U64()}
	case TypeRecoveryResponse:
		m = RecoveryResponse{
			Epoch:      d.U64(),
			View:       d.U64(),
			Nonce:      d.U64(),
			OpNum:      d.U64(),
			CommitNum:  d.U64(),
			Log:        getEntries(&d),
			HasLog:     d.Bool(),
			Config:     d.Strings(),
			OldConfig:  d.Strings(),
			Replica:    d.Int(),
			LogBase:    d.U64(),
			EpochStart: d.U64(),
		}
	case TypeGetState:
		m = GetState{Epoch: d.U64(), View: d.U64(), OpNum: d.U64(), Replica: d.Int()}
	case TypeNewState:
		m = NewState{Epoch: d.U64(), View: d.U64(), OpNum: d.U64(), CommitNum: d.U64(), Log: getEntries(&d), LogBase: d.U64()}
	case TypeStartEpoch:
		m = StartEpoch{Epoch: d.U64(), OpNum: d.U64(), OldConfig: d.Strings(), NewConfig: d.Strings(), From: d.Str()}
	case TypeEpochStarted:
		m = EpochStarted{Epoch: d.U64(), Replica: d.Int()}
	case TypeGetCheckpoint:
		m = GetCheckpoint{Epoch: d.U64(), MinOpNum: d.U64(), From: d.Str()}
	case TypeCheckpointInfo:
		m = CheckpointInfo{Epoch: d.U64(), OpNum: d.U64(), Size: d.U64(), PageSize: d.Int(), Root: d.Bytes(), Clients: d.Bytes()}
	case TypeGetMerkle:
		m = GetMerkle{OpNum: d.U64(), Level: d.Int(), Indices: d.U64s(), From: d.Str()}
	case TypeMerkleNodes:
		m = MerkleNodes{OpNum: d.U64(), Level: d.Int(), Indices: d.U64s(), Hashes: d.Bytes()}
	case TypeGetPages:
		m = GetPages{OpNum: d.U64(), Indices: d.U64s(), From: d.Str()}
	case TypePages:
		m = Pages{OpNum: d.U64(), Indices: d.U64s(), Data: d.ByteSlices()}
	default:
		return nil, ErrMalformed
	}
	if !d.Finish() {
		return nil, ErrMalformed
	}
	return m, nil
}

func putEntry(e *wire.Encoder, v Entry) {
	e.U8(byte(v.Kind))
	e.U64(v.ClientID)
	e.U64(v.RequestNum)
	e.Bytes(v.Op)
	e.Strings(v.Config)
}

func putEntries(e *wire.Encoder, v []Entry) {
	e.U64(uint64(len(v)))
	for _, x := range v {
		putEntry(e, x)
	}
}

func getEntry(d *wire.Decoder) Entry {
	k := EntryKind(d.U8())
	if k > EntryCheckEpoch {
		d.Fail()
	}
	return Entry{Kind: k, ClientID: d.U64(), RequestNum: d.U64(), Op: d.Bytes(), Config: d.Strings()}
}

func getEntries(d *wire.Decoder) []Entry {
	n := d.Count()
	if n == 0 {
		return nil
	}
	es := make([]Entry, n)
	for i := range es {
		es[i] = getEntry(d)
	}
	return es
}
