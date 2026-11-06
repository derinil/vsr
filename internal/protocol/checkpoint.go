package protocol

import (
	"maps"
	"slices"

	"github.com/derinil/vsr/internal/wire"
	"github.com/derinil/vsr/message"
)

// Checkpoint is a snapshot of replicated state as of op OpNum.
type Checkpoint struct {
	OpNum   uint64
	State   []byte
	Clients []byte
}

// AppendCheckpoint appends the binary encoding of c to dst.
func AppendCheckpoint(dst []byte, c Checkpoint) []byte {
	e := wire.Encoder(dst)
	e.U64(c.OpNum)
	e.Bytes(c.State)
	e.Bytes(c.Clients)
	return e
}

// DecodeCheckpoint decodes a checkpoint encoded by AppendCheckpoint.
func DecodeCheckpoint(b []byte) (Checkpoint, error) {
	d := wire.NewDecoder(b)
	c := Checkpoint{OpNum: d.U64(), State: d.Bytes(), Clients: d.Bytes()}
	if !d.Finish() {
		return Checkpoint{}, message.ErrMalformed
	}
	return c, nil
}

type checkpoint struct {
	Checkpoint
	tree *merkle
}

func newCheckpoint(c Checkpoint, pageSize int) checkpoint {
	return checkpoint{Checkpoint: c, tree: buildMerkle(c.State, pageSize)}
}

func (r *Replica) restoreCheckpoint(c Checkpoint) error {
	clients, err := decodeClients(c.Clients)
	if err != nil {
		return err
	}
	if err := r.snap.Restore(c.State); err != nil {
		return err
	}
	r.clientTable = clients
	r.base = c.OpNum
	r.commitNum = c.OpNum
	r.ckpts = []checkpoint{newCheckpoint(c, r.pageSize)}
	return nil
}

func (r *Replica) takeCheckpoint() {
	c := Checkpoint{OpNum: r.commitNum, State: r.snap.Snapshot(), Clients: encodeClients(r.clientTable)}
	r.ckpts = append(r.ckpts, newCheckpoint(c, r.pageSize))
	r.collectGarbage()
}

func (r *Replica) collectGarbage() {
	base := r.base
	for _, cp := range r.ckpts {
		if cp.OpNum > r.base && cp.OpNum+r.checkpointRetain <= r.commitNum {
			base = cp.OpNum
		}
	}
	if base != r.base {
		r.log = slices.Clone(r.entriesAfter(base))
		r.base = base
	}
	latest := r.ckpts[len(r.ckpts)-1].OpNum
	r.ckpts = slices.DeleteFunc(r.ckpts, func(cp checkpoint) bool {
		return cp.OpNum != r.base && cp.OpNum != latest
	})
}

func encodeClients(t map[uint64]clientRecord) []byte {
	var e wire.Encoder
	ids := slices.Sorted(maps.Keys(t))
	e.U64(uint64(len(ids)))
	for _, id := range ids {
		r := t[id]
		e.U64(id)
		e.U64(r.req)
		e.Bytes(r.result)
	}
	return e
}

func decodeClients(b []byte) (map[uint64]clientRecord, error) {
	d := wire.NewDecoder(b)
	n := d.Count()
	t := make(map[uint64]clientRecord, n)
	var last uint64
	for i := range n {
		id := d.U64()
		if i > 0 && id <= last {
			return nil, message.ErrMalformed
		}
		last = id
		t[id] = clientRecord{req: d.U64(), result: d.Bytes()}
	}
	if !d.Finish() {
		return nil, message.ErrMalformed
	}
	return t, nil
}

func (r *Replica) held(opNum uint64) (checkpoint, bool) {
	for _, cp := range r.ckpts {
		if cp.OpNum == opNum {
			return cp, true
		}
	}
	return checkpoint{}, false
}

func (r *Replica) install(cp checkpoint, clients map[uint64]clientRecord) error {
	if err := r.snap.Restore(cp.State); err != nil {
		return err
	}
	r.log = slices.Clone(r.entriesAfter(cp.OpNum))
	r.base = cp.OpNum
	r.commitNum = cp.OpNum
	r.clientTable = clients
	r.ckpts = []checkpoint{cp}
	r.fetch = nil
	r.rebuildInflight()
	r.commitUpTo(r.commitTarget)
	if r.status == StatusTransitioning {
		r.tryFinishTransition()
	}
	return nil
}
