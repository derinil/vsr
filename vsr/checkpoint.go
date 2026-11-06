package vsr

import (
	"maps"
	"slices"
)

// Checkpoint is a snapshot of replicated state as of op OpNum.
type Checkpoint struct {
	OpNum   uint64
	State   []byte
	Clients []byte
}

// AppendCheckpoint appends the binary encoding of c to dst.
func AppendCheckpoint(dst []byte, c Checkpoint) []byte {
	e := encoder(dst)
	e.u64(c.OpNum)
	e.bytes(c.State)
	e.bytes(c.Clients)
	return e
}

// DecodeCheckpoint decodes a checkpoint encoded by AppendCheckpoint.
func DecodeCheckpoint(b []byte) (Checkpoint, error) {
	d := decoder{b: b}
	c := Checkpoint{OpNum: d.u64(), State: d.bytes(), Clients: d.bytes()}
	if d.err != nil || len(d.b) > 0 {
		return Checkpoint{}, ErrMalformed
	}
	return c, nil
}

type checkpoint struct {
	Checkpoint
	tree *merkle
}

func encodeClients(t map[uint64]clientRecord) []byte {
	var e encoder
	ids := slices.Sorted(maps.Keys(t))
	e.u64(uint64(len(ids)))
	for _, id := range ids {
		r := t[id]
		e.u64(id)
		e.u64(r.req)
		e.bytes(r.result)
	}
	return e
}

func decodeClients(b []byte) (map[uint64]clientRecord, error) {
	d := decoder{b: b}
	n := d.count()
	t := make(map[uint64]clientRecord, n)
	var last uint64
	for i := range n {
		id := d.u64()
		if i > 0 && id <= last {
			return nil, ErrMalformed
		}
		last = id
		t[id] = clientRecord{req: d.u64(), result: d.bytes()}
	}
	if d.err != nil || len(d.b) > 0 {
		return nil, ErrMalformed
	}
	return t, nil
}
