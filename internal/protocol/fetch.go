package protocol

import (
	"math"
	"slices"
)

const (
	maxFetchIdle        = 3
	maxMerkleIndices    = 4096
	maxPageRequestBytes = 1 << 20
	maxFetchPageSize    = 1 << 30
)

type fetchTarget struct {
	opNum       uint64
	size        int
	pageSize    int
	pages       int
	top         int
	root        digest
	clients     []byte
	clientTable map[uint64]clientRecord
}

type fetch struct {
	minOp    uint64
	source   string
	tries    int
	idle     int
	target   *fetchTarget
	ref      *checkpoint
	cache    map[digest][]byte
	level    int
	parents  []int
	hashes   []digest
	leaves   []digest
	awaiting map[int]bool
	children map[int]digest
}

func (r *Replica) startFetch(minOp uint64, from string) {
	if r.fetch != nil && r.fetch.minOp >= minOp {
		return
	}
	f := &fetch{minOp: minOp, source: from, cache: make(map[digest][]byte)}
	if r.fetch != nil {
		f.cache = r.fetch.cache
	}
	r.fetch = f
	r.outbox.Send(from, GetCheckpoint{Epoch: r.epoch, MinOpNum: minOp, From: r.addr})
}

func (r *Replica) fetchingAt(opNum uint64) bool {
	return r.fetch != nil && r.fetch.target != nil && r.fetch.target.opNum == opNum
}

func (r *Replica) onGetCheckpoint(m GetCheckpoint) {
	if m.Epoch != r.epoch || len(r.ckpts) == 0 {
		return
	}
	if cp := r.ckpts[len(r.ckpts)-1]; cp.OpNum >= m.MinOpNum {
		r.outbox.Send(m.From, r.checkpointInfo(cp))
	}
}

func (r *Replica) checkpointInfo(cp checkpoint) CheckpointInfo {
	root := cp.tree.root()
	return CheckpointInfo{
		Epoch:    r.epoch,
		OpNum:    cp.OpNum,
		Size:     uint64(cp.tree.size),
		PageSize: cp.tree.pageSize,
		Root:     root[:],
		Clients:  cp.Clients,
	}
}

func (r *Replica) replyLatest(to string) {
	if len(r.ckpts) > 0 {
		r.outbox.Send(to, r.checkpointInfo(r.ckpts[len(r.ckpts)-1]))
	}
}

func (r *Replica) onCheckpointInfo(m CheckpointInfo) {
	f := r.fetch
	if f == nil || m.Epoch != r.epoch || m.OpNum < f.minOp || (f.target != nil && m.OpNum <= f.target.opNum) {
		return
	}
	if m.PageSize <= 0 || m.PageSize > maxFetchPageSize || m.Size > math.MaxInt || len(m.Root) != len(digest{}) {
		return
	}
	clients, err := decodeClients(m.Clients)
	if err != nil {
		return
	}
	t := &fetchTarget{
		opNum:       m.OpNum,
		size:        int(m.Size),
		pageSize:    m.PageSize,
		clients:     slices.Clone(m.Clients),
		clientTable: clients,
	}
	t.pages = pageCount(t.size, t.pageSize)
	t.top = topLevel(t.pages)
	copy(t.root[:], m.Root)
	f.target = t
	f.idle = 0
	r.startWalk()
}

func (r *Replica) latestAt(pageSize int) *checkpoint {
	if n := len(r.ckpts); n > 0 && r.ckpts[n-1].tree.pageSize == pageSize {
		cp := r.ckpts[n-1]
		return &cp
	}
	return nil
}

func (r *Replica) startWalk() {
	f := r.fetch
	t := f.target
	f.ref = r.latestAt(t.pageSize)
	f.leaves = make([]digest, t.pages)
	f.children = make(map[int]digest)
	f.awaiting = nil
	if f.ref != nil && f.ref.tree.root() == t.root {
		c := Checkpoint{OpNum: t.opNum, State: f.ref.State, Clients: t.clients}
		r.finishFetch(checkpoint{Checkpoint: c, tree: f.ref.tree}, t.clientTable)
		return
	}
	if f.ref != nil {
		for i, h := range f.ref.tree.levels[0] {
			f.cache[h] = pageOf(f.ref.State, t.pageSize, i)
		}
	}
	f.level = t.top
	f.parents = []int{0}
	f.hashes = []digest{t.root}
	if f.level == 0 {
		f.leaves[0] = t.root
		r.requestPages()
		return
	}
	r.requestChildren()
}

func (r *Replica) requestChildren() {
	f := r.fetch
	childLevel := f.level - 1
	n := levelSize(f.target.pages, childLevel)
	f.awaiting = make(map[int]bool)
	var want []uint64
	for _, p := range f.parents {
		for c := 2 * p; c <= 2*p+1 && c < n; c++ {
			if _, ok := f.children[c]; !ok {
				f.awaiting[c] = true
				want = append(want, uint64(c))
			}
		}
	}
	for len(want) > 0 {
		k := min(len(want), maxMerkleIndices)
		r.outbox.Send(f.source, GetMerkle{OpNum: f.target.opNum, Level: childLevel, Indices: slices.Clip(want[:k]), From: r.addr})
		want = want[k:]
	}
}

func (r *Replica) onGetMerkle(m GetMerkle) {
	cp, ok := r.held(m.OpNum)
	if !ok {
		r.replyLatest(m.From)
		return
	}
	var indices []uint64
	var hashes []byte
	for _, i := range m.Indices {
		if h, ok := cp.tree.node(m.Level, int(i)); ok {
			indices = append(indices, i)
			hashes = append(hashes, h[:]...)
		}
	}
	r.outbox.Send(m.From, MerkleNodes{OpNum: cp.OpNum, Level: m.Level, Indices: indices, Hashes: hashes})
}

func (r *Replica) onMerkleNodes(m MerkleNodes) {
	f := r.fetch
	if !r.fetchingAt(m.OpNum) || f.level == 0 || m.Level != f.level-1 || len(m.Hashes) != len(digest{})*len(m.Indices) {
		return
	}
	progressed := false
	for k, idx := range m.Indices {
		if i := int(idx); f.awaiting[i] {
			f.children[i] = [32]byte(m.Hashes[k*32 : k*32+32])
			delete(f.awaiting, i)
			progressed = true
		}
	}
	if progressed {
		f.idle = 0
	}
	if progressed && len(f.awaiting) == 0 {
		r.advanceWalk()
	}
}

func (r *Replica) advanceWalk() {
	f := r.fetch
	childLevel := f.level - 1
	n := levelSize(f.target.pages, childLevel)
	for j, p := range f.parents {
		want := f.children[2*p]
		if 2*p+1 < n {
			want = nodeHash(f.children[2*p], f.children[2*p+1])
		}
		if want != f.hashes[j] {
			r.restartFetch()
			return
		}
	}
	var parents []int
	var hashes []digest
	for _, p := range f.parents {
		for c := 2 * p; c <= 2*p+1 && c < n; c++ {
			h := f.children[c]
			if lo, hi, same := f.sameSpan(childLevel, c, h); same {
				copy(f.leaves[lo:hi], f.ref.tree.levels[0][lo:hi])
				continue
			}
			if childLevel == 0 {
				f.leaves[c] = h
			}
			parents = append(parents, c)
			hashes = append(hashes, h)
		}
	}
	f.parents, f.hashes = parents, hashes
	f.level = childLevel
	f.children = make(map[int]digest)
	switch {
	case len(parents) == 0:
		r.assemble()
	case childLevel == 0:
		r.requestPages()
	default:
		r.requestChildren()
	}
}

func (f *fetch) sameSpan(level, i int, h digest) (lo, hi int, same bool) {
	if f.ref == nil {
		return 0, 0, false
	}
	local, ok := f.ref.tree.node(level, i)
	if !ok || local != h {
		return 0, 0, false
	}
	lo, hi = coversIn(f.target.pages, level, i)
	llo, lhi := f.ref.tree.covers(level, i)
	return lo, hi, lo == llo && hi == lhi
}

func (r *Replica) requestPages() {
	f := r.fetch
	f.awaiting = make(map[int]bool)
	requested := make(map[digest]bool)
	var want []uint64
	for j, idx := range f.parents {
		h := f.hashes[j]
		if _, ok := f.cache[h]; ok || requested[h] {
			continue
		}
		requested[h] = true
		f.awaiting[idx] = true
		want = append(want, uint64(idx))
	}
	if len(want) == 0 {
		r.assemble()
		return
	}
	per := max(1, maxPageRequestBytes/f.target.pageSize)
	for len(want) > 0 {
		k := min(len(want), per)
		r.outbox.Send(f.source, GetPages{OpNum: f.target.opNum, Indices: slices.Clip(want[:k]), From: r.addr})
		want = want[k:]
	}
}

func (r *Replica) onGetPages(m GetPages) {
	cp, ok := r.held(m.OpNum)
	if !ok {
		r.replyLatest(m.From)
		return
	}
	var indices []uint64
	var data [][]byte
	for _, i := range m.Indices {
		if i < uint64(cp.tree.pages()) {
			indices = append(indices, i)
			data = append(data, pageOf(cp.State, cp.tree.pageSize, int(i)))
		}
	}
	r.outbox.Send(m.From, Pages{OpNum: cp.OpNum, Indices: indices, Data: data})
}

func (r *Replica) onPages(m Pages) {
	f := r.fetch
	if !r.fetchingAt(m.OpNum) || f.level != 0 || len(m.Data) != len(m.Indices) {
		return
	}
	progressed := false
	for k, idx := range m.Indices {
		i := int(idx)
		if !f.awaiting[i] {
			continue
		}
		h := leafHash(m.Data[k])
		if h != f.leaves[i] {
			continue
		}
		f.cache[h] = slices.Clone(m.Data[k])
		delete(f.awaiting, i)
		progressed = true
	}
	if progressed {
		f.idle = 0
	}
	if progressed && len(f.awaiting) == 0 {
		r.requestPages()
	}
}

func (r *Replica) assemble() {
	f := r.fetch
	t := f.target
	data := make([]byte, 0, t.size)
	for _, h := range f.leaves {
		data = append(data, f.cache[h]...)
	}
	tree := buildMerkle(data, t.pageSize)
	if len(data) != t.size || tree.root() != t.root {
		r.restartFetch()
		return
	}
	c := Checkpoint{OpNum: t.opNum, State: data, Clients: t.clients}
	r.finishFetch(checkpoint{Checkpoint: c, tree: tree}, t.clientTable)
}

func (r *Replica) finishFetch(cp checkpoint, clients map[uint64]clientRecord) {
	if err := r.install(cp, clients); err != nil {
		r.restartFetch()
	}
}

func (r *Replica) restartFetch() {
	f := r.fetch
	f.tries++
	if peers := r.fetchPeers(); len(peers) > 0 {
		f.source = peers[f.tries%len(peers)]
	}
	f.target, f.ref, f.idle = nil, nil, 0
	f.parents, f.hashes, f.leaves, f.awaiting, f.children = nil, nil, nil, nil, nil
	r.outbox.Send(f.source, GetCheckpoint{Epoch: r.epoch, MinOpNum: f.minOp, From: r.addr})
}

func (r *Replica) fetchPeers() []string {
	peers := slices.Compact(slices.Sorted(slices.Values(slices.Concat(r.config, r.oldConfig))))
	return slices.DeleteFunc(peers, func(addr string) bool { return addr == r.addr })
}

func (r *Replica) resendFetch() {
	f := r.fetch
	f.idle++
	switch {
	case f.idle > maxFetchIdle:
		r.restartFetch()
	case f.target == nil:
		r.outbox.Send(f.source, GetCheckpoint{Epoch: r.epoch, MinOpNum: f.minOp, From: r.addr})
	case f.level == 0:
		r.requestPages()
	default:
		r.requestChildren()
	}
}
