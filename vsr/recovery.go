package vsr

import "slices"

func (r *Replica) onRecovery(m Recovery) {
	if r.status != StatusNormal || !r.validReplica(m.Replica) {
		return
	}
	resp := RecoveryResponse{Epoch: r.epoch, View: r.view, Nonce: m.Nonce, Replica: r.index, EpochStart: r.epochStart}
	if r.IsPrimary() {
		lb := max(m.CheckpointNum, r.base)
		resp.HasLog = true
		resp.OpNum = r.OpNum()
		resp.CommitNum = r.commitNum
		resp.LogBase = lb
		resp.Log = slices.Clip(r.entriesAfter(lb))
	}
	r.send(m.Replica, resp)
}

func (r *Replica) onRecoveryResponse(m RecoveryResponse) {
	if m.Nonce != r.nonce || !r.validReplica(m.Replica) {
		return
	}
	r.responses[m.Replica] = m
	if len(r.responses) <= r.faults {
		return
	}
	var latest uint64
	for _, resp := range r.responses {
		latest = max(latest, resp.View)
	}
	p, ok := r.responses[r.primaryOf(latest)]
	if !ok || p.View != latest || !p.HasLog {
		return
	}
	r.epochStart = p.EpochStart
	r.adoptLog(p.LogBase, p.Log, r.config[p.Replica])
	r.enterNormal(latest)
	clear(r.responses)
	r.commitUpTo(p.CommitNum)
}
