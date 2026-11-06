package vsr

import "slices"

func (r *Replica) requestState(target uint64) {
	if target <= r.OpNum() {
		return
	}
	r.stateTarget = max(r.stateTarget, target)
	if r.stateReq {
		return
	}
	r.stateReq = true
	r.sendGetState()
}

func (r *Replica) sendGetState() {
	r.send(r.primaryOf(r.view), GetState{Epoch: r.epoch, View: r.view, OpNum: r.OpNum(), Replica: r.index})
}

func (r *Replica) resendGetState() {
	if !r.stateReq {
		return
	}
	if r.OpNum() >= r.stateTarget {
		r.stateReq = false
		return
	}
	r.sendGetState()
}

func (r *Replica) onGetState(m GetState) {
	if r.status != StatusNormal || m.View != r.view || !r.validReplica(m.Replica) || m.OpNum >= r.OpNum() {
		return
	}
	r.send(m.Replica, NewState{
		Epoch:     r.epoch,
		View:      r.view,
		OpNum:     r.OpNum(),
		CommitNum: r.commitNum,
		Log:       slices.Clip(r.log[m.OpNum:]),
	})
}

func (r *Replica) onNewState(m NewState) {
	n := uint64(len(m.Log))
	if r.status != StatusNormal || m.View != r.view || r.IsPrimary() || n == 0 || n > m.OpNum || m.OpNum <= r.OpNum() {
		return
	}
	first := m.OpNum - n + 1
	if first > r.OpNum()+1 {
		return
	}
	for _, e := range m.Log[r.OpNum()+1-first:] {
		r.appendEntry(e)
	}
	r.stateReq = false
	r.send(r.primaryOf(r.view), PrepareOK{Epoch: r.epoch, View: r.view, OpNum: r.OpNum(), Replica: r.index})
	r.commitUpTo(m.CommitNum)
}
