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
	if m.OpNum < r.epochStart && validIndex(m.Replica, len(r.config)) && r.commitNum >= r.epochStart {
		r.send(m.Replica, NewState{
			Epoch:     r.epoch,
			View:      r.view,
			OpNum:     r.epochStart,
			CommitNum: r.epochStart,
			Log:       slices.Clip(r.log[m.OpNum:r.epochStart]),
		})
		return
	}
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
	if r.status == StatusTransitioning {
		if m.OpNum == r.epochStart {
			r.appendState(m)
			r.commitUpTo(m.CommitNum)
			r.tryFinishTransition()
		}
		return
	}
	if r.status != StatusNormal || m.View != r.view || r.IsPrimary() || !r.appendState(m) {
		return
	}
	r.stateReq = false
	r.send(r.primaryOf(r.view), PrepareOK{Epoch: r.epoch, View: r.view, OpNum: r.OpNum(), Replica: r.index})
	r.commitUpTo(m.CommitNum)
}

func (r *Replica) appendState(m NewState) bool {
	n := uint64(len(m.Log))
	if n == 0 || n > m.OpNum || m.OpNum <= r.OpNum() {
		return false
	}
	first := m.OpNum - n + 1
	if first > r.OpNum()+1 {
		return false
	}
	for _, e := range m.Log[r.OpNum()+1-first:] {
		r.appendEntry(e)
	}
	return true
}
