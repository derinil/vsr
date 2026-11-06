package protocol

import (
	"slices"

	"github.com/derinil/vsr/message"
)

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
	r.send(r.primaryOf(r.view), message.GetState{Epoch: r.epoch, View: r.view, OpNum: r.OpNum(), Replica: r.index})
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

func (r *Replica) onGetState(m message.GetState) {
	if m.OpNum < r.epochStart && validIndex(m.Replica, len(r.config)) && r.commitNum >= max(r.epochStart, r.base) {
		lb := max(m.OpNum, r.base)
		hi := max(r.epochStart, lb)
		r.send(m.Replica, message.NewState{
			Epoch:     r.epoch,
			View:      r.view,
			OpNum:     hi,
			CommitNum: hi,
			Log:       slices.Clip(r.log[lb-r.base : hi-r.base]),
			LogBase:   lb,
		})
		return
	}
	if r.status != StatusNormal || m.View != r.view || !r.validReplica(m.Replica) || m.OpNum >= r.OpNum() {
		return
	}
	lb := max(m.OpNum, r.base)
	r.send(m.Replica, message.NewState{
		Epoch:     r.epoch,
		View:      r.view,
		OpNum:     r.OpNum(),
		CommitNum: r.commitNum,
		Log:       slices.Clip(r.entriesAfter(lb)),
		LogBase:   lb,
	})
}

func (r *Replica) onNewState(m message.NewState) {
	if r.status == StatusTransitioning {
		if m.OpNum >= r.epochStart && r.stateFrom != "" {
			r.appendState(m, r.stateFrom)
			r.commitUpTo(m.CommitNum)
			r.tryFinishTransition()
		}
		return
	}
	primary := r.config[r.primaryOf(r.view)]
	if r.status != StatusNormal || m.View != r.view || r.IsPrimary() || !r.appendState(m, primary) {
		return
	}
	r.stateReq = false
	r.send(r.primaryOf(r.view), message.PrepareOK{Epoch: r.epoch, View: r.view, OpNum: r.OpNum(), Replica: r.index})
	r.commitUpTo(m.CommitNum)
}

func (r *Replica) appendState(m message.NewState, from string) bool {
	if m.OpNum <= r.OpNum() || m.LogBase+uint64(len(m.Log)) != m.OpNum {
		return false
	}
	if m.LogBase > r.OpNum() {
		r.adoptLog(m.LogBase, m.Log, from)
		return true
	}
	for _, e := range m.Log[r.OpNum()-m.LogBase:] {
		r.appendEntry(e)
	}
	return true
}

func (r *Replica) adoptLog(base uint64, entries []message.Entry, from string) {
	switch {
	case base <= r.base:
		r.log = slices.Clone(entries[min(r.base-base, uint64(len(entries))):])
	case base <= r.commitNum:
		r.log = slices.Concat(r.log[:base-r.base], entries)
	default:
		r.base = base
		r.log = slices.Clone(entries)
		r.startFetch(base, from)
	}
	r.rebuildInflight()
}
