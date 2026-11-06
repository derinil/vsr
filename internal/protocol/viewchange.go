package protocol

import (
	"slices"

	"github.com/derinil/vsr/message"
)

func (r *Replica) startViewChange(v uint64) {
	r.view = v
	r.status = StatusViewChange
	clear(r.svc)
	clear(r.dvcs)
	r.dvcSent = false
	r.stateReq = false
	r.stateTarget = 0
	r.broadcast(message.StartViewChange{Epoch: r.epoch, View: v, Replica: r.index})
	if r.isLeader() {
		r.dvcs[r.index] = r.doViewChange()
	}
	r.resetTimer()
	r.tryBecomePrimary()
}

func (r *Replica) joinViewChange(v uint64) bool {
	switch {
	case v > r.view:
		r.startViewChange(v)
		return true
	case v == r.view:
		return r.status == StatusViewChange
	}
	return false
}

func (r *Replica) onStartViewChange(m message.StartViewChange) {
	if !r.validReplica(m.Replica) {
		return
	}
	if m.View == r.view && r.status == StatusNormal {
		if r.IsPrimary() {
			r.send(m.Replica, r.startViewMsg(r.commitNum))
		}
		return
	}
	if !r.joinViewChange(m.View) {
		return
	}
	r.svc[m.Replica] = true
	if !r.isLeader() {
		r.trySendDVC()
	}
}

func (r *Replica) trySendDVC() {
	if r.dvcSent || len(r.svc) < r.faults {
		return
	}
	r.dvcSent = true
	r.send(r.primaryOf(r.view), r.doViewChange())
}

func (r *Replica) onDoViewChange(m message.DoViewChange) {
	if !r.validReplica(m.Replica) {
		return
	}
	if m.View == r.view && r.status == StatusNormal {
		if r.IsPrimary() {
			r.send(m.Replica, r.startViewMsg(r.commitNum))
		}
		return
	}
	if !r.joinViewChange(m.View) || !r.isLeader() {
		return
	}
	r.dvcs[m.Replica] = m
	r.tryBecomePrimary()
}

func (r *Replica) tryBecomePrimary() {
	if r.status != StatusViewChange || !r.isLeader() || len(r.dvcs) <= r.faults {
		return
	}
	r.becomePrimary()
}

func (r *Replica) becomePrimary() {
	var best *message.DoViewChange
	var commit uint64
	for i := range r.config {
		d, ok := r.dvcs[i]
		if !ok {
			continue
		}
		commit = max(commit, d.CommitNum)
		if best == nil || moreRecent(d, *best) {
			best = &d
		}
	}
	r.adoptLog(best.LogBase, best.Log, r.config[best.Replica])
	r.enterNormal(r.view)
	r.broadcast(r.startViewMsg(commit))
	r.commitUpTo(commit)
}

func moreRecent(a, b message.DoViewChange) bool {
	if a.LastNormalView != b.LastNormalView {
		return a.LastNormalView > b.LastNormalView
	}
	return a.OpNum > b.OpNum
}

func (r *Replica) onStartView(m message.StartView) {
	if m.View < r.view || (m.View == r.view && r.status != StatusViewChange) {
		return
	}
	r.adoptLog(m.LogBase, m.Log, r.config[r.primaryOf(m.View)])
	r.enterNormal(m.View)
	if r.OpNum() > m.CommitNum {
		r.send(r.primaryOf(m.View), message.PrepareOK{Epoch: r.epoch, View: m.View, OpNum: r.OpNum(), Replica: r.index})
	}
	r.commitUpTo(m.CommitNum)
}

func (r *Replica) doViewChange() message.DoViewChange {
	return message.DoViewChange{
		Epoch:          r.epoch,
		View:           r.view,
		LastNormalView: r.lastNormalView,
		OpNum:          r.OpNum(),
		CommitNum:      r.commitNum,
		Log:            slices.Clip(r.log),
		Replica:        r.index,
		LogBase:        r.base,
	}
}

func (r *Replica) startViewMsg(commit uint64) message.StartView {
	return message.StartView{
		Epoch:     r.epoch,
		View:      r.view,
		OpNum:     r.OpNum(),
		CommitNum: commit,
		Log:       slices.Clip(r.log),
		LogBase:   r.base,
	}
}
