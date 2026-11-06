package protocol

import (
	"slices"

	"github.com/derinil/vsr/message"
)

func (r *Replica) onRequest(m message.Request) {
	r.propose(m.ClientID, m.RequestNum, message.Entry{Kind: message.EntryOp, ClientID: m.ClientID, RequestNum: m.RequestNum, Op: m.Op})
}

func (r *Replica) prepareMsg(n uint64) message.Prepare {
	return message.Prepare{Epoch: r.epoch, View: r.view, OpNum: n, CommitNum: r.commitNum, Entry: r.entry(n)}
}

func (r *Replica) onPrepare(m message.Prepare) {
	if !r.acceptPrimary(m.View) {
		return
	}
	r.resetTimer()
	n := r.OpNum()
	switch {
	case m.OpNum <= n:
	case m.OpNum == n+1:
		r.appendEntry(m.Entry)
	default:
		r.requestState(m.OpNum)
		return
	}
	r.send(r.primaryOf(r.view), message.PrepareOK{Epoch: r.epoch, View: r.view, OpNum: m.OpNum, Replica: r.index})
	r.commitUpTo(m.CommitNum)
}

func (r *Replica) onPrepareOK(m message.PrepareOK) {
	if !r.IsPrimary() || m.View != r.view || !r.validReplica(m.Replica) {
		return
	}
	r.acked[m.Replica] = max(r.acked[m.Replica], m.OpNum)
	r.commitUpTo(r.majorityAck())
}

func (r *Replica) onCommit(m message.Commit) {
	if !r.acceptPrimary(m.View) {
		return
	}
	r.resetTimer()
	r.requestState(m.CommitNum)
	r.commitUpTo(m.CommitNum)
}

func (r *Replica) acceptPrimary(v uint64) bool {
	switch {
	case v < r.view:
		return false
	case v == r.view:
		return r.status == StatusNormal
	}
	r.startViewChange(v)
	return false
}

func (r *Replica) majorityAck() uint64 {
	if r.faults == 0 {
		return r.OpNum()
	}
	r.ackBuf = r.ackBuf[:0]
	for i, a := range r.acked {
		if i != r.index {
			r.ackBuf = append(r.ackBuf, a)
		}
	}
	slices.Sort(r.ackBuf)
	return r.ackBuf[len(r.ackBuf)-r.faults]
}

func (r *Replica) commitUpTo(k uint64) {
	if r.commitNum < r.base {
		r.commitTarget = max(r.commitTarget, k)
		return
	}
	k = min(k, r.OpNum())
	for r.commitNum < k {
		e := r.entry(r.commitNum + 1)
		var result []byte
		if e.Kind == message.EntryOp {
			result = r.service.Execute(e.Op)
		}
		r.commitNum++
		r.clientTable[e.ClientID] = clientRecord{req: e.RequestNum, result: result}
		if s, ok := r.inflight[e.ClientID]; ok && s == e.RequestNum {
			delete(r.inflight, e.ClientID)
		}
		if r.checkpointInterval > 0 && r.commitNum%r.checkpointInterval == 0 {
			r.takeCheckpoint()
		}
		if e.Kind == message.EntryReconfig && r.commitNum > r.epochStart {
			r.switchEpoch(e)
			return
		}
		if r.IsPrimary() {
			r.reply(e.ClientID, e.RequestNum, result)
		}
	}
}

func (r *Replica) heartbeat() {
	r.broadcast(message.Commit{Epoch: r.epoch, View: r.view, CommitNum: r.commitNum})
	r.resendPrepares()
	r.resetTimer()
}

func (r *Replica) resendPrepares() {
	last := min(r.OpNum(), r.commitNum+maxPrepareResend)
	for i := range r.config {
		if i == r.index {
			continue
		}
		for n := max(r.acked[i], r.commitNum, r.base) + 1; n <= last; n++ {
			r.send(i, r.prepareMsg(n))
		}
	}
}
