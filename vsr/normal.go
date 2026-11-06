package vsr

import "slices"

func (r *Replica) onRequest(m Request) {
	r.propose(m.ClientID, m.RequestNum, Entry{Kind: EntryOp, ClientID: m.ClientID, RequestNum: m.RequestNum, Op: m.Op})
}

func (r *Replica) prepareMsg(n uint64) Prepare {
	return Prepare{Epoch: r.epoch, View: r.view, OpNum: n, CommitNum: r.commitNum, Entry: r.log[n-1]}
}

func (r *Replica) onPrepare(m Prepare) {
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
	r.send(r.primaryOf(r.view), PrepareOK{Epoch: r.epoch, View: r.view, OpNum: m.OpNum, Replica: r.index})
	r.commitUpTo(m.CommitNum)
}

func (r *Replica) onPrepareOK(m PrepareOK) {
	if !r.IsPrimary() || m.View != r.view || !r.validReplica(m.Replica) {
		return
	}
	r.acked[m.Replica] = max(r.acked[m.Replica], m.OpNum)
	r.commitUpTo(r.majorityAck())
}

func (r *Replica) onCommit(m Commit) {
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
	k = min(k, r.OpNum())
	for r.commitNum < k {
		e := r.log[r.commitNum]
		var result []byte
		if e.Kind == EntryOp {
			result = r.service.Execute(e.Op)
		}
		r.commitNum++
		r.clientTable[e.ClientID] = clientRecord{req: e.RequestNum, result: result}
		if s, ok := r.inflight[e.ClientID]; ok && s == e.RequestNum {
			delete(r.inflight, e.ClientID)
		}
		if e.Kind == EntryReconfig && r.commitNum > r.epochStart {
			r.switchEpoch(e)
			return
		}
		if r.IsPrimary() {
			r.reply(e.ClientID, e.RequestNum, result)
		}
	}
}

func (r *Replica) heartbeat() {
	r.broadcast(Commit{Epoch: r.epoch, View: r.view, CommitNum: r.commitNum})
	r.resendPrepares()
	r.resetTimer()
}

func (r *Replica) resendPrepares() {
	last := min(r.OpNum(), r.commitNum+maxPrepareResend)
	for i := range r.config {
		if i == r.index {
			continue
		}
		for n := max(r.acked[i], r.commitNum) + 1; n <= last; n++ {
			r.send(i, r.prepareMsg(n))
		}
	}
}
