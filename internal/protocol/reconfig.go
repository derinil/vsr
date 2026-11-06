package protocol

import (
	"slices"

	"github.com/derinil/vsr/message"
)

func (r *Replica) listening() bool {
	return !r.joining && (r.status == StatusNormal || r.status == StatusViewChange || r.status == StatusTransitioning)
}

func (r *Replica) active() bool {
	return r.status == StatusNormal || r.status == StatusViewChange
}

func (r *Replica) admit(m message.Message) bool {
	switch m := m.(type) {
	case message.Request:
		return r.admitClient(m.Epoch, m.ClientID)
	case message.Reconfiguration:
		return r.admitClient(m.Epoch, m.ClientID)
	case message.CheckEpoch:
		return r.admitClient(m.Epoch, m.ClientID)
	case message.StartEpoch:
		return r.admitStartEpoch(m)
	case message.RecoveryResponse:
		return r.status == StatusRecovering && m.Epoch == r.epoch
	case message.NewState:
		return m.Epoch == r.epoch && r.listening()
	case message.Prepare:
		return r.admitPrimary(m.Epoch, m.View, r.active())
	case message.Commit:
		return r.admitPrimary(m.Epoch, m.View, r.active())
	case message.StartView:
		return m.Epoch == r.epoch && r.active()
	case message.GetState:
		return r.admitPeer(m.Epoch, m.Replica, r.oldConfig, r.listening())
	case message.EpochStarted:
		return r.admitPeer(m.Epoch, m.Replica, r.oldConfig, r.listening())
	case message.PrepareOK:
		return r.admitPeer(m.Epoch, m.Replica, r.oldConfig, r.active())
	case message.StartViewChange:
		return r.admitPeer(m.Epoch, m.Replica, r.oldConfig, r.active())
	case message.DoViewChange:
		return r.admitPeer(m.Epoch, m.Replica, r.oldConfig, r.active())
	case message.Recovery:
		return r.admitPeer(m.Epoch, m.Replica, r.oldConfig, r.active())
	case message.GetCheckpoint, message.GetMerkle, message.GetPages:
		return r.status != StatusShutdown
	case message.CheckpointInfo:
		return r.fetch != nil && m.Epoch == r.epoch
	case message.MerkleNodes:
		return r.fetchingAt(m.OpNum)
	case message.Pages:
		return r.fetchingAt(m.OpNum)
	}
	return false
}

func (r *Replica) admitClient(epoch, client uint64) bool {
	if epoch < r.epoch && (r.status == StatusShutdown || r.listening()) {
		r.outbox.Reply(client, message.NewEpoch{Epoch: r.epoch, View: r.view, Config: r.config})
		return false
	}
	return epoch == r.epoch && r.status == StatusNormal
}

func (r *Replica) admitStartEpoch(m message.StartEpoch) bool {
	switch {
	case r.status == StatusShutdown:
		return false
	case m.Epoch > r.epoch:
		return true
	case m.Epoch == r.epoch:
		return r.status == StatusNormal && m.From != ""
	case m.Epoch+1 == r.epoch:
		r.notifyLower(m.From)
	}
	return false
}

func (r *Replica) admitPeer(epoch uint64, sender int, cfg []string, open bool) bool {
	switch {
	case epoch == r.epoch:
		return open
	case epoch+1 == r.epoch && validIndex(sender, len(cfg)):
		r.notifyLower(cfg[sender])
	}
	return false
}

func (r *Replica) admitPrimary(epoch, view uint64, open bool) bool {
	switch {
	case epoch == r.epoch:
		return open
	case epoch+1 == r.epoch && len(r.oldConfig) > 0:
		r.notifyLower(r.oldConfig[view%uint64(len(r.oldConfig))])
	}
	return false
}

func (r *Replica) notifyLower(to string) {
	if r.listening() && r.member() && to != "" && to != r.addr {
		r.outbox.Send(to, r.startEpoch())
	}
}

func (r *Replica) member() bool {
	return r.index >= 0 || slices.Contains(r.oldConfig, r.addr)
}

func (r *Replica) startEpoch() message.StartEpoch {
	return message.StartEpoch{Epoch: r.epoch, OpNum: r.epochStart, OldConfig: r.oldConfig, NewConfig: r.config, From: r.addr}
}

func (r *Replica) propose(client, req uint64, e message.Entry) {
	if !r.IsPrimary() || r.fetch != nil || r.pendingReconfig() {
		return
	}
	if rec, ok := r.clientTable[client]; ok && req <= rec.req {
		if req == rec.req {
			r.reply(client, req, rec.result)
		}
		return
	}
	if s, ok := r.inflight[client]; ok && req <= s {
		return
	}
	r.appendEntry(e)
	r.broadcast(r.prepareMsg(r.OpNum()))
	r.resetTimer()
	if r.faults == 0 {
		r.commitUpTo(r.OpNum())
	}
}

func (r *Replica) pendingReconfig() bool {
	n := r.OpNum()
	return n > r.epochStart && n > r.base && r.entry(n).Kind == message.EntryReconfig
}

func (r *Replica) onReconfiguration(m message.Reconfiguration) {
	if !validConfig(m.Config) {
		return
	}
	if slices.Equal(m.Config, r.config) {
		r.replayReconfig(m.ClientID, m.RequestNum)
		return
	}
	r.propose(m.ClientID, m.RequestNum, message.Entry{Kind: message.EntryReconfig, ClientID: m.ClientID, RequestNum: m.RequestNum, Config: slices.Clone(m.Config)})
}

func (r *Replica) replayReconfig(client, req uint64) {
	if rec, ok := r.clientTable[client]; ok && r.IsPrimary() && rec.req == req {
		r.reply(client, req, rec.result)
	}
}

func (r *Replica) onCheckEpoch(m message.CheckEpoch) {
	r.propose(m.ClientID, m.RequestNum, message.Entry{Kind: message.EntryCheckEpoch, ClientID: m.ClientID, RequestNum: m.RequestNum})
}

func validConfig(config []string) bool {
	s := slices.Clone(config)
	slices.Sort(s)
	return len(s) >= 3 && len(slices.Compact(s)) == len(s)
}

func validIndex(i, n int) bool {
	return i >= 0 && i < n
}

func (r *Replica) switchEpoch(e message.Entry) {
	n := r.commitNum
	old := r.config
	if r.IsPrimary() {
		r.reply(e.ClientID, e.RequestNum, nil)
		r.broadcast(message.Commit{Epoch: r.epoch, View: r.view, CommitNum: n})
		for _, addr := range e.Config {
			if !slices.Contains(old, addr) {
				r.outbox.Send(addr, message.StartEpoch{Epoch: r.epoch + 1, OpNum: n, OldConfig: old, NewConfig: e.Config, From: r.addr})
			}
		}
	}
	r.enterEpoch(r.epoch+1, n, old, e.Config)
}

func (r *Replica) setEpoch(epoch, n uint64, old, config []string) {
	r.epoch = epoch
	r.epochStart = n
	r.oldConfig = slices.Clone(old)
	r.config = slices.Clone(config)
	r.index = slices.Index(config, r.addr)
	r.faults = (len(config) - 1) / 2
	r.joining = false
	r.view = 0
	r.lastNormalView = 0
	r.acked = make([]uint64, len(config))
	clear(r.started)
	clear(r.svc)
	clear(r.dvcs)
	r.dvcSent = false
	clear(r.responses)
	r.stateReq = false
	r.stateTarget = 0
	if r.fetch != nil {
		r.restartFetch()
	}
}

func (r *Replica) enterEpoch(epoch, n uint64, old, config []string) {
	r.setEpoch(epoch, n, old, config)
	r.status = StatusTransitioning
	r.log = r.log[:max(min(n, r.commitNum), r.base)-r.base]
	r.rebuildInflight()
	r.tryFinishTransition()
}

func (r *Replica) tryFinishTransition() {
	switch {
	case r.index < 0 || r.fetch != nil:
		return
	case r.commitNum < r.epochStart:
		r.requestEpochState()
		return
	}
	r.enterNormal(0)
	m := message.EpochStarted{Epoch: r.epoch, Replica: r.index}
	for _, addr := range r.config {
		if addr != r.addr {
			r.outbox.Send(addr, m)
		}
	}
	for _, addr := range r.oldConfig {
		if !slices.Contains(r.config, addr) {
			r.outbox.Send(addr, m)
		}
	}
}

func (r *Replica) requestEpochState() {
	peers := slices.DeleteFunc(slices.Concat(r.config, r.oldConfig), func(addr string) bool { return addr == r.addr })
	if len(peers) == 0 {
		return
	}
	r.stateFrom = peers[r.catchUp%len(peers)]
	r.outbox.Send(r.stateFrom, message.GetState{Epoch: r.epoch, OpNum: r.commitNum, Replica: r.index})
	r.catchUp++
}

func (r *Replica) resendTransition() {
	switch {
	case r.joining:
	case r.index >= 0:
		if r.fetch == nil {
			r.requestEpochState()
		}
	default:
		r.announceStart()
	}
}

func (r *Replica) announceStart() {
	if r.epoch == 0 || !r.member() {
		return
	}
	m := r.startEpoch()
	for i, addr := range r.config {
		if i != r.index && !r.started[i] {
			r.outbox.Send(addr, m)
		}
	}
}

func (r *Replica) onStartEpoch(m message.StartEpoch) {
	if m.Epoch == r.epoch {
		r.outbox.Send(m.From, message.EpochStarted{Epoch: r.epoch, Replica: r.index})
		return
	}
	if r.status == StatusRecovering {
		r.rejoin(m)
		return
	}
	r.enterEpoch(m.Epoch, m.OpNum, m.OldConfig, m.NewConfig)
}

func (r *Replica) rejoin(m message.StartEpoch) {
	r.setEpoch(m.Epoch, m.OpNum, m.OldConfig, m.NewConfig)
	if r.index < 0 {
		r.status = StatusShutdown
		return
	}
	r.nonce = r.rng.Uint64()
}

func (r *Replica) onEpochStarted(m message.EpochStarted) {
	if !validIndex(m.Replica, len(r.config)) {
		return
	}
	switch {
	case r.index >= 0:
		r.started[m.Replica] = true
	case r.status == StatusTransitioning && r.index < 0:
		r.started[m.Replica] = true
		if len(r.started) > r.faults {
			r.status = StatusShutdown
			r.fetch = nil
		}
	}
}
