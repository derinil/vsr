package vsr

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
)

// ErrInvalidOptions is returned by NewReplica for invalid Options.
var ErrInvalidOptions = errors.New("vsr: invalid options")

const (
	defaultHeartbeatTicks  = 10
	defaultViewChangeTicks = 50
	defaultResendTicks     = 20
	maxPrepareResend       = 64
	seedSalt               = 0x9e3779b97f4a7c15
)

// Options configures a Replica. Zero tick counts select defaults.
type Options struct {
	// Addr is this replica's address in Config.
	Addr string
	// Config lists replica addresses in canonical order.
	Config []string
	// Service executes committed ops.
	Service Service
	// Outbox sends outbound messages.
	Outbox Outbox
	// Epoch, when non-zero, restarts the replica in a later epoch.
	Epoch uint64
	// OldConfig is the previous epoch's configuration.
	OldConfig []string
	// Recovering starts the replica with the recovery protocol.
	Recovering bool
	// Join starts a new replica that waits for a StartEpoch.
	Join bool
	// Seed seeds timeout jitter and recovery nonces.
	Seed int64
	// HeartbeatTicks is the primary's heartbeat interval in ticks.
	HeartbeatTicks int
	// ViewChangeTicks is the backup timeout in ticks, jittered up to 2x.
	ViewChangeTicks int
	// ResendTicks is the retransmission interval in ticks.
	ResendTicks int
}

type clientRecord struct {
	req    uint64
	result []byte
}

// Replica is a VR replica. It is not safe for concurrent use.
type Replica struct {
	addr      string
	config    []string
	oldConfig []string
	index     int
	faults    int
	joining   bool
	service   Service
	outbox    Outbox
	rng       *rand.Rand

	heartbeatTicks  int
	viewChangeTicks int
	resendTicks     int

	epoch          uint64
	epochStart     uint64
	catchUp        int
	started        map[int]bool
	view           uint64
	lastNormalView uint64
	status         Status
	log            []Entry
	commitNum      uint64
	clientTable    map[uint64]clientRecord
	inflight       map[uint64]uint64

	acked   []uint64
	ackBuf  []uint64
	svc     map[int]bool
	dvcs    map[int]DoViewChange
	dvcSent bool

	nonce     uint64
	responses map[int]RecoveryResponse

	stateReq    bool
	stateTarget uint64

	now      int
	deadline int
	resendAt int
}

// NewReplica validates o and returns a Replica.
func NewReplica(o Options) (*Replica, error) {
	index, err := validateOptions(o)
	if err != nil {
		return nil, err
	}
	n := len(o.Config)
	r := &Replica{
		addr:            o.Addr,
		config:          slices.Clone(o.Config),
		oldConfig:       slices.Clone(o.OldConfig),
		index:           index,
		faults:          (n - 1) / 2,
		joining:         o.Join,
		epoch:           o.Epoch,
		service:         o.Service,
		outbox:          o.Outbox,
		rng:             rand.New(rand.NewPCG(uint64(o.Seed), uint64(o.Seed)^seedSalt)),
		heartbeatTicks:  orDefault(o.HeartbeatTicks, defaultHeartbeatTicks),
		viewChangeTicks: orDefault(o.ViewChangeTicks, defaultViewChangeTicks),
		resendTicks:     orDefault(o.ResendTicks, defaultResendTicks),
		started:         make(map[int]bool),
		clientTable:     make(map[uint64]clientRecord),
		inflight:        make(map[uint64]uint64),
		acked:           make([]uint64, n),
		ackBuf:          make([]uint64, 0, n),
		svc:             make(map[int]bool),
		dvcs:            make(map[int]DoViewChange),
		responses:       make(map[int]RecoveryResponse),
	}
	switch {
	case o.Join:
		r.status = StatusTransitioning
	case index < 0:
		r.status = StatusShutdown
	case o.Recovering:
		r.status = StatusRecovering
		r.nonce = r.rng.Uint64()
	default:
		r.status = StatusNormal
		r.resetTimer()
	}
	return r, nil
}

func validateOptions(o Options) (int, error) {
	switch {
	case o.Service == nil:
		return 0, fmt.Errorf("%w: nil service", ErrInvalidOptions)
	case o.Outbox == nil:
		return 0, fmt.Errorf("%w: nil outbox", ErrInvalidOptions)
	case o.HeartbeatTicks < 0 || o.ViewChangeTicks < 0 || o.ResendTicks < 0:
		return 0, fmt.Errorf("%w: negative tick count", ErrInvalidOptions)
	case o.Join && o.Recovering:
		return 0, fmt.Errorf("%w: join and recovering are exclusive", ErrInvalidOptions)
	}
	seen := make(map[string]bool, len(o.Config))
	for _, addr := range o.Config {
		if seen[addr] {
			return 0, fmt.Errorf("%w: duplicate address %q", ErrInvalidOptions, addr)
		}
		seen[addr] = true
	}
	if o.Join {
		return -1, nil
	}
	if len(o.Config) == 0 {
		return 0, fmt.Errorf("%w: empty config", ErrInvalidOptions)
	}
	index := slices.Index(o.Config, o.Addr)
	switch {
	case index < 0 && o.Epoch > 0:
		return -1, nil
	case index < 0:
		return 0, fmt.Errorf("%w: address %q not in config", ErrInvalidOptions, o.Addr)
	case o.Epoch > 0 && !o.Recovering:
		return 0, fmt.Errorf("%w: epoch %d requires recovering or join", ErrInvalidOptions, o.Epoch)
	}
	return index, nil
}

func orDefault(v, d int) int {
	if v == 0 {
		return d
	}
	return v
}

// Status returns the replica's status.
func (r *Replica) Status() Status { return r.status }

// View returns the current view number.
func (r *Replica) View() uint64 { return r.view }

// OpNum returns the number of log entries.
func (r *Replica) OpNum() uint64 { return uint64(len(r.log)) }

// CommitNum returns the number of committed entries.
func (r *Replica) CommitNum() uint64 { return r.commitNum }

// Epoch returns the current epoch.
func (r *Replica) Epoch() uint64 { return r.epoch }

// Config returns a copy of the current configuration.
func (r *Replica) Config() []string { return slices.Clone(r.config) }

// OldConfig returns a copy of the previous configuration.
func (r *Replica) OldConfig() []string { return slices.Clone(r.oldConfig) }

// Index returns this replica's position in Config, or -1.
func (r *Replica) Index() int { return r.index }

// IsPrimary reports whether the replica is the active primary.
func (r *Replica) IsPrimary() bool { return r.status == StatusNormal && r.isLeader() }

// Log returns the replica's log; callers must not modify it.
func (r *Replica) Log() []Entry           { return slices.Clip(r.log) }
func (r *Replica) isLeader() bool         { return r.primaryOf(r.view) == r.index }
func (r *Replica) primaryOf(v uint64) int { return int(v % uint64(len(r.config))) }

func (r *Replica) validReplica(i int) bool {
	return i >= 0 && i < len(r.config) && i != r.index
}

// Step processes one inbound message.
func (r *Replica) Step(m Message) {
	if !r.admit(m) {
		return
	}
	switch m := m.(type) {
	case Request:
		r.onRequest(m)
	case Reconfiguration:
		r.onReconfiguration(m)
	case CheckEpoch:
		r.onCheckEpoch(m)
	case RecoveryResponse:
		r.onRecoveryResponse(m)
	case Prepare:
		r.onPrepare(m)
	case PrepareOK:
		r.onPrepareOK(m)
	case Commit:
		r.onCommit(m)
	case StartViewChange:
		r.onStartViewChange(m)
	case DoViewChange:
		r.onDoViewChange(m)
	case StartView:
		r.onStartView(m)
	case Recovery:
		r.onRecovery(m)
	case GetState:
		r.onGetState(m)
	case NewState:
		r.onNewState(m)
	case StartEpoch:
		r.onStartEpoch(m)
	case EpochStarted:
		r.onEpochStarted(m)
	}
}

// Tick advances the logical clock, driving heartbeats, resends and timeouts.
func (r *Replica) Tick() {
	r.now++
	if r.now >= r.resendAt {
		r.resendAt = r.now + r.resendTicks
		r.resend()
	}
	if r.now < r.deadline {
		return
	}
	switch {
	case r.status == StatusNormal && r.isLeader():
		r.heartbeat()
	case r.status == StatusNormal, r.status == StatusViewChange:
		r.startViewChange(r.view + 1)
	}
}

func (r *Replica) resend() {
	switch r.status {
	case StatusRecovering:
		r.broadcast(Recovery{Epoch: r.epoch, Nonce: r.nonce, Replica: r.index})
	case StatusViewChange:
		r.broadcast(StartViewChange{Epoch: r.epoch, View: r.view, Replica: r.index})
		if r.dvcSent {
			r.send(r.primaryOf(r.view), r.doViewChange())
		}
	case StatusNormal:
		r.resendGetState()
		r.announceStart()
	case StatusTransitioning:
		r.resendTransition()
	}
}

func (r *Replica) resetTimer() {
	if r.status == StatusNormal && r.isLeader() {
		r.deadline = r.now + r.heartbeatTicks
		return
	}
	r.deadline = r.now + r.viewChangeTicks + r.rng.IntN(r.viewChangeTicks)
}

func (r *Replica) send(to int, m Message) {
	r.outbox.Send(r.config[to], m)
}

func (r *Replica) broadcast(m Message) {
	for i := range r.config {
		if i != r.index {
			r.send(i, m)
		}
	}
}

func (r *Replica) reply(client, req uint64, result []byte) {
	r.outbox.Reply(client, Reply{Epoch: r.epoch, View: r.view, ClientID: client, RequestNum: req, Result: result})
}

func (r *Replica) enterNormal(v uint64) {
	r.view = v
	r.status = StatusNormal
	r.lastNormalView = v
	clear(r.svc)
	clear(r.dvcs)
	r.dvcSent = false
	clear(r.acked)
	r.stateReq = false
	r.stateTarget = 0
	r.rebuildInflight()
	r.resetTimer()
}

func (r *Replica) rebuildInflight() {
	clear(r.inflight)
	for _, e := range r.log[r.commitNum:] {
		r.inflight[e.ClientID] = e.RequestNum
	}
}

func (r *Replica) appendEntry(e Entry) {
	r.log = append(r.log, e)
	r.inflight[e.ClientID] = e.RequestNum
}
