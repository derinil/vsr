package vsr

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync/atomic"
	"time"

	"github.com/derinil/vsr/internal/protocol"
)

const (
	// DefaultPort is the default listen port.
	DefaultPort = 10188

	defaultTick               = 10 * time.Millisecond
	defaultHeartbeat          = 100 * time.Millisecond
	defaultViewChangeTimeout  = 500 * time.Millisecond
	defaultResend             = 200 * time.Millisecond
	defaultCheckpointInterval = 1000
	defaultInboxSize          = 65536
	maxBurst                  = 1024
)

var errAlreadyRunning = errors.New("vsr: already running")

// ReplicaOptions configures a Replica. Zero values select defaults.
type ReplicaOptions struct {
	// Addr is this replica's address in Config.
	Addr string
	// Listen is the listen address.
	Listen string
	// HTTPAddr is the listen address for GET /status; empty disables it.
	HTTPAddr string
	// Config lists replica addresses in canonical order.
	Config []string
	// Service is the replicated state machine.
	Service Service
	// DataDir persists the epoch and configuration; empty disables it.
	DataDir string
	// Recovering starts with the recovery protocol.
	Recovering bool
	// Join starts as a new replica added by reconfiguration.
	Join bool
	// Tick is the logical clock period.
	Tick time.Duration
	// Heartbeat is the primary's heartbeat interval.
	Heartbeat time.Duration
	// ViewChangeTimeout is how long backups wait for the primary.
	ViewChangeTimeout time.Duration
	// Resend is the retransmission interval.
	Resend time.Duration
	// CheckpointInterval is the ops between checkpoints; 0 selects 1000 for Snapshotter services, negative disables.
	CheckpointInterval int
	// CheckpointRetain is the committed ops kept before log GC; 0 uses CheckpointInterval.
	CheckpointRetain int
	// PageSize is the Merkle page size for checkpoint transfer; 0 uses 4096.
	PageSize int
	// InboxSize bounds queued inbound messages; excess is dropped.
	InboxSize int
	// Logger receives state transition logs; defaults to slog.Default.
	Logger *slog.Logger

	newTransport func(h protocol.Handler) protocol.Transport
}

// State is a snapshot of a replica's protocol state.
type State struct {
	Status    Status
	Epoch     uint64
	View      uint64
	OpNum     uint64
	CommitNum uint64
	LogBase   uint64
	Primary   bool
}

// Replica hosts a single replica.
type Replica struct {
	opts         ReplicaOptions
	log          *slog.Logger
	started      time.Time
	inbox        chan protocol.Message
	transport    protocol.Transport
	peers        peerLister
	replica      *protocol.Replica
	running      atomic.Bool
	published    atomic.Pointer[replicaView]
	received     atomic.Uint64
	inboxDropped atomic.Uint64
	last         State
	savedEpoch   uint64
	checkpointed uint64
	checkpoints  chan protocol.Checkpoint
}

// New creates a Replica, restoring persisted state, and starts listening.
func NewReplica(o ReplicaOptions) (*Replica, error) {
	o = o.withDefaults()
	var st diskState
	found := false
	if o.DataDir != "" {
		var err error
		if st, found, err = loadState(o.DataDir); err != nil {
			return nil, err
		}
	}

	ro := protocol.Options{
		Addr:               o.Addr,
		Config:             o.Config,
		Service:            o.Service,
		Recovering:         o.Recovering,
		Join:               o.Join,
		Seed:               time.Now().UnixNano(),
		HeartbeatTicks:     ticks(o.Tick, o.Heartbeat),
		ViewChangeTicks:    ticks(o.Tick, o.ViewChangeTimeout),
		ResendTicks:        ticks(o.Tick, o.Resend),
		CheckpointInterval: o.CheckpointInterval,
		CheckpointRetain:   o.CheckpointRetain,
		PageSize:           o.PageSize,
	}
	if found {
		ro.Config, ro.Epoch, ro.OldConfig = st.Config, st.Epoch, st.OldConfig
		ro.Recovering = slices.Contains(st.Config, o.Addr)
		ro.Join = false
	}

	s := &Replica{
		opts:       o,
		log:        o.Logger.With("addr", o.Addr),
		started:    time.Now(),
		inbox:      make(chan protocol.Message, o.InboxSize),
		savedEpoch: st.Epoch,
	}
	if found && len(o.Config) > 0 && !slices.Equal(st.Config, o.Config) {
		s.log.Warn("config option differs from persisted config, using persisted config", "data", o.DataDir)
	}
	if o.DataDir != "" && ro.Recovering {
		c, err := loadCheckpoint(o.DataDir)
		switch {
		case err != nil:
			s.log.Warn("ignoring checkpoint", "data", o.DataDir, "err", err)
		case c != nil:
			ro.Checkpoint = c
			s.checkpointed = c.OpNum
		}
	}
	s.transport = o.newTransport(s.deliver)
	s.peers, _ = s.transport.(peerLister)
	ro.Outbox = s.transport
	r, err := protocol.NewReplica(ro)
	if err != nil {
		s.transport.Close()
		return nil, err
	}
	s.replica = r
	if err := s.transport.Listen(o.Listen); err != nil {
		s.transport.Close()
		return nil, err
	}
	if o.DataDir != "" && !found && !o.Join {
		if err := s.persist(); err != nil {
			s.transport.Close()
			return nil, err
		}
	}

	s.last = s.snapshot()
	s.published.Store(s.buildView(s.last))
	return s, nil
}

func (o ReplicaOptions) withDefaults() ReplicaOptions {
	if o.Listen == "" {
		o.Listen = fmt.Sprintf(":%d", DefaultPort)
	}
	if o.newTransport == nil {
		o.newTransport = newTCPTransport
	}
	o.Tick = orDefault(o.Tick, defaultTick)
	o.Heartbeat = orDefault(o.Heartbeat, defaultHeartbeat)
	o.ViewChangeTimeout = orDefault(o.ViewChangeTimeout, defaultViewChangeTimeout)
	o.Resend = orDefault(o.Resend, defaultResend)
	o.InboxSize = orDefault(o.InboxSize, defaultInboxSize)
	if _, ok := o.Service.(Snapshotter); ok && o.CheckpointInterval == 0 {
		o.CheckpointInterval = defaultCheckpointInterval
	}
	o.CheckpointInterval = max(o.CheckpointInterval, 0)
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return o
}

func orDefault[T time.Duration | int](v, d T) T {
	if v <= 0 {
		return d
	}
	return v
}

func ticks(tick, d time.Duration) int {
	return max(int((d+tick-1)/tick), 1)
}

// Run drives the replica until ctx is done or persistence fails.
func (s *Replica) Run(ctx context.Context) error {
	if !s.running.CompareAndSwap(false, true) {
		return errAlreadyRunning
	}
	s.log.Info("replica started", "listen", s.opts.Listen, "recovering", s.last.Status == protocol.StatusRecovering)
	if s.opts.HTTPAddr != "" {
		stop, err := s.serveStatus()
		if err != nil {
			s.transport.Close()
			return err
		}
		defer stop()
	}
	if s.opts.DataDir != "" {
		s.checkpoints = make(chan protocol.Checkpoint, 1)
		done := make(chan struct{})
		go s.writeCheckpoints(done)
		defer func() {
			close(s.checkpoints)
			<-done
		}()
	}

	ticker := time.NewTicker(s.opts.Tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return s.transport.Close()
		case m := <-s.inbox:
			s.step(m)
		case <-ticker.C:
			s.replica.Tick()
		}
		if err := s.observe(); err != nil {
			s.transport.Close()
			return err
		}
	}
}

// State returns the latest replica state. It is safe for concurrent use.
func (s *Replica) State() State {
	return s.published.Load().State
}

func (s *Replica) deliver(m protocol.Message) {
	select {
	case s.inbox <- m:
		s.received.Add(1)
	default:
		s.inboxDropped.Add(1)
	}
}

func (s *Replica) step(first protocol.Message) {
	s.replica.Step(first)
	for range maxBurst {
		select {
		case m := <-s.inbox:
			s.replica.Step(m)
		default:
			return
		}
	}
}

func (s *Replica) snapshot() State {
	r := s.replica
	return State{
		Status:    r.Status(),
		Epoch:     r.Epoch(),
		View:      r.View(),
		OpNum:     r.OpNum(),
		CommitNum: r.CommitNum(),
		LogBase:   r.LogBase(),
		Primary:   r.IsPrimary(),
	}
}

func (s *Replica) persist() error {
	st := diskState{Epoch: s.replica.Epoch(), Config: s.replica.Config(), OldConfig: s.replica.OldConfig()}
	if err := saveState(s.opts.DataDir, st); err != nil {
		return err
	}
	s.savedEpoch = st.Epoch
	return nil
}

func (s *Replica) observe() error {
	cur := s.snapshot()
	if s.opts.DataDir != "" && cur.Epoch != s.savedEpoch {
		if err := s.persist(); err != nil {
			return err
		}
	}
	if cur == s.last {
		return nil
	}
	s.logTransition(s.last, cur)
	s.last = cur
	s.handOffCheckpoint()
	s.published.Store(s.buildView(cur))
	return nil
}

func (s *Replica) handOffCheckpoint() {
	c := s.replica.Checkpoint()
	if c == nil || c.OpNum <= s.checkpointed {
		return
	}
	s.checkpointed = c.OpNum
	s.log.Info("checkpoint updated", "op", c.OpNum)
	if s.checkpoints != nil {
		s.queueCheckpoint(*c)
	}
}

func (s *Replica) queueCheckpoint(c protocol.Checkpoint) {
	select {
	case s.checkpoints <- c:
		return
	default:
	}
	select {
	case <-s.checkpoints:
	default:
	}
	s.checkpoints <- c
}

func (s *Replica) writeCheckpoints(done chan<- struct{}) {
	defer close(done)
	for c := range s.checkpoints {
		if err := saveCheckpoint(s.opts.DataDir, c); err != nil {
			s.log.Error("save checkpoint", "op", c.OpNum, "err", err)
		}
	}
}

func (s *Replica) logTransition(prev, cur State) {
	attrs := []any{"epoch", cur.Epoch, "view", cur.View, "status", cur.Status, "op", cur.OpNum, "commit", cur.CommitNum}
	if cur.Epoch != prev.Epoch {
		s.log.Info("epoch changed", attrs...)
	}
	if cur.LogBase != prev.LogBase {
		s.log.Info("log base advanced", "base", cur.LogBase, "op", cur.OpNum, "commit", cur.CommitNum)
	}
	switch {
	case cur.Status == protocol.StatusShutdown && prev.Status != protocol.StatusShutdown:
		s.log.Info("replica shut down; replaced by reconfiguration", attrs...)
	case prev.Status == protocol.StatusRecovering && cur.Status == protocol.StatusNormal:
		s.log.Info("recovered", attrs...)
	case cur.Status == protocol.StatusViewChange && prev.Status != protocol.StatusViewChange:
		s.log.Info("view change", attrs...)
	case cur.Status == protocol.StatusNormal && prev.Status != protocol.StatusRecovering && cur.View != prev.View:
		s.log.Info("view started", attrs...)
	case cur.Status != prev.Status:
		s.log.Info("status changed", attrs...)
	}
	if cur.Primary && !prev.Primary {
		s.log.Info("became primary", attrs...)
	}
}
