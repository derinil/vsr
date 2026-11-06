package vsrreplica

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync/atomic"
	"time"

	"github.com/derinil/vsr/vsr"
	"github.com/derinil/vsr/vsrtcp"
)

const (
	DefaultPort = 10188

	defaultTick              = 10 * time.Millisecond
	defaultHeartbeat         = 100 * time.Millisecond
	defaultViewChangeTimeout = 500 * time.Millisecond
	defaultResend            = 200 * time.Millisecond
	defaultInboxSize         = 65536
	maxBurst                 = 1024
)

var errAlreadyRunning = errors.New("vsrreplica: already running")

type Options struct {
	Addr              string
	Listen            string
	Config            []string
	Service           vsr.Service
	NewTransport      func(h vsr.Handler) vsr.Transport
	DataDir           string
	Recovering        bool
	Join              bool
	Tick              time.Duration
	Heartbeat         time.Duration
	ViewChangeTimeout time.Duration
	Resend            time.Duration
	InboxSize         int
	Logger            *slog.Logger
}

type State struct {
	Status    vsr.Status
	Epoch     uint64
	View      uint64
	OpNum     uint64
	CommitNum uint64
	Primary   bool
}

type Server struct {
	opts       Options
	log        *slog.Logger
	inbox      chan vsr.Message
	transport  vsr.Transport
	replica    *vsr.Replica
	running    atomic.Bool
	state      atomic.Pointer[State]
	last       State
	savedEpoch uint64
}

func New(o Options) (*Server, error) {
	o = o.withDefaults()
	var st diskState
	found := false
	if o.DataDir != "" {
		var err error
		if st, found, err = loadState(o.DataDir); err != nil {
			return nil, err
		}
	}

	ro := vsr.Options{
		Addr:            o.Addr,
		Config:          o.Config,
		Service:         o.Service,
		Recovering:      o.Recovering,
		Join:            o.Join,
		Seed:            time.Now().UnixNano(),
		HeartbeatTicks:  ticks(o.Tick, o.Heartbeat),
		ViewChangeTicks: ticks(o.Tick, o.ViewChangeTimeout),
		ResendTicks:     ticks(o.Tick, o.Resend),
	}
	if found {
		ro.Config, ro.Epoch, ro.OldConfig = st.Config, st.Epoch, st.OldConfig
		ro.Recovering = slices.Contains(st.Config, o.Addr)
		ro.Join = false
	}

	s := &Server{
		opts:       o,
		log:        o.Logger.With("addr", o.Addr),
		inbox:      make(chan vsr.Message, o.InboxSize),
		savedEpoch: st.Epoch,
	}
	if found && len(o.Config) > 0 && !slices.Equal(st.Config, o.Config) {
		s.log.Warn("config option differs from persisted config, using persisted config", "data", o.DataDir)
	}
	s.transport = o.NewTransport(s.deliver)
	ro.Outbox = s.transport
	r, err := vsr.NewReplica(ro)
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

	snap := s.snapshot()
	s.last = snap
	s.state.Store(&snap)
	return s, nil
}

func (o Options) withDefaults() Options {
	if o.Listen == "" {
		o.Listen = fmt.Sprintf(":%d", DefaultPort)
	}
	if o.NewTransport == nil {
		o.NewTransport = func(h vsr.Handler) vsr.Transport {
			return vsrtcp.New(h, vsrtcp.Options{})
		}
	}
	o.Tick = orDefault(o.Tick, defaultTick)
	o.Heartbeat = orDefault(o.Heartbeat, defaultHeartbeat)
	o.ViewChangeTimeout = orDefault(o.ViewChangeTimeout, defaultViewChangeTimeout)
	o.Resend = orDefault(o.Resend, defaultResend)
	o.InboxSize = orDefault(o.InboxSize, defaultInboxSize)
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

func (s *Server) Run(ctx context.Context) error {
	if !s.running.CompareAndSwap(false, true) {
		return errAlreadyRunning
	}
	s.log.Info("replica started", "listen", s.opts.Listen, "recovering", s.last.Status == vsr.StatusRecovering)

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

func (s *Server) State() State {
	return *s.state.Load()
}

func (s *Server) deliver(m vsr.Message) {
	select {
	case s.inbox <- m:
	default:
	}
}

func (s *Server) step(first vsr.Message) {
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

func (s *Server) snapshot() State {
	r := s.replica
	return State{
		Status:    r.Status(),
		Epoch:     r.Epoch(),
		View:      r.View(),
		OpNum:     r.OpNum(),
		CommitNum: r.CommitNum(),
		Primary:   r.IsPrimary(),
	}
}

func (s *Server) persist() error {
	st := diskState{Epoch: s.replica.Epoch(), Config: s.replica.Config(), OldConfig: s.replica.OldConfig()}
	if err := saveState(s.opts.DataDir, st); err != nil {
		return err
	}
	s.savedEpoch = st.Epoch
	return nil
}

func (s *Server) observe() error {
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
	s.state.Store(&cur)
	return nil
}

func (s *Server) logTransition(prev, cur State) {
	attrs := []any{"epoch", cur.Epoch, "view", cur.View, "status", cur.Status, "op", cur.OpNum, "commit", cur.CommitNum}
	if cur.Epoch != prev.Epoch {
		s.log.Info("epoch changed", attrs...)
	}
	switch {
	case cur.Status == vsr.StatusShutdown && prev.Status != vsr.StatusShutdown:
		s.log.Info("replica shut down; replaced by reconfiguration", attrs...)
	case prev.Status == vsr.StatusRecovering && cur.Status == vsr.StatusNormal:
		s.log.Info("recovered", attrs...)
	case cur.Status == vsr.StatusViewChange && prev.Status != vsr.StatusViewChange:
		s.log.Info("view change", attrs...)
	case cur.Status == vsr.StatusNormal && prev.Status != vsr.StatusRecovering && cur.View != prev.View:
		s.log.Info("view started", attrs...)
	case cur.Status != prev.Status:
		s.log.Info("status changed", attrs...)
	}
	if cur.Primary && !prev.Primary {
		s.log.Info("became primary", attrs...)
	}
}
