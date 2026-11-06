package vsr

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"slices"
	"time"

	"github.com/derinil/vsr/internal/vsrstatus"
)

const (
	statusTimeout         = 5 * time.Second
	statusShutdownTimeout = time.Second
)

type peerLister interface {
	Peers() []vsrstatus.Peer
}

type replicaView struct {
	State
	config          []string
	oldConfig       []string
	primaryAddr     string
	checkpointOp    uint64
	checkpointBytes int
}

func (s *Replica) buildView(st State) *replicaView {
	r := s.replica
	v := &replicaView{State: st, config: r.Config(), oldConfig: r.OldConfig()}
	if n := len(v.config); n > 0 {
		v.primaryAddr = v.config[st.View%uint64(n)]
	}
	if c := r.Checkpoint(); c != nil {
		v.checkpointOp = c.OpNum
		v.checkpointBytes = len(c.State) + len(c.Clients)
	}
	return v
}

// status returns the replica's status document. It is safe for concurrent use.
func (s *Replica) status() vsrstatus.Node {
	v := s.published.Load()
	peers := []vsrstatus.Peer{}
	if s.peers != nil {
		peers = s.peers.Peers()
	}
	return vsrstatus.Node{
		Addr:            s.opts.Addr,
		Status:          v.Status.String(),
		Primary:         v.Primary,
		PrimaryAddr:     v.primaryAddr,
		Epoch:           v.Epoch,
		View:            v.View,
		OpNum:           v.OpNum,
		CommitNum:       v.CommitNum,
		LogBase:         v.LogBase,
		LogLen:          v.OpNum - v.LogBase,
		CheckpointOp:    v.checkpointOp,
		CheckpointBytes: v.checkpointBytes,
		Config:          slices.Clone(v.config),
		OldConfig:       slices.Clone(v.oldConfig),
		Received:        s.received.Load(),
		InboxDropped:    s.inboxDropped.Load(),
		Started:         s.started,
		Now:             time.Now(),
		Peers:           peers,
	}
}

func (s *Replica) serveStatus() (func(), error) {
	ln, err := net.Listen("tcp", s.opts.HTTPAddr)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", s.handleStatus)
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: statusTimeout, WriteTimeout: statusTimeout}
	served := make(chan struct{})
	go func() {
		defer close(served)
		if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("status server", "err", err)
		}
	}()
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), statusShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			s.log.Warn("status server shutdown", "err", err)
		}
		<-served
	}, nil
}

func (s *Replica) handleStatus(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.status())
}
