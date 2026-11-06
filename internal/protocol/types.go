// Package protocol implements the Viewstamped Replication Revisited protocol as a
// deterministic, single-threaded state machine driven by Step and Tick.
package protocol

import "fmt"

// Status is a replica's protocol status.
type Status uint8

const (
	// StatusNormal processes requests in the current view.
	StatusNormal Status = iota
	// StatusViewChange is electing a new primary.
	StatusViewChange
	// StatusRecovering is rebuilding state after a restart.
	StatusRecovering
	// StatusTransitioning is moving into a new epoch.
	StatusTransitioning
	// StatusShutdown is no longer part of the configuration.
	StatusShutdown
)

var statusNames = [...]string{"normal", "view-change", "recovering", "transitioning", "shutdown"}

func (s Status) String() string {
	if int(s) < len(statusNames) {
		return statusNames[s]
	}
	return fmt.Sprintf("Status(%d)", uint8(s))
}

// EntryKind identifies what a log entry carries.
type EntryKind uint8

const (
	// EntryOp is a client operation for the Service.
	EntryOp EntryKind = iota
	// EntryReconfig switches the cluster to a new configuration.
	EntryReconfig
	// EntryCheckEpoch confirms a client has reached the current epoch.
	EntryCheckEpoch
)

// Entry is a single replicated log entry.
type Entry struct {
	Kind       EntryKind
	ClientID   uint64
	RequestNum uint64
	Op         []byte
	Config     []string
}

// Service is the deterministic state machine being replicated.
// Execute applies a committed op and returns its result.
type Service interface {
	Execute(op []byte) []byte
}

// Snapshotter is a Service that can capture and restore its state deterministically.
type Snapshotter interface {
	Snapshot() []byte
	Restore(state []byte) error
}

// Outbox sends messages to replicas by address and replies to clients by ID.
type Outbox interface {
	Send(to string, m Message)
	Reply(client uint64, m Message)
}

// Handler receives inbound messages from a Transport.
type Handler func(m Message)

// Transport is a network Outbox that can listen for inbound messages.
type Transport interface {
	Outbox
	Listen(addr string) error
	Close() error
}
