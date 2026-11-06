// Package protocol implements the Viewstamped Replication Revisited protocol as a
// deterministic, single-threaded state machine driven by Step and Tick.
package protocol

import (
	"fmt"

	"github.com/derinil/vsr/message"
)

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
	Send(to string, m message.Message)
	Reply(client uint64, m message.Message)
}
