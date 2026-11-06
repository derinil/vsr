// Package vsr replicates a deterministic Service across a cluster using
// Viewstamped Replication Revisited. Run a Replica on each node and talk to
// the cluster with a Client.
package vsr

import "github.com/derinil/vsr/internal/protocol"

// Service is the deterministic state machine being replicated.
// Execute applies a committed op and returns its result.
type Service = protocol.Service

// Snapshotter is a Service that can capture and restore its state
// deterministically, enabling checkpoints and log truncation.
type Snapshotter = protocol.Snapshotter

// Status is a replica's protocol status.
type Status = protocol.Status

const (
	StatusNormal        = protocol.StatusNormal
	StatusViewChange    = protocol.StatusViewChange
	StatusRecovering    = protocol.StatusRecovering
	StatusTransitioning = protocol.StatusTransitioning
	StatusShutdown      = protocol.StatusShutdown
)
