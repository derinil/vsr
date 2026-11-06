// Package vsrstatus defines the JSON status documents served by replicas and the monitor.
package vsrstatus

import "time"

// Peer is a replica's outbound connection to another replica.
type Peer struct {
	Addr      string    `json:"addr"`
	Connected bool      `json:"connected"`
	Since     time.Time `json:"since"`
	Queued    int       `json:"queued"`
	Sent      uint64    `json:"sent"`
	Dropped   uint64    `json:"dropped"`
	Dials     uint64    `json:"dials"`
}

// Node is a replica's status, served at GET /status.
type Node struct {
	Addr            string    `json:"addr"`
	Status          string    `json:"status"`
	Primary         bool      `json:"primary"`
	PrimaryAddr     string    `json:"primaryAddr"`
	Epoch           uint64    `json:"epoch"`
	View            uint64    `json:"view"`
	OpNum           uint64    `json:"opNum"`
	CommitNum       uint64    `json:"commitNum"`
	LogBase         uint64    `json:"logBase"`
	LogLen          uint64    `json:"logLen"`
	CheckpointOp    uint64    `json:"checkpointOp"`
	CheckpointBytes int       `json:"checkpointBytes"`
	Config          []string  `json:"config"`
	OldConfig       []string  `json:"oldConfig,omitempty"`
	Received        uint64    `json:"received"`
	InboxDropped    uint64    `json:"inboxDropped"`
	Started         time.Time `json:"started"`
	Now             time.Time `json:"now"`
	Peers           []Peer    `json:"peers"`
}

// Target is one polled replica as seen by the monitor.
type Target struct {
	URL       string  `json:"url"`
	Reachable bool    `json:"reachable"`
	Error     string  `json:"error,omitempty"`
	LatencyMs float64 `json:"latencyMs"`
	Node      *Node   `json:"node,omitempty"`
}

// Cluster is the monitor's aggregate view, served at GET /api/cluster and streamed at GET /api/stream.
type Cluster struct {
	Time    time.Time `json:"time"`
	Targets []Target  `json:"targets"`
	Spares  []string  `json:"spares,omitempty"`
}
