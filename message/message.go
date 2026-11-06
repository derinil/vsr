// Package message defines the protocol messages exchanged by replicas and
// clients, and their binary encoding.
package message

import "fmt"

// Type identifies a message on the wire.
type Type uint8

const (
	TypeRequest Type = iota + 1
	TypeReconfiguration
	TypeCheckEpoch
	TypeReply
	TypeNewEpoch
	TypePrepare
	TypePrepareOK
	TypeCommit
	TypeStartViewChange
	TypeDoViewChange
	TypeStartView
	TypeRecovery
	TypeRecoveryResponse
	TypeGetState
	TypeNewState
	TypeStartEpoch
	TypeEpochStarted
	// TypeGetCheckpoint requests a checkpoint at or after a given op.
	TypeGetCheckpoint
	// TypeCheckpointInfo describes the sender's latest checkpoint.
	TypeCheckpointInfo
	// TypeGetMerkle requests Merkle node hashes of a checkpoint.
	TypeGetMerkle
	// TypeMerkleNodes answers TypeGetMerkle.
	TypeMerkleNodes
	// TypeGetPages requests checkpoint pages.
	TypeGetPages
	// TypePages answers TypeGetPages.
	TypePages
)

var typeNames = [...]string{
	TypeRequest:          "Request",
	TypeReconfiguration:  "Reconfiguration",
	TypeCheckEpoch:       "CheckEpoch",
	TypeReply:            "Reply",
	TypeNewEpoch:         "NewEpoch",
	TypePrepare:          "Prepare",
	TypePrepareOK:        "PrepareOK",
	TypeCommit:           "Commit",
	TypeStartViewChange:  "StartViewChange",
	TypeDoViewChange:     "DoViewChange",
	TypeStartView:        "StartView",
	TypeRecovery:         "Recovery",
	TypeRecoveryResponse: "RecoveryResponse",
	TypeGetState:         "GetState",
	TypeNewState:         "NewState",
	TypeStartEpoch:       "StartEpoch",
	TypeEpochStarted:     "EpochStarted",
	TypeGetCheckpoint:    "GetCheckpoint",
	TypeCheckpointInfo:   "CheckpointInfo",
	TypeGetMerkle:        "GetMerkle",
	TypeMerkleNodes:      "MerkleNodes",
	TypeGetPages:         "GetPages",
	TypePages:            "Pages",
}

func (t Type) String() string {
	if int(t) < len(typeNames) && typeNames[t] != "" {
		return typeNames[t]
	}
	return fmt.Sprintf("Type(%d)", uint8(t))
}

// Message is any protocol message.
type Message interface {
	Type() Type
}

// Request asks the primary to execute Op for a client.
type Request struct {
	Epoch, ClientID, RequestNum uint64
	Op                          []byte
}

// Reconfiguration asks the primary to switch to Config in a new epoch.
type Reconfiguration struct {
	Epoch, ClientID, RequestNum uint64
	Config                      []string
}

// CheckEpoch asks the new epoch's primary to confirm the epoch is active.
type CheckEpoch struct {
	Epoch, ClientID, RequestNum uint64
}

// Reply returns a request's result to its client.
type Reply struct {
	Epoch, View, ClientID, RequestNum uint64
	Result                            []byte
}

// NewEpoch tells a client its request targeted a stale epoch.
type NewEpoch struct {
	Epoch, View uint64
	Config      []string
}

// Prepare replicates a log entry from primary to backups.
type Prepare struct {
	Epoch, View, OpNum, CommitNum uint64
	Entry                         Entry
}

// PrepareOK acknowledges a Prepare up to OpNum.
type PrepareOK struct {
	Epoch, View, OpNum uint64
	Replica            int
}

// Commit is the primary's heartbeat carrying its commit number.
type Commit struct {
	Epoch, View, CommitNum uint64
}

// StartViewChange announces a replica's move to View.
type StartViewChange struct {
	Epoch, View uint64
	Replica     int
}

// DoViewChange sends a replica's log to the new view's primary.
type DoViewChange struct {
	Epoch, View, LastNormalView, OpNum, CommitNum uint64
	Log                                           []Entry
	Replica                                       int
	LogBase                                       uint64
}

// StartView installs the new view's log on backups.
type StartView struct {
	Epoch, View, OpNum, CommitNum uint64
	Log                           []Entry
	LogBase                       uint64
}

// Recovery asks peers for state after a restart.
type Recovery struct {
	Epoch, Nonce  uint64
	Replica       int
	CheckpointNum uint64
}

// RecoveryResponse answers Recovery; only the primary sets HasLog.
type RecoveryResponse struct {
	Epoch, View, Nonce, OpNum, CommitNum uint64
	Log                                  []Entry
	HasLog                               bool
	Config, OldConfig                    []string
	Replica                              int
	LogBase, EpochStart                  uint64
}

// GetState asks a peer for log entries after OpNum.
type GetState struct {
	Epoch, View, OpNum uint64
	Replica            int
}

// NewState answers GetState with missing log entries.
type NewState struct {
	Epoch, View, OpNum, CommitNum uint64
	Log                           []Entry
	LogBase                       uint64
}

// StartEpoch tells a replica that the epoch has begun.
type StartEpoch struct {
	Epoch, OpNum         uint64
	OldConfig, NewConfig []string
	From                 string
}

// EpochStarted tells a replica that a new-config replica has started the epoch.
type EpochStarted struct {
	Epoch   uint64
	Replica int
}

// GetCheckpoint asks for a checkpoint at or after MinOpNum from a replica in Epoch.
type GetCheckpoint struct {
	Epoch, MinOpNum uint64
	From            string
}

// CheckpointInfo describes the sender's latest checkpoint and its epoch.
type CheckpointInfo struct {
	Epoch, OpNum, Size uint64
	PageSize           int
	Root               []byte
	Clients            []byte
}

// GetMerkle asks for the Merkle node hashes at Level and Indices in checkpoint OpNum.
type GetMerkle struct {
	OpNum   uint64
	Level   int
	Indices []uint64
	From    string
}

// MerkleNodes answers GetMerkle with a 32-byte hash per requested node.
type MerkleNodes struct {
	OpNum   uint64
	Level   int
	Indices []uint64
	Hashes  []byte
}

// GetPages asks for the pages at Indices in checkpoint OpNum.
type GetPages struct {
	OpNum   uint64
	Indices []uint64
	From    string
}

// Pages answers GetPages with page contents.
type Pages struct {
	OpNum   uint64
	Indices []uint64
	Data    [][]byte
}

func (Request) Type() Type          { return TypeRequest }
func (Reconfiguration) Type() Type  { return TypeReconfiguration }
func (CheckEpoch) Type() Type       { return TypeCheckEpoch }
func (Reply) Type() Type            { return TypeReply }
func (NewEpoch) Type() Type         { return TypeNewEpoch }
func (Prepare) Type() Type          { return TypePrepare }
func (PrepareOK) Type() Type        { return TypePrepareOK }
func (Commit) Type() Type           { return TypeCommit }
func (StartViewChange) Type() Type  { return TypeStartViewChange }
func (DoViewChange) Type() Type     { return TypeDoViewChange }
func (StartView) Type() Type        { return TypeStartView }
func (Recovery) Type() Type         { return TypeRecovery }
func (RecoveryResponse) Type() Type { return TypeRecoveryResponse }
func (GetState) Type() Type         { return TypeGetState }
func (NewState) Type() Type         { return TypeNewState }
func (StartEpoch) Type() Type       { return TypeStartEpoch }
func (EpochStarted) Type() Type     { return TypeEpochStarted }
func (GetCheckpoint) Type() Type    { return TypeGetCheckpoint }
func (CheckpointInfo) Type() Type   { return TypeCheckpointInfo }
func (GetMerkle) Type() Type        { return TypeGetMerkle }
func (MerkleNodes) Type() Type      { return TypeMerkleNodes }
func (GetPages) Type() Type         { return TypeGetPages }
func (Pages) Type() Type            { return TypePages }

// ClientOf returns the client ID of a client-originated message.
func ClientOf(m Message) (uint64, bool) {
	switch m := m.(type) {
	case Request:
		return m.ClientID, true
	case Reconfiguration:
		return m.ClientID, true
	case CheckEpoch:
		return m.ClientID, true
	}
	return 0, false
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
