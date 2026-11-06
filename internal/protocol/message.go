package protocol

import "fmt"

// MsgType identifies a protocol message on the wire.
type MsgType uint8

const (
	MsgRequest MsgType = iota + 1
	MsgReconfiguration
	MsgCheckEpoch
	MsgReply
	MsgNewEpoch
	MsgPrepare
	MsgPrepareOK
	MsgCommit
	MsgStartViewChange
	MsgDoViewChange
	MsgStartView
	MsgRecovery
	MsgRecoveryResponse
	MsgGetState
	MsgNewState
	MsgStartEpoch
	MsgEpochStarted
	// MsgGetCheckpoint requests a checkpoint at or after a given op.
	MsgGetCheckpoint
	// MsgCheckpointInfo describes the sender's latest checkpoint.
	MsgCheckpointInfo
	// MsgGetMerkle requests Merkle node hashes of a checkpoint.
	MsgGetMerkle
	// MsgMerkleNodes answers MsgGetMerkle.
	MsgMerkleNodes
	// MsgGetPages requests checkpoint pages.
	MsgGetPages
	// MsgPages answers MsgGetPages.
	MsgPages
)

var msgTypeNames = [...]string{
	MsgRequest:          "Request",
	MsgReconfiguration:  "Reconfiguration",
	MsgCheckEpoch:       "CheckEpoch",
	MsgReply:            "Reply",
	MsgNewEpoch:         "NewEpoch",
	MsgPrepare:          "Prepare",
	MsgPrepareOK:        "PrepareOK",
	MsgCommit:           "Commit",
	MsgStartViewChange:  "StartViewChange",
	MsgDoViewChange:     "DoViewChange",
	MsgStartView:        "StartView",
	MsgRecovery:         "Recovery",
	MsgRecoveryResponse: "RecoveryResponse",
	MsgGetState:         "GetState",
	MsgNewState:         "NewState",
	MsgStartEpoch:       "StartEpoch",
	MsgEpochStarted:     "EpochStarted",
	MsgGetCheckpoint:    "GetCheckpoint",
	MsgCheckpointInfo:   "CheckpointInfo",
	MsgGetMerkle:        "GetMerkle",
	MsgMerkleNodes:      "MerkleNodes",
	MsgGetPages:         "GetPages",
	MsgPages:            "Pages",
}

func (t MsgType) String() string {
	if int(t) < len(msgTypeNames) && msgTypeNames[t] != "" {
		return msgTypeNames[t]
	}
	return fmt.Sprintf("MsgType(%d)", uint8(t))
}

// Message is any protocol message.
type Message interface {
	Type() MsgType
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

func (Request) Type() MsgType          { return MsgRequest }
func (Reconfiguration) Type() MsgType  { return MsgReconfiguration }
func (CheckEpoch) Type() MsgType       { return MsgCheckEpoch }
func (Reply) Type() MsgType            { return MsgReply }
func (NewEpoch) Type() MsgType         { return MsgNewEpoch }
func (Prepare) Type() MsgType          { return MsgPrepare }
func (PrepareOK) Type() MsgType        { return MsgPrepareOK }
func (Commit) Type() MsgType           { return MsgCommit }
func (StartViewChange) Type() MsgType  { return MsgStartViewChange }
func (DoViewChange) Type() MsgType     { return MsgDoViewChange }
func (StartView) Type() MsgType        { return MsgStartView }
func (Recovery) Type() MsgType         { return MsgRecovery }
func (RecoveryResponse) Type() MsgType { return MsgRecoveryResponse }
func (GetState) Type() MsgType         { return MsgGetState }
func (NewState) Type() MsgType         { return MsgNewState }
func (StartEpoch) Type() MsgType       { return MsgStartEpoch }
func (EpochStarted) Type() MsgType     { return MsgEpochStarted }
func (GetCheckpoint) Type() MsgType    { return MsgGetCheckpoint }
func (CheckpointInfo) Type() MsgType   { return MsgCheckpointInfo }
func (GetMerkle) Type() MsgType        { return MsgGetMerkle }
func (MerkleNodes) Type() MsgType      { return MsgMerkleNodes }
func (GetPages) Type() MsgType         { return MsgGetPages }
func (Pages) Type() MsgType            { return MsgPages }

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
