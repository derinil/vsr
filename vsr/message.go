package vsr

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
}

// StartView installs the new view's log on backups.
type StartView struct {
	Epoch, View, OpNum, CommitNum uint64
	Log                           []Entry
}

// Recovery asks peers for state after a restart.
type Recovery struct {
	Epoch, Nonce uint64
	Replica      int
}

// RecoveryResponse answers Recovery; only the primary sets HasLog.
type RecoveryResponse struct {
	Epoch, View, Nonce, OpNum, CommitNum uint64
	Log                                  []Entry
	HasLog                               bool
	Config, OldConfig                    []string
	Replica                              int
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
}

// StartEpoch tells new replicas to begin an epoch.
type StartEpoch struct {
	Epoch, OpNum         uint64
	OldConfig, NewConfig []string
	Replica              int
}

// EpochStarted tells old replicas a new replica has started the epoch.
type EpochStarted struct {
	Epoch   uint64
	Replica int
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
