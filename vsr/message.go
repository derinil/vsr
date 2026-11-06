package vsr

import "fmt"

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

type Message interface {
	Type() MsgType
}

type Request struct {
	Epoch, ClientID, RequestNum uint64
	Op                          []byte
}

type Reconfiguration struct {
	Epoch, ClientID, RequestNum uint64
	Config                      []string
}

type CheckEpoch struct {
	Epoch, ClientID, RequestNum uint64
}

type Reply struct {
	Epoch, View, ClientID, RequestNum uint64
	Result                            []byte
}

type NewEpoch struct {
	Epoch, View uint64
	Config      []string
}

type Prepare struct {
	Epoch, View, OpNum, CommitNum uint64
	Entry                         Entry
}

type PrepareOK struct {
	Epoch, View, OpNum uint64
	Replica            int
}

type Commit struct {
	Epoch, View, CommitNum uint64
}

type StartViewChange struct {
	Epoch, View uint64
	Replica     int
}

type DoViewChange struct {
	Epoch, View, LastNormalView, OpNum, CommitNum uint64
	Log                                           []Entry
	Replica                                       int
}

type StartView struct {
	Epoch, View, OpNum, CommitNum uint64
	Log                           []Entry
}

type Recovery struct {
	Epoch, Nonce uint64
	Replica      int
}

type RecoveryResponse struct {
	Epoch, View, Nonce, OpNum, CommitNum uint64
	Log                                  []Entry
	HasLog                               bool
	Config, OldConfig                    []string
	Replica                              int
}

type GetState struct {
	Epoch, View, OpNum uint64
	Replica            int
}

type NewState struct {
	Epoch, View, OpNum, CommitNum uint64
	Log                           []Entry
}

type StartEpoch struct {
	Epoch, OpNum         uint64
	OldConfig, NewConfig []string
	Replica              int
}

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
