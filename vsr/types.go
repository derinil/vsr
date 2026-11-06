package vsr

import "fmt"

type Status uint8

const (
	StatusNormal Status = iota
	StatusViewChange
	StatusRecovering
	StatusTransitioning
	StatusShutdown
)

var statusNames = [...]string{"normal", "view-change", "recovering", "transitioning", "shutdown"}

func (s Status) String() string {
	if int(s) < len(statusNames) {
		return statusNames[s]
	}
	return fmt.Sprintf("Status(%d)", uint8(s))
}

type EntryKind uint8

const (
	EntryOp EntryKind = iota
	EntryReconfig
	EntryCheckEpoch
)

type Entry struct {
	Kind       EntryKind
	ClientID   uint64
	RequestNum uint64
	Op         []byte
	Config     []string
}

type Service interface {
	Execute(op []byte) []byte
}

type Outbox interface {
	Send(to string, m Message)
	Reply(client uint64, m Message)
}

type Handler func(m Message)

type Transport interface {
	Outbox
	Listen(addr string) error
	Close() error
}
