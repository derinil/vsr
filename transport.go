package vsr

import (
	"github.com/derinil/vsr/internal/vsrtcp"
	"github.com/derinil/vsr/message"
)

// Transport carries messages between replicas and clients and owns their
// encoding. It must be safe for concurrent use, must not block, and may drop
// messages; the protocol retransmits.
type Transport interface {
	// Send delivers m to the replica at addr.
	Send(addr string, m message.Message)
	// Reply delivers m to a client. Transports learn where a client is from
	// inbound messages, using message.ClientOf.
	Reply(client uint64, m message.Message)
	// Listen accepts inbound messages on addr and passes them to the Handler.
	Listen(addr string) error
	// Close stops the transport.
	Close() error
}

// Handler receives one inbound message.
type Handler func(m message.Message)

func newTCPTransport(h Handler) Transport {
	return vsrtcp.New(h, vsrtcp.Options{})
}
