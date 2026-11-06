package vsr

import (
	"github.com/derinil/vsr/internal/protocol"
	"github.com/derinil/vsr/internal/vsrtcp"
)

// Transport carries encoded messages between replicas and clients. It must be
// safe for concurrent use and may drop messages; the protocol retransmits.
// Transports must not modify msg.
type Transport interface {
	// Send delivers msg to the replica at addr.
	Send(addr string, msg []byte)
	// Reply delivers msg to a client. Transports learn where a client is by
	// calling ClientOf on inbound messages before passing them to the Handler.
	Reply(client uint64, msg []byte)
	// Listen accepts inbound messages on addr and passes them to the Handler.
	Listen(addr string) error
	// Close stops the transport.
	Close() error
}

// Handler receives one inbound message and returns an error when it is
// malformed. msg may be reused once Handler returns.
type Handler func(msg []byte) error

// ClientOf returns the sending client's ID if msg came from a client.
func ClientOf(msg []byte) (client uint64, ok bool) {
	return protocol.PeekClient(msg)
}

func newTCPTransport(h Handler) Transport {
	return vsrtcp.New(h, vsrtcp.Options{})
}

func encode(m protocol.Message) []byte {
	return protocol.AppendMessage(make([]byte, 0, 64), m)
}

func handler(deliver func(protocol.Message)) Handler {
	return func(msg []byte) error {
		m, err := protocol.DecodeMessage(msg)
		if err != nil {
			return err
		}
		deliver(m)
		return nil
	}
}

type outbox struct{ t Transport }

func (o outbox) Send(to string, m protocol.Message)      { o.t.Send(to, encode(m)) }
func (o outbox) Reply(client uint64, m protocol.Message) { o.t.Reply(client, encode(m)) }
