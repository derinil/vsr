package vsr

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/derinil/vsr/internal/protocol"
)

const defaultRequestTimeout = 500 * time.Millisecond

// ErrClosed is returned by calls on a closed Client.
var ErrClosed = errors.New("vsr: closed")

// ClientOptions configures a Client.
type ClientOptions struct {
	// Config lists replica addresses in canonical order.
	Config []string
	// ClientID identifies the client; zero picks a random ID.
	ClientID uint64
	// RequestTimeout is how long to wait before retrying with all replicas.
	RequestTimeout time.Duration

	newTransport func(h protocol.Handler) protocol.Transport
}

// Client sends requests to a cluster one at a time, following view and
// epoch changes. It is safe for concurrent use.
type Client struct {
	id        uint64
	timeout   time.Duration
	transport protocol.Transport

	sem       chan struct{}
	reply     chan protocol.Reply
	wake      chan struct{}
	done      chan struct{}
	closeOnce sync.Once

	mu         sync.Mutex
	config     []string
	epoch      uint64
	view       uint64
	requestNum uint64
	waiting    bool
}

type buildFunc func(epoch, requestNum uint64) protocol.Message

// NewClient returns a Client for o.Config.
func NewClient(o ClientOptions) (*Client, error) {
	if len(o.Config) == 0 {
		return nil, errors.New("vsr: empty config")
	}
	if err := checkConfig(o.Config); err != nil {
		return nil, err
	}
	if o.newTransport == nil {
		o.newTransport = newTCPTransport
	}
	id := o.ClientID
	if id == 0 {
		var err error
		if id, err = randomID(); err != nil {
			return nil, err
		}
	}
	timeout := o.RequestTimeout
	if timeout <= 0 {
		timeout = defaultRequestTimeout
	}
	c := &Client{
		id:      id,
		timeout: timeout,
		config:  slices.Clone(o.Config),
		sem:     make(chan struct{}, 1),
		reply:   make(chan protocol.Reply, 1),
		wake:    make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
	c.transport = o.newTransport(c.handle)
	return c, nil
}

// Invoke executes op on the cluster and returns its result.
func (c *Client) Invoke(ctx context.Context, op []byte) ([]byte, error) {
	if err := c.acquire(ctx); err != nil {
		return nil, err
	}
	defer c.release()
	r, err := c.roundTrip(ctx, func(epoch, n uint64) protocol.Message {
		return protocol.Request{Epoch: epoch, ClientID: c.id, RequestNum: n, Op: op}
	})
	if err != nil {
		return nil, err
	}
	return r.Result, nil
}

// Reconfigure moves the cluster to config, which needs at least 3 replicas,
// and waits until the new epoch is active.
func (c *Client) Reconfigure(ctx context.Context, config []string) error {
	if len(config) < 3 {
		return fmt.Errorf("vsr: reconfiguration needs at least 3 replicas, got %d", len(config))
	}
	if err := checkConfig(config); err != nil {
		return err
	}
	if err := c.acquire(ctx); err != nil {
		return err
	}
	defer c.release()

	cfg := slices.Clone(config)
	start := c.Epoch()
	if _, err := c.roundTrip(ctx, func(epoch, n uint64) protocol.Message {
		return protocol.Reconfiguration{Epoch: epoch, ClientID: c.id, RequestNum: n, Config: cfg}
	}); err != nil {
		return err
	}

	c.mu.Lock()
	if c.epoch == start {
		c.config, c.epoch, c.view = cfg, start+1, 0
	}
	c.mu.Unlock()

	_, err := c.roundTrip(ctx, func(epoch, n uint64) protocol.Message {
		return protocol.CheckEpoch{Epoch: epoch, ClientID: c.id, RequestNum: n}
	})
	return err
}

// ID returns the client ID.
func (c *Client) ID() uint64 {
	return c.id
}

// View returns the last known view.
func (c *Client) View() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.view
}

// Epoch returns the last known epoch.
func (c *Client) Epoch() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.epoch
}

// Config returns the last known configuration.
func (c *Client) Config() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.config)
}

// Close closes the client and its transport.
func (c *Client) Close() error {
	var err error
	c.closeOnce.Do(func() {
		close(c.done)
		err = c.transport.Close()
	})
	return err
}

func (c *Client) handle(m protocol.Message) {
	switch m := m.(type) {
	case protocol.Reply:
		if m.ClientID != c.id {
			return
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if m.Epoch == c.epoch && m.View > c.view {
			c.view = m.View
		}
		if c.waiting && m.RequestNum == c.requestNum {
			select {
			case c.reply <- m:
			default:
			}
		}
	case protocol.NewEpoch:
		c.mu.Lock()
		defer c.mu.Unlock()
		if m.Epoch <= c.epoch || len(m.Config) == 0 {
			return
		}
		c.epoch, c.view, c.config = m.Epoch, m.View, slices.Clone(m.Config)
		if c.waiting {
			select {
			case c.wake <- struct{}{}:
			default:
			}
		}
	}
}

func (c *Client) roundTrip(ctx context.Context, build buildFunc) (protocol.Reply, error) {
	n := c.begin()
	defer c.end()

	timer := time.NewTimer(c.timeout)
	defer timer.Stop()
	c.transmit(build, n, false)
	for {
		select {
		case r := <-c.reply:
			return r, nil
		case <-c.wake:
			c.transmit(build, n, false)
		case <-timer.C:
			c.transmit(build, n, true)
			timer.Reset(c.timeout)
		case <-ctx.Done():
			return protocol.Reply{}, ctx.Err()
		case <-c.done:
			return protocol.Reply{}, ErrClosed
		}
	}
}

func (c *Client) transmit(build buildFunc, n uint64, all bool) {
	c.mu.Lock()
	m := build(c.epoch, n)
	config := c.config
	primary := config[c.view%uint64(len(config))]
	c.mu.Unlock()

	if !all {
		c.transport.Send(primary, m)
		return
	}
	for _, addr := range config {
		c.transport.Send(addr, m)
	}
}

func (c *Client) begin() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requestNum++
	c.waiting = true
	return c.requestNum
}

func (c *Client) end() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.waiting = false
	select {
	case <-c.reply:
	default:
	}
	select {
	case <-c.wake:
	default:
	}
}

func (c *Client) acquire(ctx context.Context) error {
	select {
	case <-c.done:
		return ErrClosed
	default:
	}
	select {
	case c.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return ErrClosed
	}
}

func (c *Client) release() {
	<-c.sem
}

func checkConfig(config []string) error {
	seen := make(map[string]struct{}, len(config))
	for _, addr := range config {
		if _, dup := seen[addr]; dup {
			return fmt.Errorf("vsr: duplicate replica address %q", addr)
		}
		seen[addr] = struct{}{}
	}
	return nil
}

func randomID() (uint64, error) {
	var b [8]byte
	for {
		if _, err := rand.Read(b[:]); err != nil {
			return 0, err
		}
		if id := binary.LittleEndian.Uint64(b[:]); id != 0 {
			return id, nil
		}
	}
}
