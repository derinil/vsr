package vsrclient

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/derinil/vsr/vsr"
)

const defaultRequestTimeout = 500 * time.Millisecond

var ErrClosed = errors.New("vsrclient: closed")

type Options struct {
	Config         []string
	NewTransport   func(h vsr.Handler) vsr.Transport
	ClientID       uint64
	RequestTimeout time.Duration
}

type Client struct {
	id        uint64
	timeout   time.Duration
	transport vsr.Transport

	sem       chan struct{}
	reply     chan vsr.Reply
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

type buildFunc func(epoch, requestNum uint64) vsr.Message

func New(o Options) (*Client, error) {
	if len(o.Config) == 0 {
		return nil, errors.New("vsrclient: empty config")
	}
	if err := checkConfig(o.Config); err != nil {
		return nil, err
	}
	if o.NewTransport == nil {
		return nil, errors.New("vsrclient: nil NewTransport")
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
		reply:   make(chan vsr.Reply, 1),
		wake:    make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
	c.transport = o.NewTransport(c.handle)
	return c, nil
}

func (c *Client) Invoke(ctx context.Context, op []byte) ([]byte, error) {
	if err := c.acquire(ctx); err != nil {
		return nil, err
	}
	defer c.release()
	r, err := c.roundTrip(ctx, func(epoch, n uint64) vsr.Message {
		return vsr.Request{Epoch: epoch, ClientID: c.id, RequestNum: n, Op: op}
	})
	if err != nil {
		return nil, err
	}
	return r.Result, nil
}

func (c *Client) Reconfigure(ctx context.Context, config []string) error {
	if len(config) < 3 {
		return fmt.Errorf("vsrclient: reconfiguration needs at least 3 replicas, got %d", len(config))
	}
	if err := checkConfig(config); err != nil {
		return err
	}
	if err := c.acquire(ctx); err != nil {
		return err
	}
	defer c.release()

	cfg := slices.Clone(config)
	next := c.Epoch() + 1
	if _, err := c.roundTrip(ctx, func(epoch, n uint64) vsr.Message {
		return vsr.Reconfiguration{Epoch: epoch, ClientID: c.id, RequestNum: n, Config: cfg}
	}); err != nil {
		return err
	}

	c.mu.Lock()
	c.config, c.epoch, c.view = cfg, next, 0
	c.mu.Unlock()

	_, err := c.roundTrip(ctx, func(epoch, n uint64) vsr.Message {
		return vsr.CheckEpoch{Epoch: epoch, ClientID: c.id, RequestNum: n}
	})
	return err
}

func (c *Client) ID() uint64 {
	return c.id
}

func (c *Client) View() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.view
}

func (c *Client) Epoch() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.epoch
}

func (c *Client) Config() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.config)
}

func (c *Client) Close() error {
	var err error
	c.closeOnce.Do(func() {
		close(c.done)
		err = c.transport.Close()
	})
	return err
}

func (c *Client) handle(m vsr.Message) {
	switch m := m.(type) {
	case vsr.Reply:
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
	case vsr.NewEpoch:
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

func (c *Client) roundTrip(ctx context.Context, build buildFunc) (vsr.Reply, error) {
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
			return vsr.Reply{}, ctx.Err()
		case <-c.done:
			return vsr.Reply{}, ErrClosed
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
			return fmt.Errorf("vsrclient: duplicate replica address %q", addr)
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
