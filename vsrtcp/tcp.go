// Package vsrtcp is a TCP vsr.Transport using length-prefixed frames.
package vsrtcp

import (
	"bufio"
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"maps"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/derinil/vsr/vsr"
	"github.com/derinil/vsr/vsrstatus"
)

const (
	defaultDialTimeout  = time.Second
	defaultQueueSize    = 4096
	defaultMaxFrameSize = 64 << 20
	defaultBackoff      = 100 * time.Millisecond
	defaultWriteTimeout = 5 * time.Second
	frameHeaderSize     = 4
	ioBufferSize        = 64 << 10
)

var (
	errListening     = errors.New("vsrtcp: already listening")
	errClosed        = errors.New("vsrtcp: transport closed")
	errFrameTooLarge = errors.New("vsrtcp: frame too large")
)

// Options configures a Transport. Zero values select defaults.
type Options struct {
	// DialTimeout bounds each dial.
	DialTimeout time.Duration
	// QueueSize bounds queued outbound frames per peer; excess is dropped.
	QueueSize int
	// MaxFrameSize bounds inbound frame size.
	MaxFrameSize int
	// Backoff is the delay before redialing a peer.
	Backoff time.Duration
	// WriteTimeout bounds each write.
	WriteTimeout time.Duration
}

func (o Options) withDefaults() Options {
	if o.DialTimeout <= 0 {
		o.DialTimeout = defaultDialTimeout
	}
	if o.QueueSize <= 0 {
		o.QueueSize = defaultQueueSize
	}
	if o.MaxFrameSize <= 0 {
		o.MaxFrameSize = defaultMaxFrameSize
	}
	if o.Backoff <= 0 {
		o.Backoff = defaultBackoff
	}
	if o.WriteTimeout <= 0 {
		o.WriteTimeout = defaultWriteTimeout
	}
	return o
}

// Transport sends messages best-effort, dropping them when queues are full.
type Transport struct {
	h    vsr.Handler
	opts Options

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu     sync.Mutex
	closed bool
	ln     net.Listener
	peers  map[string]*peer
	conns  map[*conn]struct{}
	routes map[uint64]*conn
}

var _ vsr.Transport = (*Transport)(nil)

type peer struct {
	addr    string
	out     chan []byte
	sent    atomic.Uint64
	dropped atomic.Uint64
	dials   atomic.Uint64

	mu    sync.Mutex
	up    bool
	since time.Time
}

func (p *peer) markConnected(at time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.up, p.since = true, at
}

func (p *peer) markDisconnected() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.up, p.since = false, time.Time{}
}

func (p *peer) status() vsrstatus.Peer {
	p.mu.Lock()
	defer p.mu.Unlock()
	return vsrstatus.Peer{
		Addr:      p.addr,
		Connected: p.up,
		Since:     p.since,
		Queued:    len(p.out),
		Sent:      p.sent.Load(),
		Dropped:   p.dropped.Load(),
		Dials:     p.dials.Load(),
	}
}

type conn struct {
	nc   net.Conn
	out  chan []byte
	peer *peer
	quit chan struct{}
	once sync.Once
}

func (c *conn) close() {
	c.once.Do(func() {
		close(c.quit)
		c.nc.Close()
		if c.peer != nil {
			c.peer.markDisconnected()
		}
	})
}

// New returns a Transport that delivers inbound messages to h.
func New(h vsr.Handler, o Options) *Transport {
	ctx, cancel := context.WithCancel(context.Background())
	return &Transport{
		h:      h,
		opts:   o.withDefaults(),
		ctx:    ctx,
		cancel: cancel,
		peers:  make(map[string]*peer),
		conns:  make(map[*conn]struct{}),
		routes: make(map[uint64]*conn),
	}
}

// Listen accepts inbound connections on addr.
func (t *Transport) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	switch {
	case t.closed:
		ln.Close()
		return errClosed
	case t.ln != nil:
		ln.Close()
		return errListening
	}
	t.ln = ln
	t.wg.Go(func() { t.acceptLoop(ln) })
	return nil
}

// Addr returns the listen address, or nil if not listening.
func (t *Transport) Addr() net.Addr {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ln == nil {
		return nil
	}
	return t.ln.Addr()
}

// Send queues m for the replica at to, dialing it if needed.
func (t *Transport) Send(to string, m vsr.Message) {
	b := t.frame(m)
	if b == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}
	p, ok := t.peers[to]
	if !ok {
		p = &peer{addr: to, out: make(chan []byte, t.opts.QueueSize)}
		t.peers[to] = p
		t.wg.Go(func() { t.runPeer(p) })
	}
	if !enqueue(p.out, b) {
		p.dropped.Add(1)
	}
}

// Peers returns outbound peer stats sorted by address; it is safe for concurrent use.
func (t *Transport) Peers() []vsrstatus.Peer {
	t.mu.Lock()
	out := make([]vsrstatus.Peer, 0, len(t.peers))
	for _, p := range t.peers {
		out = append(out, p.status())
	}
	t.mu.Unlock()
	slices.SortFunc(out, func(a, b vsrstatus.Peer) int { return cmp.Compare(a.Addr, b.Addr) })
	return out
}

// Reply sends m to a client over the connection it last sent from.
func (t *Transport) Reply(client uint64, m vsr.Message) {
	b := t.frame(m)
	if b == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}
	if c := t.routes[client]; c != nil {
		enqueue(c.out, b)
	}
}

// Close closes all connections and waits for goroutines to exit.
func (t *Transport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.cancel()
	ln := t.ln
	conns := slices.Collect(maps.Keys(t.conns))
	t.mu.Unlock()

	var err error
	if ln != nil {
		err = ln.Close()
	}
	for _, c := range conns {
		c.close()
	}
	t.wg.Wait()
	return err
}

func (t *Transport) frame(m vsr.Message) []byte {
	b := vsr.AppendMessage(make([]byte, frameHeaderSize, 64), m)
	n := len(b) - frameHeaderSize
	if n > t.opts.MaxFrameSize {
		return nil
	}
	binary.BigEndian.PutUint32(b, uint32(n))
	return b
}

func enqueue(out chan<- []byte, b []byte) bool {
	select {
	case out <- b:
		return true
	default:
		return false
	}
}

func (t *Transport) sleep(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-t.ctx.Done():
		return false
	}
}

func (t *Transport) acceptLoop(ln net.Listener) {
	for {
		nc, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			if !t.sleep(t.opts.Backoff) {
				return
			}
			continue
		}
		c, ok := t.newConn(nc, make(chan []byte, t.opts.QueueSize), nil)
		if !ok {
			return
		}
		t.wg.Go(func() { t.runConn(c, true) })
	}
}

func (t *Transport) runPeer(p *peer) {
	for {
		p.dials.Add(1)
		if nc, err := t.dial(p.addr); err == nil {
			c, ok := t.newConn(nc, p.out, p)
			if !ok {
				return
			}
			t.runConn(c, false)
		}
		if !t.sleep(t.opts.Backoff) {
			return
		}
	}
}

func (t *Transport) dial(addr string) (net.Conn, error) {
	d := net.Dialer{Timeout: t.opts.DialTimeout}
	return d.DialContext(t.ctx, "tcp", addr)
}

func (t *Transport) newConn(nc net.Conn, out chan []byte, p *peer) (*conn, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		nc.Close()
		return nil, false
	}
	c := &conn{nc: nc, out: out, peer: p, quit: make(chan struct{})}
	t.conns[c] = struct{}{}
	if p != nil {
		p.markConnected(time.Now())
	}
	return c, true
}

func (t *Transport) untrack(c *conn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.conns, c)
	for id, r := range t.routes {
		if r == c {
			delete(t.routes, id)
		}
	}
}

func (t *Transport) runConn(c *conn, inbound bool) {
	t.wg.Go(func() { t.readLoop(c, inbound) })
	t.writeLoop(c)
	c.close()
	t.untrack(c)
}

func (t *Transport) readLoop(c *conn, inbound bool) {
	defer c.close()
	r := bufio.NewReaderSize(c.nc, ioBufferSize)
	var buf []byte
	for {
		frame, err := readFrame(r, buf, t.opts.MaxFrameSize)
		if err != nil {
			return
		}
		buf = frame
		m, err := vsr.DecodeMessage(frame)
		if err != nil {
			return
		}
		if inbound {
			t.route(m, c)
		}
		t.h(m)
	}
}

func (t *Transport) route(m vsr.Message, c *conn) {
	id, ok := vsr.ClientOf(m)
	if !ok {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, live := t.conns[c]; live {
		t.routes[id] = c
	}
}

func (t *Transport) writeLoop(c *conn) {
	w := bufio.NewWriterSize(c.nc, ioBufferSize)
	for {
		select {
		case b := <-c.out:
			c.nc.SetWriteDeadline(time.Now().Add(t.opts.WriteTimeout))
			if _, err := w.Write(b); err != nil {
				return
			}
		case <-c.quit:
			return
		}
		written := 1
		for pending := len(c.out); pending > 0; pending-- {
			if _, err := w.Write(<-c.out); err != nil {
				return
			}
			written++
		}
		if err := w.Flush(); err != nil {
			return
		}
		if c.peer != nil {
			c.peer.sent.Add(uint64(written))
		}
	}
}

func readFrame(r io.Reader, buf []byte, maxSize int) ([]byte, error) {
	var hdr [frameHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if uint64(n) > uint64(maxSize) {
		return nil, errFrameTooLarge
	}
	buf = slices.Grow(buf[:0], int(n))[:n]
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
