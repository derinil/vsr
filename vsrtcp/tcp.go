package vsrtcp

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"maps"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/derinil/vsr/vsr"
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

type Options struct {
	DialTimeout  time.Duration
	QueueSize    int
	MaxFrameSize int
	Backoff      time.Duration
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

type Transport struct {
	h    vsr.Handler
	opts Options

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu     sync.Mutex
	closed bool
	ln     net.Listener
	peers  map[string]chan []byte
	conns  map[*conn]struct{}
	routes map[uint64]*conn
}

var _ vsr.Transport = (*Transport)(nil)

type conn struct {
	nc   net.Conn
	out  chan []byte
	quit chan struct{}
	once sync.Once
}

func (c *conn) close() {
	c.once.Do(func() {
		close(c.quit)
		c.nc.Close()
	})
}

func New(h vsr.Handler, o Options) *Transport {
	ctx, cancel := context.WithCancel(context.Background())
	return &Transport{
		h:      h,
		opts:   o.withDefaults(),
		ctx:    ctx,
		cancel: cancel,
		peers:  make(map[string]chan []byte),
		conns:  make(map[*conn]struct{}),
		routes: make(map[uint64]*conn),
	}
}

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
	t.goRun(func() { t.acceptLoop(ln) })
	return nil
}

func (t *Transport) Addr() net.Addr {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ln == nil {
		return nil
	}
	return t.ln.Addr()
}

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
	out, ok := t.peers[to]
	if !ok {
		out = make(chan []byte, t.opts.QueueSize)
		t.peers[to] = out
		t.goRun(func() { t.runPeer(to, out) })
	}
	enqueue(out, b)
}

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

func (t *Transport) goRun(f func()) {
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		f()
	}()
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

func enqueue(out chan<- []byte, b []byte) {
	select {
	case out <- b:
	default:
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
		c, ok := t.newConn(nc, make(chan []byte, t.opts.QueueSize))
		if !ok {
			return
		}
		t.goRun(func() { t.runConn(c, true) })
	}
}

func (t *Transport) runPeer(addr string, out chan []byte) {
	for {
		if nc, err := t.dial(addr); err == nil {
			c, ok := t.newConn(nc, out)
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

func (t *Transport) newConn(nc net.Conn, out chan []byte) (*conn, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		nc.Close()
		return nil, false
	}
	c := &conn{nc: nc, out: out, quit: make(chan struct{})}
	t.conns[c] = struct{}{}
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
	t.goRun(func() { t.readLoop(c, inbound) })
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
		for n := len(c.out); n > 0; n-- {
			if _, err := w.Write(<-c.out); err != nil {
				return
			}
		}
		if err := w.Flush(); err != nil {
			return
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
