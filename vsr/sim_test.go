package vsr

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
)

const (
	simClientRetry = 50
	simMaxQueue    = 1 << 15
	simSettleLimit = 1 << 22
)

type envelope struct {
	from     int
	to       int
	client   uint64
	toClient bool
	msg      Message
}

type execution struct {
	op     string
	result string
}

type cluster struct {
	t            *testing.T
	seed         int64
	config       []string
	addrIndex    map[string]int
	rng          *rand.Rand
	replicas     []*Replica
	services     []*recorder
	incarnations []int
	queue        []envelope
	group        []int
	dropRate     float64
	filter       func(envelope) bool
	clients      []*simClient
	history      []execution
	position     map[string]int
	replies      []Reply
	err          error
}

type recorder struct {
	c       *cluster
	replica int
	ops     []string
}

func (s *recorder) Execute(op []byte) []byte {
	key := string(op)
	pos := len(s.ops)
	s.ops = append(s.ops, key)
	result := fmt.Sprintf("ok:%d:%s", pos, key)
	s.c.observe(s.replica, pos, key, result)
	return []byte(result)
}

type simClient struct {
	id        uint64
	remaining int
	issued    uint64
	pending   bool
	waited    int
	completed int
}

type simOutbox struct {
	c    *cluster
	from int
}

func (o simOutbox) Send(to string, m Message) {
	o.c.enqueue(envelope{from: o.from, to: o.c.addrIndex[to], msg: m})
}

func (o simOutbox) Reply(client uint64, m Message) {
	o.c.enqueue(envelope{from: o.from, to: -1, client: client, toClient: true, msg: m})
}

func opKey(client, req uint64) string {
	return fmt.Sprintf("c%d/r%d", client, req)
}

func newCluster(t *testing.T, n int, seed int64) *cluster {
	t.Helper()
	c := &cluster{
		t:            t,
		seed:         seed,
		config:       make([]string, n),
		addrIndex:    make(map[string]int, n),
		rng:          rand.New(rand.NewPCG(uint64(seed), 0xc0ffee)),
		replicas:     make([]*Replica, n),
		services:     make([]*recorder, n),
		incarnations: make([]int, n),
		position:     make(map[string]int),
	}
	for i := range n {
		c.config[i] = fmt.Sprintf("r%d", i)
		c.addrIndex[c.config[i]] = i
	}
	for i := range n {
		c.start(i, false)
	}
	return c
}

func (c *cluster) start(i int, recovering bool) {
	c.incarnations[i]++
	svc := &recorder{c: c, replica: i}
	r, err := NewReplica(Options{
		Addr:       c.config[i],
		Config:     c.config,
		Service:    svc,
		Outbox:     simOutbox{c: c, from: i},
		Recovering: recovering,
		Seed:       c.seed*1000 + int64(i)*100 + int64(c.incarnations[i]),
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.replicas[i] = r
	c.services[i] = svc
}

func (c *cluster) crash(i int) {
	c.replicas[i] = nil
}

func (c *cluster) restart(i int) {
	c.start(i, true)
}

func (c *cluster) isolate(i int) {
	g := make([]int, len(c.config))
	g[i] = 1
	c.group = g
}

func (c *cluster) heal() {
	c.group = nil
}

func (c *cluster) addClient(remaining int) *simClient {
	cl := &simClient{id: uint64(len(c.clients) + 1), remaining: remaining}
	c.clients = append(c.clients, cl)
	return cl
}

func (c *cluster) fail(format string, args ...any) {
	if c.err == nil {
		c.err = fmt.Errorf(format, args...)
	}
}

func (c *cluster) check() {
	c.t.Helper()
	if c.err != nil {
		c.t.Fatalf("seed %d: %v", c.seed, c.err)
	}
}

func (c *cluster) observe(replica, pos int, op, result string) {
	if pos < len(c.history) {
		if c.history[pos] != (execution{op: op, result: result}) {
			c.fail("replica %d diverged at position %d: executed %s/%s, expected %s/%s",
				replica, pos, op, result, c.history[pos].op, c.history[pos].result)
		}
		return
	}
	if prev, dup := c.position[op]; dup {
		c.fail("replica %d executed %s at position %d, already executed at %d", replica, op, pos, prev)
		return
	}
	c.position[op] = len(c.history)
	c.history = append(c.history, execution{op: op, result: result})
}

func (c *cluster) observeReply(m Reply) {
	key := opKey(m.ClientID, m.RequestNum)
	pos, ok := c.position[key]
	if !ok {
		c.fail("reply for %s before it was executed", key)
		return
	}
	if string(m.Result) != c.history[pos].result {
		c.fail("reply for %s carries %q, executed as %q", key, m.Result, c.history[pos].result)
	}
}

func (c *cluster) enqueue(e envelope) {
	if len(c.queue) >= simMaxQueue {
		return
	}
	c.queue = append(c.queue, e)
}

func (c *cluster) lost(e envelope) bool {
	if c.filter != nil && c.filter(e) {
		return true
	}
	if !e.toClient && e.from >= 0 && c.group != nil && c.group[e.from] != c.group[e.to] {
		return true
	}
	return c.dropRate > 0 && c.rng.Float64() < c.dropRate
}

func (c *cluster) deliver(e envelope) {
	if c.lost(e) {
		return
	}
	if e.toClient {
		c.receiveClient(e)
		return
	}
	if r := c.replicas[e.to]; r != nil {
		r.Step(e.msg)
	}
}

func (c *cluster) receiveClient(e envelope) {
	rep, ok := e.msg.(Reply)
	if !ok {
		return
	}
	c.replies = append(c.replies, rep)
	c.observeReply(rep)
	if e.client == 0 || e.client > uint64(len(c.clients)) {
		return
	}
	cl := c.clients[e.client-1]
	if !cl.pending || rep.RequestNum != cl.issued {
		return
	}
	cl.pending = false
	cl.completed++
	if cl.remaining > 0 {
		c.startRequest(cl)
	}
}

func (c *cluster) requestOf(cl *simClient) Request {
	return Request{ClientID: cl.id, RequestNum: cl.issued, Op: []byte(opKey(cl.id, cl.issued))}
}

func (c *cluster) startRequest(cl *simClient) {
	cl.remaining--
	cl.issued++
	cl.pending = true
	cl.waited = 0
	req := c.requestOf(cl)
	if p := c.leader(); p >= 0 {
		c.enqueue(envelope{from: -1, to: p, msg: req})
		return
	}
	c.broadcastRequest(req)
}

func (c *cluster) broadcastRequest(req Request) {
	for i := range c.replicas {
		c.enqueue(envelope{from: -1, to: i, msg: req})
	}
}

func (c *cluster) submit(cl *simClient) {
	cl.remaining++
	if !cl.pending {
		c.startRequest(cl)
	}
}

func (c *cluster) tickClient(cl *simClient) {
	if !cl.pending {
		if cl.remaining > 0 {
			c.startRequest(cl)
		}
		return
	}
	cl.waited++
	if cl.waited >= simClientRetry {
		cl.waited = 0
		c.broadcastRequest(c.requestOf(cl))
	}
}

func (c *cluster) leader() int {
	best := -1
	var bestView uint64
	for i, r := range c.replicas {
		if r != nil && r.IsPrimary() && (best < 0 || r.View() > bestView) {
			best, bestView = i, r.View()
		}
	}
	return best
}

func (c *cluster) settle() {
	for steps := 0; len(c.queue) > 0; steps++ {
		if steps > simSettleLimit {
			c.fail("network did not quiesce")
			return
		}
		e := c.queue[0]
		c.queue = c.queue[1:]
		c.deliver(e)
	}
}

func (c *cluster) deliverRandom() {
	i := c.rng.IntN(len(c.queue))
	e := c.queue[i]
	last := len(c.queue) - 1
	c.queue[i] = c.queue[last]
	c.queue = c.queue[:last]
	c.deliver(e)
}

func (c *cluster) tickAll() {
	for _, r := range c.replicas {
		if r != nil {
			r.Tick()
		}
	}
	for _, cl := range c.clients {
		c.tickClient(cl)
	}
}

func (c *cluster) tickReplicas(only []int) {
	for _, i := range only {
		if r := c.replicas[i]; r != nil {
			r.Tick()
		}
	}
}

func (c *cluster) run(ticks int) {
	for range ticks {
		c.tickAll()
		c.settle()
	}
}

func (c *cluster) waitFor(cond func() bool, limit int) bool {
	for range limit {
		if cond() {
			return true
		}
		c.tickAll()
		c.settle()
	}
	return cond()
}

func (c *cluster) waitForReplicas(cond func() bool, limit int, only ...int) bool {
	for range limit {
		if cond() {
			return true
		}
		c.tickReplicas(only)
		c.settle()
	}
	return cond()
}

func (c *cluster) replied(cl *simClient) bool {
	return !cl.pending && cl.remaining == 0
}

func (c *cluster) describe() string {
	var b strings.Builder
	for i, r := range c.replicas {
		if r == nil {
			fmt.Fprintf(&b, "[r%d down] ", i)
			continue
		}
		fmt.Fprintf(&b, "[r%d %s view=%d op=%d commit=%d] ", i, r.Status(), r.View(), r.OpNum(), r.CommitNum())
	}
	return b.String()
}
