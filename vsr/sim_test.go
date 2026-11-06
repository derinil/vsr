package vsr

import (
	"fmt"
	"math/rand/v2"
	"slices"
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

type snapshot struct {
	epoch  uint64
	config []string
	old    []string
}

type cluster struct {
	t            *testing.T
	seed         int64
	epoch        uint64
	config       []string
	addrs        []string
	addrIndex    map[string]int
	rng          *rand.Rand
	replicas     []*Replica
	services     []*recorder
	incarnations []int
	saved        []snapshot
	queue        []envelope
	group        []int
	dropRate     float64
	filter       func(envelope) bool
	clients      []*simClient
	control      map[string]bool
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

type requestKind uint8

const (
	kindRequest requestKind = iota
	kindReconfig
	kindCheck
)

type simClient struct {
	id          uint64
	epoch       uint64
	config      []string
	remaining   int
	issued      uint64
	pending     bool
	kind        requestKind
	target      []string
	targetEpoch uint64
	waited      int
	completed   int
}

type simOutbox struct {
	c    *cluster
	from int
}

func (o simOutbox) Send(to string, m Message) {
	i, ok := o.c.addrIndex[to]
	if !ok {
		o.c.fail("send to unknown address %q", to)
		return
	}
	o.c.enqueue(envelope{from: o.from, to: i, msg: m})
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
		t:         t,
		seed:      seed,
		config:    make([]string, n),
		addrIndex: make(map[string]int, n),
		rng:       rand.New(rand.NewPCG(uint64(seed), 0xc0ffee)),
		control:   make(map[string]bool),
		position:  make(map[string]int),
	}
	for i := range n {
		c.config[i] = fmt.Sprintf("r%d", i)
	}
	for _, addr := range c.config {
		c.addSlot(addr)
	}
	for i := range n {
		c.start(i, Options{Config: c.config})
	}
	return c
}

func (c *cluster) addSlot(addr string) int {
	i := len(c.addrs)
	c.addrs = append(c.addrs, addr)
	c.addrIndex[addr] = i
	c.replicas = append(c.replicas, nil)
	c.services = append(c.services, nil)
	c.incarnations = append(c.incarnations, 0)
	c.saved = append(c.saved, snapshot{})
	if c.group != nil {
		c.group = append(c.group, 0)
	}
	return i
}

func (c *cluster) spawn(addr string) int {
	i := c.addSlot(addr)
	c.start(i, Options{Join: true})
	return i
}

func (c *cluster) start(i int, o Options) {
	c.incarnations[i]++
	svc := &recorder{c: c, replica: i}
	o.Addr = c.addrs[i]
	o.Service = svc
	o.Outbox = simOutbox{c: c, from: i}
	o.Seed = c.seed*1000 + int64(i)*100 + int64(c.incarnations[i])
	r, err := NewReplica(o)
	if err != nil {
		c.t.Fatal(err)
	}
	c.replicas[i] = r
	c.services[i] = svc
}

func (c *cluster) crash(i int) {
	r := c.replicas[i]
	c.saved[i] = snapshot{epoch: r.Epoch(), config: r.Config(), old: r.OldConfig()}
	c.replicas[i] = nil
}

func (c *cluster) restart(i int) {
	if c.replicas[i] != nil {
		c.crash(i)
	}
	s := c.saved[i]
	if len(s.config) == 0 {
		c.start(i, Options{Join: true})
		return
	}
	c.start(i, Options{Config: s.config, OldConfig: s.old, Epoch: s.epoch, Recovering: true})
}

func (c *cluster) isolate(i int) {
	g := make([]int, len(c.addrs))
	g[i] = 1
	c.group = g
}

func (c *cluster) heal() {
	c.group = nil
}

func (c *cluster) addClient(remaining int) *simClient {
	cl := &simClient{
		id:        uint64(len(c.clients) + 1),
		epoch:     c.epoch,
		config:    slices.Clone(c.config),
		remaining: remaining,
	}
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
	switch m := e.msg.(type) {
	case Reply:
		c.onReply(e.client, m)
	case NewEpoch:
		c.onNewEpoch(e.client, m)
	}
}

func (c *cluster) client(id uint64) *simClient {
	if id == 0 || id > uint64(len(c.clients)) {
		return nil
	}
	return c.clients[id-1]
}

func (c *cluster) onReply(id uint64, rep Reply) {
	c.replies = append(c.replies, rep)
	cl := c.client(id)
	if c.control[opKey(id, rep.RequestNum)] {
		c.onControlReply(cl, rep)
		return
	}
	c.observeReply(rep)
	if cl == nil || !cl.pending || cl.kind != kindRequest || rep.RequestNum != cl.issued {
		return
	}
	cl.pending = false
	cl.completed++
	if cl.remaining > 0 {
		c.startRequest(cl)
	}
}

func (c *cluster) onControlReply(cl *simClient, rep Reply) {
	if cl == nil || !cl.pending || rep.RequestNum != cl.issued {
		return
	}
	switch cl.kind {
	case kindReconfig:
		cl.epoch, cl.config = cl.targetEpoch, cl.target
		c.issue(cl, kindCheck)
	case kindCheck:
		cl.pending = false
	}
}

func (c *cluster) onNewEpoch(id uint64, m NewEpoch) {
	cl := c.client(id)
	if cl == nil || m.Epoch <= cl.epoch {
		return
	}
	cl.epoch, cl.config = m.Epoch, m.Config
	if cl.pending {
		c.sendCurrent(cl)
	}
}

func (c *cluster) message(cl *simClient) Message {
	switch cl.kind {
	case kindReconfig:
		return Reconfiguration{Epoch: cl.epoch, ClientID: cl.id, RequestNum: cl.issued, Config: cl.target}
	case kindCheck:
		return CheckEpoch{Epoch: cl.epoch, ClientID: cl.id, RequestNum: cl.issued}
	}
	return Request{Epoch: cl.epoch, ClientID: cl.id, RequestNum: cl.issued, Op: []byte(opKey(cl.id, cl.issued))}
}

func (c *cluster) sendCurrent(cl *simClient) {
	msg := c.message(cl)
	if p := c.leader(); cl.kind == kindRequest && p >= 0 && c.replicas[p].Epoch() == cl.epoch && slices.Contains(cl.config, c.addrs[p]) {
		c.enqueue(envelope{from: -1, to: p, msg: msg})
		return
	}
	for _, addr := range cl.config {
		c.enqueue(envelope{from: -1, to: c.addrIndex[addr], msg: msg})
	}
}

func (c *cluster) issue(cl *simClient, kind requestKind) {
	cl.issued++
	cl.kind = kind
	cl.pending = true
	cl.waited = 0
	if kind != kindRequest {
		c.control[opKey(cl.id, cl.issued)] = true
	}
	c.sendCurrent(cl)
}

func (c *cluster) startRequest(cl *simClient) {
	cl.remaining--
	c.issue(cl, kindRequest)
}

func (c *cluster) submit(cl *simClient) {
	cl.remaining++
	if !cl.pending {
		c.startRequest(cl)
	}
}

func (c *cluster) reconfigure(cl *simClient, config []string) {
	for _, addr := range config {
		i, ok := c.addrIndex[addr]
		switch {
		case !ok:
			c.spawn(addr)
		case c.replicas[i] != nil && c.replicas[i].Status() == StatusShutdown:
			c.start(i, Options{Join: true})
		}
	}
	cl.target = slices.Clone(config)
	cl.targetEpoch = cl.epoch + 1
	c.epoch = cl.targetEpoch
	c.config = slices.Clone(config)
	c.issue(cl, kindReconfig)
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
		c.sendCurrent(cl)
	}
}

func (c *cluster) leader() int {
	best := -1
	for i, r := range c.replicas {
		if r == nil || !r.IsPrimary() {
			continue
		}
		if best < 0 || r.Epoch() > c.replicas[best].Epoch() || r.Epoch() == c.replicas[best].Epoch() && r.View() > c.replicas[best].View() {
			best = i
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
		fmt.Fprintf(&b, "[r%d %s epoch=%d view=%d op=%d commit=%d] ", i, r.Status(), r.Epoch(), r.View(), r.OpNum(), r.CommitNum())
	}
	return b.String()
}
