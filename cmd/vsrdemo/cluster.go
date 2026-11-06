package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/derinil/vsr/internal/vsrstatus"
)

const (
	restartDelay = 500 * time.Millisecond
	stopTimeout  = 3 * time.Second
	surveyWait   = 300 * time.Millisecond
)

type cluster struct {
	nodes  []*node
	byAddr map[string]*node
	http   *http.Client
}

type node struct {
	addr   string
	http   string
	data   string
	exe    string
	args   []string
	logger *slog.Logger

	mu        sync.Mutex
	proc      *os.Process
	stdin     io.WriteCloser
	paused    bool
	downUntil time.Time
	block     []string
	loss      float64
	delay     time.Duration
}

func newCluster(exe, dir string, o options, logger *slog.Logger) *cluster {
	c := &cluster{byAddr: map[string]*node{}, http: &http.Client{Timeout: surveyWait}}
	for i, addr := range slices.Concat(o.replicas, o.spares) {
		data := filepath.Join(dir, fmt.Sprint(i))
		n := &node{
			addr: addr,
			http: o.status[i],
			data: data,
			exe:  exe,
			args: []string{
				nodeCommand,
				"-addr", addr,
				"-http", o.status[i],
				"-config", strings.Join(o.replicas, ","),
				"-data", data,
				fmt.Sprintf("-join=%t", i >= len(o.replicas)),
			},
			logger: logger.With("node", addr),
		}
		c.nodes = append(c.nodes, n)
		c.byAddr[addr] = n
	}
	return c
}

func (n *node) supervise(ctx context.Context) {
	for {
		if err := n.runOnce(ctx); err != nil {
			n.logger.Warn("node exited", "err", err)
		}
		n.mu.Lock()
		wait := max(time.Until(n.downUntil), restartDelay)
		n.mu.Unlock()
		if !sleep(ctx, wait) {
			return
		}
	}
}

func (n *node) runOnce(ctx context.Context) error {
	if err := os.MkdirAll(n.data, 0o755); err != nil {
		return err
	}
	cmd := exec.Command(n.exe, n.args...)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	n.mu.Lock()
	n.proc, n.stdin, n.paused = cmd.Process, stdin, false
	n.writeFaults()
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		n.proc, n.stdin, n.paused = nil, nil, false
		n.mu.Unlock()
	}()

	select {
	case err := <-done:
		if n.crashed() {
			return nil
		}
		return err
	case <-ctx.Done():
		cmd.Process.Signal(syscall.SIGCONT)
		cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(stopTimeout):
			cmd.Process.Kill()
			<-done
		}
		return nil
	}
}

func (n *node) crashed() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return time.Now().Before(n.downUntil)
}

func (n *node) down() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.proc == nil || n.paused || time.Now().Before(n.downUntil)
}

func (n *node) crash(d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.downUntil = time.Now().Add(d)
	if n.proc != nil {
		n.proc.Kill()
	}
}

func (n *node) pause(paused bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.proc == nil {
		return
	}
	sig := syscall.SIGCONT
	if paused {
		sig = syscall.SIGSTOP
	}
	if n.proc.Signal(sig) == nil {
		n.paused = paused
	}
}

func (n *node) setBlock(addrs []string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.block = addrs
	n.writeFaults()
}

func (n *node) setNet(loss float64, delay time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.loss, n.delay = loss, delay
	n.writeFaults()
}

func (n *node) writeFaults() {
	if n.stdin == nil {
		return
	}
	fmt.Fprintf(n.stdin, "block %s\nnet %g %s\n", strings.Join(n.block, " "), n.loss, n.delay)
}

type survey struct {
	config  []string
	primary string
	status  map[string]*vsrstatus.Node
}

func (s survey) healthy(c *cluster, addr string) bool {
	st := s.status[addr]
	return st != nil && st.Status == "normal" && !c.byAddr[addr].down()
}

func (c *cluster) survey(ctx context.Context) survey {
	statuses := make([]*vsrstatus.Node, len(c.nodes))
	var wg sync.WaitGroup
	for i, n := range c.nodes {
		wg.Go(func() { statuses[i] = c.fetch(ctx, n) })
	}
	wg.Wait()
	s := survey{status: map[string]*vsrstatus.Node{}}
	var best *vsrstatus.Node
	for _, st := range statuses {
		if st == nil {
			continue
		}
		s.status[st.Addr] = st
		if best == nil || st.Epoch > best.Epoch || st.Epoch == best.Epoch && st.Status == "normal" && (best.Status != "normal" || st.View > best.View) {
			best = st
		}
	}
	if best != nil {
		s.config = best.Config
		if best.Status == "normal" {
			s.primary = best.PrimaryAddr
		}
	}
	return s
}

func (c *cluster) fetch(ctx context.Context, n *node) *vsrstatus.Node {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+n.http+"/status", nil)
	if err != nil {
		return nil
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	var st vsrstatus.Node
	if json.NewDecoder(resp.Body).Decode(&st) != nil || st.Status == "shutdown" {
		return nil
	}
	return &st
}
