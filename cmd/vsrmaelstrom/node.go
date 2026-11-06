package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/derinil/vsr/kvservice"
)

const (
	codeTimeout      = 0
	codeNotSupported = 10
	codeCrash        = 13
)

type envelope[B any] struct {
	Src  string `json:"src"`
	Dest string `json:"dest"`
	Body B      `json:"body"`
}

type body struct {
	Type    string          `json:"type"`
	MsgID   int             `json:"msg_id"`
	Key     json.RawMessage `json:"key"`
	Value   json.RawMessage `json:"value"`
	From    json.RawMessage `json:"from"`
	To      json.RawMessage `json:"to"`
	NodeID  string          `json:"node_id"`
	NodeIDs []string        `json:"node_ids"`
}

type invokeFunc func(ctx context.Context, op []byte) ([]byte, error)

type node struct {
	timeout time.Duration
	invoke  invokeFunc
	wg      sync.WaitGroup

	mu    sync.Mutex
	id    string
	out   *bufio.Writer
	enc   *json.Encoder
	msgID int
}

func newNode(out io.Writer, timeout time.Duration, invoke invokeFunc) *node {
	bw := bufio.NewWriter(out)
	return &node{
		timeout: timeout,
		invoke:  invoke,
		out:     bw,
		enc:     json.NewEncoder(bw),
	}
}

func (n *node) wait() {
	n.wg.Wait()
}

func (n *node) handle(line []byte) {
	var env envelope[body]
	if err := json.Unmarshal(line, &env); err != nil {
		slog.Error("decode message", "err", err, "line", string(line))
		return
	}
	switch env.Body.Type {
	case "init":
		n.mu.Lock()
		n.id = env.Body.NodeID
		n.mu.Unlock()
		n.send(env.Src, env.Body.MsgID, map[string]any{"type": "init_ok"})
	case "read", "write", "cas":
		n.wg.Go(func() { n.serve(env) })
	default:
		n.send(env.Src, env.Body.MsgID, errorBody(codeNotSupported, fmt.Sprintf("unsupported type %q", env.Body.Type)))
	}
}

func (n *node) serve(env envelope[body]) {
	op, err := encodeOp(env.Body)
	if err != nil {
		n.send(env.Src, env.Body.MsgID, errorBody(kvservice.CodeMalformed, err.Error()))
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), n.timeout)
	defer cancel()
	result, err := n.invoke(ctx, op)
	n.send(env.Src, env.Body.MsgID, respond(env.Body.Type, result, err))
}

func (n *node) send(dest string, inReplyTo int, b map[string]any) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.msgID++
	b["msg_id"] = n.msgID
	b["in_reply_to"] = inReplyTo
	if err := n.enc.Encode(envelope[map[string]any]{Src: n.id, Dest: dest, Body: b}); err != nil {
		slog.Error("encode reply", "err", err)
		return
	}
	if err := n.out.Flush(); err != nil {
		slog.Error("write reply", "err", err)
	}
}

func encodeOp(b body) ([]byte, error) {
	switch b.Type {
	case "read":
		return kvservice.Read(b.Key)
	case "write":
		return kvservice.Write(b.Key, b.Value)
	case "cas":
		return kvservice.CAS(b.Key, b.From, b.To)
	}
	return nil, fmt.Errorf("unsupported type %q", b.Type)
}

func respond(typ string, result []byte, err error) map[string]any {
	if err != nil {
		return errorBody(codeTimeout, err.Error())
	}
	r, err := kvservice.DecodeResult(result)
	if err != nil {
		return errorBody(codeCrash, fmt.Sprintf("decode result: %v", err))
	}
	switch r.Code {
	case 0:
		switch typ {
		case "read":
			return map[string]any{"type": "read_ok", "value": r.Value}
		case "write":
			return map[string]any{"type": "write_ok"}
		case "cas":
			return map[string]any{"type": "cas_ok"}
		}
		return errorBody(codeNotSupported, fmt.Sprintf("unsupported type %q", typ))
	case kvservice.CodeMalformed, kvservice.CodeNotFound, kvservice.CodePreconditionFailed:
		return errorBody(r.Code, r.Error)
	}
	return errorBody(codeCrash, fmt.Sprintf("unexpected result code %d: %s", r.Code, r.Error))
}

func errorBody(code int, text string) map[string]any {
	return map[string]any{"type": "error", "code": code, "text": text}
}
