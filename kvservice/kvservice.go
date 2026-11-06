// Package kvservice is a JSON key-value vsr.Service supporting read, write
// and compare-and-swap.
package kvservice

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
)

// Result codes for failed operations.
const (
	CodeMalformed          = 12
	CodeNotFound           = 20
	CodePreconditionFailed = 22
)

var (
	errNotFound          = errors.New("key does not exist")
	errMalformedSnapshot = errors.New("malformed snapshot")
)

// Op is an encoded operation. Kind is read, write or cas.
type Op struct {
	Kind  string          `json:"k"`
	Key   json.RawMessage `json:"key"`
	Value json.RawMessage `json:"value,omitempty"`
	From  json.RawMessage `json:"from,omitempty"`
	To    json.RawMessage `json:"to,omitempty"`
}

// Result is an operation's outcome; non-zero Code means failure.
type Result struct {
	Code  int             `json:"code,omitempty"`
	Value json.RawMessage `json:"value,omitempty"`
	Error string          `json:"error,omitempty"`
}

// Store is an in-memory key-value Service.
type Store struct {
	data map[string][]byte
}

// New returns an empty Store.
func New() *Store {
	return &Store{data: make(map[string][]byte)}
}

// Execute applies op and returns an encoded Result.
func (s *Store) Execute(op []byte) []byte {
	var o Op
	if err := json.Unmarshal(op, &o); err != nil {
		return failure(CodeMalformed, err)
	}
	key, err := canonical(o.Key)
	if err != nil {
		return failure(CodeMalformed, fmt.Errorf("key: %w", err))
	}
	k := string(key)
	switch o.Kind {
	case "read":
		v, ok := s.data[k]
		if !ok {
			return failure(CodeNotFound, errNotFound)
		}
		return encode(Result{Value: v})
	case "write":
		v, err := canonical(o.Value)
		if err != nil {
			return failure(CodeMalformed, fmt.Errorf("value: %w", err))
		}
		s.data[k] = v
		return encode(Result{})
	case "cas":
		from, err := canonical(o.From)
		if err != nil {
			return failure(CodeMalformed, fmt.Errorf("from: %w", err))
		}
		to, err := canonical(o.To)
		if err != nil {
			return failure(CodeMalformed, fmt.Errorf("to: %w", err))
		}
		cur, ok := s.data[k]
		if !ok {
			return failure(CodeNotFound, errNotFound)
		}
		if !bytes.Equal(cur, from) {
			return failure(CodePreconditionFailed, fmt.Errorf("expected %s, had %s", from, cur))
		}
		s.data[k] = to
		return encode(Result{})
	default:
		return failure(CodeMalformed, fmt.Errorf("unknown kind %q", o.Kind))
	}
}

// Snapshot encodes the store with keys in sorted order, so equal stores produce equal bytes.
func (s *Store) Snapshot() []byte {
	keys := slices.Sorted(maps.Keys(s.data))
	b := binary.BigEndian.AppendUint32(nil, uint32(len(keys)))
	for _, k := range keys {
		b = appendBytes(b, []byte(k))
		b = appendBytes(b, s.data[k])
	}
	return b
}

// Restore replaces the store's contents with a state encoded by Snapshot.
func (s *Store) Restore(state []byte) error {
	if len(state) < 4 {
		return errMalformedSnapshot
	}
	n := binary.BigEndian.Uint32(state)
	b := state[4:]
	data := make(map[string][]byte)
	for range n {
		var k, v []byte
		var ok bool
		if k, b, ok = readBytes(b); !ok {
			return errMalformedSnapshot
		}
		if v, b, ok = readBytes(b); !ok {
			return errMalformedSnapshot
		}
		if _, dup := data[string(k)]; dup {
			return errMalformedSnapshot
		}
		data[string(k)] = slices.Clone(v)
	}
	if len(b) > 0 {
		return errMalformedSnapshot
	}
	s.data = data
	return nil
}

// Read encodes a read of key.
func Read(key any) ([]byte, error) {
	k, err := raw(key)
	if err != nil {
		return nil, err
	}
	return json.Marshal(Op{Kind: "read", Key: k})
}

// Write encodes a write of value to key.
func Write(key, value any) ([]byte, error) {
	k, err := raw(key)
	if err != nil {
		return nil, err
	}
	v, err := raw(value)
	if err != nil {
		return nil, err
	}
	return json.Marshal(Op{Kind: "write", Key: k, Value: v})
}

// CAS encodes a compare-and-swap of key from from to to.
func CAS(key, from, to any) ([]byte, error) {
	k, err := raw(key)
	if err != nil {
		return nil, err
	}
	f, err := raw(from)
	if err != nil {
		return nil, err
	}
	t, err := raw(to)
	if err != nil {
		return nil, err
	}
	return json.Marshal(Op{Kind: "cas", Key: k, From: f, To: t})
}

// DecodeResult decodes a Result returned by Execute.
func DecodeResult(b []byte) (Result, error) {
	var r Result
	err := json.Unmarshal(b, &r)
	return r, err
}

func canonical(raw json.RawMessage) ([]byte, error) {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

func raw(v any) (json.RawMessage, error) {
	b, err := json.Marshal(v)
	return b, err
}

func appendBytes(b, v []byte) []byte {
	b = binary.BigEndian.AppendUint32(b, uint32(len(v)))
	return append(b, v...)
}

func readBytes(b []byte) (v, rest []byte, ok bool) {
	if len(b) < 4 {
		return nil, nil, false
	}
	n := uint64(binary.BigEndian.Uint32(b))
	b = b[4:]
	if n > uint64(len(b)) {
		return nil, nil, false
	}
	return b[:n], b[n:], true
}

func failure(code int, err error) []byte {
	return encode(Result{Code: code, Error: err.Error()})
}

func encode(r Result) []byte {
	b, _ := json.Marshal(r)
	return b
}
