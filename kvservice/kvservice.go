package kvservice

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

const (
	CodeMalformed          = 12
	CodeNotFound           = 20
	CodePreconditionFailed = 22
)

var errNotFound = errors.New("key does not exist")

type Op struct {
	Kind  string          `json:"k"`
	Key   json.RawMessage `json:"key"`
	Value json.RawMessage `json:"value,omitempty"`
	From  json.RawMessage `json:"from,omitempty"`
	To    json.RawMessage `json:"to,omitempty"`
}

type Result struct {
	Code  int             `json:"code,omitempty"`
	Value json.RawMessage `json:"value,omitempty"`
	Error string          `json:"error,omitempty"`
}

type Store struct {
	data map[string][]byte
}

func New() *Store {
	return &Store{data: make(map[string][]byte)}
}

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

func Read(key any) ([]byte, error) {
	k, err := raw(key)
	if err != nil {
		return nil, err
	}
	return json.Marshal(Op{Kind: "read", Key: k})
}

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

func failure(code int, err error) []byte {
	return encode(Result{Code: code, Error: err.Error()})
}

func encode(r Result) []byte {
	b, _ := json.Marshal(r)
	return b
}
