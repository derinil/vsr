// Package sqlservice is a SQLite vsr.Service. Each op is a list of statements
// executed atomically in one transaction against an in-memory database.
//
// Statements that could make replicas diverge are rejected: transaction
// control, ATTACH, temporary schema objects, writing pragmas and functions
// reading the clock or randomness.
package sqlservice

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"modernc.org/sqlite"
)

// Stmt is one SQL statement with positional arguments. Arguments are JSON
// scalars: integral numbers bind as INTEGER, other numbers as REAL.
type Stmt struct {
	SQL  string `json:"sql"`
	Args []any  `json:"args,omitempty"`
}

// Rows is a statement's outcome: the rows it returned or, for statements
// returning no columns, the number of rows it changed.
type Rows struct {
	Columns []string `json:"columns,omitempty"`
	Rows    [][]any  `json:"rows,omitempty"`
	Changes int64    `json:"changes,omitempty"`
}

// Result is an op's outcome, one Rows per statement. A non-empty Error means
// the op failed and changed nothing.
type Result struct {
	Results []Rows `json:"results,omitempty"`
	Error   string `json:"error,omitempty"`
}

var (
	errNoStatements = errors.New("no statements")
	errCorrupt      = errors.New("corrupt snapshot")
)

var readOnlyPragmas = map[string]bool{
	"table_info":       true,
	"table_xinfo":      true,
	"table_list":       true,
	"index_list":       true,
	"index_info":       true,
	"index_xinfo":      true,
	"foreign_key_list": true,
}

var nondeterministic = map[string]bool{
	"random":            true,
	"randomblob":        true,
	"date":              true,
	"time":              true,
	"datetime":          true,
	"julianday":         true,
	"unixepoch":         true,
	"strftime":          true,
	"timediff":          true,
	"current_date":      true,
	"current_time":      true,
	"current_timestamp": true,
	"load_extension":    true,
}

type serializer interface {
	Serialize() ([]byte, error)
	Deserialize([]byte) error
}

// Store is an in-memory SQLite Service.
type Store struct {
	db   *sql.DB
	conn *sql.Conn
	user bool
}

// New returns an empty Store.
func New() (*Store, error) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, err
	}
	s, err := open(db)
	if err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func open(db *sql.DB) (*Store, error) {
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	s := &Store{db: db, conn: conn}
	if _, err := conn.ExecContext(ctx, "PRAGMA foreign_keys = ON; PRAGMA user_version = 0"); err != nil {
		return nil, err
	}
	err = conn.Raw(func(c any) error {
		return c.(sqlite.AuthorizerRegisterer).RegisterAuthorizer(s.authorize)
	})
	if err != nil {
		return nil, err
	}
	empty, err := s.serialize()
	if err != nil {
		return nil, err
	}
	if err := s.deserialize(empty); err != nil {
		return nil, err
	}
	return s, nil
}

// Close releases the database.
func (s *Store) Close() error {
	return errors.Join(s.conn.Close(), s.db.Close())
}

// Execute runs the statements encoded in op in one transaction and returns an encoded Result.
func (s *Store) Execute(op []byte) []byte {
	stmts, err := decodeOp(op)
	if err != nil {
		return failure(err)
	}
	rows, err := s.run(stmts)
	if err != nil {
		return failure(err)
	}
	b, err := json.Marshal(Result{Results: rows})
	if err != nil {
		return failure(err)
	}
	return b
}

func (s *Store) run(stmts []Stmt) (_ []Rows, err error) {
	ctx := context.Background()
	tx, err := s.conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	s.user = true
	defer func() {
		s.user = false
		if err != nil {
			tx.Rollback()
		}
	}()
	out := make([]Rows, len(stmts))
	for i, st := range stmts {
		if out[i], err = query(ctx, tx, st); err != nil {
			return nil, fmt.Errorf("statement %d: %w", i, err)
		}
	}
	s.user = false
	return out, tx.Commit()
}

func query(ctx context.Context, tx *sql.Tx, st Stmt) (Rows, error) {
	rows, err := tx.QueryContext(ctx, st.SQL, st.Args...)
	if err != nil {
		return Rows{}, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return Rows{}, err
	}
	r := Rows{Columns: cols}
	for rows.Next() {
		row := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range row {
			ptrs[i] = &row[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return Rows{}, err
		}
		r.Rows = append(r.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return Rows{}, err
	}
	if len(cols) == 0 {
		err = tx.QueryRowContext(ctx, "SELECT changes()").Scan(&r.Changes)
	}
	return r, err
}

func (s *Store) authorize(action sqlite.AuthorizerActionCode, arg1, arg2, _, _ string) sqlite.AuthorizerReturnCode {
	if !s.user {
		return sqlite.AuthorizerOK
	}
	switch action {
	case sqlite.AuthTransaction, sqlite.AuthSavepoint, sqlite.AuthAttach, sqlite.AuthDetach,
		sqlite.AuthCreateTempTable, sqlite.AuthCreateTempIndex, sqlite.AuthCreateTempView, sqlite.AuthCreateTempTrigger:
		return sqlite.AuthorizerDeny
	case sqlite.AuthPragma:
		if !readOnlyPragmas[strings.ToLower(arg1)] {
			return sqlite.AuthorizerDeny
		}
	case sqlite.AuthFunction:
		if nondeterministic[strings.ToLower(arg2)] {
			return sqlite.AuthorizerDeny
		}
	}
	return sqlite.AuthorizerOK
}

// Snapshot returns the database file image, which is identical on replicas
// that executed the same ops.
func (s *Store) Snapshot() []byte {
	b, err := s.serialize()
	if err != nil {
		panic(fmt.Sprintf("sqlservice: snapshot: %v", err))
	}
	return b
}

// Restore replaces the database with an image returned by Snapshot.
func (s *Store) Restore(state []byte) error {
	if err := validate(state); err != nil {
		return err
	}
	return s.deserialize(state)
}

func validate(state []byte) error {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return err
	}
	defer db.Close()
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	err = conn.Raw(func(c any) error {
		return c.(serializer).Deserialize(state)
	})
	if err != nil {
		return fmt.Errorf("%w: %w", errCorrupt, err)
	}
	var check string
	if err := conn.QueryRowContext(ctx, "PRAGMA quick_check(1)").Scan(&check); err != nil {
		return fmt.Errorf("%w: %w", errCorrupt, err)
	}
	if check != "ok" {
		return fmt.Errorf("%w: %s", errCorrupt, check)
	}
	return nil
}

func (s *Store) serialize() (b []byte, err error) {
	err = s.conn.Raw(func(c any) error {
		b, err = c.(serializer).Serialize()
		return err
	})
	return b, err
}

func (s *Store) deserialize(b []byte) error {
	return s.conn.Raw(func(c any) error {
		return c.(serializer).Deserialize(b)
	})
}

// Query returns a Stmt running sql with args.
func Query(sql string, args ...any) Stmt {
	return Stmt{SQL: sql, Args: args}
}

// Encode encodes stmts as one op.
func Encode(stmts ...Stmt) ([]byte, error) {
	return json.Marshal(stmts)
}

// DecodeResult decodes a Result returned by Execute. Numbers decode as json.Number.
func DecodeResult(b []byte) (Result, error) {
	var r Result
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	err := d.Decode(&r)
	return r, err
}

func decodeOp(op []byte) ([]Stmt, error) {
	var stmts []Stmt
	d := json.NewDecoder(bytes.NewReader(op))
	d.UseNumber()
	if err := d.Decode(&stmts); err != nil {
		return nil, err
	}
	if len(stmts) == 0 {
		return nil, errNoStatements
	}
	for i := range stmts {
		for j, a := range stmts[i].Args {
			v, err := bindable(a)
			if err != nil {
				return nil, fmt.Errorf("statement %d arg %d: %w", i, j, err)
			}
			stmts[i].Args[j] = v
		}
	}
	return stmts, nil
}

func bindable(a any) (any, error) {
	switch v := a.(type) {
	case nil, string, bool:
		return v, nil
	case json.Number:
		if n, err := v.Int64(); err == nil {
			return n, nil
		}
		return v.Float64()
	default:
		return nil, fmt.Errorf("unsupported type %T", a)
	}
}

func failure(err error) []byte {
	b, _ := json.Marshal(Result{Error: err.Error()})
	return b
}
