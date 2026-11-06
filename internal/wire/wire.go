// Package wire holds the binary encoding primitives shared by messages and
// checkpoints.
package wire

import (
	"encoding/binary"
	"math"
	"slices"
)

// Encoder appends values to a byte slice.
type Encoder []byte

func (e *Encoder) U8(v byte) {
	*e = append(*e, v)
}

func (e *Encoder) U64(v uint64) {
	*e = binary.AppendUvarint(*e, v)
}

func (e *Encoder) Int(v int) {
	if v < 0 {
		panic("vsr: negative int field")
	}
	e.U64(uint64(v))
}

func (e *Encoder) Bool(v bool) {
	if v {
		e.U8(1)
	} else {
		e.U8(0)
	}
}

func (e *Encoder) Bytes(v []byte) {
	e.U64(uint64(len(v)))
	*e = append(*e, v...)
}

func (e *Encoder) Str(v string) {
	e.U64(uint64(len(v)))
	*e = append(*e, v...)
}

func (e *Encoder) Strings(v []string) {
	e.U64(uint64(len(v)))
	for _, s := range v {
		e.Str(s)
	}
}

func (e *Encoder) U64s(v []uint64) {
	e.U64(uint64(len(v)))
	for _, x := range v {
		e.U64(x)
	}
}

func (e *Encoder) ByteSlices(v [][]byte) {
	e.U64(uint64(len(v)))
	for _, x := range v {
		e.Bytes(x)
	}
}

// Decoder reads values, recording whether any read failed.
type Decoder struct {
	b      []byte
	failed bool
}

func NewDecoder(b []byte) Decoder {
	return Decoder{b: b}
}

// Fail marks the input malformed.
func (d *Decoder) Fail() {
	d.failed = true
}

// Finish reports whether decoding succeeded and consumed all input.
func (d *Decoder) Finish() bool {
	return !d.failed && len(d.b) == 0
}

func (d *Decoder) take(n int) []byte {
	if d.failed || n > len(d.b) {
		d.failed = true
		return nil
	}
	v := d.b[:n]
	d.b = d.b[n:]
	return v
}

func (d *Decoder) U8() byte {
	if b := d.take(1); b != nil {
		return b[0]
	}
	return 0
}

func (d *Decoder) U64() uint64 {
	if d.failed {
		return 0
	}
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.failed = true
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *Decoder) Int() int {
	v := d.U64()
	if v > math.MaxInt {
		d.failed = true
		return 0
	}
	return int(v)
}

func (d *Decoder) Bool() bool {
	switch d.U8() {
	case 0:
		return false
	case 1:
		return true
	}
	d.failed = true
	return false
}

// Count reads a length that cannot exceed the remaining input.
func (d *Decoder) Count() int {
	n := d.U64()
	if n > uint64(len(d.b)) {
		d.failed = true
		return 0
	}
	return int(n)
}

func (d *Decoder) Bytes() []byte {
	n := d.Count()
	if n == 0 {
		return nil
	}
	return slices.Clone(d.take(n))
}

func (d *Decoder) Str() string {
	return string(d.take(d.Count()))
}

func (d *Decoder) Strings() []string {
	n := d.Count()
	if n == 0 {
		return nil
	}
	ss := make([]string, n)
	for i := range ss {
		ss[i] = d.Str()
	}
	return ss
}

func (d *Decoder) U64s() []uint64 {
	n := d.Count()
	if n == 0 {
		return nil
	}
	vs := make([]uint64, n)
	for i := range vs {
		vs[i] = d.U64()
	}
	return vs
}

func (d *Decoder) ByteSlices() [][]byte {
	n := d.Count()
	if n == 0 {
		return nil
	}
	vs := make([][]byte, n)
	for i := range vs {
		vs[i] = d.Bytes()
	}
	return vs
}
