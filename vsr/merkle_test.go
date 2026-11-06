package vsr

import (
	"bytes"
	"crypto/sha256"
	"slices"
	"testing"
)

const testPageSize = 4

func testData(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + 3)
	}
	return b
}

func TestBuildMerkleShape(t *testing.T) {
	tests := []struct {
		name   string
		size   int
		pages  int
		levels int
	}{
		{"empty", 0, 1, 1},
		{"one byte", 1, 1, 1},
		{"pageSize-1", testPageSize - 1, 1, 1},
		{"pageSize", testPageSize, 1, 1},
		{"pageSize+1", testPageSize + 1, 2, 2},
		{"7 pages", 7 * testPageSize, 7, 4},
		{"8 pages", 8 * testPageSize, 8, 4},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := buildMerkle(testData(tc.size), testPageSize)
			if got := m.pages(); got != tc.pages {
				t.Fatalf("pages() = %d, want %d", got, tc.pages)
			}
			if got := len(m.levels[0]); got != tc.pages {
				t.Fatalf("leaves = %d, want %d", got, tc.pages)
			}
			if got := len(m.levels); got != tc.levels {
				t.Fatalf("levels = %d, want %d", got, tc.levels)
			}
			if got := len(m.levels[len(m.levels)-1]); got != 1 {
				t.Fatalf("top level has %d nodes, want 1", got)
			}
		})
	}
}

func TestMerkleRootDeterministic(t *testing.T) {
	tests := []struct {
		name string
		size int
	}{
		{"empty", 0},
		{"one byte", 1},
		{"pageSize+1", testPageSize + 1},
		{"8 pages", 8 * testPageSize},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data := testData(tc.size)
			a := buildMerkle(data, testPageSize).root()
			b := buildMerkle(slices.Clone(data), testPageSize).root()
			if a != b {
				t.Fatal("equal data produced different roots")
			}
			if tc.size > 0 {
				other := slices.Clone(data)
				other[0] ^= 1
				if buildMerkle(other, testPageSize).root() == a {
					t.Fatal("different data produced equal roots")
				}
			}
		})
	}
}

func TestMerkleSingleByteChange(t *testing.T) {
	tests := []struct {
		name string
		size int
	}{
		{"one page", 3},
		{"pageSize+1", testPageSize + 1},
		{"7 pages", 7 * testPageSize},
		{"8 pages", 8 * testPageSize},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data := testData(tc.size)
			base := buildMerkle(data, testPageSize)
			for i := range data {
				changed := slices.Clone(data)
				changed[i] ^= 0xFF
				m := buildMerkle(changed, testPageSize)
				leaf := i / testPageSize
				for level := range base.levels {
					for j := range base.levels[level] {
						differs := base.levels[level][j] != m.levels[level][j]
						if onPath := j == leaf>>level; differs != onPath {
							t.Fatalf("byte %d: node (%d,%d) differs=%t, want %t", i, level, j, differs, onPath)
						}
					}
				}
			}
		})
	}
}

func TestMerkleCovers(t *testing.T) {
	tests := []struct {
		name         string
		size         int
		level, index int
		lo, hi       int
	}{
		{"leaf first", 7 * testPageSize, 0, 0, 0, 1},
		{"leaf last", 7 * testPageSize, 0, 6, 6, 7},
		{"pair", 7 * testPageSize, 1, 0, 0, 2},
		{"promoted pair", 7 * testPageSize, 1, 3, 6, 7},
		{"quad", 7 * testPageSize, 2, 1, 4, 7},
		{"root", 7 * testPageSize, 3, 0, 0, 7},
		{"past end", 7 * testPageSize, 1, 4, 7, 7},
		{"single page root", 0, 0, 0, 0, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := buildMerkle(testData(tc.size), testPageSize)
			lo, hi := m.covers(tc.level, tc.index)
			if lo != tc.lo || hi != tc.hi {
				t.Fatalf("covers(%d,%d) = [%d,%d), want [%d,%d)", tc.level, tc.index, lo, hi, tc.lo, tc.hi)
			}
		})
	}
}

func TestMerklePromotedNode(t *testing.T) {
	tests := []struct {
		name               string
		size               int
		level, index       int
		fromLevel, fromIdx int
	}{
		{"3 pages", 3 * testPageSize, 1, 1, 0, 2},
		{"5 pages leaf", 5 * testPageSize, 1, 2, 0, 4},
		{"5 pages promoted again", 5 * testPageSize, 2, 1, 1, 2},
		{"7 pages", 7 * testPageSize, 1, 3, 0, 6},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := buildMerkle(testData(tc.size), testPageSize)
			got, ok := m.node(tc.level, tc.index)
			want, _ := m.node(tc.fromLevel, tc.fromIdx)
			if !ok || got != want {
				t.Fatalf("node(%d,%d) != node(%d,%d)", tc.level, tc.index, tc.fromLevel, tc.fromIdx)
			}
		})
	}
}

func TestMerkleThreePagesStructure(t *testing.T) {
	data := testData(3 * testPageSize)
	m := buildMerkle(data, testPageSize)
	a, b, c := leafHash(pageOf(data, testPageSize, 0)), leafHash(pageOf(data, testPageSize, 1)), leafHash(pageOf(data, testPageSize, 2))
	if got, want := m.root(), nodeHash(nodeHash(a, b), c); got != want {
		t.Fatalf("root = %x, want %x", got, want)
	}
}

func TestMerkleNode(t *testing.T) {
	m := buildMerkle(testData(2*testPageSize), testPageSize)
	tests := []struct {
		name  string
		level int
		index int
		ok    bool
	}{
		{"leaf", 0, 1, true},
		{"root", 1, 0, true},
		{"negative level", -1, 0, false},
		{"level past top", 2, 0, false},
		{"negative index", 0, -1, false},
		{"index past level", 0, 2, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, ok := m.node(tc.level, tc.index); ok != tc.ok {
				t.Fatalf("ok = %t, want %t", ok, tc.ok)
			}
		})
	}
}

func TestPageOf(t *testing.T) {
	data := testData(10)
	tests := []struct {
		name  string
		index int
		want  []byte
	}{
		{"first", 0, data[0:4]},
		{"middle", 1, data[4:8]},
		{"partial last", 2, data[8:10]},
		{"past end", 3, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := pageOf(data, testPageSize, tc.index); !bytes.Equal(got, tc.want) {
				t.Fatalf("pageOf = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestHashDomains(t *testing.T) {
	l := sha256.Sum256([]byte("left"))
	r := sha256.Sum256([]byte("right"))
	tests := []struct {
		name string
		got  digest
		want digest
	}{
		{"leaf", leafHash([]byte("page")), sha256.Sum256(append([]byte{0}, "page"...))},
		{"empty leaf", leafHash(nil), sha256.Sum256([]byte{0})},
		{"node", nodeHash(l, r), sha256.Sum256(append(append([]byte{1}, l[:]...), r[:]...))},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Fatalf("got %x, want %x", tc.got, tc.want)
			}
		})
	}
	if nodeHash(l, r) == leafHash(append(l[:], r[:]...)) {
		t.Fatal("node hash collides with leaf hash of its children")
	}
}
