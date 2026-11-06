package protocol

import "crypto/sha256"

type digest = [32]byte

type merkle struct {
	pageSize int
	size     int
	levels   [][]digest
}

func buildMerkle(data []byte, pageSize int) *merkle {
	leaves := make([]digest, max(1, (len(data)+pageSize-1)/pageSize))
	for i := range leaves {
		leaves[i] = leafHash(pageOf(data, pageSize, i))
	}
	levels := [][]digest{leaves}
	for len(levels[len(levels)-1]) > 1 {
		prev := levels[len(levels)-1]
		next := make([]digest, (len(prev)+1)/2)
		for i := range next {
			next[i] = prev[2*i]
			if 2*i+1 < len(prev) {
				next[i] = nodeHash(prev[2*i], prev[2*i+1])
			}
		}
		levels = append(levels, next)
	}
	return &merkle{pageSize: pageSize, size: len(data), levels: levels}
}

func (t *merkle) root() digest {
	return t.levels[len(t.levels)-1][0]
}

func (t *merkle) pages() int {
	return pageCount(t.size, t.pageSize)
}

func pageCount(size, pageSize int) int {
	return max(1, (size+pageSize-1)/pageSize)
}

func levelSize(pages, level int) int {
	return (pages-1)>>level + 1
}

func topLevel(pages int) int {
	top := 0
	for levelSize(pages, top) > 1 {
		top++
	}
	return top
}

func (t *merkle) node(level, i int) (digest, bool) {
	if level < 0 || level >= len(t.levels) || i < 0 || i >= len(t.levels[level]) {
		return digest{}, false
	}
	return t.levels[level][i], true
}

func (t *merkle) covers(level, i int) (lo, hi int) {
	return coversIn(t.pages(), level, i)
}

func coversIn(pages, level, i int) (lo, hi int) {
	return min(i<<level, pages), min((i+1)<<level, pages)
}

func pageOf(data []byte, pageSize, i int) []byte {
	lo := min(i*pageSize, len(data))
	return data[lo:min(lo+pageSize, len(data))]
}

func leafHash(p []byte) digest {
	h := sha256.New()
	h.Write([]byte{0})
	h.Write(p)
	var d digest
	h.Sum(d[:0])
	return d
}

func nodeHash(l, r digest) digest {
	h := sha256.New()
	h.Write([]byte{1})
	h.Write(l[:])
	h.Write(r[:])
	var d digest
	h.Sum(d[:0])
	return d
}
