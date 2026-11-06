package vsr

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/derinil/vsr/internal/protocol"
)

const checkpointFileName = "checkpoint.bin"

var errChecksum = errors.New("checkpoint checksum mismatch")

// loadCheckpoint reads dir/checkpoint.bin, returning nil when it does not exist.
func loadCheckpoint(dir string) (*protocol.Checkpoint, error) {
	b, err := os.ReadFile(filepath.Join(dir, checkpointFileName))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(b) < sha256.Size {
		return nil, errChecksum
	}
	payload := b[sha256.Size:]
	if sum := sha256.Sum256(payload); !bytes.Equal(b[:sha256.Size], sum[:]) {
		return nil, errChecksum
	}
	c, err := protocol.DecodeCheckpoint(payload)
	if err != nil {
		return nil, err
	}
	return &c, nil
}

// saveCheckpoint atomically writes c to dir/checkpoint.bin as a SHA-256 digest followed by the encoded checkpoint.
func saveCheckpoint(dir string, c protocol.Checkpoint) error {
	payload := protocol.AppendCheckpoint(nil, c)
	sum := sha256.Sum256(payload)
	return writeFileAtomic(dir, checkpointFileName, append(sum[:], payload...))
}
