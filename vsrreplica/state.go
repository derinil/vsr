package vsrreplica

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

const stateFileName = "state.json"

type diskState struct {
	Epoch     uint64   `json:"epoch"`
	Config    []string `json:"config"`
	OldConfig []string `json:"old_config"`
}

func loadState(dir string) (diskState, bool, error) {
	b, err := os.ReadFile(filepath.Join(dir, stateFileName))
	if errors.Is(err, fs.ErrNotExist) {
		return diskState{}, false, nil
	}
	if err != nil {
		return diskState{}, false, err
	}
	var st diskState
	if err := json.Unmarshal(b, &st); err != nil {
		return diskState{}, false, fmt.Errorf("decode %s: %w", stateFileName, err)
	}
	return st, true, nil
}

func saveState(dir string, st diskState) error {
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return writeFileAtomic(dir, stateFileName, b)
}
