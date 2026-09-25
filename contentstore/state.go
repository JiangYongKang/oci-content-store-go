package contentstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"unicode"
)

// storeState is the persisted metadata envelope.
type storeState struct {
	Version int               `json:"version"`
	Tags    map[string]string `json:"tags"`
}

const stateVersion = 1

// maxTagLen mirrors the common registry tag length limit.
const maxTagLen = 128

func newState() *storeState {
	return &storeState{Version: stateVersion, Tags: map[string]string{}}
}

// loadState reads state.json. An absent file means a freshly initialized
// store and yields an empty state; a state written by a *newer* version is
// rejected so older binaries never corrupt newer metadata.
func loadState(root string) (*storeState, error) {
	data, err := os.ReadFile(statePath(root))
	if errors.Is(err, fs.ErrNotExist) {
		return newState(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("read state: %w", err)
	}
	var st storeState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("%w: state.json: %v", ErrInvalidContent, err)
	}
	if st.Version > stateVersion {
		return nil, fmt.Errorf("state.json version %d is newer than supported %d", st.Version, stateVersion)
	}
	if st.Tags == nil {
		st.Tags = map[string]string{}
	}
	return &st, nil
}

// writeState atomically replaces state.json: write a temp file in the same
// directory, fsync it, rename it into place, then fsync the directory so the
// rename survives a power failure.
func writeState(root string, st *storeState) error {
	if st.Version == 0 {
		st.Version = stateVersion
	}
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("encode state: %w", err)
	}
	dir := root
	tmp, err := os.CreateTemp(dir, ".state.json.tmp-*")
	if err != nil {
		return fmt.Errorf("create state temp: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("write state temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("fsync state temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close state temp: %w", err)
	}
	if err := os.Rename(tmpName, statePath(root)); err != nil {
		cleanup()
		return fmt.Errorf("rename state: %w", err)
	}
	if err := fsyncDir(dir); err != nil {
		return fmt.Errorf("fsync state dir: %w", err)
	}
	return nil
}

// fsyncDir flushes directory entry changes (needed after rename/unlink).
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// validateTag rejects empty, over-long or non-graphical tags.
func validateTag(tag string) bool {
	if tag == "" || len(tag) > maxTagLen {
		return false
	}
	if tag == "." || tag == ".." {
		return false
	}
	for _, r := range tag {
		if r == unicode.ReplacementChar {
			return false
		}
		if !unicode.IsPrint(r) || unicode.IsControl(r) {
			return false
		}
		if r == '/' || r == filepath.Separator {
			return false
		}
	}
	return true
}
