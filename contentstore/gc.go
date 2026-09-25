package contentstore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// markReachableLocked computes the set of blob digests reachable from tags:
// every tagged manifest, plus the config and layers each manifest references.
// Caller holds the exclusive lock.
//
// Missing manifests or referenced blobs contradict tag/state invariants
// (they are enforced at Tag/PutManifest time and repaired at Open time); they
// are logged as inconsistencies and skipped rather than crashing GC.
func markReachableLocked(ctx context.Context, s *Store, st *storeState) (map[string]bool, error) {
	reachable := map[string]bool{}
	for tag, raw := range st.Tags {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		md, err := ParseDigest(raw)
		if err != nil {
			s.logf("gc mark: tag %q has malformed manifest digest %q; skipping", tag, raw)
			continue
		}
		if !s.blobExistsLocked(md) {
			s.logf("gc mark: tag %q points to missing manifest %s; skipping", tag, md.raw)
			continue
		}
		data, err := s.readAllBlobLocked(md)
		if err != nil {
			return nil, fmt.Errorf("gc read manifest %s: %w", md.raw, err)
		}
		m, err := parseManifest(data)
		if err != nil {
			s.logf("gc mark: tag %q manifest %s unparseable (%v); skipping tag", tag, md.raw, err)
			continue
		}
		reachable[md.raw] = true
		reachable[m.Config.raw] = true
		for _, ld := range m.Layers {
			reachable[ld.raw] = true
		}
	}
	return reachable, nil
}

// sweepLocked removes every committed blob whose digest is not reachable.
// Caller holds the exclusive lock. Unlinks are followed by a directory fsync
// so an unreachable blob cannot "come back" after a crash.
func sweepLocked(ctx context.Context, s *Store, reachable map[string]bool) (int, error) {
	dir := filepath.Join(s.root, "blobs", "sha256")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, fmt.Errorf("gc read blobs: %w", err)
	}
	removed := 0
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return removed, err
		}
		if e.IsDir() {
			continue
		}
		name := e.Name()
		// Only remove files that look like committed sha256 digests;
		// anything foreign is left untouched for forward compatibility.
		d, err := ParseDigest("sha256:" + name)
		if err != nil {
			s.logf("gc sweep: ignoring non-digest file blobs/sha256/%s", name)
			continue
		}
		if reachable[d.raw] {
			continue
		}
		if err := os.Remove(filepath.Join(dir, name)); err != nil {
			return removed, fmt.Errorf("gc remove %s: %w", d.raw, err)
		}
		removed++
		s.logf("gc sweep: removed unreachable blob %s", d.raw)
	}
	if removed > 0 {
		if err := fsyncDir(dir); err != nil {
			return removed, fmt.Errorf("gc fsync blob dir: %w", err)
		}
	}
	return removed, nil
}
