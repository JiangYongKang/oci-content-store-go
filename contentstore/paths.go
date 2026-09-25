package contentstore

import "path/filepath"

// On-disk layout:
//
//	root/
//	  blobs/sha256/<hex>      committed, immutable content
//	  ingest/<random>         in-progress writes (never visible)
//	  state.json              tags -> manifest digest (atomic writes)
//	  lock                    cross-process flock
func blobPath(root string, d Digest) string {
	return filepath.Join(root, "blobs", d.alg, d.hex)
}

func ingestDir(root string) string { return filepath.Join(root, "ingest") }
func statePath(root string) string { return filepath.Join(root, "state.json") }
func lockPath(root string) string  { return filepath.Join(root, "lock") }
