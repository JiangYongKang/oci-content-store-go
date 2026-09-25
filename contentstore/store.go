package contentstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// Logger receives key store operations and GC decisions. Tests wire it to
// t.Logf so judgments are visible in test output.
type Logger interface {
	Printf(format string, args ...any)
}

type discardLogger struct{}

func (discardLogger) Printf(string, ...any) {}

// Store is a local content-addressed image store.
type Store struct {
	root string
	lk   *fileLock
	log  Logger
}

// SetLogger installs an operation logger (default: discard).
func (s *Store) SetLogger(l Logger) {
	if l == nil {
		l = discardLogger{}
	}
	s.log = l
}

func (s *Store) logf(format string, args ...any) { s.log.Printf(format, args...) }

// Open opens (and if necessary initializes) a store under root and performs
// crash recovery: incomplete ingest uploads are discarded and tags pointing
// at missing manifests are pruned. It takes the exclusive lock while doing so.
func Open(ctx context.Context, root string) (*Store, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("init store root: %w", err)
	}
	s := &Store{root: root, lk: newFileLock(root), log: discardLogger{}}
	for _, d := range []string{"blobs", "blobs/sha256", "ingest"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			return nil, fmt.Errorf("init %s: %w", d, err)
		}
	}
	if err := s.recover(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// recover clears unfinished uploads and repairs metadata after a crash.
func (s *Store) recover(ctx context.Context) error {
	if err := s.lk.lock(); err != nil {
		return err
	}
	defer s.lk.unlock()

	// Rule: anything left in ingest/ never participated in dedup or
	// reference counting; delete it wholesale.
	id := ingestDir(s.root)
	entries, err := os.ReadDir(id)
	if err != nil {
		return fmt.Errorf("recovery read ingest: %w", err)
	}
	removed := 0
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(id, e.Name())); err != nil {
			return fmt.Errorf("recovery remove ingest %s: %w", e.Name(), err)
		}
		removed++
	}
	if removed > 0 {
		s.logf("recovery: discarded %d incomplete ingest upload(s)", removed)
	}

	st, err := loadState(s.root)
	if err != nil {
		return err
	}
	pruned := 0
	for tag, md := range st.Tags {
		if err := ctx.Err(); err != nil {
			return err
		}
		d, perr := ParseDigest(md)
		var missing bool
		switch {
		case perr != nil:
			missing = true
		default:
			if _, err := os.Stat(blobPath(s.root, d)); errors.Is(err, fs.ErrNotExist) {
				missing = true
			} else if err != nil {
				return fmt.Errorf("recovery stat tag %s: %w", tag, err)
			}
		}
		if missing {
			s.logf("recovery: prune tag %q -> %s (manifest missing)", tag, md)
			delete(st.Tags, tag)
			pruned++
		}
	}
	if pruned > 0 {
		if err := writeState(s.root, st); err != nil {
			return fmt.Errorf("recovery write state: %w", err)
		}
	}
	s.logf("recovery: store self-consistent (%d tags live, %d tag(s) pruned)", len(st.Tags), pruned)
	return nil
}

// Root returns the root directory of the store.
func (s *Store) Root() string { return s.root }

// copyContext copies src to dst in chunks, aborting when ctx is canceled.
func copyContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	buf := make([]byte, 32*1024)
	var n int64
	for {
		if err := ctx.Err(); err != nil {
			return n, err
		}
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw < 0 || nr < nw {
				return n, io.ErrShortWrite
			}
			n += int64(nw)
			if ew != nil {
				return n, ew
			}
			if nr != nw {
				return n, io.ErrShortWrite
			}
		}
		if er != nil {
			if errors.Is(er, io.EOF) {
				return n, nil
			}
			return n, er
		}
	}
}

// PutBlob verifies the stream's length and sha256 digest against declaredSize
// and declaredDigest, then commits it atomically. Identical content reuses the
// existing blob (deduplication). On any failure the partial ingest file is
// removed and nothing visible is left behind.
func (s *Store) PutBlob(ctx context.Context, declaredDigest string, declaredSize int64, r io.Reader) error {
	d, err := ParseDigest(declaredDigest)
	if err != nil {
		return err
	}
	if declaredSize < 0 {
		return fmt.Errorf("%w: declared size %d is negative", ErrSizeMismatch, declaredSize)
	}

	tmp, err := os.CreateTemp(ingestDir(s.root), "upload-*")
	if err != nil {
		return fmt.Errorf("create ingest: %w", err)
	}
	tmpName := tmp.Name()
	abort := func(cause error) error {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		if cause != nil {
			s.logf("put %s: REJECTED: %v", d.raw, cause)
		}
		return cause
	}

	h := sha256.New()
	n, err := copyContext(ctx, io.MultiWriter(tmp, h), r)
	if err != nil {
		return abort(err)
	}
	if n != declaredSize {
		return abort(fmt.Errorf("%w: declared %d, actual %d", ErrSizeMismatch, declaredSize, n))
	}
	got := "sha256:" + hex.EncodeToString(h.Sum(nil))
	if got != d.raw {
		return abort(fmt.Errorf("%w: declared %s, actual %s", ErrDigestMismatch, d.raw, got))
	}
	if err := tmp.Sync(); err != nil {
		return abort(fmt.Errorf("fsync ingest: %w", err))
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("close ingest: %w", err)
	}

	// Commit phase: exclusive lock makes the exists-check + rename atomic
	// against concurrent writers and against GC.
	if err := s.lk.lock(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	defer s.lk.unlock()

	if _, err := os.Stat(blobPath(s.root, d)); err == nil {
		_ = os.Remove(tmpName)
		s.logf("put %s: content already exists, reused (dedup, %d bytes)", d.raw, n)
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		_ = os.Remove(tmpName)
		return fmt.Errorf("stat blob: %w", err)
	}

	if err := os.Rename(tmpName, blobPath(s.root, d)); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("commit blob: %w", err)
	}
	if err := fsyncDir(filepath.Join(s.root, "blobs", "sha256")); err != nil {
		return fmt.Errorf("fsync blob dir: %w", err)
	}
	s.logf("put %s: committed %d bytes", d.raw, n)
	return nil
}

// lockedReadCloser keeps the shared store lock held until the caller closes
// the blob, so concurrent GC cannot reclaim an in-use blob.
type lockedReadCloser struct {
	rc io.ReadCloser
	lk *fileLock
}

func (l *lockedReadCloser) Read(p []byte) (int, error) { return l.rc.Read(p) }
func (l *lockedReadCloser) Close() error {
	err := l.rc.Close()
	l.lk.unlock()
	return err
}

// GetBlob returns a reader for a committed blob. The caller must close the
// returned reader; the blob is protected from GC while it is open.
func (s *Store) GetBlob(ctx context.Context, digest string) (io.ReadCloser, error) {
	d, err := ParseDigest(digest)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.lk.rlock(); err != nil {
		return nil, err
	}
	f, err := os.Open(blobPath(s.root, d))
	if err != nil {
		s.lk.unlock()
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, d.raw)
		}
		return nil, fmt.Errorf("open blob %s: %w", d.raw, err)
	}
	return &lockedReadCloser{rc: f, lk: s.lk}, nil
}

// blobExistsLocked reports a committed blob's presence; caller holds a lock.
func (s *Store) blobExistsLocked(d Digest) bool {
	_, err := os.Stat(blobPath(s.root, d))
	return err == nil
}

// commitBytesLocked stores data under its own sha256 digest; caller holds the
// exclusive lock. Existing identical blobs are reused.
func (s *Store) commitBytesLocked(data []byte) (Digest, error) {
	sum := sha256.Sum256(data)
	d := Digest{alg: "sha256", hex: hex.EncodeToString(sum[:]), raw: "sha256:" + hex.EncodeToString(sum[:])}
	if s.blobExistsLocked(d) {
		s.logf("put %s: content already exists, reused (dedup, %d bytes)", d.raw, len(data))
		return d, nil
	}
	tmp, err := os.CreateTemp(ingestDir(s.root), "upload-*")
	if err != nil {
		return Digest{}, fmt.Errorf("create ingest: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return Digest{}, fmt.Errorf("write ingest: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return Digest{}, fmt.Errorf("fsync ingest: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return Digest{}, fmt.Errorf("close ingest: %w", err)
	}
	if err := os.Rename(tmpName, blobPath(s.root, d)); err != nil {
		_ = os.Remove(tmpName)
		return Digest{}, fmt.Errorf("commit blob: %w", err)
	}
	if err := fsyncDir(filepath.Join(s.root, "blobs", "sha256")); err != nil {
		return Digest{}, fmt.Errorf("fsync blob dir: %w", err)
	}
	s.logf("put %s: committed %d bytes", d.raw, len(data))
	return d, nil
}

// readAllBlobLocked reads a committed blob; caller holds at least a shared lock.
func (s *Store) readAllBlobLocked(d Digest) ([]byte, error) {
	b, err := os.ReadFile(blobPath(s.root, d))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, d.raw)
	}
	return b, err
}

// PutManifest validates the manifest JSON (well-formed, all referenced config
// and layer digests syntactically valid) and stores it as a blob under the
// digest of its canonical serialization. All referenced blobs must already be
// present; otherwise ErrReferencedBlobMissing is returned and nothing is
// written.
func (s *Store) PutManifest(ctx context.Context, data []byte) (Digest, error) {
	if err := ctx.Err(); err != nil {
		return Digest{}, err
	}
	m, err := parseManifest(data)
	if err != nil {
		s.logf("put manifest: REJECTED: %v", err)
		return Digest{}, err
	}
	canon := manifestJSON(m)
	if err := s.lk.lock(); err != nil {
		return Digest{}, err
	}
	defer s.lk.unlock()

	if !s.blobExistsLocked(m.Config) {
		err := fmt.Errorf("%w: config %s", ErrReferencedBlobMissing, m.Config.raw)
		s.logf("put manifest: REJECTED: %v", err)
		return Digest{}, err
	}
	for _, ld := range m.Layers {
		if !s.blobExistsLocked(ld) {
			err := fmt.Errorf("%w: layer %s", ErrReferencedBlobMissing, ld.raw)
			s.logf("put manifest: REJECTED: %v", err)
			return Digest{}, err
		}
	}
	d, err := s.commitBytesLocked(canon)
	if err != nil {
		return Digest{}, err
	}
	s.logf("put manifest: stored as %s (config=%s, layers=%d)", d.raw, m.Config.raw, len(m.Layers))
	return d, nil
}

// Tag points tag at an already-stored manifest digest.
func (s *Store) Tag(ctx context.Context, tag string, manifestDigest string) (Digest, error) {
	if !validateTag(tag) {
		return Digest{}, fmt.Errorf("%w: %q", ErrInvalidTag, tag)
	}
	d, err := ParseDigest(manifestDigest)
	if err != nil {
		return Digest{}, err
	}
	if err := ctx.Err(); err != nil {
		return Digest{}, err
	}
	if err := s.lk.lock(); err != nil {
		return Digest{}, err
	}
	defer s.lk.unlock()

	if !s.blobExistsLocked(d) {
		return Digest{}, fmt.Errorf("%w: manifest %s", ErrNotFound, d.raw)
	}
	// The target must actually parse as a manifest, otherwise tags could
	// protect arbitrary blobs from collection.
	raw, err := s.readAllBlobLocked(d)
	if err != nil {
		return Digest{}, err
	}
	if _, err := parseManifest(raw); err != nil {
		return Digest{}, fmt.Errorf("%w: target %s is not a valid manifest: %v", ErrInvalidContent, d.raw, err)
	}
	st, err := loadState(s.root)
	if err != nil {
		return Digest{}, err
	}
	prev := st.Tags[tag]
	st.Tags[tag] = d.raw
	if err := writeState(s.root, st); err != nil {
		return Digest{}, err
	}
	s.logf("tag %q: %s -> %s", tag, prev, d.raw)
	return d, nil
}

// ResolveTag maps a tag to its current manifest digest.
func (s *Store) ResolveTag(ctx context.Context, tag string) (Digest, error) {
	if !validateTag(tag) {
		return Digest{}, fmt.Errorf("%w: %q", ErrInvalidTag, tag)
	}
	if err := ctx.Err(); err != nil {
		return Digest{}, err
	}
	if err := s.lk.rlock(); err != nil {
		return Digest{}, err
	}
	defer s.lk.unlock()

	st, err := loadState(s.root)
	if err != nil {
		return Digest{}, err
	}
	raw, ok := st.Tags[tag]
	if !ok {
		return Digest{}, fmt.Errorf("%w: tag %q", ErrNotFound, tag)
	}
	return ParseDigest(raw)
}

// GetManifest resolves tag, reads and parses its manifest. The returned
// digest identifies the manifest blob.
func (s *Store) GetManifest(ctx context.Context, tag string) (Digest, Manifest, error) {
	d, err := s.ResolveTag(ctx, tag)
	if err != nil {
		return Digest{}, Manifest{}, err
	}
	if err := s.lk.rlock(); err != nil {
		return Digest{}, Manifest{}, err
	}
	defer s.lk.unlock()
	raw, err := s.readAllBlobLocked(d)
	if err != nil {
		return Digest{}, Manifest{}, err
	}
	m, err := parseManifest(raw)
	if err != nil {
		return Digest{}, Manifest{}, fmt.Errorf("%w: stored manifest %s corrupt: %v", ErrInvalidContent, d.raw, err)
	}
	return d, m, nil
}

// Untag removes a tag reference. Unknown tags return ErrNotFound.
func (s *Store) Untag(ctx context.Context, tag string) error {
	if !validateTag(tag) {
		return fmt.Errorf("%w: %q", ErrInvalidTag, tag)
	}
	if err := s.lk.lock(); err != nil {
		return err
	}
	defer s.lk.unlock()
	st, err := loadState(s.root)
	if err != nil {
		return err
	}
	if _, ok := st.Tags[tag]; !ok {
		return fmt.Errorf("%w: tag %q", ErrNotFound, tag)
	}
	delete(st.Tags, tag)
	if err := writeState(s.root, st); err != nil {
		return err
	}
	s.logf("tag %q: removed (content becomes unreferenced and is GC eligible)", tag)
	return nil
}

// GC performs mark-and-sweep under the exclusive lock: it keeps blobs
// reachable from tags (manifests plus transitively referenced config/layers)
// and removes every other committed blob. Returns the number of blobs removed.
// Open GetBlob readers hold the shared lock and therefore block GC until they
// close, so live content is never reclaimed.
func (s *Store) GC(ctx context.Context) (int, error) {
	if err := s.lk.lock(); err != nil {
		return 0, err
	}
	defer s.lk.unlock()

	st, err := loadState(s.root)
	if err != nil {
		return 0, err
	}
	reachable, err := markReachableLocked(ctx, s, st)
	if err != nil {
		return 0, err
	}
	removed, err := sweepLocked(ctx, s, reachable)
	if err != nil {
		return removed, err
	}
	s.logf("gc: %d blob(s) reachable, %d blob(s) removed", len(reachable), removed)
	return removed, nil
}

// Stats reports committed blob and tag counts (for tests/observability).
func (s *Store) Stats(ctx context.Context) (Stats, error) {
	if err := s.lk.rlock(); err != nil {
		return Stats{}, err
	}
	defer s.lk.unlock()
	st, err := loadState(s.root)
	if err != nil {
		return Stats{}, err
	}
	blobs := 0
	entries, err := os.ReadDir(filepath.Join(s.root, "blobs", "sha256"))
	if err != nil {
		return Stats{}, fmt.Errorf("stat blobs: %w", err)
	}
	for _, e := range entries {
		if !e.IsDir() {
			blobs++
		}
	}
	return Stats{Blobs: blobs, Tags: len(st.Tags)}, nil
}

// Stats is a point-in-time summary of the store.
type Stats struct {
	Blobs int
	Tags  int
}
