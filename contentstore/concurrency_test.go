package contentstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// verifyRead opens digest, reads it fully, checks the sha256, and closes the
// reader promptly (while it is open, GC is blocked from removing the blob).
func verifyRead(t *testing.T, s *Store, digest string) error {
	t.Helper()
	rc, err := s.GetBlob(context.Background(), digest)
	if err != nil {
		return err // ErrNotFound is acceptable if the caller expects churn
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("read %s: %w", digest, err)
	}
	if "sha256:"+sha256Hex(b) != digest {
		return fmt.Errorf("integrity violation: blob %s content hashes to sha256:%s", digest, sha256Hex(b))
	}
	return nil
}

// TestConcurrentDedup: many goroutines write the same handful of payloads.
// Regardless of interleaving, each digest must occupy exactly one blob and
// always read back byte-identical.
func TestConcurrentDedup(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	payloads := [][]byte{
		[]byte("shared-payload-A"), []byte("shared-payload-B"),
		[]byte("shared-payload-C"), bytes.Repeat([]byte("Z"), 1<<20),
	}
	var wg sync.WaitGroup
	errCh := make(chan error, 64)
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for round := 0; round < 20; round++ {
				p := payloads[(g+round)%len(payloads)]
				d := digestOf(p)
				if err := s.PutBlob(ctx, d, int64(len(p)), bytes.NewReader(p)); err != nil {
					errCh <- fmt.Errorf("writer g=%d round=%d: %w", g, round, err)
					return
				}
				if err := verifyRead(t, s, d); err != nil {
					errCh <- fmt.Errorf("reader g=%d round=%d: %w", g, round, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if names := ingestEntries(t, s); len(names) != 0 {
		t.Fatalf("ingest residue after concurrent writes: %v", names)
	}
	if names := blobFiles(t, s); len(names) != len(payloads) {
		t.Fatalf("blob count = %d, want %d (dedup across concurrent writers)", len(names), len(payloads))
	}
	t.Logf("concurrent dedup: %d goroutines produced exactly %d blobs", 16, len(payloads))
}

// TestConcurrentInterleaved runs tag/manifest writers, tag resolvers,
// integrity-checking readers and GC in parallel.
func TestConcurrentInterleaved(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Seed a stable tag that never moves: its closure must survive all GCs.
	stableMD, stableCfg, stableLayers := putImage(t, s, "stable", manifestSpec{
		config: []byte("stable-config"),
		layers: [][]byte{[]byte("stable-layer-0"), []byte("stable-layer-1")},
	})

	const writers = 6
	const readers = 8
	const gcWorkers = 2
	var wg sync.WaitGroup
	var notFoundOK, readsOK, writesOK, gcRuns int64
	errCh := make(chan error, 1024)
	fail := func(format string, a ...any) {
		select {
		case errCh <- fmt.Errorf(format, a...):
		default:
		}
	}

	// Writers: unique blobs -> manifest -> move "latest". Each image's
	// payloads are unique to that iteration, so after GC each old image is
	// collected unless a reader happens to hold it open.
	// Writers publish a full image and move "latest" in one logical step.
	// GC is allowed to reclaim blobs that were uploaded but not yet
	// referenced (they are unreferenced by definition), so assembly is
	// retried when GC wins that window. Once Tag returns, the closure is
	// protected.
	publish := func(w, i int) error {
		cfg := []byte(fmt.Sprintf("cfg-w%d-i%d-%d", w, i, time.Now().UnixNano()))
		layer := []byte(fmt.Sprintf("layer-w%d-i%d", w, i))
		cfgD, err := putBlobOnce(ctx, s, cfg)
		if err != nil {
			return err
		}
		layerD, err := putBlobOnce(ctx, s, layer)
		if err != nil {
			return err
		}
		mj, _ := json.Marshal(wireManifest{
			SchemaVersion: 2,
			Config:        wireDescriptor{Digest: cfgD},
			Layers:        []wireDescriptor{{Digest: layerD}},
		})
		md, err := s.PutManifest(ctx, mj)
		if err != nil {
			return err
		}
		if _, err := s.Tag(ctx, "latest", md.String()); err != nil {
			return err
		}
		return nil
	}
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			i := 0
			for ctx.Err() == nil {
				err := publish(w, i)
				switch {
				case err == nil:
					atomic.AddInt64(&writesOK, 1)
					i++
				case ctx.Err() != nil:
					return
				case errorIs(err, ErrReferencedBlobMissing) || errorIs(err, ErrNotFound):
					// GC reclaimed an unreferenced blob between upload and
					// tagging: a legitimate, distinguishable race. Retry.
					atomic.AddInt64(&notFoundOK, 1)
					time.Sleep(100 * time.Microsecond)
				default:
					fail("writer %d: %v", w, err)
					return
				}
			}
		}(w)
	}

	// Readers: continuously resolve both tags and verify the full closure
	// of whatever they successfully resolve.
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for ctx.Err() == nil {
				tag := "latest"
				if r%2 == 0 {
					tag = "stable"
				}
				d, err := s.ResolveTag(ctx, tag)
				if err != nil {
					if ctx.Err() != nil {
						return // shutdown
					}
					if errorIs(err, ErrNotFound) && tag == "latest" {
						time.Sleep(100 * time.Microsecond) // before first publish
						continue
					}
					fail("reader %d resolve %s: unexpected %v", r, tag, err)
					return
				}
				// Resolve and open are separate lock scopes; a "latest"
				// manifest can be untagged+collected in between, and that
				// NotFound is a legitimate concurrent outcome.
				rc, err := s.GetBlob(ctx, d.String())
				if err != nil {
					if ctx.Err() != nil {
						return // shutdown
					}
					if errorIs(err, ErrNotFound) && tag == "latest" {
						atomic.AddInt64(&notFoundOK, 1)
						continue
					}
					fail("reader %d open manifest %s: %v", r, d, err)
					return
				}
				raw, _ := io.ReadAll(rc)
				rc.Close()
				if "sha256:"+sha256Hex(raw) != d.String() {
					fail("reader %d: manifest %s corrupted in store", r, d)
					return
				}
				m, err := parseManifest(raw)
				if err != nil {
					fail("reader %d: stored manifest %s unparseable: %w", r, d, err)
					return
				}
				// Config/layers of a *currently tagged* manifest must be
				// readable: for "stable" always; for "latest" only while this
				// exact digest is still the tag value (re-resolve to check).
				cur, err := s.ResolveTag(ctx, tag)
				if err != nil {
					if ctx.Err() != nil {
						return // shutdown
					}
					fail("reader %d re-resolve: %v", r, err)
					return
				}
				stillTagged := cur.String() == d.String()
				if !stillTagged && tag == "latest" {
					atomic.AddInt64(&readsOK, 1)
					continue
				}
				raced := false
				for _, ref := range append([]Digest{m.Config}, m.Layers...) {
					if err := verifyRead(t, s, ref.String()); err != nil {
						// "latest" may have moved in the gap after our
						// re-resolve check; then the old closure is garbage.
						if errorIs(err, ErrNotFound) && tag == "latest" {
							raced = true
							break
						}
						fail("reader %d closure of %s: %v", r, d, err)
						return
					}
				}
				if raced {
					atomic.AddInt64(&notFoundOK, 1)
					continue
				}
				atomic.AddInt64(&readsOK, 1)
			}
		}(r)
	}

	// GC workers.
	for g := 0; g < gcWorkers; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				if _, err := s.GC(ctx); err != nil {
					if ctx.Err() != nil {
						return // shutdown
					}
					fail("gc: %v", err)
					return
				}
				atomic.AddInt64(&gcRuns, 1)
				time.Sleep(2 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
	cancel()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	t.Logf("interleaved: writes=%d verified-reads=%d gc-runs=%d benign-notfound=%d",
		atomic.LoadInt64(&writesOK), atomic.LoadInt64(&readsOK),
		atomic.LoadInt64(&gcRuns), atomic.LoadInt64(&notFoundOK))

	// Post-run invariants.
	if names := ingestEntries(t, s); len(names) != 0 {
		t.Fatalf("ingest residue: %v", names)
	}
	// The stable image's complete closure survived every GC run.
	for _, d := range append([]string{stableMD, stableCfg}, stableLayers...) {
		if err := verifyRead(t, s, d); err != nil {
			t.Fatalf("stable closure damaged after concurrent run: %v", err)
		}
	}
	if _, _, err := s.GetManifest(context.Background(), "stable"); err != nil {
		t.Fatalf("stable tag broken: %v", err)
	}
	// Final GC converges; latest still resolves and its closure reads whole.
	if _, err := s.GC(context.Background()); err != nil {
		t.Fatalf("final GC: %v", err)
	}
	d, m, err := s.GetManifest(context.Background(), "latest")
	if err != nil {
		t.Fatalf("latest after run: %v", err)
	}
	for _, ref := range append([]Digest{m.Config}, m.Layers...) {
		if err := verifyRead(t, s, ref.String()); err != nil {
			t.Fatalf("latest closure %s damaged: %v", ref, err)
		}
	}
	t.Logf("final state consistent: latest=%s, stable intact, no ingest residue", d)
}

// putBlobOnce is a concurrency-safe PutBlob for unique/known payloads.
func putBlobOnce(ctx context.Context, s *Store, b []byte) (string, error) {
	d := digestOf(b)
	return d, s.PutBlob(ctx, d, int64(len(b)), bytes.NewReader(b))
}
