package contentstore

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestRecoverFromInterruptedIngest: a crash leaves a partial upload in
// ingest/. On reopen it must be discarded and must not participate in dedup.
func TestRecoverFromInterruptedIngest(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := Open(ctx, dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	s.SetLogger(testLogger{t})

	content := []byte("content-for-dedup-check")
	d := putBytes(t, s, content)

	// Simulate a crashed writer: partial file left behind in ingest/.
	junk := filepath.Join(ingestDir(s.root), "upload-crashed-001")
	if err := os.WriteFile(junk, []byte("partial-bytes"), 0o644); err != nil {
		t.Fatalf("seed junk: %v", err)
	}
	t.Log("simulated crash: left partial ingest/upload-crashed-001")

	s2, err := Open(ctx, dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	s2.SetLogger(testLogger{t})

	if names := ingestEntries(t, s2); len(names) != 0 {
		t.Fatalf("ingest not cleaned after reopen: %v", names)
	}
	t.Log("after reopen: ingest/ is empty (partial upload discarded)")

	// Historical data is readable without any rebuild/migration.
	if got := readBlob(t, s2, d); string(got) != string(content) {
		t.Fatalf("historical blob mismatch: %q", got)
	}
	t.Logf("historical blob %s still readable after reopen (no rebuild)", d)

	// The discarded partial content must not have poisoned dedup: writing the
	// real content still succeeds and produces exactly one blob.
	if err := s2.PutBlob(ctx, d, int64(len(content)), bytesReader(content)); err != nil {
		t.Fatalf("PutBlob after recovery: %v", err)
	}
	if names := blobFiles(t, s2); len(names) != 1 {
		t.Fatalf("blob count = %d, want 1 (partial ingest must not dedup)", len(names))
	}
}

// TestRecoverPrunesDanglingTag: state.json references a manifest whose blob
// vanished (e.g. manual deletion or lost directory entry). Reopen prunes the
// tag; intact tags keep working.
func TestRecoverPrunesDanglingTag(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := Open(ctx, dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	s.SetLogger(testLogger{t})
	md, _, _ := putImage(t, s, "good", manifestSpec{config: []byte("cfg"), layers: [][]byte{[]byte("L")}})

	// Inject a tag pointing at a syntactically valid but absent digest.
	st, err := loadState(dir)
	if err != nil {
		t.Fatal(err)
	}
	st.Tags["ghost"] = digestOf([]byte("missing-manifest"))
	if err := writeState(dir, st); err != nil {
		t.Fatal(err)
	}
	// And one with a malformed digest string.
	st.Tags["badref"] = "not-a-digest"
	if err := writeState(dir, st); err != nil {
		t.Fatal(err)
	}
	t.Logf("injected dangling tags (ghost -> %s, badref -> malformed); good -> %s", st.Tags["ghost"], md)

	s2, err := Open(ctx, dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	s2.SetLogger(testLogger{t})

	if _, err := s2.ResolveTag(ctx, "ghost"); !errorsIsNotFound(err) {
		t.Fatalf("ghost tag after reopen: got %v, want ErrNotFound", err)
	}
	if _, err := s2.ResolveTag(ctx, "badref"); !errorsIsNotFound(err) {
		t.Fatalf("badref tag after reopen: got %v, want ErrNotFound", err)
	}
	got, m, err := s2.GetManifest(ctx, "good")
	if err != nil || got.String() != md {
		t.Fatalf("good tag broken after recovery: %s %+v %v", got, m, err)
	}
	t.Log("after reopen: dangling tags pruned, intact tag 'good' resolves and parses")

	// Store is self-consistent: GC removes nothing because the good image is
	// still fully referenced.
	if n, err := s2.GC(ctx); err != nil || n != 0 {
		t.Fatalf("GC after recovery = (%d, %v), want (0, nil)", n, err)
	}
}

// TestStateFileFormatStable: the on-disk state.json format is plain JSON with
// a version field; a hand-written v1 file is accepted as-is.
func TestStateFileFormatStable(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "blobs", "sha256"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "ingest"), 0o755); err != nil {
		t.Fatal(err)
	}
	manual := storeState{Version: 1, Tags: map[string]string{}}
	b, _ := json.MarshalIndent(manual, "", "  ")
	if err := os.WriteFile(statePath(dir), b, 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), dir)
	if err != nil {
		t.Fatalf("Open on hand-written v1 state: %v", err)
	}
	st, err := loadState(s.root)
	if err != nil || st.Version != 1 {
		t.Fatalf("v1 state not accepted: %+v %v", st, err)
	}
	t.Logf("state.json v1 accepted unchanged: %s", b)
}

func errorsIsNotFound(err error) bool { return err != nil && errorIs(err, ErrNotFound) }
