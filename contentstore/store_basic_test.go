package contentstore

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestPutGetAndDedup(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	content := []byte("hello-image-layer-0001")
	d := digestOf(content)

	if err := s.PutBlob(ctx, d, int64(len(content)), bytes.NewReader(content)); err != nil {
		t.Fatalf("PutBlob: %v", err)
	}
	got := readBlob(t, s, d)
	if !bytes.Equal(got, content) {
		t.Fatalf("content round-trip mismatch: got %q want %q", got, content)
	}
	t.Logf("round-trip ok: digest=%s size=%d", d, len(content))

	// Write the same content again: must reuse the single stored copy.
	before := blobFiles(t, s)
	if err := s.PutBlob(ctx, d, int64(len(content)), bytes.NewReader(content)); err != nil {
		t.Fatalf("PutBlob duplicate: %v", err)
	}
	after := blobFiles(t, s)
	if len(before) != 1 || len(after) != 1 {
		t.Fatalf("dedup failed: before=%v after=%v", before, after)
	}
	if before[0] != after[0] {
		t.Fatalf("dedup reused different file: %s vs %s", before[0], after[0])
	}
	t.Logf("dedup ok: duplicate write reused %s (still exactly 1 blob)", after[0])

	stats, err := s.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.Blobs != 1 {
		t.Fatalf("Stats.Blobs = %d, want 1", stats.Blobs)
	}
}

func TestPutFailuresLeaveNoResidue(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	content := []byte("payload-that-will-not-match")
	d := digestOf(content)

	// 1) digest mismatch: declare a different digest.
	other := digestOf([]byte("totally-different-payload"))
	err := s.PutBlob(ctx, other, int64(len(content)), bytes.NewReader(content))
	requireErrIs(t, err, ErrDigestMismatch, "digest mismatch")

	// 2) size mismatch.
	err = s.PutBlob(ctx, d, int64(len(content)+10), bytes.NewReader(content))
	requireErrIs(t, err, ErrSizeMismatch, "size mismatch")

	// 3) size larger than stream: EOF mid-way must also be a size mismatch.
	err = s.PutBlob(ctx, d, 1<<20, bytes.NewReader(content))
	requireErrIs(t, err, ErrSizeMismatch, "stream shorter than declared")

	// 4) malformed digest values.
	for _, bad := range []string{"", "nocolon", "sha1:aaaa", "sha256:xyz", "sha256:" + d[7:8] + "short"} {
		err := s.PutBlob(ctx, bad, 1, bytes.NewReader([]byte("x")))
		requireErrIs(t, err, ErrInvalidDigest, "invalid digest "+bad)
	}

	// Nothing may be visible or left behind.
	if names := blobFiles(t, s); len(names) != 0 {
		t.Fatalf("failed writes left committed blobs: %v", names)
	}
	if names := ingestEntries(t, s); len(names) != 0 {
		t.Fatalf("failed writes left ingest files: %v", names)
	}
	// And the digest must not resolve to anything.
	if _, err := s.GetBlob(ctx, d); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetBlob after failed put: got %v, want ErrNotFound", err)
	}
	t.Log("no residue: blobs/ empty, ingest/ empty, digest unreadable")
}

func TestGetMissing(t *testing.T) {
	s := newTestStore(t)
	d := digestOf([]byte("never-stored"))
	if _, err := s.GetBlob(context.Background(), d); err == nil {
		t.Fatal("GetBlob on missing content unexpectedly succeeded")
	} else {
		requireErrIs(t, err, ErrNotFound, "missing blob")
	}
}

func TestManifestValidation(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	cfg := []byte("{}")
	cfgD := putBytes(t, s, cfg)
	layer := []byte("layer-data")
	layerD := putBytes(t, s, layer)

	good := `{"schemaVersion":2,"config":{"digest":"` + cfgD + `"},"layers":[{"digest":"` + layerD + `"}]}`
	md, err := s.PutManifest(ctx, []byte(good))
	if err != nil {
		t.Fatalf("PutManifest good: %v", err)
	}
	t.Logf("valid manifest stored: %s", md)

	for name, body := range map[string]string{
		"empty":           ``,
		"not json":        `not-json`,
		"unknown field":   `{"schemaVersion":2,"bogus":1,"config":{"digest":"` + cfgD + `"},"layers":[]}`,
		"bad cfg digest":  `{"config":{"digest":"sha256:bad"},"layers":[]}`,
		"bad layer d":     `{"config":{"digest":"` + cfgD + `"},"layers":[{"digest":"nope"}]}`,
		"dup layer":       `{"config":{"digest":"` + cfgD + `"},"layers":[{"digest":"` + layerD + `"},{"digest":"` + layerD + `"}]}`,
		"layer is config": `{"config":{"digest":"` + layerD + `"},"layers":[{"digest":"` + layerD + `"}]}`,
	} {
		_, err := s.PutManifest(ctx, []byte(body))
		requireErrIs(t, err, ErrInvalidContent, "manifest "+name)
	}

	// Referenced blob missing: syntactically fine but nothing stored.
	ghost := digestOf([]byte("ghost"))
	badRef := `{"config":{"digest":"` + ghost + `"},"layers":[]}`
	_, err = s.PutManifest(ctx, []byte(badRef))
	requireErrIs(t, err, ErrReferencedBlobMissing, "missing config")

	// Failed manifest attempts must not have created blobs (only cfg+layer+good manifest).
	stats, _ := s.Stats(ctx)
	if stats.Blobs != 3 {
		t.Fatalf("Blob count after rejected manifests = %d, want 3", stats.Blobs)
	}
	t.Logf("rejected manifests left no blobs (total still %d)", stats.Blobs)
}

func TestTagResolveAndGetManifest(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	md, cfgD, layerDs := putImage(t, s, "v1", manifestSpec{
		config: []byte(`{"os":"linux"}`),
		layers: [][]byte{[]byte("layer-A")},
	})
	resolved, err := s.ResolveTag(ctx, "v1")
	if err != nil {
		t.Fatalf("ResolveTag: %v", err)
	}
	if resolved.String() != md {
		t.Fatalf("ResolveTag = %s, want %s", resolved, md)
	}
	gotD, m, err := s.GetManifest(ctx, "v1")
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	if gotD.String() != md || m.Config.String() != cfgD || len(m.Layers) != 1 || m.Layers[0].String() != layerDs[0] {
		t.Fatalf("parsed manifest mismatch: %+v cfg=%s layer0=%s", m, m.Config, m.Layers[0])
	}
	t.Logf("tag v1 -> manifest %s (config %s, layers %v)", md, cfgD, layerDs)

	// Tagging a non-manifest blob is rejected.
	if _, err := s.Tag(ctx, "bad", cfgD); err == nil {
		t.Fatal("Tag of plain config blob unexpectedly succeeded")
	} else {
		requireErrIs(t, err, ErrInvalidContent, "tag non-manifest")
	}
	// Invalid tag names.
	for _, bad := range []string{"", "a/b", string([]byte{0, 1})} {
		_, err := s.Tag(ctx, bad, md)
		requireErrIs(t, err, ErrInvalidTag, "invalid tag")
	}
}
