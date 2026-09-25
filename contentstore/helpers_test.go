package contentstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

type testLogger struct{ t *testing.T }

func (l testLogger) Printf(format string, args ...any) {
	l.t.Helper()
	l.t.Logf("[store] "+format, args...)
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(context.Background(), dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	s.SetLogger(testLogger{t})
	return s
}

func digestOf(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func putBytes(t *testing.T, s *Store, b []byte) string {
	t.Helper()
	d := digestOf(b)
	if err := s.PutBlob(context.Background(), d, int64(len(b)), bytes.NewReader(b)); err != nil {
		t.Fatalf("PutBlob %s: %v", d, err)
	}
	return d
}

func readBlob(t *testing.T, s *Store, d string) []byte {
	t.Helper()
	rc, err := s.GetBlob(context.Background(), d)
	if err != nil {
		t.Fatalf("GetBlob %s: %v", d, err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read %s: %v", d, err)
	}
	return b
}

// manifestSpec describes one image: config bytes + layer payloads.
type manifestSpec struct {
	config []byte
	layers [][]byte
}

// putImage stores config, layers and manifest, then tags it.
func putImage(t *testing.T, s *Store, tag string, spec manifestSpec) (string, string, []string) {
	t.Helper()
	cfgD := putBytes(t, s, spec.config)
	layerDs := make([]string, 0, len(spec.layers))
	descs := make([]wireDescriptor, 0, len(spec.layers))
	for _, l := range spec.layers {
		ld := putBytes(t, s, l)
		layerDs = append(layerDs, ld)
		descs = append(descs, wireDescriptor{Digest: ld, Size: int64(len(l))})
	}
	mj, err := json.Marshal(wireManifest{
		SchemaVersion: 2,
		Config:        wireDescriptor{Digest: cfgD, Size: int64(len(spec.config))},
		Layers:        descs,
	})
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	md, err := s.PutManifest(context.Background(), mj)
	if err != nil {
		t.Fatalf("PutManifest: %v", err)
	}
	if _, err := s.Tag(context.Background(), tag, md.String()); err != nil {
		t.Fatalf("Tag %s: %v", tag, err)
	}
	return md.String(), cfgD, layerDs
}

func ingestEntries(t *testing.T, s *Store) []string {
	t.Helper()
	es, err := os.ReadDir(ingestDir(s.root))
	if err != nil {
		t.Fatalf("read ingest: %v", err)
	}
	names := make([]string, 0, len(es))
	for _, e := range es {
		names = append(names, e.Name())
	}
	return names
}

func blobFiles(t *testing.T, s *Store) []string {
	t.Helper()
	es, err := os.ReadDir(filepath.Join(s.root, "blobs", "sha256"))
	if err != nil {
		t.Fatalf("read blobs: %v", err)
	}
	names := make([]string, 0, len(es))
	for _, e := range es {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	return names
}

func requireErrIs(t *testing.T, err error, target error, msg string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected error wrapping %v, got nil", msg, target)
	}
	if !errors.Is(err, target) {
		t.Fatalf("%s: expected error wrapping %v, got %v", msg, target, err)
	}
	t.Logf("%s: got expected failure (%v): %v", msg, target, err)
}

func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }

func errorIs(err, target error) bool { return errors.Is(err, target) }
