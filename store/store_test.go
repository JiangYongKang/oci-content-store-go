package store

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// testLogger 把存储内部的关键操作与判定依据打进测试日志。
type testLogger struct{ t *testing.T }

func (l testLogger) Printf(format string, args ...any) { l.t.Logf("store: "+format, args...) }

func openTestStore(t *testing.T) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(dir, testLogger{t})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s, dir
}

// mustPut 写入内容并断言成功，返回其描述符。
func mustPut(t *testing.T, s *Store, content []byte) Descriptor {
	t.Helper()
	desc := Descriptor{Digest: DigestOf(content), Size: int64(len(content))}
	if err := s.PutContent(context.Background(), desc, bytes.NewReader(content)); err != nil {
		t.Fatalf("PutContent(%s): %v", desc.Digest, err)
	}
	return desc
}

func blobCount(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, blobsDir))
	if err != nil {
		t.Fatalf("readdir blobs: %v", err)
	}
	return len(entries)
}

func TestPutGetRoundtrip(t *testing.T) {
	s, _ := openTestStore(t)
	ctx := context.Background()
	content := []byte("hello image layer")
	desc := mustPut(t, s, content)

	got, err := s.GetContent(ctx, desc.Digest)
	if err != nil {
		t.Fatalf("GetContent: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("roundtrip mismatch: got %q want %q", got, content)
	}

	stat, err := s.StatContent(ctx, desc.Digest)
	if err != nil {
		t.Fatalf("StatContent: %v", err)
	}
	if stat.Size != int64(len(content)) || stat.Digest != desc.Digest {
		t.Fatalf("stat = %+v, want digest=%s size=%d", stat, desc.Digest, len(content))
	}
	t.Logf("roundtrip ok digest=%s size=%d", desc.Digest, len(content))
}

func TestPutDuplicateReusesStorage(t *testing.T) {
	s, dir := openTestStore(t)
	content := []byte("deduplicated content")
	d1 := mustPut(t, s, content)
	d2 := mustPut(t, s, content)
	if d1 != d2 {
		t.Fatalf("same content produced different descriptors: %+v vs %+v", d1, d2)
	}
	if n := blobCount(t, dir); n != 1 {
		t.Fatalf("expected 1 blob on disk after duplicate put, got %d", n)
	}
	t.Log("duplicate put reused the same blob (1 file on disk)")
}

func TestPutDigestMismatchRejected(t *testing.T) {
	s, dir := openTestStore(t)
	content := []byte("actual content")
	// 声明一个与内容不符的（合法格式）摘要。
	wrong := Descriptor{Digest: DigestOf([]byte("other")), Size: int64(len(content))}
	err := s.PutContent(context.Background(), wrong, bytes.NewReader(content))
	if !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("expected ErrDigestMismatch, got %v", err)
	}
	if errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("digest mismatch must be distinguishable from size mismatch: %v", err)
	}
	// 不得留下残缺内容：blobs 与 uploads 都必须干净。
	if n := blobCount(t, dir); n != 0 {
		t.Fatalf("rejected put left %d blobs behind", n)
	}
	leftovers, _ := os.ReadDir(filepath.Join(dir, uploadsDir))
	if len(leftovers) != 0 {
		t.Fatalf("rejected put left %d temp files in uploads", len(leftovers))
	}
	// 用正确摘要重新写入必须成功（没有残留干扰）。
	mustPut(t, s, content)
	t.Log("digest mismatch rejected distinctly, no residue, retry succeeded")
}

func TestPutSizeMismatchRejected(t *testing.T) {
	s, dir := openTestStore(t)
	content := []byte("some bytes")
	desc := Descriptor{Digest: DigestOf(content), Size: int64(len(content)) + 5}
	err := s.PutContent(context.Background(), desc, bytes.NewReader(content))
	if !errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("expected ErrSizeMismatch, got %v", err)
	}
	if errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("size mismatch must be distinguishable from digest mismatch: %v", err)
	}
	if n := blobCount(t, dir); n != 0 {
		t.Fatalf("rejected put left %d blobs behind", n)
	}
	t.Log("size mismatch rejected distinctly, no residue")
}

func TestPutInvalidDigestRejected(t *testing.T) {
	s, _ := openTestStore(t)
	err := s.PutContent(context.Background(), Descriptor{Digest: "not-a-digest", Size: 1}, strings.NewReader("x"))
	if !errors.Is(err, ErrInvalidDigest) {
		t.Fatalf("expected ErrInvalidDigest, got %v", err)
	}
	t.Log("invalid digest rejected distinctly")
}

func TestGetNotFound(t *testing.T) {
	s, _ := openTestStore(t)
	_, err := s.GetContent(context.Background(), DigestOf([]byte("absent")))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	t.Log("missing content reported as ErrNotFound")
}
