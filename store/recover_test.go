package store

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// reopen 关闭并重新打开同一目录的存储（模拟进程重启）。
func reopen(t *testing.T, s *Store, dir string) *Store {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ns, err := Open(dir, testLogger{t})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	return ns
}

func TestReopenKeepsExistingData(t *testing.T) {
	s, dir := openTestStore(t)
	ctx := context.Background()
	config := []byte("cfg-persist")
	layer := []byte("layer-persist")
	m, mdesc := seedImage(t, s, "v1", config, layer)

	s = reopen(t, s, dir)

	// 历史数据必须在不重建的前提下继续可读。
	rd, err := s.Resolve(ctx, "v1")
	if err != nil {
		t.Fatalf("Resolve after reopen: %v", err)
	}
	if rd.Digest != mdesc.Digest {
		t.Fatalf("tag moved: got %s want %s", rd.Digest, mdesc.Digest)
	}
	gotM, err := s.GetManifest(ctx, rd.Digest)
	if err != nil {
		t.Fatalf("GetManifest after reopen: %v", err)
	}
	if gotM.Config.Digest != m.Config.Digest || len(gotM.Layers) != len(m.Layers) {
		t.Fatalf("manifest changed across restart: %+v vs %+v", gotM, m)
	}
	got, err := s.GetContent(ctx, DigestOf(layer))
	if err != nil || !bytes.Equal(got, layer) {
		t.Fatalf("layer unreadable after reopen: %v", err)
	}

	// 继续写入也必须工作（增量写，非重建）。
	layer2 := []byte("layer-after-restart")
	mustPut(t, s, layer2)
	m2 := buildManifest(config, layer, layer2)
	if _, err := s.PutManifest(ctx, "v2", m2); err != nil {
		t.Fatalf("PutManifest after reopen: %v", err)
	}
	t.Log("existing data readable and writable after restart, no rebuild")
}

func TestRecoverCleansIncompleteUpload(t *testing.T) {
	s, dir := openTestStore(t)
	ctx := context.Background()

	// 模拟崩溃：uploads 下留下未完成的临时文件（半截内容）。
	partial := []byte("partial content never finished")
	tmp := filepath.Join(dir, uploadsDir, "x.tmp-deadbeef")
	if err := os.WriteFile(tmp, partial, 0o644); err != nil {
		t.Fatal(err)
	}

	s = reopen(t, s, dir)

	// 临时文件必须被清除，且不得参与去重/引用统计。
	if _, err := os.Stat(tmp); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("incomplete upload must be removed on recovery, stat err=%v", err)
	}
	// 与残留内容同摘要的正式写入必须完整重写（不能复用到残留）。
	full := []byte("partial content never finished") // 同前缀内容完整写入
	desc := Descriptor{Digest: DigestOf(full), Size: int64(len(full))}
	if err := s.PutContent(ctx, desc, bytes.NewReader(full)); err != nil {
		t.Fatalf("PutContent after recovery: %v", err)
	}
	got, err := s.GetContent(ctx, desc.Digest)
	if err != nil || !bytes.Equal(got, full) {
		t.Fatalf("content corrupted after recovery: %v", err)
	}
	t.Log("incomplete upload cleaned on recovery; content re-stored completely")
}

func TestRecoverCleansStaleTempTag(t *testing.T) {
	s, dir := openTestStore(t)
	ctx := context.Background()
	seedImage(t, s, "v1", []byte("cfg"))

	// 模拟标签发布中途崩溃留下的临时文件。
	stale := filepath.Join(dir, tagsDir, "v1"+tmpSuffix+".new")
	if err := os.WriteFile(stale, []byte("sha256:garbage"), 0o644); err != nil {
		t.Fatal(err)
	}

	s = reopen(t, s, dir)

	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale temp tag must be removed, stat err=%v", err)
	}
	tags, err := s.ListTags(ctx)
	if err != nil || len(tags) != 1 || tags[0] != "v1" {
		t.Fatalf("ListTags = %v, %v; temp file must not appear as tag", tags, err)
	}
	if _, err := s.Resolve(ctx, "v1"); err != nil {
		t.Fatalf("real tag must still resolve: %v", err)
	}
	t.Log("stale temp tag cleaned; real tag intact")
}

func TestRecoverRemovesCorruptAndDanglingTags(t *testing.T) {
	s, dir := openTestStore(t)
	ctx := context.Background()
	seedImage(t, s, "good", []byte("cfg-good"))

	// 损坏标签：内容不是合法摘要。
	if err := os.WriteFile(filepath.Join(dir, tagsDir, "corrupt"), []byte("not-a-digest"), 0o644); err != nil {
		t.Fatal(err)
	}
	// 悬挂标签：摘要合法但目标 blob 不存在。
	ghost := DigestOf([]byte("ghost manifest"))
	if err := os.WriteFile(filepath.Join(dir, tagsDir, "dangling"), []byte(string(ghost)), 0o644); err != nil {
		t.Fatal(err)
	}

	s = reopen(t, s, dir)

	tags, err := s.ListTags(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(tags) != 1 || tags[0] != "good" {
		t.Fatalf("corrupt/dangling tags must be removed on recovery, tags=%v", tags)
	}
	if _, err := s.Resolve(ctx, "good"); err != nil {
		t.Fatalf("healthy tag must survive recovery: %v", err)
	}
	for _, bad := range []string{"corrupt", "dangling"} {
		if _, err := s.Resolve(ctx, bad); !errors.Is(err, ErrNotFound) {
			t.Fatalf("tag %q should be gone, got %v", bad, err)
		}
	}
	t.Log("corrupt and dangling tags removed; healthy tag preserved")
}

func TestRecoverDoesNotTouchPublishedBlobs(t *testing.T) {
	s, dir := openTestStore(t)
	ctx := context.Background()
	content := []byte("published immutable blob")
	desc := mustPut(t, s, content)

	s = reopen(t, s, dir)

	// 已发布 blob 不受恢复影响；未被引用时由 GC 而非恢复流程处理。
	got, err := s.GetContent(ctx, desc.Digest)
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("published blob damaged by recovery: %v", err)
	}
	removed, err := s.GC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || removed[0].Digest != desc.Digest {
		t.Fatalf("unreferenced published blob should be GC-able, removed=%+v", removed)
	}
	t.Log("recovery left published blobs alone; GC handles unreferenced ones")
}
