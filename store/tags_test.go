package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// 命名边界用例：所有符合命名规则的标签名（含点号）都必须
// 能创建、解析、列举、删除，并正确参与引用统计与回收。

// dottedTags 覆盖含点号的各类合法标签名。
var dottedTags = []string{
	"v1.tmp",        // 以 .tmp 结尾：曾与临时文件判定冲突
	"release-1.0.1", // 多段点号
	"a.b.c",         // 连续点号分段
	"x.tmp.old",     // 中间含 .tmp
	"UPPER.1_2-3",   // 混合大小写与符号
	"t.",            // 以点号结尾
	"a..b",          // 连续两个点号
}

// TestDottedTagsFullLifecycle: 带点标签的完整生命周期——
// 写入可解析、可列举，GC 不误回收其引用内容，删除后可回收。
func TestDottedTagsFullLifecycle(t *testing.T) {
	s, dir := openTestStore(t)
	ctx := context.Background()

	descs := make(map[string]Descriptor)
	for i, tag := range dottedTags {
		config := []byte("cfg-" + tag)
		layer := []byte("layer-" + tag)
		_, desc := seedImage(t, s, tag, config, layer)
		descs[tag] = desc
		t.Logf("seeded dotted tag %q -> manifest %s", tag, desc.Digest)
		_ = i
	}

	// 全部可解析且指向正确清单。
	for _, tag := range dottedTags {
		rd, err := s.Resolve(ctx, tag)
		if err != nil {
			t.Fatalf("Resolve(%q): %v", tag, err)
		}
		if rd.Digest != descs[tag].Digest {
			t.Fatalf("tag %q resolved to %s, want %s", tag, rd.Digest, descs[tag].Digest)
		}
	}
	// 全部出现在列举中（一个都不能被静默吞掉）。
	tags, err := s.ListTags(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(tags) != len(dottedTags) {
		t.Fatalf("ListTags = %v, want all %d dotted tags", tags, len(dottedTags))
	}

	// GC 不得回收任何被这些标签引用的内容。
	removed, err := s.GC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 0 {
		t.Fatalf("GC collected %d blobs although all are referenced by dotted tags", len(removed))
	}
	wantBlobs := 3 * len(dottedTags) // 每标签：config + layer + manifest
	if got := blobCount(t, dir); got != wantBlobs {
		t.Fatalf("blob count = %d, want %d (referenced by dotted tags)", got, wantBlobs)
	}

	// 删除一半标签：其独占内容可回收，另一半不受影响。
	deleted := len(dottedTags) / 2
	for _, tag := range dottedTags[:deleted] {
		if err := s.DeleteTag(ctx, tag); err != nil {
			t.Fatalf("DeleteTag(%q): %v", tag, err)
		}
	}
	removed, err = s.GC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if want := 3 * deleted; len(removed) != want {
		t.Fatalf("GC collected %d blobs, want %d (deleted %d dotted tags)",
			len(removed), want, deleted)
	}
	for _, tag := range dottedTags[deleted:] {
		if _, err := s.Resolve(ctx, tag); err != nil {
			t.Fatalf("surviving tag %q must still resolve: %v", tag, err)
		}
	}
	t.Logf("dotted tags: full lifecycle ok (%d tags, create/resolve/list/gc/delete)", len(dottedTags))
}

// TestDottedTagSurvivesRestart: 带点标签在重启后依然存在、可解析，
// 恢复流程不得把它误判为临时文件清掉。
func TestDottedTagSurvivesRestart(t *testing.T) {
	s, dir := openTestStore(t)
	ctx := context.Background()
	layer := []byte("layer-of-dotted-tag")
	_, mdesc := seedImage(t, s, "v1.tmp", []byte("cfg-dotted"), layer)

	s = reopen(t, s, dir)

	tags, err := s.ListTags(ctx)
	if err != nil || len(tags) != 1 || tags[0] != "v1.tmp" {
		t.Fatalf("dotted tag lost across restart: tags=%v err=%v", tags, err)
	}
	rd, err := s.Resolve(ctx, "v1.tmp")
	if err != nil || rd.Digest != mdesc.Digest {
		t.Fatalf("dotted tag unresolvable after restart: %+v err=%v", rd, err)
	}
	got, err := s.GetContent(ctx, DigestOf(layer))
	if err != nil {
		t.Fatalf("content of dotted tag unreadable after restart: %v", err)
	}
	if string(got) != string(layer) {
		t.Fatalf("content corrupted after restart")
	}
	// 重启后的 GC 同样不得误伤。
	if removed, err := s.GC(ctx); err != nil || len(removed) != 0 {
		t.Fatalf("GC after restart collected referenced content: %v %v", removed, err)
	}
	t.Log("dotted tag v1.tmp survived restart and post-restart GC")
}

// TestTagReservedSuffixRejected: 以历史临时文件后缀 ".tmp.new" 结尾的
// 名字与恢复清理逻辑无法区分，属于保留名，必须明确报错而非静默丢数据。
func TestTagReservedSuffixRejected(t *testing.T) {
	s, _ := openTestStore(t)
	ctx := context.Background()
	m := buildManifest([]byte("cfg"))
	if _, err := s.PutManifest(ctx, "a.tmp.new", m); !errors.Is(err, ErrInvalidTag) {
		t.Fatalf("PutManifest with reserved suffix: got %v, want ErrInvalidTag", err)
	}
	if _, err := s.Resolve(ctx, "a.tmp.new"); !errors.Is(err, ErrInvalidTag) {
		t.Fatalf("Resolve with reserved suffix: got %v, want ErrInvalidTag", err)
	}
	if err := s.DeleteTag(ctx, "a.tmp.new"); !errors.Is(err, ErrInvalidTag) {
		t.Fatalf("DeleteTag with reserved suffix: got %v, want ErrInvalidTag", err)
	}
	t.Log(`reserved suffix ".tmp.new" rejected distinctly as ErrInvalidTag`)
}

// TestLegacyTempTagFileDoesNotEatDottedTag: 老版本崩溃可能在 tags/
// 留下 <tag>.tmp.new 临时文件；恢复必须清掉它，同时保留真正的
// 带点标签（如 v1.tmp），且 GC 不得因其存在而错乱。
func TestLegacyTempTagFileDoesNotEatDottedTag(t *testing.T) {
	s, dir := openTestStore(t)
	ctx := context.Background()
	_, mdesc := seedImage(t, s, "v1.tmp", []byte("cfg-legacy"))

	// 模拟老版本标签发布中途崩溃留下的临时文件。
	stale := filepath.Join(dir, tagsDir, "v1"+tagTempSuffix)
	if err := os.WriteFile(stale, []byte("sha256:garbage"), 0o644); err != nil {
		t.Fatal(err)
	}

	s = reopen(t, s, dir)

	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("legacy temp tag file must be removed, stat err=%v", err)
	}
	tags, err := s.ListTags(ctx)
	if err != nil || len(tags) != 1 || tags[0] != "v1.tmp" {
		t.Fatalf("tags = %v, %v; real dotted tag must survive legacy temp cleanup", tags, err)
	}
	rd, err := s.Resolve(ctx, "v1.tmp")
	if err != nil || rd.Digest != mdesc.Digest {
		t.Fatalf("resolve after legacy temp cleanup: %+v err=%v", rd, err)
	}
	if removed, err := s.GC(ctx); err != nil || len(removed) != 0 {
		t.Fatalf("GC must not collect content referenced by dotted tag: %v %v", removed, err)
	}
	t.Log("legacy temp file cleaned; dotted tag v1.tmp intact and GC-safe")
}
