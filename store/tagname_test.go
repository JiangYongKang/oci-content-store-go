package store

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// TestTagNameBoundaryValid: 所有符合命名规则的标签名——尤其是含点号、
// 含 ".tmp" 子串、以 ".tmp.new" 结尾这类历史上会与临时文件命名混淆的
// 名字——都必须能正常创建、解析、列举、删除，正确参与引用统计与回收，
// 且重启后不消失。
func TestTagNameBoundaryValid(t *testing.T) {
	long := strings.Repeat("a", 127) + "1" // 128 字符，上限
	valid := []string{
		"v1.2.3",      // 常规点号
		"release.tmp", // 含 ".tmp" 子串
		"x.tmp",       // 恰好以 ".tmp" 结尾
		"a.tmp.new",   // 以旧临时文件后缀结尾
		"tmp",         // 短名
		"name.with.many.dots",
		"a..b",    // 连续点号
		"A_b-c.1", // 大小写/下划线/连字符/点号混合
		"_under",  // 下划线开头
		"TMP.9-z",
		long, // 长度上限
	}

	s, dir := openTestStore(t)
	ctx := context.Background()

	// 每个标签指向一份独立镜像（独立 config），便于精确核对引用统计。
	configs := make(map[string][]byte, len(valid))
	manifests := make(map[string]Descriptor, len(valid))
	for _, tag := range valid {
		cfg := []byte("cfg-for-" + tag)
		configs[tag] = cfg
		_, desc := seedImage(t, s, tag, cfg)
		manifests[tag] = desc
		t.Logf("created tag %q -> manifest %s", tag, desc.Digest)
	}

	// 全部可解析、可列举。
	tags, err := s.ListTags(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(tags) != len(valid) {
		t.Fatalf("ListTags = %v (%d), want %d tags", tags, len(tags), len(valid))
	}
	for _, tag := range valid {
		rd, err := s.Resolve(ctx, tag)
		if err != nil {
			t.Fatalf("Resolve(%q): %v", tag, err)
		}
		if rd.Digest != manifests[tag].Digest {
			t.Fatalf("Resolve(%q) = %s, want %s", tag, rd.Digest, manifests[tag].Digest)
		}
	}
	t.Logf("all %d boundary tags created, listed and resolved", len(valid))

	// GC 不得误回收任何被引用的内容（含 ".tmp" 类名字的标签）。
	removed, err := s.GC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 0 {
		t.Fatalf("GC collected %d blobs though every blob is referenced: %v", len(removed), removed)
	}
	t.Log("GC with all tags referenced collected nothing (judged by removed==0)")

	// 重启（重新 Open）：所有标签必须原样保留、仍可解析。
	s = reopen(t, s, dir)
	tags, err = s.ListTags(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(tags) != len(valid) {
		t.Fatalf("after reopen ListTags = %v (%d), want %d", tags, len(tags), len(valid))
	}
	for _, tag := range valid {
		rd, err := s.Resolve(ctx, tag)
		if err != nil {
			t.Fatalf("after reopen Resolve(%q): %v", tag, err)
		}
		if rd.Digest != manifests[tag].Digest {
			t.Fatalf("after reopen tag %q moved: %s != %s", tag, rd.Digest, manifests[tag].Digest)
		}
		if _, err := s.GetContent(ctx, DigestOf(configs[tag])); err != nil {
			t.Fatalf("after reopen config of %q unreadable: %v", tag, err)
		}
	}
	t.Log("all boundary tags survived reopen with identical targets")

	// 删除一半标签：被删标签独占的内容必须可回收，保留标签的内容不受影响。
	for _, tag := range valid[:len(valid)/2] {
		if err := s.DeleteTag(ctx, tag); err != nil {
			t.Fatalf("DeleteTag(%q): %v", tag, err)
		}
	}
	removed, err = s.GC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	removedSet := map[Digest]bool{}
	for _, d := range removed {
		removedSet[d.Digest] = true
	}
	for i, tag := range valid {
		cfgD := DigestOf(configs[tag])
		if i < len(valid)/2 {
			if !removedSet[cfgD] {
				t.Fatalf("config of deleted tag %q must be collected", tag)
			}
			if _, err := s.Resolve(ctx, tag); !errors.Is(err, ErrNotFound) {
				t.Fatalf("deleted tag %q still resolves: %v", tag, err)
			}
		} else {
			if removedSet[cfgD] {
				t.Fatalf("config of live tag %q must NOT be collected", tag)
			}
			if _, err := s.GetContent(ctx, cfgD); err != nil {
				t.Fatalf("live tag %q config unreadable after GC: %v", tag, err)
			}
		}
	}
	t.Logf("deleted %d tags: their configs collected, %d live tags untouched",
		len(valid)/2, len(valid)-len(valid)/2)
}

// TestTagNameInvalidRejected: 不符合命名规则的名字必须在
// PutManifest/Resolve/DeleteTag 上一致地返回 ErrInvalidTag。
func TestTagNameInvalidRejected(t *testing.T) {
	s, _ := openTestStore(t)
	ctx := context.Background()
	invalid := []string{
		"",
		".hidden",                // 点号开头
		"-lead-dash",             // 连字符开头
		"a/b",                    // 含路径分隔符
		"a b",                    // 含空格
		"a:b",                    // 含冒号
		strings.Repeat("x", 129), // 超长
	}
	for _, tag := range invalid {
		m := buildManifest([]byte("cfg"))
		if _, err := s.PutManifest(ctx, tag, m); !errors.Is(err, ErrInvalidTag) {
			t.Fatalf("PutManifest(%q): want ErrInvalidTag, got %v", tag, err)
		}
		if _, err := s.Resolve(ctx, tag); !errors.Is(err, ErrInvalidTag) {
			t.Fatalf("Resolve(%q): want ErrInvalidTag, got %v", tag, err)
		}
		if err := s.DeleteTag(ctx, tag); !errors.Is(err, ErrInvalidTag) {
			t.Fatalf("DeleteTag(%q): want ErrInvalidTag, got %v", tag, err)
		}
	}
	t.Logf("%d invalid names consistently rejected with ErrInvalidTag", len(invalid))
}

// TestDottedTmpTagSurvivesGCAndReopen 是针对历史缺陷的回归测试：
// 标签临时文件曾与正式标签同处 tags/ 目录、按 ".tmp" 子串识别，
// 导致名为 "v1.tmp" 的正式标签被列举/回收/恢复流程误当临时文件。
// 现在临时文件移入 uploads/，此类标签必须与普通标签完全同权。
func TestDottedTmpTagSurvivesGCAndReopen(t *testing.T) {
	s, dir := openTestStore(t)
	ctx := context.Background()
	cfg := []byte("cfg-dotted-tmp")
	layer := []byte("layer-dotted-tmp")
	_, mdesc := seedImage(t, s, "v1.tmp", cfg, layer)

	// GC：被 "v1.tmp" 引用的全部内容必须存活。
	removed, err := s.GC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 0 {
		t.Fatalf("GC collected %d blobs referenced by tag v1.tmp: %v", len(removed), removed)
	}
	t.Log("GC collected nothing while v1.tmp references everything")

	// 重启：标签不消失、目标不变、内容完整。
	s = reopen(t, s, dir)
	rd, err := s.Resolve(ctx, "v1.tmp")
	if err != nil {
		t.Fatalf("v1.tmp disappeared after reopen: %v", err)
	}
	if rd.Digest != mdesc.Digest {
		t.Fatalf("v1.tmp target changed across reopen: %s != %s", rd.Digest, mdesc.Digest)
	}
	tags, err := s.ListTags(ctx)
	if err != nil || len(tags) != 1 || tags[0] != "v1.tmp" {
		t.Fatalf("ListTags after reopen = %v, %v; want [v1.tmp]", tags, err)
	}
	for _, d := range []Digest{mdesc.Digest, DigestOf(cfg), DigestOf(layer)} {
		if _, err := s.GetContent(ctx, d); err != nil {
			t.Fatalf("content %s referenced by v1.tmp unreadable: %v", d, err)
		}
	}
	t.Log("tag v1.tmp fully functional across GC and reopen")
}
