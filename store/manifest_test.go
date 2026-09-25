package store

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

// buildManifest 用已写入的 config 与 layers 组装一份合法清单。
func buildManifest(config []byte, layers ...[]byte) Manifest {
	m := Manifest{
		SchemaVersion: 2,
		MediaType:     "application/vnd.oci.image.manifest.v1+json",
		Layers:        []Descriptor{},
		Config: Descriptor{
			MediaType: "application/vnd.oci.image.config.v1+json",
			Digest:    DigestOf(config),
			Size:      int64(len(config)),
		},
	}
	for _, l := range layers {
		m.Layers = append(m.Layers, Descriptor{
			MediaType: "application/vnd.oci.image.layer.v1.tar",
			Digest:    DigestOf(l),
			Size:      int64(len(l)),
		})
	}
	return m
}

func seedImage(t *testing.T, s *Store, tag string, config []byte, layers ...[]byte) (Manifest, Descriptor) {
	t.Helper()
	ctx := context.Background()
	mustPut(t, s, config)
	for _, l := range layers {
		mustPut(t, s, l)
	}
	m := buildManifest(config, layers...)
	desc, err := s.PutManifest(ctx, tag, m)
	if err != nil {
		t.Fatalf("PutManifest: %v", err)
	}
	return m, desc
}

func TestPutManifestResolveGet(t *testing.T) {
	s, _ := openTestStore(t)
	ctx := context.Background()
	config := []byte(`{"architecture":"amd64"}`)
	layer1 := []byte("layer-one-data")
	layer2 := []byte("layer-two-data")
	m, desc := seedImage(t, s, "v1", config, layer1, layer2)

	got, err := s.Resolve(ctx, "v1")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.Digest != desc.Digest || got.Size != desc.Size {
		t.Fatalf("Resolve = %+v, want %+v", got, desc)
	}

	gotM, err := s.GetManifest(ctx, desc.Digest)
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	if !reflect.DeepEqual(gotM, m) {
		t.Fatalf("manifest roundtrip changed:\n got %+v\nwant %+v", gotM, m)
	}

	tags, err := s.ListTags(ctx)
	if err != nil || len(tags) != 1 || tags[0] != "v1" {
		t.Fatalf("ListTags = %v, %v", tags, err)
	}
	t.Logf("manifest put/resolve/get ok tag=v1 manifest=%s layers=%d", desc.Digest, len(m.Layers))
}

func TestPutManifestMissingReference(t *testing.T) {
	s, _ := openTestStore(t)
	ctx := context.Background()
	missing := []byte("never written")
	m := Manifest{
		SchemaVersion: 2,
		Config: Descriptor{
			MediaType: "application/vnd.oci.image.config.v1+json",
			Digest:    DigestOf(missing),
			Size:      int64(len(missing)),
		},
		Layers: []Descriptor{},
	}
	_, err := s.PutManifest(ctx, "v1", m)
	if !errors.Is(err, ErrMissingReference) {
		t.Fatalf("expected ErrMissingReference, got %v", err)
	}
	tags, _ := s.ListTags(ctx)
	if len(tags) != 0 {
		t.Fatalf("rejected manifest left tag behind: %v", tags)
	}
	if _, err := s.Resolve(ctx, "v1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("tag must not exist after rejection, got %v", err)
	}
	t.Log("manifest with missing reference rejected, no tag created")
}

func TestPutInvalidManifest(t *testing.T) {
	s, _ := openTestStore(t)
	ctx := context.Background()
	config := []byte("cfg")
	mustPut(t, s, config)

	// schemaVersion 非法。
	bad := Manifest{SchemaVersion: 1, Config: Descriptor{Digest: DigestOf(config), Size: 2}, Layers: []Descriptor{}}
	if _, err := s.PutManifest(ctx, "bad", bad); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("bad schemaVersion: want ErrInvalidManifest, got %v", err)
	}

	// 直接向 GetManifest 喂非法 JSON。
	desc := mustPut(t, s, []byte("{not json"))
	if _, err := s.GetManifest(ctx, desc.Digest); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("bad json: want ErrInvalidManifest, got %v", err)
	}
	t.Log("structurally invalid manifests classified as ErrInvalidManifest")
}

func TestPutManifestSharedLayerDedup(t *testing.T) {
	s, dir := openTestStore(t)
	ctx := context.Background()
	sharedLayer := []byte("shared layer bytes")
	cfg1, cfg2 := []byte("config-one"), []byte("config-two")
	m1 := buildManifest(cfg1, sharedLayer)
	m2 := buildManifest(cfg2, sharedLayer)
	for _, b := range [][]byte{sharedLayer, cfg1, cfg2} {
		mustPut(t, s, b)
	}
	d1, err := s.PutManifest(ctx, "a", m1)
	if err != nil {
		t.Fatalf("PutManifest a: %v", err)
	}
	d2, err := s.PutManifest(ctx, "b", m2)
	if err != nil {
		t.Fatalf("PutManifest b: %v", err)
	}
	// blobs: sharedLayer + cfg1 + cfg2 + manifest a + manifest b = 5
	if n := blobCount(t, dir); n != 5 {
		t.Fatalf("expected 5 blobs (shared layer deduplicated), got %d", n)
	}
	if d1.Digest == d2.Digest {
		t.Fatal("different configs must yield different manifests")
	}
	t.Logf("shared layer deduplicated across two tags: 5 blobs on disk")
}

func TestGCCollectsUnreferenced(t *testing.T) {
	s, dir := openTestStore(t)
	ctx := context.Background()
	config := []byte("config")
	layer := []byte("layer-keep")
	_, mdesc := seedImage(t, s, "v1", config, layer)

	// 孤儿 blob：未被任何清单引用。
	orphan := []byte("orphan blob")
	odesc := mustPut(t, s, orphan)

	removed, err := s.GC(ctx)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if len(removed) != 1 || removed[0].Digest != odesc.Digest {
		t.Fatalf("GC removed %+v, want only %s", removed, odesc.Digest)
	}
	// 被标签引用的内容必须全部可读。
	for _, want := range []Digest{mdesc.Digest, DigestOf(config), DigestOf(layer)} {
		if _, err := s.GetContent(ctx, want); err != nil {
			t.Fatalf("reachable blob %s disappeared: %v", want, err)
		}
	}
	if n := blobCount(t, dir); n != 3 {
		t.Fatalf("expected 3 reachable blobs on disk, got %d", n)
	}

	// 再跑一次 GC：没有可回收的，结果必须稳定。
	removed2, err := s.GC(ctx)
	if err != nil || len(removed2) != 0 {
		t.Fatalf("second GC = %v, %v; want empty, nil", removed2, err)
	}
	t.Logf("GC collected 1 orphan, 3 reachable blobs remain; idempotent")
}

func TestGCAfterTagDeleteKeepsSharedUntilLastTag(t *testing.T) {
	s, dir := openTestStore(t)
	ctx := context.Background()
	shared := []byte("shared base layer")
	cfg1, cfg2 := []byte("cfg-1"), []byte("cfg-2")
	m1 := buildManifest(cfg1, shared)
	m2 := buildManifest(cfg2, shared)
	for _, b := range [][]byte{shared, cfg1, cfg2} {
		mustPut(t, s, b)
	}
	if _, err := s.PutManifest(ctx, "a", m1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutManifest(ctx, "b", m2); err != nil {
		t.Fatal(err)
	}

	// 删除标签 a：a 的清单与 cfg1 变为不可达，shared 因 b 仍存活。
	if err := s.DeleteTag(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	removed, err := s.GC(ctx)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	got := map[Digest]bool{}
	for _, d := range removed {
		got[d.Digest] = true
	}
	if !got[DigestOf(cfg1)] {
		t.Fatalf("cfg1 should be collected; removed=%v", got)
	}
	if got[DigestOf(shared)] {
		t.Fatalf("shared layer still referenced by tag b, must not be collected")
	}
	// 标签 b 的完整镜像必须仍可解析与读取。
	desc, err := s.Resolve(ctx, "b")
	if err != nil {
		t.Fatalf("Resolve b: %v", err)
	}
	mb, err := s.GetManifest(ctx, desc.Digest)
	if err != nil {
		t.Fatalf("GetManifest b: %v", err)
	}
	raw, err := json.Marshal(mb)
	if err != nil {
		t.Fatal(err)
	}
	_ = raw
	for _, ref := range mb.references() {
		if _, err := s.GetContent(ctx, ref.Digest); err != nil {
			t.Fatalf("tag b content %s unreadable: %v", ref.Digest, err)
		}
	}
	if n := blobCount(t, dir); n != 3 {
		t.Fatalf("expected 3 blobs after GC (shared+cfg2+manifest b), got %d", n)
	}
	t.Log("tag delete + GC: unreachable collected, shared layer protected by remaining tag")
}

func TestRetagFreesOldManifest(t *testing.T) {
	s, dir := openTestStore(t)
	ctx := context.Background()
	// 同一标签先后指向两份清单（不同 config）。
	_, _ = seedImage(t, s, "v1", []byte("old-cfg"))
	oldResolved, _ := s.Resolve(ctx, "v1")
	_, newDesc := seedImage(t, s, "v1", []byte("new-cfg"))
	if newDesc.Digest == oldResolved.Digest {
		t.Fatal("expected new manifest digest")
	}
	// 旧清单与旧 config 不再可达。
	removed, err := s.GC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := map[Digest]bool{}
	for _, d := range removed {
		got[d.Digest] = true
	}
	if !got[oldResolved.Digest] || !got[DigestOf([]byte("old-cfg"))] {
		t.Fatalf("old manifest/config should be collected, got %v", got)
	}
	if n := blobCount(t, dir); n != 2 {
		t.Fatalf("expected 2 blobs (new cfg + new manifest), got %d", n)
	}
	t.Log("retag frees the old manifest and its config")
}

func TestGCPinProtectsUnreferenced(t *testing.T) {
	s, _ := openTestStore(t)
	ctx := context.Background()
	pinned := []byte("pinned but unreferenced")
	orphan := []byte("unpinned orphan")
	pd := mustPut(t, s, pinned)
	od := mustPut(t, s, orphan)

	s.Pin(pd.Digest)
	removed, err := s.GC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || removed[0].Digest != od.Digest {
		t.Fatalf("GC removed %+v, want only unpinned orphan %s", removed, od.Digest)
	}
	if _, err := s.GetContent(ctx, pd.Digest); err != nil {
		t.Fatalf("pinned content must survive GC: %v", err)
	}
	t.Log("pinned unreferenced blob survived GC; unpinned orphan collected")

	s.Unpin(pd.Digest)
	removed, err = s.GC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || removed[0].Digest != pd.Digest {
		t.Fatalf("after unpin, GC removed %+v, want %s", removed, pd.Digest)
	}
	t.Log("after unpin the blob became collectable")
}
