package contentstore

import (
	"context"
	"fmt"
	"testing"
)

func TestGCReclaimsOnlyUnreferenced(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	md1, cfg1, layer1s := putImage(t, s, "v1", manifestSpec{
		config: []byte("config-v1"),
		layers: [][]byte{[]byte("layer-shared"), []byte("layer-v1-only")},
	})
	md2, cfg2, layer2s := putImage(t, s, "v2", manifestSpec{
		config: []byte("config-v2"),
		layers: [][]byte{[]byte("layer-shared"), []byte("layer-v2-only")},
	})

	// Untagged blob + an untagged manifest are immediate garbage.
	orphan := putBytes(t, s, []byte("nobody references me"))
	orphanManifest := func() string {
		body := fmt.Sprintf(`{"config":{"digest":"%s"},"layers":[]}`, cfg1)
		d, err := s.PutManifest(ctx, []byte(body))
		if err != nil {
			t.Fatalf("orphan manifest: %v", err)
		}
		return d.String()
	}()

	stats, _ := s.Stats(ctx)
	t.Logf("before GC: %d blobs (two tagged images + orphan blob + orphan manifest)", stats.Blobs)

	removed, err := s.GC(ctx)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if removed != 2 {
		t.Fatalf("GC removed %d blobs, want 2 (orphan blob %s, orphan manifest %s)", removed, orphan, orphanManifest)
	}
	t.Logf("GC removed exactly the two unreferenced blobs: %s and %s", orphan, orphanManifest)

	// Every blob reachable from v1 or v2 must still read correctly.
	for _, d := range append([]string{md1, cfg1, md2, cfg2}, append(layer1s, layer2s...)...) {
		func() {
			rc, err := s.GetBlob(ctx, d)
			if err != nil {
				t.Fatalf("reachable blob %s vanished after GC: %v", d, err)
			}
			defer rc.Close()
		}()
	}
	t.Log("all v1/v2-reachable blobs (incl. shared layer) still readable")

	// Drop v1: v1-only content goes; the shared layer survives via v2.
	if err := s.Untag(ctx, "v1"); err != nil {
		t.Fatalf("Untag: %v", err)
	}
	removed, err = s.GC(ctx)
	if err != nil {
		t.Fatalf("GC2: %v", err)
	}
	// v1 manifest, v1 config, v1-only layer = 3.
	if removed != 3 {
		t.Fatalf("GC2 removed %d, want 3", removed)
	}
	if rc, err := s.GetBlob(ctx, layer1s[0]); err != nil {
		t.Fatalf("shared layer %s reclaimed while v2 references it: %v", layer1s[0], err)
	} else {
		rc.Close()
	}
	if rc, err := s.GetBlob(ctx, layer2s[1]); err != nil {
		t.Fatalf("v2-only layer missing: %v", err)
	} else {
		rc.Close()
	}
	t.Logf("after untag v1: 3 v1-only blobs removed; shared layer %s kept via v2", layer1s[0])

	// Drop v2 too: everything goes.
	if err := s.Untag(ctx, "v2"); err != nil {
		t.Fatalf("Untag v2: %v", err)
	}
	removed, _ = s.GC(ctx)
	stats, _ = s.Stats(ctx)
	if stats.Blobs != 0 {
		t.Fatalf("after final GC: %d blobs remain, want 0", stats.Blobs)
	}
	t.Logf("all tags removed: final GC swept %d blobs, store empty", removed)
}

func TestGCIdempotent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	putImage(t, s, "v1", manifestSpec{config: []byte("c"), layers: [][]byte{[]byte("l")}})
	if n, err := s.GC(ctx); err != nil || n != 0 {
		t.Fatalf("first GC = (%d,%v), want (0,nil)", n, err)
	}
	if n, err := s.GC(ctx); err != nil || n != 0 {
		t.Fatalf("second GC = (%d,%v), want (0,nil)", n, err)
	}
	t.Log("repeated GC on stable reachable set removes nothing")
}
