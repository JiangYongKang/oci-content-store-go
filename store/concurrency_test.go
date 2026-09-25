package store

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConcurrentSameContentDedup: 多 goroutine 同时写同一份内容，
// 必须全部成功，且磁盘上只有一份 blob。
func TestConcurrentSameContentDedup(t *testing.T) {
	s, dir := openTestStore(t)
	content := make([]byte, 4096)
	for i := range content {
		content[i] = byte(i % 251)
	}
	desc := Descriptor{Digest: DigestOf(content), Size: int64(len(content))}

	const n = 32
	var wg sync.WaitGroup
	errs := make(chan error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			err := s.PutContent(context.Background(), desc, bytes.NewReader(content))
			if err != nil {
				errs <- fmt.Errorf("PutContent: %w", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := blobCount(t, dir); got != 1 {
		t.Fatalf("expected exactly 1 blob after %d concurrent puts, got %d", n, got)
	}
	got, err := s.GetContent(context.Background(), desc.Digest)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(content) {
		t.Fatalf("content length corrupted: %d", len(got))
	}
	t.Logf("32 concurrent identical puts -> single blob, content intact")
}

// TestConcurrentPutResolveGC: 持续写入、解析与回收交织时，
// 任何已被标签成功引用的内容在引用有效期间必须始终可读，
// 不得被误回收，也不得读到损坏/不完整数据。
func TestConcurrentPutResolveGC(t *testing.T) {
	s, dir := openTestStore(t)
	ctx := context.Background()

	const (
		workers   = 8
		iter      = 40
		nShLayers = 4
	)
	sharedLayers := make([][]byte, nShLayers)
	for i := range sharedLayers {
		sharedLayers[i] = []byte(fmt.Sprintf("shared-layer-%d-blob-bytes", i))
	}

	stop := make(chan struct{})
	var gcWG sync.WaitGroup
	gcWG.Add(1)
	go func() {
		defer gcWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
				rem, err := s.GC(ctx)
				if err != nil {
					t.Errorf("GC: %v", err)
					return
				}
				t.Logf("GC round collected=%d", len(rem))
				// 模拟真实回收器按周期运行：周期间隔远大于
				// "上传 blob -> 建立引用"的客户端操作耗时，
				// 配合存储的一个周期宽限期，客户端重传必然收敛。
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	start := make(chan struct{})
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			tag := fmt.Sprintf("img-%d", id)
			config := []byte(fmt.Sprintf(`{"worker":%d}`, id))
			m := buildManifest(config, sharedLayers...)
			putAll := func() bool {
				for _, b := range append([][]byte{config}, sharedLayers...) {
					d := Descriptor{Digest: DigestOf(b), Size: int64(len(b))}
					if err := s.PutContent(ctx, d, bytes.NewReader(b)); err != nil {
						errCh <- fmt.Errorf("worker %d put: %w", id, err)
						return false
					}
				}
				return true
			}
			// 客户端钉住本镜像引用的全部摘要，覆盖"上传 -> 建立引用"
			// 的窗口期：被钉住的内容不会被并发 GC 回收，因此 PutManifest
			// 一次成功、无需重试，结果与时间调度无关、可稳定复现。
			pins := []Digest{DigestOf(config)}
			for _, l := range sharedLayers {
				pins = append(pins, DigestOf(l))
			}
			for _, d := range pins {
				s.Pin(d)
			}
			defer func() {
				for _, d := range pins {
					s.Unpin(d)
				}
			}()
			for k := 0; k < iter; k++ {
				if !putAll() {
					return
				}
				rd, err := s.PutManifest(ctx, tag, m)
				if err != nil {
					errCh <- fmt.Errorf("worker %d manifest: %w", id, err)
					return
				}
				// 解析标签并读全量引用内容：引用始终有效，必须全部成功。
				rd, err = s.Resolve(ctx, tag)
				if err != nil {
					errCh <- fmt.Errorf("worker %d resolve: %w", id, err)
					return
				}
				rm, err := s.GetManifest(ctx, rd.Digest)
				if err != nil {
					errCh <- fmt.Errorf("worker %d get-manifest: %w", id, err)
					return
				}
				for _, ref := range rm.references() {
					b, err := s.GetContent(ctx, ref.Digest)
					if err != nil {
						errCh <- fmt.Errorf("worker %d referenced blob %s unreadable: %w", id, ref.Digest, err)
						return
					}
					if DigestOf(b) != ref.Digest || int64(len(b)) != ref.Size {
						errCh <- fmt.Errorf("worker %d corrupted blob %s: hash/size mismatch", id, ref.Digest)
						return
					}
				}
			}
		}(w)
	}
	close(start)
	wg.Wait()
	close(stop)
	gcWG.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	// 确定性收尾：所有 worker 标签可达，最终状态 =
	// workers 份 config + nShLayers 个共享层 + workers 份清单。
	removed, err := s.GC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("final GC collected=%d", len(removed))
	tags, err := s.ListTags(ctx)
	if err != nil || len(tags) != workers {
		t.Fatalf("ListTags = %v, %v; want %d tags", tags, err, workers)
	}
	wantBlobs := workers + nShLayers + workers
	if got := blobCount(t, dir); got != wantBlobs {
		t.Fatalf("final blob count = %d, want %d (configs+shared layers+manifests)", got, wantBlobs)
	}
	// 每个标签引用的内容全部可读且完整。
	for _, tag := range tags {
		rd, err := s.Resolve(ctx, tag)
		if err != nil {
			t.Fatal(err)
		}
		rm, err := s.GetManifest(ctx, rd.Digest)
		if err != nil {
			t.Fatal(err)
		}
		if len(rm.Layers) != nShLayers {
			t.Fatalf("%s: expected %d layers, got %d", tag, nShLayers, len(rm.Layers))
		}
	}
	t.Logf("interleaved put/resolve/GC stable: %d tags, %d blobs on disk", workers, wantBlobs)
}
