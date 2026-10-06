package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// 本文件的用例在同一进程内打开多个 Store 实例指向同一目录。
// 每个实例持有独立的 flock fd，跨实例互斥真实经过文件锁，
// 因此等价于多个进程并发操作同一存储目录。

// openExtraStore 在同一目录上再打开一个实例（模拟另一个进程）。
func openExtraStore(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(dir, testLogger{t})
	if err != nil {
		t.Fatalf("Open extra instance: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// TestMultiInstanceConcurrentPutResolveGC: 多个实例并行推镜像、
// 解析标签，另一个实例持续回收。任何已被标签成功引用的内容在
// 引用有效期间必须始终可读，最终状态精确可断言。
func TestMultiInstanceConcurrentPutResolveGC(t *testing.T) {
	_, dir := openTestStore(t)
	ctx := context.Background()

	const (
		workers   = 6 // 6 个"进程"写镜像
		iter      = 25
		nShLayers = 3
	)
	writers := make([]*Store, workers)
	for i := range writers {
		writers[i] = openExtraStore(t, dir)
	}
	gcStore := openExtraStore(t, dir) // 专职回收的"进程"

	sharedLayers := make([][]byte, nShLayers)
	for i := range sharedLayers {
		sharedLayers[i] = []byte(fmt.Sprintf("mp-shared-layer-%d", i))
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
				rem, err := gcStore.GC(ctx)
				if err != nil {
					t.Errorf("GC: %v", err)
					return
				}
				t.Logf("cross-instance GC round collected=%d", len(rem))
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
			s := writers[id]
			tag := fmt.Sprintf("mp-img-%d", id)
			config := []byte(fmt.Sprintf(`{"worker":%d}`, id))
			m := buildManifest(config, sharedLayers...)
			pins := []Digest{DigestOf(config)}
			for _, l := range sharedLayers {
				pins = append(pins, DigestOf(l))
			}
			// 钉住覆盖"上传 -> 建立引用"窗口：钉住状态落盘，
			// 其他实例（进程）的 GC 同样会尊重。
			for _, d := range pins {
				s.Pin(d)
			}
			defer func() {
				for _, d := range pins {
					s.Unpin(d)
				}
			}()
			for k := 0; k < iter; k++ {
				for _, b := range append([][]byte{config}, sharedLayers...) {
					d := Descriptor{Digest: DigestOf(b), Size: int64(len(b))}
					if err := s.PutContent(ctx, d, bytes.NewReader(b)); err != nil {
						errCh <- fmt.Errorf("instance %d put: %w", id, err)
						return
					}
				}
				if _, err := s.PutManifest(ctx, tag, m); err != nil {
					errCh <- fmt.Errorf("instance %d manifest: %w", id, err)
					return
				}
				rd, err := s.Resolve(ctx, tag)
				if err != nil {
					errCh <- fmt.Errorf("instance %d resolve: %w", id, err)
					return
				}
				rm, err := s.GetManifest(ctx, rd.Digest)
				if err != nil {
					errCh <- fmt.Errorf("instance %d get-manifest: %w", id, err)
					return
				}
				for _, ref := range rm.references() {
					b, err := s.GetContent(ctx, ref.Digest)
					if err != nil {
						errCh <- fmt.Errorf("instance %d referenced blob %s unreadable: %w", id, ref.Digest, err)
						return
					}
					if DigestOf(b) != ref.Digest || int64(len(b)) != ref.Size {
						errCh <- fmt.Errorf("instance %d corrupted blob %s", id, ref.Digest)
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

	// 确定性收尾：所有实例的标签可达，最终 blob 数 =
	// workers 份 (config+manifest) + 共享层数。
	checker := openExtraStore(t, dir)
	removed, err := checker.GC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("final GC collected=%d", len(removed))
	tags, err := checker.ListTags(ctx)
	if err != nil || len(tags) != workers {
		t.Fatalf("ListTags = %v, %v; want %d tags", tags, err, workers)
	}
	wantBlobs := workers*2 + nShLayers
	if got := blobCount(t, dir); got != wantBlobs {
		t.Fatalf("final blob count = %d, want %d", got, wantBlobs)
	}
	for _, tag := range tags {
		rd, err := checker.Resolve(ctx, tag)
		if err != nil {
			t.Fatalf("final resolve %q: %v", tag, err)
		}
		rm, err := checker.GetManifest(ctx, rd.Digest)
		if err != nil {
			t.Fatalf("final get-manifest %q: %v", tag, err)
		}
		for _, ref := range rm.references() {
			if _, err := checker.StatContent(ctx, ref.Digest); err != nil {
				t.Fatalf("final state: referenced blob %s of tag %q missing: %v", ref.Digest, tag, err)
			}
		}
	}
	t.Logf("multi-instance interleave stable: %d instances, %d tags, %d blobs", workers+1, workers, wantBlobs)
}

// TestCrossInstancePinProtection: 实例 A 钉住并写入但尚未引用的内容，
// 实例 B 的 GC 不得回收；A 解钉后 B 的 GC 才能回收。
func TestCrossInstancePinProtection(t *testing.T) {
	_, dir := openTestStore(t)
	ctx := context.Background()
	a := openExtraStore(t, dir)
	b := openExtraStore(t, dir)

	content := []byte("uploaded-by-A-not-yet-referenced")
	d := DigestOf(content)

	a.Pin(d)
	mustPut(t, a, content)

	// B 回收：A 钉住的内容必须存活。
	removed, err := b.GC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 0 {
		t.Fatalf("B's GC collected %d blobs pinned by A", len(removed))
	}
	if _, err := b.StatContent(ctx, d); err != nil {
		t.Fatalf("blob pinned by A must be visible to B: %v", err)
	}
	t.Log("instance B's GC respected instance A's persistent pin")

	// A 解钉后，B 的 GC 可回收。
	a.Unpin(d)
	removed, err = b.GC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || removed[0].Digest != d {
		t.Fatalf("after unpin, GC should collect exactly the blob, removed=%+v", removed)
	}
	t.Log("after A's unpin, B's GC collected the blob")
}

// deadPID 返回一个确定已退出的进程 pid（启动一个立即结束的进程）。
func deadPID(t *testing.T) int {
	t.Helper()
	cmd := exec.Command("true")
	if err := cmd.Run(); err != nil {
		t.Fatalf("spawn probe process: %v", err)
	}
	return cmd.ProcessState.Pid()
}

// plantPinFile 直接写入一个钉住文件（模拟其他进程的状态）。
func plantPinFile(t *testing.T, dir string, d Digest, pid int, inst string) string {
	t.Helper()
	p := filepath.Join(dir, pinsDir, pinFileName(fmt.Sprintf("%d.%s", pid, inst), d))
	if err := os.WriteFile(p, []byte(string(d)+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestStalePinFromDeadProcessPruned: 属主进程已死的残留钉住文件
// 必须被 GC（以及 Open 恢复）清理，内容重新可被回收——一次崩溃
// 不能让内容永远滞留；属主存活的钉住则必须被尊重。
func TestStalePinFromDeadProcessPruned(t *testing.T) {
	s, dir := openTestStore(t)
	ctx := context.Background()

	deadContent := []byte("pinned-by-dead-process")
	liveContent := []byte("pinned-by-live-process")
	deadD := mustPut(t, s, deadContent).Digest
	liveD := mustPut(t, s, liveContent).Digest

	deadPin := plantPinFile(t, dir, deadD, deadPID(t), "deadinst")
	livePin := plantPinFile(t, dir, liveD, os.Getpid(), "liveinst")

	// GC：死进程的钉住被清理、内容回收；活进程的钉住生效、内容保留。
	removed, err := s.GC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || removed[0].Digest != deadD {
		t.Fatalf("GC should collect only dead-pinned blob, removed=%+v", removed)
	}
	if _, err := os.Stat(deadPin); !os.IsNotExist(err) {
		t.Fatalf("stale pin file of dead process must be pruned, stat err=%v", err)
	}
	if _, err := os.Stat(livePin); err != nil {
		t.Fatalf("live process pin file must survive GC: %v", err)
	}
	if _, err := s.StatContent(ctx, liveD); err != nil {
		t.Fatalf("blob pinned by live process must survive GC: %v", err)
	}
	t.Log("GC pruned dead-process pin, respected live-process pin")

	// Open 恢复路径同样清理死进程钉住。
	deadD2 := mustPut(t, s, []byte("pinned-by-dead-process-2")).Digest
	deadPin2 := plantPinFile(t, dir, deadD2, deadPID(t), "deadinst2")
	s = reopen(t, s, dir)
	if _, err := os.Stat(deadPin2); !os.IsNotExist(err) {
		t.Fatalf("recover must prune dead-process pin file, stat err=%v", err)
	}
	removed, err = s.GC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, d := range removed {
		if d.Digest == deadD2 {
			found = true
		}
	}
	if !found {
		t.Fatalf("after recover, formerly dead-pinned blob must be collectable, removed=%+v", removed)
	}
	if _, err := s.StatContent(ctx, liveD); err != nil {
		t.Fatalf("live-pinned blob must still survive after reopen+GC: %v", err)
	}
	t.Log("recover pruned dead-process pin; live-process pin still honored")
}

// TestCrossInstanceTagLinearizability: 两个实例并发改指同一标签，
// 第三个实例反复解析并读取：每次读到的都必须是某个完整写入的
// 清单及其全部引用，不得出现撕裂状态。
func TestCrossInstanceTagLinearizability(t *testing.T) {
	_, dir := openTestStore(t)
	ctx := context.Background()
	const tag = "contended.tag-1.0"

	w1 := openExtraStore(t, dir)
	w2 := openExtraStore(t, dir)
	reader := openExtraStore(t, dir)

	// 两位写者各自的完整镜像（config 不同，共享一层）。
	shared := []byte("shared-layer-for-contended-tag")
	mustPut(t, w1, shared)
	cfg1 := []byte("config-writer-1")
	cfg2 := []byte("config-writer-2")
	mustPut(t, w1, cfg1)
	mustPut(t, w2, cfg2)
	m1 := buildManifest(cfg1, shared)
	m2 := buildManifest(cfg2, shared)
	d1 := DigestOf(mustJSON(t, m1))
	d2 := DigestOf(mustJSON(t, m2))

	const iter = 60
	start := make(chan struct{})
	errCh := make(chan error, 3)
	var wg sync.WaitGroup
	writer := func(s *Store, m Manifest) {
		defer wg.Done()
		<-start
		for i := 0; i < iter; i++ {
			if _, err := s.PutManifest(ctx, tag, m); err != nil {
				errCh <- fmt.Errorf("writer: %w", err)
				return
			}
		}
	}
	wg.Add(2)
	go writer(w1, m1)
	go writer(w2, m2)
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		seen := false // 标签是否已被某个写者首次发布
		for i := 0; i < iter*2; i++ {
			rd, err := reader.Resolve(ctx, tag)
			if err != nil {
				if !seen && errors.Is(err, ErrNotFound) {
					continue // 首个写者尚未发布，合法
				}
				errCh <- fmt.Errorf("reader resolve: %w", err)
				return
			}
			seen = true
			if rd.Digest != d1 && rd.Digest != d2 {
				errCh <- fmt.Errorf("reader saw torn tag -> unknown manifest %s", rd.Digest)
				return
			}
			rm, err := reader.GetManifest(ctx, rd.Digest)
			if err != nil {
				errCh <- fmt.Errorf("reader get-manifest %s: %w", rd.Digest, err)
				return
			}
			for _, ref := range rm.references() {
				b, err := reader.GetContent(ctx, ref.Digest)
				if err != nil || DigestOf(b) != ref.Digest {
					errCh <- fmt.Errorf("reader saw inconsistent ref %s: %v", ref.Digest, err)
					return
				}
			}
		}
	}()
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	// 最终标签必须指向两者之一且完整可读。
	rd, err := reader.Resolve(ctx, tag)
	if err != nil {
		t.Fatal(err)
	}
	if rd.Digest != d1 && rd.Digest != d2 {
		t.Fatalf("final tag points to unknown manifest %s", rd.Digest)
	}
	t.Logf("contended tag always resolved to a complete manifest (final=%s)", rd.Digest)
}

// mustJSON 序列化清单（与 PutManifest 内部一致），用于计算预期摘要。
func mustJSON(t *testing.T, m Manifest) []byte {
	t.Helper()
	b, err := json.Marshal(&m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
