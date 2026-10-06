package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// 本文件覆盖多进程场景：若干独立进程（测试二进制以 helper 模式
// 重新 exec 自身）同时打开同一个存储目录，并行推送镜像、解析标签、
// 回收空间，以及进程被 SIGKILL 后的恢复。
//
// helper 进程的全部日志经 stderr 回传给父进程并打进测试日志，
// 每个判定（为何通过/失败）都能在日志中找到依据。

// helperCommand 构造一个以 helper 模式运行本测试二进制的子进程。
func helperCommand(t *testing.T, args ...string) (*exec.Cmd, *bytes.Buffer) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$", "--")
	cmd.Args = append(cmd.Args, args...)
	cmd.Env = append(os.Environ(), "STORE_HELPER=1")
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	return cmd, &out
}

// TestHelperProcess 是子进程入口；正常测试运行时环境变量未设置，直接返回。
func TestHelperProcess(t *testing.T) {
	if os.Getenv("STORE_HELPER") != "1" {
		return
	}
	var rest []string
	for i, a := range os.Args {
		if a == "--" {
			rest = os.Args[i+1:]
			break
		}
	}
	if len(rest) < 2 {
		fmt.Fprintln(os.Stderr, "helper: bad args")
		os.Exit(2)
	}
	dir, mode := rest[0], rest[1]
	logger := log.New(os.Stderr, "", 0)
	say := func(format string, a ...any) { fmt.Fprintf(os.Stderr, "helper[%s/%d]: %s\n", mode, os.Getpid(), fmt.Sprintf(format, a...)) }

	s, err := Open(dir, logger)
	if err != nil {
		say("open: %v", err)
		os.Exit(1)
	}
	ctx := context.Background()

	switch mode {
	case "push":
		// push <dir> push <tag> <iters>：钉住 -> 上传 -> 建清单 -> 解钉 -> 解析校验。
		tag, iters := rest[2], mustAtoi(say, rest[3])
		config, layers := helperImageBlobs(tag)
		m := buildManifest(config, layers...)
		digests := []Digest{DigestOf(config)}
		for _, l := range layers {
			digests = append(digests, DigestOf(l))
		}
		for k := 0; k < iters; k++ {
			for _, d := range digests {
				s.Pin(d)
			}
			for _, b := range append([][]byte{config}, layers...) {
				d := Descriptor{Digest: DigestOf(b), Size: int64(len(b))}
				if err := s.PutContent(ctx, d, bytes.NewReader(b)); err != nil {
					say("put: %v", err)
					os.Exit(1)
				}
			}
			md, err := s.PutManifest(ctx, tag, m)
			if err != nil {
				say("manifest: %v", err)
				os.Exit(1)
			}
			for _, d := range digests {
				s.Unpin(d)
			}
			rd, err := s.Resolve(ctx, tag)
			if err != nil {
				say("resolve: %v", err)
				os.Exit(1)
			}
			if rd.Digest != md.Digest {
				say("resolve mismatch: %s != %s", rd.Digest, md.Digest)
				os.Exit(1)
			}
			rm, err := s.GetManifest(ctx, rd.Digest)
			if err != nil {
				say("get-manifest: %v", err)
				os.Exit(1)
			}
			for _, ref := range rm.references() {
				b, err := s.GetContent(ctx, ref.Digest)
				if err != nil {
					say("referenced blob unreadable: %v", err)
					os.Exit(1)
				}
				if DigestOf(b) != ref.Digest {
					say("referenced blob corrupted: %s", ref.Digest)
					os.Exit(1)
				}
			}
			say("iter %d ok tag=%s manifest=%s", k, tag, md.Digest)
		}
	case "gc":
		// gc <dir> gc <rounds>：周期性回收。
		rounds := mustAtoi(say, rest[2])
		for k := 0; k < rounds; k++ {
			removed, err := s.GC(ctx)
			if err != nil {
				say("gc: %v", err)
				os.Exit(1)
			}
			say("round %d collected=%d", k, len(removed))
			time.Sleep(3 * time.Millisecond)
		}
	case "gc-die":
		// gc-die <dir> gc-die <killAfter>：GC 删除 killAfter 个 blob 后
		// 立即 SIGKILL 自杀，模拟回收中途进程被杀。
		killAfter := mustAtoi(say, rest[2])
		count := 0
		s.gcSweepHook = func(d Digest) {
			count++
			if count >= killAfter {
				say("killing self mid-GC after %d deletions (last=%s)", count, d)
				syscall.Kill(syscall.Getpid(), syscall.SIGKILL)
			}
		}
		if _, err := s.GC(ctx); err != nil {
			say("gc: %v", err)
			os.Exit(1)
		}
		say("ERROR: gc completed without being killed")
		os.Exit(1)
	case "pin-hold":
		// pin-hold <dir> pin-hold <digest> <readyFile>：钉住摘要后挂起，
		// 直到被父进程杀死（模拟持钉进程崩溃）。
		d := Digest(rest[2])
		ready := rest[3]
		s.Pin(d)
		if err := os.WriteFile(ready, []byte("1"), 0o644); err != nil {
			say("ready file: %v", err)
			os.Exit(1)
		}
		say("pinned %s, holding until killed", d)
		select {}
	case "put-die":
		// put-die <dir> put-die <size> <killAt>：上传过程中 SIGKILL 自杀，
		// 模拟写了一半进程被杀。
		size, killAt := mustAtoi(say, rest[2]), mustAtoi(say, rest[3])
		content := deterministicBytes(size)
		desc := Descriptor{Digest: DigestOf(content), Size: int64(size)}
		err := s.PutContent(ctx, desc, &killReader{data: content, killAt: killAt, say: say})
		say("ERROR: PutContent returned %v (expected to be killed)", err)
		os.Exit(1)
	default:
		say("unknown mode %q", mode)
		os.Exit(2)
	}
	os.Exit(0)
}

func mustAtoi(say func(string, ...any), s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		say("bad number %q: %v", s, err)
		os.Exit(2)
	}
	return n
}

// helperImageBlobs 生成 helper 镜像内容：config 按 tag 区分，
// layers 为所有 helper 共享（验证跨进程去重）。
func helperImageBlobs(tag string) (config []byte, layers [][]byte) {
	return []byte(`{"helper":"` + tag + `"}`), [][]byte{
		[]byte("shared-layer-0-bytes"),
		[]byte("shared-layer-1-bytes"),
		[]byte("shared-layer-2-bytes"),
	}
}

// deterministicBytes 生成父子进程可各自独立算出的确定性内容。
func deterministicBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

// killReader 在流出 killAt 字节后 SIGKILL 本进程。
type killReader struct {
	data   []byte
	off    int
	killAt int
	say    func(string, ...any)
}

func (r *killReader) Read(p []byte) (int, error) {
	if r.off >= r.killAt {
		r.say("killing self mid-upload at offset %d/%d", r.off, len(r.data))
		syscall.Kill(syscall.Getpid(), syscall.SIGKILL)
	}
	n := copy(p, r.data[r.off:])
	r.off += n
	if r.off >= len(r.data) {
		return n, io.EOF
	}
	return n, nil
}

// runHelperOK 运行 helper 并断言成功退出，日志并入测试日志。
func runHelperOK(t *testing.T, what string, args ...string) {
	t.Helper()
	cmd, out := helperCommand(t, args...)
	if err := cmd.Run(); err != nil {
		t.Fatalf("%s: helper failed: %v\noutput:\n%s", what, err, out.String())
	}
	t.Logf("%s: helper ok, output:\n%s", what, out.String())
}

// TestMultiProcessConcurrentPushResolveGC: 4 个独立进程并行推送镜像
// （钉住->上传->建清单->解析->逐引用读校验），同时另有 2 个进程周期
// 性 GC。所有成功建立引用的内容必须在引用有效期内完整可读；跨进程
// 去重要求共享层在磁盘上只有一份。
func TestMultiProcessConcurrentPushResolveGC(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := Open(dir, testLogger{t})
	if err != nil {
		t.Fatal(err)
	}

	const helpers = 4
	const iters = 8
	type running struct {
		tag string
		cmd *exec.Cmd
		out *bytes.Buffer
	}
	var pushes []running
	for i := 0; i < helpers; i++ {
		tag := fmt.Sprintf("img-%d", i)
		cmd, out := helperCommand(t, dir, "push", tag, strconv.Itoa(iters))
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		pushes = append(pushes, running{tag, cmd, out})
	}
	var gcs []running
	for i := 0; i < 2; i++ {
		cmd, out := helperCommand(t, dir, "gc", "12")
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		gcs = append(gcs, running{fmt.Sprintf("gc-%d", i), cmd, out})
	}
	for _, p := range pushes {
		if err := p.cmd.Wait(); err != nil {
			t.Fatalf("push helper tag=%s failed: %v\noutput:\n%s", p.tag, err, p.out.String())
		}
		t.Logf("push helper tag=%s done, output:\n%s", p.tag, p.out.String())
	}
	for _, g := range gcs {
		if err := g.cmd.Wait(); err != nil {
			t.Fatalf("gc helper failed: %v\noutput:\n%s", err, g.out.String())
		}
		t.Logf("gc helper %s done, output:\n%s", g.tag, g.out.String())
	}

	// 判定 1：每个 helper 的标签都能解析，且全部引用内容逐字节正确。
	for i := 0; i < helpers; i++ {
		tag := fmt.Sprintf("img-%d", i)
		rd, err := s.Resolve(ctx, tag)
		if err != nil {
			t.Fatalf("Resolve(%q) after concurrent run: %v", tag, err)
		}
		rm, err := s.GetManifest(ctx, rd.Digest)
		if err != nil {
			t.Fatalf("GetManifest(%q): %v", tag, err)
		}
		for _, ref := range rm.references() {
			b, err := s.GetContent(ctx, ref.Digest)
			if err != nil {
				t.Fatalf("tag %q: referenced blob %s unreadable: %v", tag, ref.Digest, err)
			}
			if DigestOf(b) != ref.Digest || int64(len(b)) != ref.Size {
				t.Fatalf("tag %q: referenced blob %s corrupted", tag, ref.Digest)
			}
		}
	}
	t.Logf("all %d tags resolve; every referenced blob verified byte-exact", helpers)

	// 判定 2：最终 GC 无可回收（全部内容被引用），blob 总数精确等于
	// helpers 份 config + helpers 份清单 + 3 份共享层（跨进程去重）。
	removed, err := s.GC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 0 {
		t.Fatalf("final GC collected %d referenced blobs: %v", len(removed), removed)
	}
	wantBlobs := helpers + helpers + 3
	if got := blobCount(t, dir); got != wantBlobs {
		t.Fatalf("blob count = %d, want %d (4 configs + 4 manifests + 3 shared layers, deduped)", got, wantBlobs)
	}
	t.Logf("final state exact: %d tags, %d blobs on disk, GC stable", helpers, wantBlobs)
}

// TestCrossProcessPinProtectionAndReaping: 一个进程钉住未引用内容后，
// 另一个进程的 GC 不得回收它；持钉进程被 SIGKILL 后，残留钉住必须被
// 自动收割，内容恢复可回收——全程无需人工清理。
func TestCrossProcessPinProtectionAndReaping(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := Open(dir, testLogger{t})
	if err != nil {
		t.Fatal(err)
	}
	orphan := []byte("orphan protected cross-process")
	od := mustPut(t, s, orphan)

	sigDir := t.TempDir()
	ready := filepath.Join(sigDir, "ready")
	cmd, out := helperCommand(t, dir, "pin-hold", string(od.Digest), ready)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waitForFile(t, ready, 10*time.Second)
	t.Log("pin-hold helper is up and holds the pin")

	// 判定 1：跨进程钉住生效——GC 不得回收被其他进程钉住的内容。
	removed, err := s.GC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 0 {
		t.Fatalf("GC collected %d blobs though pinned by another process: %v", len(removed), removed)
	}
	if _, err := s.GetContent(ctx, od.Digest); err != nil {
		t.Fatalf("cross-process pinned blob collected: %v", err)
	}
	t.Log("GC skipped the blob pinned by the live helper (removed==0)")

	// 杀掉持钉进程（模拟崩溃）。
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	t.Logf("pin-hold helper killed, output:\n%s", out.String())

	// 判定 2：残留钉住被自动收割，内容恢复可回收。
	removed, err = s.GC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || removed[0].Digest != od.Digest {
		t.Fatalf("after owner death GC removed %+v, want only %s", removed, od.Digest)
	}
	if _, err := s.GetContent(ctx, od.Digest); !errors.Is(err, ErrNotFound) {
		t.Fatalf("blob should be collected after owner death, got %v", err)
	}
	// 判定 3：pins/ 目录无残留（收割而非泄漏）。
	entries, err := os.ReadDir(filepath.Join(dir, pinsDir))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("stale pin files left behind: %v", entries)
	}
	t.Log("stale pin reaped automatically; blob collected; pins/ clean")
}

// TestGCInterruptMidSweepRecovery: 回收进程在删除中途被 SIGKILL，
// 存储必须保持自洽：被引用的内容完好、剩余孤儿可被后续任意进程
// 继续回收、锁不卡死；重复多轮"中断->重启"结论不变。
func TestGCInterruptMidSweepRecovery(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := Open(dir, testLogger{t})
	if err != nil {
		t.Fatal(err)
	}
	keepCfg, keepLayer := []byte("keep-cfg"), []byte("keep-layer")
	_, keepDesc := seedImage(t, s, "keep", keepCfg, keepLayer)

	seedOrphans := func(n int, prefix string) {
		t.Helper()
		for i := 0; i < n; i++ {
			mustPut(t, s, []byte(fmt.Sprintf("%s-orphan-%d", prefix, i)))
		}
	}
	assertKeepIntact := func(round string) {
		t.Helper()
		rd, err := s.Resolve(ctx, "keep")
		if err != nil {
			t.Fatalf("%s: keep tag lost: %v", round, err)
		}
		if rd.Digest != keepDesc.Digest {
			t.Fatalf("%s: keep tag moved: %s != %s", round, rd.Digest, keepDesc.Digest)
		}
		for _, d := range []Digest{keepDesc.Digest, DigestOf(keepCfg), DigestOf(keepLayer)} {
			if _, err := s.GetContent(ctx, d); err != nil {
				t.Fatalf("%s: referenced blob %s unreadable: %v", round, d, err)
			}
		}
	}

	const rounds = 3
	orphansPer := 10
	for round := 0; round < rounds; round++ {
		prefix := fmt.Sprintf("round%d", round)
		seedOrphans(orphansPer, prefix)

		// 回收进程删除 3 个后被 SIGKILL。
		cmd, out := helperCommand(t, dir, "gc-die", "3")
		err := cmd.Run()
		if err == nil {
			t.Fatalf("round %d: gc-die helper should have been killed, output:\n%s", round, out.String())
		}
		t.Logf("round %d: GC process killed mid-sweep as expected (%v), output:\n%s", round, err, out.String())

		// 模拟另一个进程接手：重新 Open（触发恢复），立即可用、不卡锁。
		s = reopen(t, s, dir)
		assertKeepIntact(prefix)

		// 剩余孤儿必须能被本轮 GC 全部收走（上一轮删了 3 个）。
		removed, err := s.GC(ctx)
		if err != nil {
			t.Fatalf("round %d: GC after kill: %v", round, err)
		}
		if len(removed) != orphansPer-3 {
			t.Fatalf("round %d: GC collected %d, want %d remaining orphans", round, len(removed), orphansPer-3)
		}
		assertKeepIntact(prefix + "-post-gc")
		if got := blobCount(t, dir); got != 3 {
			t.Fatalf("round %d: blob count = %d, want 3 (keep cfg+layer+manifest)", round, got)
		}
		t.Logf("round %d: store self-consistent after kill; %d leftover orphans collected by successor", round, orphansPer-3)
	}
	t.Log("repeated kill/restart cycles converge without manual cleanup")
}

// TestUploadInterruptMidWriteRecovery: 上传进程写了一半被 SIGKILL，
// 另一个进程 Open 后：半截临时文件被清理、内容不被误判为已存在、
// 完整重传成功。
func TestUploadInterruptMidWriteRecovery(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := Open(dir, testLogger{t})
	if err != nil {
		t.Fatal(err)
	}

	const size = 1 << 20 // 1 MiB
	content := deterministicBytes(size)
	desc := Descriptor{Digest: DigestOf(content), Size: int64(size)}

	cmd, out := helperCommand(t, dir, "put-die", strconv.Itoa(size), "65536")
	err = cmd.Run()
	if err == nil {
		t.Fatalf("put-die helper should have been killed, output:\n%s", out.String())
	}
	t.Logf("upload process killed mid-write as expected (%v), output:\n%s", err, out.String())

	// 另一个进程接手：恢复清理半截临时文件。
	s = reopen(t, s, dir)
	leftovers, err := os.ReadDir(filepath.Join(dir, uploadsDir))
	if err != nil {
		t.Fatal(err)
	}
	if len(leftovers) != 0 {
		t.Fatalf("incomplete upload not cleaned by recovery: %v", leftovers)
	}
	// 半截内容绝不参与去重：按完整摘要读取必须 NotFound。
	if _, err := s.GetContent(ctx, desc.Digest); !errors.Is(err, ErrNotFound) {
		t.Fatalf("half-written content must not be visible, got %v", err)
	}
	// 完整重传成功且内容正确。
	if err := s.PutContent(ctx, desc, bytes.NewReader(content)); err != nil {
		t.Fatalf("re-put after recovery: %v", err)
	}
	got, err := s.GetContent(ctx, desc.Digest)
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("content wrong after re-put: %v", err)
	}
	t.Log("half-written upload cleaned; full re-upload succeeded byte-exact")
}

// TestFileLockMutualExclusion: 直接验证跨进程互斥语义——独占锁持有
// 期间其他句柄的读必须阻塞，释放后立即放行；共享锁之间互不阻塞。
// （同一进程内开两个 Store 实例：进程内锁互不相干，隔离完全来自
// 文件锁，等价于两个进程。）
func TestFileLockMutualExclusion(t *testing.T) {
	dir := t.TempDir()
	s1, err := Open(dir, testLogger{t})
	if err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir, testLogger{t})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// s1 持独占锁时，s2 的读必须阻塞。
	release, err := s1.acquireFileLock(false)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := s2.Resolve(ctx, "nonexistent")
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("read completed (%v) while exclusive lock held by another handle", err)
	case <-time.After(300 * time.Millisecond):
		t.Log("read blocked while exclusive lock held (judged by 300ms timeout)")
	}
	release()
	select {
	case err := <-done:
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("read after release: want ErrNotFound, got %v", err)
		}
		t.Log("read proceeded immediately after lock release")
	case <-time.After(5 * time.Second):
		t.Fatal("read still blocked after lock release (lock leaked)")
	}

	// 共享锁之间互不阻塞。
	r1, err := s1.acquireFileLock(true)
	if err != nil {
		t.Fatal(err)
	}
	defer r1()
	r2Done := make(chan struct{})
	go func() {
		r2, err := s2.acquireFileLock(true)
		if err != nil {
			t.Errorf("second shared lock: %v", err)
		} else {
			r2()
		}
		close(r2Done)
	}()
	select {
	case <-r2Done:
		t.Log("two shared locks held concurrently (no blocking)")
	case <-time.After(5 * time.Second):
		t.Fatal("shared lock blocked behind shared lock")
	}
}

func waitForFile(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}
