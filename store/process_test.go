package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// 本文件的用例通过 os/exec 启动真正的子进程并发操作同一存储目录，
// 验证跨进程互斥、并发交织与回收中断后的恢复。
//
// 子进程复用测试二进制：设置 STORE_HELPER_MODE 后，TestHelperProcess
// 执行真正的 helper 逻辑并 os.Exit；未设置时直接返回（对父进程无害）。

// TestHelperProcess 是子进程入口；父进程中它是一个立即返回的空测试。
func TestHelperProcess(t *testing.T) {
	mode := os.Getenv("STORE_HELPER_MODE")
	if mode == "" {
		return
	}
	logger := log.New(os.Stderr, "["+mode+"] ", log.Lmicroseconds)
	dir := os.Getenv("STORE_DIR")
	var code int
	switch mode {
	case "writer":
		code = runWriterHelper(dir, logger)
	case "gc":
		code = runGCHelper(dir, logger)
	default:
		fmt.Fprintf(os.Stderr, "unknown helper mode %q\n", mode)
		code = 2
	}
	os.Exit(code)
}

// helperSharedLayers 是所有 helper 共用的共享层（内容固定，便于父进程
// 做精确的最终状态断言）。
var helperSharedLayers = [][]byte{
	[]byte("helper-shared-layer-0"),
	[]byte("helper-shared-layer-1"),
}

// helperConfig 生成指定写者的固定 config 内容（每写者一份，逐次迭代不变）。
func helperConfig(id int) []byte {
	return []byte(fmt.Sprintf(`{"writer":%d}`, id))
}

// runWriterHelper 模拟一个推镜像的工具进程：钉住 -> 上传 -> 建清单 ->
// 解析 -> 逐引用读校验，循环往复；同时改指一个共享标签以制造写冲突。
func runWriterHelper(dir string, logger *log.Logger) int {
	id, _ := strconv.Atoi(os.Getenv("STORE_ID"))
	iters, _ := strconv.Atoi(os.Getenv("STORE_ITERS"))
	s, err := Open(dir, logAdapter{logger})
	if err != nil {
		logger.Printf("open: %v", err)
		return 1
	}
	defer s.Close()
	ctx := context.Background()
	config := helperConfig(id)
	m := buildManifest(config, helperSharedLayers...)
	tag := fmt.Sprintf("img-%d", id)
	pins := []Digest{DigestOf(config)}
	for _, l := range helperSharedLayers {
		pins = append(pins, DigestOf(l))
	}
	for k := 0; k < iters; k++ {
		for _, d := range pins {
			s.Pin(d)
		}
		fail := func(err error) int {
			logger.Printf("iter=%d: %v", k, err)
			return 1
		}
		for _, b := range append([][]byte{config}, helperSharedLayers...) {
			d := Descriptor{Digest: DigestOf(b), Size: int64(len(b))}
			if err := s.PutContent(ctx, d, bytes.NewReader(b)); err != nil {
				return fail(fmt.Errorf("put: %w", err))
			}
		}
		if _, err := s.PutManifest(ctx, tag, m); err != nil {
			return fail(fmt.Errorf("put-manifest: %w", err))
		}
		// 共享标签：所有写者改指同一个名字，制造跨进程写冲突。
		if _, err := s.PutManifest(ctx, "shared.tag", m); err != nil {
			return fail(fmt.Errorf("put-shared-manifest: %w", err))
		}
		rd, err := s.Resolve(ctx, tag)
		if err != nil {
			return fail(fmt.Errorf("resolve: %w", err))
		}
		rm, err := s.GetManifest(ctx, rd.Digest)
		if err != nil {
			return fail(fmt.Errorf("get-manifest: %w", err))
		}
		for _, ref := range rm.references() {
			b, err := s.GetContent(ctx, ref.Digest)
			if err != nil {
				return fail(fmt.Errorf("referenced blob %s unreadable: %w", ref.Digest, err))
			}
			if DigestOf(b) != ref.Digest {
				return fail(fmt.Errorf("referenced blob %s corrupted", ref.Digest))
			}
		}
		for _, d := range pins {
			s.Unpin(d)
		}
		if k%5 == 0 {
			logger.Printf("iter=%d ok tag=%s manifest=%s", k, tag, rd.Digest)
		}
	}
	logger.Printf("done: %d iterations, tag=%s", iters, tag)
	return 0
}

// runGCHelper 模拟周期性回收的清理进程；STORE_GC_MS>0 时运行指定
// 毫秒数后退出，否则一直运行直到被杀。
func runGCHelper(dir string, logger *log.Logger) int {
	ms, _ := strconv.Atoi(os.Getenv("STORE_GC_MS"))
	s, err := Open(dir, logAdapter{logger})
	if err != nil {
		logger.Printf("open: %v", err)
		return 1
	}
	defer s.Close()
	ctx := context.Background()
	deadline := time.Now().Add(time.Duration(ms) * time.Millisecond)
	for round := 0; ; round++ {
		removed, err := s.GC(ctx)
		if err != nil {
			logger.Printf("gc round=%d: %v", round, err)
			return 1
		}
		logger.Printf("gc round=%d collected=%d", round, len(removed))
		if ms > 0 && time.Now().After(deadline) {
			logger.Printf("done: %d rounds", round+1)
			return 0
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// logAdapter 把 *log.Logger 适配到 store.Logger 接口。
type logAdapter struct{ l *log.Logger }

func (a logAdapter) Printf(format string, args ...any) { a.l.Printf(format, args...) }

// helperCmd 构造一个运行指定 helper 模式的子进程命令。
func helperCmd(t *testing.T, mode string, extraEnv ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$", "-test.v")
	cmd.Env = append(os.Environ(),
		"STORE_HELPER_MODE="+mode,
	)
	cmd.Env = append(cmd.Env, extraEnv...)
	return cmd
}

// runHelper 同步运行 helper 子进程并把其输出并入测试日志。
func runHelper(t *testing.T, name string, cmd *exec.Cmd) {
	t.Helper()
	out, err := cmd.CombinedOutput()
	for _, line := range bytes.Split(bytes.TrimRight(out, "\n"), []byte("\n")) {
		t.Logf("%s | %s", name, line)
	}
	if err != nil {
		t.Fatalf("helper %s failed: %v", name, err)
	}
}

// TestCrossProcessInterleaved: 多个真实进程并行推镜像（含共享标签
// 写冲突），同时另一个进程持续回收。全部结束后父进程验证最终状态
// 精确符合预期：每个写者标签可解析、引用完整、共享标签指向某个
// 完整清单、blob 数量精确。
func TestCrossProcessInterleaved(t *testing.T) {
	dir := t.TempDir()
	const writers = 3

	var cmds []*exec.Cmd
	names := []string{}
	for i := 0; i < writers; i++ {
		cmds = append(cmds, helperCmd(t, "writer",
			"STORE_DIR="+dir,
			"STORE_ID="+strconv.Itoa(i),
			"STORE_ITERS=20",
		))
		names = append(names, fmt.Sprintf("writer-%d", i))
	}
	cmds = append(cmds, helperCmd(t, "gc", "STORE_DIR="+dir, "STORE_GC_MS=1500"))
	names = append(names, "gc")

	// 全部并发启动，再逐个等待。
	outputs := make([]*bytes.Buffer, len(cmds))
	for i, cmd := range cmds {
		buf := &bytes.Buffer{}
		cmd.Stdout, cmd.Stderr = buf, buf
		outputs[i] = buf
		if err := cmd.Start(); err != nil {
			t.Fatalf("start %s: %v", names[i], err)
		}
	}
	for i, cmd := range cmds {
		err := cmd.Wait()
		for _, line := range bytes.Split(bytes.TrimRight(outputs[i].Bytes(), "\n"), []byte("\n")) {
			t.Logf("%s | %s", names[i], line)
		}
		if err != nil {
			t.Fatalf("helper %s failed: %v", names[i], err)
		}
	}

	// 父进程验证最终状态。
	s, err := Open(dir, testLogger{t})
	if err != nil {
		t.Fatalf("parent open: %v", err)
	}
	defer s.Close()
	ctx := context.Background()

	wantManifests := make(map[Digest]bool)
	for i := 0; i < writers; i++ {
		m := buildManifest(helperConfig(i), helperSharedLayers...)
		b, _ := json.Marshal(&m)
		wantManifests[DigestOf(b)] = true

		rd, err := s.Resolve(ctx, fmt.Sprintf("img-%d", i))
		if err != nil {
			t.Fatalf("tag img-%d must resolve after cross-process run: %v", i, err)
		}
		if !wantManifests[rd.Digest] {
			t.Fatalf("tag img-%d points to unexpected manifest %s", i, rd.Digest)
		}
		assertManifestReadable(t, s, rd.Digest)
	}
	// 共享标签必须指向某个写者的完整清单（互斥改指的结果）。
	rd, err := s.Resolve(ctx, "shared.tag")
	if err != nil {
		t.Fatalf("shared.tag must resolve: %v", err)
	}
	if !wantManifests[rd.Digest] {
		t.Fatalf("shared.tag points to unknown manifest %s (torn write?)", rd.Digest)
	}
	assertManifestReadable(t, s, rd.Digest)

	// 最终 GC 后 blob 数精确：writers 份 (config+manifest) + 共享层。
	removed, err := s.GC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("parent final GC collected=%d", len(removed))
	wantBlobs := writers*2 + len(helperSharedLayers)
	if got := blobCount(t, dir); got != wantBlobs {
		t.Fatalf("final blob count = %d, want %d", got, wantBlobs)
	}
	t.Logf("cross-process interleave converged: %d writers + 1 gc, %d blobs on disk", writers, wantBlobs)
}

// assertManifestReadable 读取清单并逐引用校验内容完整。
func assertManifestReadable(t *testing.T, s *Store, d Digest) {
	t.Helper()
	m, err := s.GetManifest(context.Background(), d)
	if err != nil {
		t.Fatalf("manifest %s unreadable: %v", d, err)
	}
	for _, ref := range m.references() {
		b, err := s.GetContent(context.Background(), ref.Digest)
		if err != nil {
			t.Fatalf("referenced blob %s of manifest %s missing: %v", ref.Digest, d, err)
		}
		if DigestOf(b) != ref.Digest {
			t.Fatalf("referenced blob %s of manifest %s corrupted", ref.Digest, d)
		}
	}
}

// TestGCProcessKilledMidSweep: 回收进程在清扫中途被 kill -9。
// 之后无论哪个进程接手：存储必须自洽（标签可解析、引用完整）、
// 不卡锁、可继续写入与回收，无需任何人工清理。
func TestGCProcessKilledMidSweep(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// 父进程先布置：若干健康标签 + 大量待回收垃圾 blob。
	s, err := Open(dir, testLogger{t})
	if err != nil {
		t.Fatal(err)
	}
	const nTags = 3
	for i := 0; i < nTags; i++ {
		seedImage(t, s, fmt.Sprintf("keep-%d", i),
			[]byte(fmt.Sprintf("cfg-keep-%d", i)),
			[]byte(fmt.Sprintf("layer-keep-%d", i)))
	}
	const nGarbage = 2000
	for i := 0; i < nGarbage; i++ {
		mustPut(t, s, []byte(fmt.Sprintf("garbage-blob-%d", i)))
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("seeded %d tags and %d garbage blobs", nTags, nGarbage)

	// 回收进程在清扫中途被 kill -9：父进程监视 blobs 目录，
	// 等清扫明确已经开始（垃圾已被删掉一批）才动手，保证
	// 被打断的是"删了一半"的回收而不是空闲循环。
	gc := helperCmd(t, "gc", "STORE_DIR="+dir, "STORE_GC_MS=0") // 无限循环
	var gcOut bytes.Buffer
	gc.Stdout, gc.Stderr = &gcOut, &gcOut
	if err := gc.Start(); err != nil {
		t.Fatal(err)
	}
	initialBlobs := blobCount(t, dir)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if blobCount(t, dir) <= initialBlobs-nGarbage/2 {
			break // 清扫已删除一半以上垃圾：正处于删了一半的状态
		}
		if time.Now().After(deadline) {
			t.Logf("warning: sweep progress not observed in time, killing anyway")
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err := gc.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	midBlobs := blobCount(t, dir)
	gc.Wait()
	t.Logf("killed gc process mid-sweep (blobs %d -> %d at kill time); its log tail:", initialBlobs, midBlobs)
	for _, line := range bytes.Split(bytes.TrimRight(gcOut.Bytes(), "\n"), []byte("\n")) {
		t.Logf("gc | %s", line)
	}

	// 另一个进程接手：Open 恢复 + 正常读写回收，不得卡住。
	s2, err := Open(dir, testLogger{t})
	if err != nil {
		t.Fatalf("reopen after killing gc: %v", err)
	}
	defer s2.Close()
	for i := 0; i < nTags; i++ {
		rd, err := s2.Resolve(ctx, fmt.Sprintf("keep-%d", i))
		if err != nil {
			t.Fatalf("tag keep-%d must survive killed gc: %v", i, err)
		}
		assertManifestReadable(t, s2, rd.Digest)
	}
	// 继续写入正常。
	seedImage(t, s2, "after-kill", []byte("cfg-after"), []byte("layer-after"))
	// 剩余垃圾可被后续 GC 清完，且收敛（第二轮 0）。
	removed1, err := s2.GC(ctx)
	if err != nil {
		t.Fatalf("GC after kill: %v", err)
	}
	removed2, err := s2.GC(ctx)
	if err != nil {
		t.Fatalf("second GC after kill: %v", err)
	}
	if len(removed2) != 0 {
		t.Fatalf("GC must converge after kill, second round collected %d", len(removed2))
	}
	wantBlobs := (nTags + 1) * 3 // 每个标签：config + layer + manifest
	if got := blobCount(t, dir); got != wantBlobs {
		t.Fatalf("blob count after recovery = %d, want %d", got, wantBlobs)
	}
	t.Logf("after kill: tags intact, GC converged (round1=%d round2=0), %d blobs remain",
		len(removed1), wantBlobs)
}

// TestWriterKilledMidUpload: 写进程在上传/建引用中途被 kill -9。
// 重启恢复后：已发布的标签完整可读，未完成的写入被清理，
// 死进程残留的钉住文件被清除，后续访问不卡住。
func TestWriterKilledMidUpload(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	w := helperCmd(t, "writer",
		"STORE_DIR="+dir,
		"STORE_ID=7",
		"STORE_ITERS=1000000", // 实际跑不完，中途被杀
	)
	var wOut bytes.Buffer
	w.Stdout, w.Stderr = &wOut, &wOut
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	wpid := w.Process.Pid
	if err := w.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	w.Wait()
	t.Logf("killed writer (pid=%d) mid-upload; its log tail:", wpid)
	lines := bytes.Split(bytes.TrimRight(wOut.Bytes(), "\n"), []byte("\n"))
	if len(lines) > 6 {
		lines = lines[len(lines)-6:]
	}
	for _, line := range lines {
		t.Logf("writer | %s", line)
	}

	// 重启恢复（由另一个进程接手）。
	s, err := Open(dir, testLogger{t})
	if err != nil {
		t.Fatalf("reopen after killing writer: %v", err)
	}
	defer s.Close()

	// 已发布的标签必须完整可读（无论被杀时循环到第几轮）。
	tags, err := s.ListTags(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("tags visible after recovery: %v", tags)
	for _, tag := range tags {
		rd, err := s.Resolve(ctx, tag)
		if err != nil {
			t.Fatalf("published tag %q must resolve after recovery: %v", tag, err)
		}
		assertManifestReadable(t, s, rd.Digest)
	}
	// 未完成上传的临时文件必须被清理。
	uploads, err := os.ReadDir(filepath.Join(dir, uploadsDir))
	if err != nil {
		t.Fatal(err)
	}
	if len(uploads) != 0 {
		t.Fatalf("incomplete uploads must be cleaned by recovery, left=%d", len(uploads))
	}
	// 死进程的钉住文件必须被清除（不阻塞后续回收）。
	pinFiles, err := os.ReadDir(filepath.Join(dir, pinsDir))
	if err != nil {
		t.Fatal(err)
	}
	for _, pf := range pinFiles {
		_, pid, ok := parsePinFileName(pf.Name())
		if ok && !pidAlive(pid) {
			t.Fatalf("stale pin file %q of dead process must be pruned", pf.Name())
		}
	}
	// 后续写入与回收正常工作：孤儿 blob（被杀前上传但未引用的）可回收。
	seedImage(t, s, "post-crash", []byte("cfg-post"), []byte("layer-post"))
	if _, err := s.GC(ctx); err != nil {
		t.Fatalf("GC after writer kill: %v", err)
	}
	removed, err := s.GC(ctx)
	if err != nil || len(removed) != 0 {
		t.Fatalf("store must be at GC fixpoint after recovery: removed=%v err=%v", removed, err)
	}
	t.Logf("writer kill recovered: %d tags intact, uploads/pins clean, store fully functional", len(tags))
}
