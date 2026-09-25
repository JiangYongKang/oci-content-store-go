package contentstore

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestRecoverFromInterruptedGC: GC never edits state.json and only unlinks
// unreachable blobs. Simulate a crash mid-sweep by removing a subset of the
// unreachable blobs, then reopen: tagged content must be intact and a
// follow-up GC simply finishes the job.
func TestRecoverFromInterruptedGC(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	md, cfgD, layerDs := putImage(t, s, "v1", manifestSpec{
		config: []byte("cfg-v1"),
		layers: [][]byte{[]byte("layer-v1")},
	})
	orphans := []string{
		putBytes(t, s, []byte("orphan-1")),
		putBytes(t, s, []byte("orphan-2")),
	}

	// "Crash" after deleting just one of the two orphan blobs.
	if err := os.Remove(filepath.Join(s.root, "blobs", "sha256", orphans[0][7:])); err != nil {
		t.Fatal(err)
	}
	t.Logf("simulated interrupted GC: removed only %s, left %s", orphans[0], orphans[1])

	s2, err := Open(ctx, s.root)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	s2.SetLogger(testLogger{t})
	if _, _, err := s2.GetManifest(ctx, "v1"); err != nil {
		t.Fatalf("tagged image broken after interrupted GC: %v", err)
	}
	for _, d := range append([]string{md, cfgD}, layerDs...) {
		rc, err := s2.GetBlob(ctx, d)
		if err != nil {
			t.Fatalf("tagged blob %s missing: %v", d, err)
		}
		rc.Close()
	}
	n, err := s2.GC(ctx)
	if err != nil {
		t.Fatalf("resume GC: %v", err)
	}
	if n != 1 {
		t.Fatalf("resume GC removed %d, want 1 (the surviving orphan)", n)
	}
	stats, _ := s2.Stats(ctx)
	if stats.Blobs != 3 {
		t.Fatalf("blobs after resumed GC = %d, want 3 (manifest+config+layer)", stats.Blobs)
	}
	t.Log("interrupted GC resumed cleanly: tagged closure intact, remaining orphan swept")
}

// TestCrossProcessLock verifies flock(2) actually excludes another process.
// The child process holds the exclusive lock; the parent's GC must block on
// flock until the child is killed. The measurement only starts once the lock
// is observed to be held via a non-blocking probe, so scheduler jitter in the
// child cannot start the clock early.
func TestCrossProcessLock(t *testing.T) {
	dir := t.TempDir()
	// Initialize and recover before the child locks, so the parent's Open
	// doesn't itself queue behind the flock we are about to test.
	s, err := Open(context.Background(), dir)
	if err != nil {
		t.Fatalf("parent Open: %v", err)
	}
	child := exec.Command(os.Args[0], "-test.run=TestLockHolderProcess")
	child.Env = append(os.Environ(), "OCS_LOCK_CHILD=1", "OCS_STORE_DIR="+dir)
	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	child.Stdout = pw
	var childErr strings.Builder
	child.Stderr = &childErr
	if err := child.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	defer func() { _ = child.Process.Kill() }()

	// Wait for the child's LOCKED line, ignoring testing framework banners.
	sc := bufio.NewScanner(pr)
	signaled := make(chan struct{})
	go func() {
		for sc.Scan() {
			if strings.TrimSpace(sc.Text()) == "LOCKED" {
				close(signaled)
				return
			}
		}
	}()
	select {
	case <-signaled:
	case <-time.After(10 * time.Second):
		t.Fatalf("child never signaled LOCKED; stderr=%s", childErr.String())
	}

	// Confirm the flock is externally visible before measuring.
	lockFile := filepath.Join(dir, "lock")
	visibleDeadline := time.Now().Add(5 * time.Second)
	for {
		f, err := os.OpenFile(lockFile, os.O_RDWR, 0o644)
		if err != nil {
			t.Fatalf("probe open: %v; child stderr=%s", err, childErr.String())
		}
		probeErr := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if probeErr == nil {
			_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
			_ = f.Close()
			if time.Now().After(visibleDeadline) {
				t.Fatalf("child lock never became visible; stderr=%s", childErr.String())
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		_ = f.Close()
		t.Logf("child flock is externally held (non-blocking probe: %v)", probeErr)
		break
	}

	// Start the release timer only after the lock is provably held and GC is
	// about to wait on it.
	kill := time.AfterFunc(time.Second, func() { _ = child.Process.Kill() })
	start := time.Now()
	if _, err := s.GC(context.Background()); err != nil {
		t.Fatalf("GC: %v", err)
	}
	kill.Stop()
	_ = child.Wait()
	if waited := time.Since(start); waited < 800*time.Millisecond {
		t.Fatalf("GC returned after %v although another process held the lock", waited)
	} else {
		t.Logf("parent GC blocked %v until the child released the flock, then completed", waited.Round(time.Millisecond))
	}
}

// TestLockHolderProcess is the helper process for TestCrossProcessLock.
func TestLockHolderProcess(t *testing.T) {
	if os.Getenv("OCS_LOCK_CHILD") != "1" {
		t.Skip("helper process only")
	}
	dir := os.Getenv("OCS_STORE_DIR")
	s, err := Open(context.Background(), dir)
	if err != nil {
		t.Logf("child Open: %v", err)
		os.Exit(1)
	}
	if err := s.lk.lock(); err != nil {
		t.Logf("child lock: %v", err)
		os.Exit(1)
	}
	fmt.Println("LOCKED")
	time.Sleep(30 * time.Second)
}
