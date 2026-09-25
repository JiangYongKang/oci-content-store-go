package contentstore

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// fileLock is a cross-process advisory lock backed by flock(2) on a single
// lock file. Multiple readers may hold shared locks; one writer excludes all
// other shared and exclusive holders, both across goroutines and processes.
//
// The lock is NOT re-entrant: a goroutine must not request lock/rlock while
// already holding one (that would deadlock). Public Store methods all acquire
// the lock at most once and never nest.
//
// The mutex l.mu is held while the (possibly blocking) flock syscall runs.
// Store operations are short, so the resulting serialization is acceptable,
// and it makes "has the shared flock actually been taken yet?" observable to
// every goroutine.
type fileLock struct {
	path string

	mu      sync.Mutex
	cond    *sync.Cond
	f       *os.File
	shared  int  // number of in-process shared holders
	excl    bool // an in-process exclusive holder exists
	waiters int  // queued exclusive acquisitions (prevents reader starvation of writers)
}

func newFileLock(root string) *fileLock {
	l := &fileLock{path: lockPath(root)}
	l.cond = sync.NewCond(&l.mu)
	return l
}

func (l *fileLock) openFile() (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(l.path), 0o755); err != nil {
		return nil, fmt.Errorf("create lock dir: %w", err)
	}
	return os.OpenFile(l.path, os.O_CREATE|os.O_RDWR, 0o644)
}

// lock acquires an exclusive lock, blocking until every shared holder (local
// or in another process) releases it.
func (l *fileLock) lock() error {
	l.mu.Lock()
	l.waiters++
	for l.excl || l.shared > 0 {
		l.cond.Wait()
	}

	f, err := l.openFile()
	if err != nil {
		l.waiters--
		l.cond.Broadcast()
		l.mu.Unlock()
		return err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		_ = f.Close()
		l.waiters--
		l.cond.Broadcast()
		l.mu.Unlock()
		return fmt.Errorf("acquire exclusive lock: %w", err)
	}
	l.f = f
	l.excl = true
	l.waiters--
	l.mu.Unlock()
	return nil
}

// rlock acquires a shared lock, blocking while an exclusive holder exists or
// an exclusive acquisition is queued. Only the first local shared holder
// performs the flock; later holders share it via the reference count.
func (l *fileLock) rlock() error {
	l.mu.Lock()
	for l.excl || l.waiters > 0 {
		l.cond.Wait()
	}
	l.shared++
	if l.shared > 1 {
		// A prior local holder already owns the shared flock.
		l.mu.Unlock()
		return nil
	}

	f, err := l.openFile()
	if err != nil {
		l.shared--
		l.cond.Broadcast()
		l.mu.Unlock()
		return err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH); err != nil {
		_ = f.Close()
		l.shared--
		l.cond.Broadcast()
		l.mu.Unlock()
		return fmt.Errorf("acquire shared lock: %w", err)
	}
	l.f = f
	l.mu.Unlock()
	return nil
}

// unlock releases one held lock (exclusive or one shared reference).
func (l *fileLock) unlock() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.excl {
		if l.f != nil {
			_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
			_ = l.f.Close()
			l.f = nil
		}
		l.excl = false
		l.cond.Broadcast()
		return
	}
	if l.shared > 0 {
		l.shared--
		if l.shared == 0 {
			if l.f != nil {
				_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
				_ = l.f.Close()
				l.f = nil
			}
			l.cond.Broadcast()
		}
	}
}
