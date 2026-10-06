package store

import (
	"os"
	"path/filepath"
	"syscall"
)

// lockFileName 是存储根目录下的跨进程锁文件。文件本身不存数据，
// 仅作为 flock 的载体；老版本存储目录中没有它，Open 时按需创建，
// 因此老数据无需任何迁移。
const lockFileName = "lock"

// acquireFileLock 对锁文件获取一个 flock（shared=true 为共享锁，
// 否则为独占锁），返回释放函数。
//
// 每次获取都独立 open 一次：flock 关联的是"打开文件描述"而非进程，
// 这样同一进程内多个 Store 实例、同一 Store 的多个并发操作各自持有
// 独立的锁，释放互不影响；关闭 fd 即释放锁（进程被杀时内核自动关闭，
// 锁随之释放，不会卡死后续进程）。
func (s *Store) acquireFileLock(shared bool) (func(), error) {
	p := filepath.Join(s.dir, lockFileName)
	f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	mode := syscall.LOCK_EX
	if shared {
		mode = syscall.LOCK_SH
	}
	if err := syscall.Flock(int(f.Fd()), mode); err != nil {
		f.Close()
		return nil, err
	}
	return func() { f.Close() }, nil
}

// lockRead 获取读侧锁：进程内 RWMutex 读锁 + 跨进程共享文件锁。
// 返回的释放函数先放文件锁再放进程内锁（与获取顺序相反）。
func (s *Store) lockRead() (func(), error) {
	s.mu.RLock()
	release, err := s.acquireFileLock(true)
	if err != nil {
		s.mu.RUnlock()
		return nil, err
	}
	return func() {
		release()
		s.mu.RUnlock()
	}, nil
}

// lockWrite 获取写侧锁：进程内 RWMutex 写锁 + 跨进程独占文件锁。
func (s *Store) lockWrite() (func(), error) {
	s.mu.Lock()
	release, err := s.acquireFileLock(false)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	return func() {
		release()
		s.mu.Unlock()
	}, nil
}
