package store

import (
	"os"
	"syscall"
)

// 本文件提供跨进程互斥：同一存储目录可被多个进程同时打开，
// 所有操作在进程内 RWMutex 之外再叠加一把基于 flock 的文件锁，
// 使多进程的操作历史与单进程一样可线性化。
//
// 设计要点：
//   - 锁载体是 <root>/lock 文件，每次操作独立 open/close：
//     关闭 fd 即释放锁，不存在"忘了解锁"的路径；
//   - 进程被 kill 时内核自动关闭其全部 fd，锁随之释放，
//     因此崩溃不会留下卡死后续访问的残锁，无需人工清理；
//   - flock 按"打开文件描述"而非进程加锁：同一进程内用不同 fd
//     打开同一文件的两个 Store 实例也会正确互斥（测试借此在
//     单进程内模拟多进程），而进程内的并发由 RWMutex 先行串行化，
//     不会对同一 fd 重复加锁；
//   - 锁顺序固定为 进程内 RWMutex -> 文件锁，单向获取，不会死锁。

// fileLock 是存储目录级的跨进程锁。
type fileLock struct {
	path string
}

// withLock 获取文件锁（exclusive=true 为排他锁，否则为共享锁），
// 在持锁期间执行 fn，fn 返回后关闭 fd 释放锁。
// 获取失败（如目录被删）或 fn 本身的错误都会向上返回。
func (l *fileLock) withLock(exclusive bool, fn func() error) error {
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	how := syscall.LOCK_SH
	if exclusive {
		how = syscall.LOCK_EX
	}
	// 阻塞等待：锁持有者崩溃时内核会释放其锁，等待方总能继续。
	if err := syscall.Flock(int(f.Fd()), how); err != nil {
		return err
	}
	return fn()
}
