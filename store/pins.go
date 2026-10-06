package store

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// pinsDir 是跨进程钉住（Pin）信息的存放目录。
//
// "已上传、待引用"窗口期的回收保护必须跨进程生效，因此钉住状态
// 持久化为 pins/ 下的空文件，文件名编码了所有者与受保护摘要：
//
//	pins/<hex>.<pid>.<rand>
//
// 其中 hex 是受保护摘要的十六进制部分，pid 是创建进程的 PID。
// 钉住文件的生命周期与创建进程绑定：进程正常 Unpin 时删除；进程
// 被杀时文件残留，但任何后续进程（GC 或 Open 恢复）发现 pid 已不
// 存在时会自动收割——无需人工清理，也不会因此永久占用空间。
// 老版本存储目录没有 pins/，Open 时自动创建，老数据无需迁移。
const pinsDir = "pins"

// pinsPath 返回 pins 目录路径。
func (s *Store) pinsPath() string {
	return filepath.Join(s.dir, pinsDir)
}

// createPinFileLocked 为摘要 d 创建一个钉住文件，返回文件名。
// 前置条件：调用方持有写锁。
func (s *Store) createPinFileLocked(d Digest) (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	name := fmt.Sprintf("%s.%d.%s", d.hexPart(), os.Getpid(), hex.EncodeToString(b[:]))
	p := filepath.Join(s.pinsPath(), name)
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
	if err != nil {
		return "", err
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	if err := fsyncDir(s.pinsPath()); err != nil {
		return "", err
	}
	return name, nil
}

// parsePinName 解析钉住文件名，返回受保护摘要与所有者 PID。
func parsePinName(name string) (Digest, int, error) {
	parts := strings.Split(name, ".")
	if len(parts) != 3 {
		return "", 0, errors.New("bad pin file name")
	}
	d := Digest("sha256:" + parts[0])
	if err := d.Validate(); err != nil {
		return "", 0, err
	}
	pid, err := strconv.Atoi(parts[1])
	if err != nil || pid <= 0 {
		return "", 0, errors.New("bad pin owner pid")
	}
	return d, pid, nil
}

// pidAlive 判断同机进程是否存在（本存储面向单机多进程场景）。
func pidAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

// livePinsLocked 读取 pins/ 目录，返回仍属存活进程的钉住摘要集合；
// 属主进程已不存在的钉住文件顺手收割删除。
// 前置条件：调用方持有写锁（GC 或恢复流程）。
func (s *Store) livePinsLocked() (map[Digest]struct{}, error) {
	live := make(map[Digest]struct{})
	entries, err := os.ReadDir(s.pinsPath())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return live, nil // 老数据目录：尚无 pins/
		}
		return nil, err
	}
	reaped := false
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		d, pid, err := parsePinName(e.Name())
		if err != nil {
			// 无法识别的文件不属于本存储的钉住信息，跳过不删。
			s.logf("pins: skip unrecognized file %q", e.Name())
			continue
		}
		if !pidAlive(pid) {
			s.logf("pins: reaping stale pin digest=%s pid=%d (owner gone)", d, pid)
			if err := os.Remove(filepath.Join(s.pinsPath(), e.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
				return nil, err
			}
			reaped = true
			continue
		}
		live[d] = struct{}{}
	}
	if reaped {
		if err := fsyncDir(s.pinsPath()); err != nil {
			return nil, err
		}
	}
	return live, nil
}
