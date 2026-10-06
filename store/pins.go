package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// 本文件把"已写入、待引用"窗口期的钉住保护扩展到跨进程：
//
//   - Pin 时除内存计数外，在 pins/ 下落盘一个钉住文件，
//     任何进程的 GC 都会跳过有钉住文件的摘要；
//   - 钉住文件名形如 <hex>.<pid>.<instance>：pid 用于在属主
//     进程崩溃后识别残留（内核会回收 pid，文件却不会自己消失），
//     instance 区分同进程内的多个 Store 实例；
//   - 属主已死的钉住文件由 Open 恢复与每次 GC 主动清理，
//     因此一次崩溃不会让内容永远不被回收，也不需要人工介入；
//   - 钉住文件是建议性状态：崩溃即失效，无需 fsync 保证持久。

// pinFileName 拼接属主标识与摘要为钉住文件名。
func pinFileName(owner string, d Digest) string {
	return d.hexPart() + "." + owner
}

// parsePinFileName 解析钉住文件名，返回摘要与属主 pid。
// 无法识别的名字（非本存储产生的文件）返回 ok=false，调用方保留不动。
func parsePinFileName(name string) (d Digest, pid int, ok bool) {
	parts := strings.Split(name, ".")
	if len(parts) != 3 {
		return "", 0, false
	}
	d = Digest("sha256:" + parts[0])
	if err := d.Validate(); err != nil {
		return "", 0, false
	}
	pid, err := strconv.Atoi(parts[1])
	if err != nil || pid <= 0 {
		return "", 0, false
	}
	return d, pid, true
}

// pidAlive 判断进程是否存在。EPERM 视为存活（存在但无权信号）。
func pidAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || err == syscall.EPERM
}

// persistentPins 管理落盘的钉住文件。所有方法都须在持有
// 排他文件锁的前提下调用，保证与并发 GC 的扫描互斥。
type persistentPins struct {
	dir   string
	owner string // "<pid>.<instance>"，本实例钉住文件的属主标识
}

// add 为摘要建立钉住文件（写临时文件再 rename，崩溃不留半截文件）。
func (p *persistentPins) add(d Digest) error {
	final := filepath.Join(p.dir, pinFileName(p.owner, d))
	tmp := final + ".new"
	if err := os.WriteFile(tmp, []byte(string(d)+"\n"), 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// remove 删除本实例为摘要建立的钉住文件；不存在视为成功（幂等）。
func (p *persistentPins) remove(d Digest) error {
	err := os.Remove(filepath.Join(p.dir, pinFileName(p.owner, d)))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// removeAll 删除本实例的全部钉住文件（Close 时调用）。
func (p *persistentPins) removeAll() error {
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return err
	}
	suffix := "." + p.owner
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), suffix) {
			continue
		}
		if err := os.Remove(filepath.Join(p.dir, e.Name())); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// pinnedDigests 返回当前被钉住的摘要集合，并顺带清理属主进程
// 已死的残留钉住文件（含 ".new" 临时残留），返回清理数量。
func (p *persistentPins) pinnedDigests() (map[Digest]struct{}, int, error) {
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return nil, 0, err
	}
	pinned := make(map[Digest]struct{})
	pruned := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		d, pid, ok := parsePinFileName(name)
		if !ok {
			// 无法识别的文件：若是钉住发布的临时残留则清理，否则保留。
			if strings.HasSuffix(name, ".new") {
				os.Remove(filepath.Join(p.dir, name))
				pruned++
			}
			continue
		}
		if !pidAlive(pid) {
			// 属主进程已死：残留钉住，清理以免内容永远不被回收。
			if err := os.Remove(filepath.Join(p.dir, name)); err == nil {
				pruned++
			}
			continue
		}
		pinned[d] = struct{}{}
	}
	return pinned, pruned, nil
}

// pruneDead 仅做清理（Open 恢复用），返回清理数量。
func (p *persistentPins) pruneDead() (int, error) {
	_, pruned, err := p.pinnedDigests()
	if err != nil {
		return 0, fmt.Errorf("store: prune pins: %w", err)
	}
	return pruned, nil
}
