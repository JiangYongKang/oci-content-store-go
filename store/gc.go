package store

import (
	"os"
	"path/filepath"
	"sort"
)

// gcLocked 在持有写锁与排他文件锁的前提下执行标记-清除回收：
//
//  1. 标记：从所有标签出发，把标签指向的清单及其引用的
//     config/layers 全部计入可达集合；
//  2. 清除：遍历 blobs 目录，删除不在可达集合、且未被任何进程
//     钉住的 blob。
//
// 因为整个过程持有排他文件锁，标记期间集合不会变化：被删除的内容在
// 删除那一刻必然没有任何有效引用，被引用的内容必然不会被删除。
//
// 中断安全：每个 blob 的删除都是单次 unlink，回收进程在任意时刻
// 被 kill 都只会留下"部分垃圾尚未清掉"的自洽状态，不会出现删了
// 一半的可用内容；下一次 GC（无论由哪个进程发起）会接着完成。
func (s *Store) gcLocked() ([]Descriptor, error) {
	reachable, err := s.reachableLocked()
	if err != nil {
		return nil, err
	}
	s.logf("gc: reachable set has %d digests", len(reachable))

	// 跨进程钉住集合：读取的同时清理属主进程已死的残留钉住文件，
	// 保证崩溃过的进程不会让内容永远得不到回收。
	pinned, pruned, err := s.ppins.pinnedDigests()
	if err != nil {
		return nil, err
	}
	if pruned > 0 {
		s.logf("gc: pruned %d stale pin files from dead processes", pruned)
	}

	entries, err := os.ReadDir(filepath.Join(s.dir, blobsDir))
	if err != nil {
		return nil, err
	}
	var removed []Descriptor
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		d := Digest("sha256:" + e.Name())
		if err := d.Validate(); err != nil {
			// 无法识别的文件不属于本存储的内容，跳过不删。
			s.logf("gc: skip unrecognized file %q", e.Name())
			continue
		}
		if _, ok := reachable[d]; ok {
			continue
		}
		if s.pins[d] > 0 {
			// 本进程钉住的内容：处于"已上传、待引用"窗口期，跳过。
			s.logf("gc: skip digest=%s (pinned in-process)", d)
			continue
		}
		if _, ok := pinned[d]; ok {
			// 其他进程钉住的内容：同样处于上传窗口期，跳过。
			s.logf("gc: skip digest=%s (pinned by another process)", d)
			continue
		}
		info, err := e.Info()
		if err != nil {
			return nil, err
		}
		if err := os.Remove(s.blobPath(d)); err != nil {
			return nil, err
		}
		s.logf("gc: collected digest=%s size=%d (unreachable)", d, info.Size())
		removed = append(removed, Descriptor{Digest: d, Size: info.Size()})
	}
	if len(removed) > 0 {
		if err := fsyncDir(filepath.Join(s.dir, blobsDir)); err != nil {
			return nil, err
		}
	}
	sort.Slice(removed, func(i, j int) bool { return removed[i].Digest < removed[j].Digest })
	s.logf("gc: collected %d blobs", len(removed))
	return removed, nil
}

// reachableLocked 从所有标签出发计算可达内容摘要集合。
// 损坏的标签（摘要非法或指向缺失 blob）被跳过并记录日志，
// 不影响其余标签的可达性判定。
func (s *Store) reachableLocked() (map[Digest]struct{}, error) {
	reachable := make(map[Digest]struct{})
	tags, err := os.ReadDir(filepath.Join(s.dir, tagsDir))
	if err != nil {
		return nil, err
	}
	for _, e := range tags {
		if e.IsDir() || isTempTagName(e.Name()) {
			continue
		}
		d, err := s.readTagLocked(e.Name())
		if err != nil {
			s.logf("gc: skip corrupt tag %q: %v", e.Name(), err)
			continue
		}
		if err := s.markManifestLocked(d, reachable); err != nil {
			s.logf("gc: skip unreachable root tag=%q digest=%s: %v", e.Name(), d, err)
			continue
		}
	}
	return reachable, nil
}

// markManifestLocked 把清单及其引用的所有内容标记为可达。
func (s *Store) markManifestLocked(d Digest, reachable map[Digest]struct{}) error {
	if _, ok := reachable[d]; ok {
		return nil
	}
	b, err := os.ReadFile(s.blobPath(d))
	if err != nil {
		return err
	}
	m, err := decodeManifest(b)
	if err != nil {
		return err
	}
	reachable[d] = struct{}{}
	for _, ref := range m.references() {
		reachable[ref.Digest] = struct{}{}
	}
	return nil
}
