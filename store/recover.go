package store

import (
	"errors"
	"os"
	"path/filepath"
)

// recover 在 Open 时执行崩溃恢复，使存储重新自洽：
//
//  1. 删除 uploads/ 下的全部残留：它们是崩溃前未完成的写入（内容上传
//     或标签发布的中间产物），从未被 rename 发布，因此不可能被任何
//     标签或清单引用，也绝不能参与去重——直接清除；
//  2. 校验 tags/ 下每个标签：摘要非法或目标 blob 缺失的标签是损坏的，
//     删除它们使"标签 -> 内容"关系重新自洽。
//
// 标签的临时文件写在 uploads/ 下、tags/ 目录中只可能出现真实标签，
// 因此合法标签名不受任何保留字/后缀限制：含 ".tmp" 等字符的名字与
// 普通名字一视同仁。旧版本崩溃可能在 tags/ 下留下 "<tag>.tmp.new"
// 临时文件：内容损坏的会被第 2 步当作损坏标签清除；内容恰好完整的
// 会被视为一个同名普通标签（指向的清单客观存在），语义自洽、不丢数据。
//
// blobs/ 下只存在经原子 rename 发布的完整内容，无需校验即可信；
// 无法识别的文件保留不动（向前兼容）。
//
// 整个恢复过程在跨进程独占文件锁下执行：此刻不可能有其他进程正在
// 写入暂存文件或发布标签，因此 uploads/ 与 tags/ 下的残留一定属于
// 已崩溃（或已退出）的进程，可以安全清除。
func (s *Store) recover() error {
	unlock, err := s.lockWrite()
	if err != nil {
		return err
	}
	defer unlock()

	// 1. 清理未完成上传。
	uploads := s.uploadsPath()
	entries, err := os.ReadDir(uploads)
	if err != nil {
		return err
	}
	for _, e := range entries {
		p := filepath.Join(uploads, e.Name())
		s.logf("recover: removing incomplete upload %s", e.Name())
		if err := os.RemoveAll(p); err != nil {
			return err
		}
	}
	if len(entries) > 0 {
		if err := fsyncDir(uploads); err != nil {
			return err
		}
	}

	// 2. 校验标签目录：剔除损坏与悬挂标签。
	tagsPath := filepath.Join(s.dir, tagsDir)
	tentries, err := os.ReadDir(tagsPath)
	if err != nil {
		return err
	}
	dirty := false
	for _, e := range tentries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		d, err := s.readTagLocked(name)
		if err != nil {
			s.logf("recover: removing corrupt tag %q: %v", name, err)
			if rmErr := os.Remove(filepath.Join(tagsPath, name)); rmErr != nil {
				return rmErr
			}
			dirty = true
			continue
		}
		if _, err := os.Stat(s.blobPath(d)); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return err
			}
			s.logf("recover: removing dangling tag %q -> missing blob %s", name, d)
			if rmErr := os.Remove(filepath.Join(tagsPath, name)); rmErr != nil {
				return rmErr
			}
			dirty = true
		}
	}
	if dirty {
		if err := fsyncDir(tagsPath); err != nil {
			return err
		}
	}
	return nil
}
