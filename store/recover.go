package store

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// isTempTagName 判断是否为标签发布过程中的临时文件名。
func isTempTagName(name string) bool {
	return strings.Contains(name, tmpSuffix)
}

// recover 在 Open 时执行崩溃恢复，使存储重新自洽：
//
//  1. 删除 uploads/ 下的全部残留：它们是崩溃前未完成的写入，
//     从未被 rename 发布，因此不可能被任何标签或清单引用，
//     也绝不能参与去重——直接清除；
//  2. 删除 tags/ 下的临时文件（标签原子发布的中间产物）；
//  3. 校验每个正式标签：摘要非法或目标 blob 缺失的标签是损坏的，
//     删除它们使"标签 -> 内容"关系重新自洽。
//
// blobs/ 下只存在经原子 rename 发布的完整内容，无需校验即可信；
// 无法识别的文件保留不动（向前兼容）。
func (s *Store) recover() error {
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

	// 2./3. 清理标签目录。
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
		if isTempTagName(name) {
			s.logf("recover: removing stale temp tag file %s", name)
			if err := os.Remove(filepath.Join(tagsPath, name)); err != nil {
				return err
			}
			dirty = true
			continue
		}
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
