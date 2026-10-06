// Package store 提供本地 OCI 风格内容存储：按摘要寻址、标签解析、
// 引用追踪与垃圾回收，并保证并发与崩溃恢复下的一致性。
//
// 磁盘布局（v1，后续版本保持向后兼容）：
//
//	<root>/
//	  blobs/sha256/<hex>   内容寻址的不可变 blob
//	  tags/<tag>           标签文件，内容为其指向的清单摘要
//	  uploads/             写入暂存区，成功的写入会原子 rename 进 blobs
//	  pins/                跨进程钉住文件（打开老数据时自动创建）
//	  lock                 跨进程文件锁载体（打开老数据时自动创建）
//
// 所有 blob 一经 rename 发布即不可变。进程内由一把 RWMutex 串行化
// （读共享、写独占）；进程间由 lock 文件上的 flock 串行化（读共享、
// 写独占）。两层锁的获取顺序固定为 RWMutex -> flock，因此同一目录
// 被多个进程同时打开时，整体操作历史与单进程一样可线性化。
package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
)

// Logger 是可选的日志接口，标准库 *log.Logger 直接满足。
type Logger interface {
	Printf(format string, args ...any)
}

const (
	blobsDir   = "blobs/sha256"
	tagsDir    = "tags"
	uploadsDir = "uploads"
	// pinsDir 存放跨进程钉住文件（v1 新增目录，老数据打开时自动创建）。
	pinsDir = "pins"
	// lockFile 是跨进程文件锁的载体（v1 新增文件，老数据打开时自动创建）。
	lockFile = "lock"
	// tmpSuffix 是发布前临时文件的后缀；恢复时据此识别残留。
	tmpSuffix = ".tmp"
	// tagTempSuffix 是历史版本标签临时文件（<tag>.tmp.new）的后缀。
	// 该后缀保留给原子发布协议，标签名不得以它结尾，否则与恢复
	// 清理逻辑无法区分（这正是带点标签曾被静默吞掉的根源）。
	tagTempSuffix = tmpSuffix + ".new"
)

// validTagRe 限定标签字符集，标签直接映射为文件名，必须可安全落盘。
var validTagRe = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$`)

func validateTag(tag string) error {
	if !validTagRe.MatchString(tag) {
		return fmt.Errorf("%w: %q", ErrInvalidTag, tag)
	}
	if strings.HasSuffix(tag, tagTempSuffix) {
		return fmt.Errorf("%w: %q ends with reserved suffix %q", ErrInvalidTag, tag, tagTempSuffix)
	}
	return nil
}

// Store 是本地内容存储。零值不可用，须通过 Open 创建。
type Store struct {
	mu     sync.RWMutex
	dir    string
	logger Logger
	// flock 是跨进程文件锁：进程内并发由 mu 串行化，进程间并发
	// 由 flock 串行化，二者叠加使多进程历史与单进程一样可线性化。
	flock fileLock
	// pins 是内存中的引用计数钉住表：计数 > 0 的摘要不会被 GC
	// 回收。客户端在"上传 blob -> 建立清单引用"的窗口期内钉住
	// 内容，即可获得确定性的回收保护。
	pins map[Digest]int
	// ppins 是落盘的钉住状态（pins/ 目录），把窗口期保护扩展到
	// 跨进程：任何进程的 GC 都会尊重，属主崩溃后由恢复/回收清理。
	ppins persistentPins
}

// Open 打开（必要时创建）位于 dir 的存储，并执行崩溃恢复：
// 清理未完成上传、剔除指向缺失/非法内容的损坏标签、清理属主
// 已死的残留钉住文件。恢复在排他文件锁下进行，可与并发的其他
// 进程安全交错。老版本数据目录无需迁移：pins/ 与 lock 缺失时
// 自动创建，既有 blobs/tags/uploads 原样沿用。
func Open(dir string, logger Logger) (*Store, error) {
	if dir == "" {
		return nil, errors.New("store: empty directory")
	}
	for _, sub := range []string{blobsDir, tagsDir, uploadsDir, pinsDir} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			return nil, fmt.Errorf("store: init %s: %w", sub, err)
		}
	}
	inst, err := newInstanceID()
	if err != nil {
		return nil, err
	}
	s := &Store{
		dir:    dir,
		logger: logger,
		pins:   make(map[Digest]int),
		flock:  fileLock{path: filepath.Join(dir, lockFile)},
		ppins: persistentPins{
			dir:   filepath.Join(dir, pinsDir),
			owner: fmt.Sprintf("%d.%s", os.Getpid(), inst),
		},
	}
	// 恢复可能与其他进程的操作并发，须在排他文件锁下执行。
	if err := s.flock.withLock(true, s.recover); err != nil {
		return nil, err
	}
	s.logf("store opened at %s (owner=%s)", dir, s.ppins.owner)
	return s, nil
}

// newInstanceID 生成本实例的随机标识，用于区分同进程内多个
// Store 实例的钉住文件属主。
func newInstanceID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// Close 关闭存储：释放本实例持有的全部钉住（内存计数与落盘
// 文件），使未被引用的内容重新可被回收。进程若被强行杀死，
// 残留钉住文件会由后续 Open 恢复或 GC 按属主存活状态清理。
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flock.withLock(true, func() error {
		n := len(s.pins)
		if err := s.ppins.removeAll(); err != nil {
			return err
		}
		s.pins = make(map[Digest]int)
		if n > 0 {
			s.logf("close: released %d pinned digests", n)
		}
		return nil
	})
}

// Pin 钉住一个摘要：计数 +1，被钉住的内容不会被任何进程的 GC
// 回收。不要求内容已存在，可在上传前调用以覆盖整个"上传+引用"
// 窗口。每次 Pin 必须配对一次 Unpin。钉住状态会落盘（pins/），
// 因此跨进程生效；进程崩溃后残留钉住由恢复/回收自动清理。
func (s *Store) Pin(d Digest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.flock.withLock(true, func() error {
		if s.pins[d] == 0 {
			// 0->1 转换时落盘。I/O 失败无法经返回值上报（保持接口
			// 兼容），记录日志；内存钉住仍然生效，进程内保护不降级。
			if err := s.ppins.add(d); err != nil {
				s.logf("pin digest=%s: persist failed (in-process pin still active): %v", d, err)
			}
		}
		s.pins[d]++
		s.logf("pin digest=%s count=%d", d, s.pins[d])
		return nil
	})
}

// Unpin 解除一次钉住；计数归零后内容重新可被 GC 回收。
func (s *Store) Unpin(d Digest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.flock.withLock(true, func() error {
		if s.pins[d] <= 1 {
			delete(s.pins, d)
			if err := s.ppins.remove(d); err != nil {
				s.logf("unpin digest=%s: remove pin file failed: %v", d, err)
			}
			s.logf("unpin digest=%s count=0", d)
			return nil
		}
		s.pins[d]--
		s.logf("unpin digest=%s count=%d", d, s.pins[d])
		return nil
	})
}

func (s *Store) logf(format string, args ...any) {
	if s.logger != nil {
		s.logger.Printf(format, args...)
	}
}

func (s *Store) blobPath(d Digest) string {
	return filepath.Join(s.dir, blobsDir, d.hexPart())
}

func (s *Store) tagPath(tag string) string {
	return filepath.Join(s.dir, tagsDir, tag)
}

func (s *Store) uploadsPath() string {
	return filepath.Join(s.dir, uploadsDir)
}

// fsyncDir 对目录执行 fsync，保证其中的 rename/unlink 在崩溃后持久。
func fsyncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// PutContent 按声明的摘要与长度写入内容。
//
//   - desc.Digest 必须合法，且实际内容 sha256 必须与其一致，否则 ErrDigestMismatch；
//   - desc.Size >= 0 时实际长度必须精确相等，否则 ErrSizeMismatch；
//     desc.Size < 0 表示长度未知，由存储自行计算；
//   - 相同摘要的内容若已存在则直接复用（不产生第二份副本）；
//   - 失败时临时文件会被清除，存储中不会留下可被后续复用的残缺内容。
func (s *Store) PutContent(ctx context.Context, desc Descriptor, r io.Reader) error {
	if err := desc.Digest.Validate(); err != nil {
		return err
	}
	if desc.Size < 0 {
		desc.Size = -1 // 规范化：未知长度，由存储实际计算
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flock.withLock(true, func() error {
		return s.putContentLocked(ctx, desc, r)
	})
}

// putContentLocked 在持有进程内写锁与排他文件锁的前提下执行写入。
func (s *Store) putContentLocked(ctx context.Context, desc Descriptor, r io.Reader) error {
	finalPath := s.blobPath(desc.Digest)
	if info, err := os.Stat(finalPath); err == nil {
		// 内容已存在：必须复用同一份存储。校验已有文件与声明一致，
		// 确保后续永远不会复用一份损坏或被声明成别的长度的数据。
		if desc.Size >= 0 && info.Size() != desc.Size {
			return fmt.Errorf("%w: digest %s existing size %d, declared %d",
				ErrSizeMismatch, desc.Digest, info.Size(), desc.Size)
		}
		if err := s.verifyBlobLocked(desc); err != nil {
			return err
		}
		s.logf("put digest=%s size=%d -> reused existing blob", desc.Digest, info.Size())
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	// 流式写入暂存文件，同时计算 sha256 与字节数。
	tmpName, err := s.newTempName()
	if err != nil {
		return err
	}
	tmpPath := filepath.Join(s.uploadsPath(), tmpName)
	cleanup := true
	defer func() {
		if cleanup {
			os.Remove(tmpPath)
		}
	}()

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	h := newDigestHasher()
	var n int64
	rd := io.TeeReader(r, h)
	if n, err = io.Copy(f, rd); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	got := h.Digest()
	if got != desc.Digest {
		s.logf("put REJECT digest-mismatch declared=%s actual=%s bytes=%d", desc.Digest, got, n)
		return fmt.Errorf("%w: declared %s, actual %s", ErrDigestMismatch, desc.Digest, got)
	}
	if desc.Size >= 0 && n != desc.Size {
		s.logf("put REJECT size-mismatch digest=%s declared=%d actual=%d", desc.Digest, desc.Size, n)
		return fmt.Errorf("%w: declared %d, actual %d", ErrSizeMismatch, desc.Size, n)
	}

	// 原子发布：rename + 目录 fsync，使"已发布"与"完整持久"同时成立。
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return err
	}
	if err := fsyncDir(filepath.Dir(finalPath)); err != nil {
		return err
	}
	if err := fsyncDir(s.uploadsPath()); err != nil {
		return err
	}
	cleanup = false
	s.logf("put digest=%s size=%d -> stored new blob", desc.Digest, n)
	return nil
}

// newTempName 生成 uploads 下的唯一临时文件名：<hex>.tmp-<rand>。
func (s *Store) newTempName() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "x" + tmpSuffix + "-" + hex.EncodeToString(b[:]), nil
}

// verifyBlobLocked 读取已发布 blob 并校验其摘要与（如声明的）长度。
func (s *Store) verifyBlobLocked(desc Descriptor) error {
	f, err := os.Open(s.blobPath(desc.Digest))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: %s", ErrNotFound, desc.Digest)
		}
		return err
	}
	defer f.Close()
	h := newDigestHasher()
	n, err := io.Copy(h, f)
	if err != nil {
		return err
	}
	if got := h.Digest(); got != desc.Digest {
		return fmt.Errorf("%w: stored %s but content hashes to %s", ErrDigestMismatch, desc.Digest, got)
	}
	if desc.Size >= 0 && n != desc.Size {
		return fmt.Errorf("%w: digest %s stored size %d, declared %d", ErrSizeMismatch, desc.Digest, n, desc.Size)
	}
	return nil
}

// GetContent 按摘要读取内容。返回 ErrNotFound 当内容不存在。
func (s *Store) GetContent(ctx context.Context, d Digest) ([]byte, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var b []byte
	err := s.flock.withLock(false, func() error {
		var err error
		b, err = os.ReadFile(s.blobPath(d))
		return err
	})
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, d)
		}
		return nil, err
	}
	return b, nil
}

// StatContent 返回已存储内容的描述符（摘要、长度），不读取内容本体。
func (s *Store) StatContent(ctx context.Context, d Digest) (Descriptor, error) {
	if err := d.Validate(); err != nil {
		return Descriptor{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var info os.FileInfo
	err := s.flock.withLock(false, func() error {
		var err error
		info, err = os.Stat(s.blobPath(d))
		return err
	})
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Descriptor{}, fmt.Errorf("%w: %s", ErrNotFound, d)
		}
		return Descriptor{}, err
	}
	return Descriptor{Digest: d, Size: info.Size()}, nil
}

// PutManifest 校验并写入清单，然后原子地建立 tag -> manifest 引用。
//
// 清单引用的 config 与每一层必须已经存在，否则返回 ErrMissingReference，
// 且不会写入任何内容。清单本身作为普通内容寻址 blob 存储，因此相同
// 清单天然去重。
func (s *Store) PutManifest(ctx context.Context, tag string, m Manifest) (Descriptor, error) {
	if err := validateTag(tag); err != nil {
		return Descriptor{}, err
	}
	if err := m.validate(); err != nil {
		return Descriptor{}, err
	}
	b, err := json.Marshal(&m)
	if err != nil {
		return Descriptor{}, fmt.Errorf("%w: %v", ErrInvalidManifest, err)
	}
	desc := Descriptor{
		MediaType: m.MediaType,
		Digest:    DigestOf(b),
		Size:      int64(len(b)),
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.flock.withLock(true, func() error {
		return s.putManifestLocked(tag, m, desc, b)
	}); err != nil {
		return Descriptor{}, err
	}
	return desc, nil
}

// putManifestLocked 在持有写锁与排他文件锁的前提下执行引用预检、
// 清单落盘与标签发布，三步对任何进程都表现为一个原子整体。
func (s *Store) putManifestLocked(tag string, m Manifest, desc Descriptor, b []byte) error {
	// 引用完整性预检：缺失引用时拒绝，保证标签永远不会悬挂。
	for _, ref := range m.references() {
		if _, err := os.Stat(s.blobPath(ref.Digest)); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				s.logf("put-manifest REJECT missing-reference tag=%s ref=%s", tag, ref.Digest)
				return fmt.Errorf("%w: %s required by manifest %s for tag %q",
					ErrMissingReference, ref.Digest, desc.Digest, tag)
			}
			return err
		}
	}

	// 写入清单 blob（已存在则复用）。
	if err := s.putBlobLocked(desc, b); err != nil {
		return err
	}

	// 原子发布标签：先写临时文件再 rename，标签要么指向旧清单要么指向新清单。
	if err := s.writeTagLocked(tag, desc.Digest); err != nil {
		return err
	}
	s.logf("put-manifest tag=%s manifest=%s refs=%d", tag, desc.Digest, len(m.references()))
	return nil
}

// putBlobLocked 写入或复用一个已在内存中的 blob。
// 前置条件：调用方持有写锁，且 desc 与 b 已经过校验。
func (s *Store) putBlobLocked(desc Descriptor, b []byte) error {
	finalPath := s.blobPath(desc.Digest)
	if _, err := os.Stat(finalPath); err == nil {
		return nil // 不可变内容，直接复用
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	tmpName, err := s.newTempName()
	if err != nil {
		return err
	}
	tmpPath := filepath.Join(s.uploadsPath(), tmpName)
	if err := os.WriteFile(tmpPath, b, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := fsyncDir(filepath.Dir(finalPath)); err != nil {
		return err
	}
	return fsyncDir(s.uploadsPath())
}

// writeTagLocked 原子写入标签文件。
//
// 临时文件写在 uploads/ 而非 tags/：tags/ 目录里任何时候都只存在
// 完整发布的标签，带点号（如 "v1.tmp"）的合法标签名因此不可能与
// 发布中间产物混淆。rename 跨目录但同文件系统，原子性不变。
func (s *Store) writeTagLocked(tag string, d Digest) error {
	tmpName, err := s.newTempName()
	if err != nil {
		return err
	}
	tmp := filepath.Join(s.uploadsPath(), tmpName)
	if err := os.WriteFile(tmp, []byte(string(d)+"\n"), 0o644); err != nil {
		return err
	}
	final := s.tagPath(tag)
	if err := os.Rename(tmp, final); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := fsyncDir(filepath.Dir(final)); err != nil {
		return err
	}
	return fsyncDir(s.uploadsPath())
}

// Resolve 将标签解析为其清单描述符。
func (s *Store) Resolve(ctx context.Context, tag string) (Descriptor, error) {
	if err := validateTag(tag); err != nil {
		return Descriptor{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var desc Descriptor
	err := s.flock.withLock(false, func() error {
		d, err := s.readTagLocked(tag)
		if err != nil {
			return err
		}
		info, err := os.Stat(s.blobPath(d))
		if err != nil {
			// 正常运行中标签与其目标由同一把锁维护，不会走到这里；
			// 走到了说明磁盘被外部破坏，按未找到处理。
			return fmt.Errorf("%w: tag %q points to missing blob %s", ErrNotFound, tag, d)
		}
		desc = Descriptor{Digest: d, Size: info.Size()}
		return nil
	})
	if err != nil {
		return Descriptor{}, err
	}
	return desc, nil
}

func (s *Store) readTagLocked(tag string) (Digest, error) {
	b, err := os.ReadFile(s.tagPath(tag))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("%w: tag %q", ErrNotFound, tag)
		}
		return "", err
	}
	// 容忍末尾换行；空文件视为损坏。
	str := string(b)
	if n := len(str); n > 0 && str[n-1] == '\n' {
		str = str[:n-1]
	}
	d := Digest(str)
	if err := d.Validate(); err != nil {
		return "", fmt.Errorf("%w: tag %q has corrupt digest %q", ErrInvalidDigest, tag, str)
	}
	return d, nil
}

// GetManifest 按摘要读取并解析清单。
func (s *Store) GetManifest(ctx context.Context, d Digest) (Manifest, error) {
	b, err := s.GetContent(ctx, d)
	if err != nil {
		return Manifest{}, err
	}
	return decodeManifest(b)
}

// ListTags 返回所有标签，按字典序排序。
func (s *Store) ListTags(ctx context.Context) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var tags []string
	err := s.flock.withLock(false, func() error {
		entries, err := os.ReadDir(filepath.Join(s.dir, tagsDir))
		if err != nil {
			return err
		}
		tags = make([]string, 0, len(entries))
		for _, e := range entries {
			if e.IsDir() || isTempTagName(e.Name()) {
				continue
			}
			tags = append(tags, e.Name())
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(tags)
	return tags, nil
}

// DeleteTag 删除标签；删除后其独占的内容将在下一次 GC 时回收。
func (s *Store) DeleteTag(ctx context.Context, tag string) error {
	if err := validateTag(tag); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flock.withLock(true, func() error {
		if err := os.Remove(s.tagPath(tag)); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("%w: tag %q", ErrNotFound, tag)
			}
			return err
		}
		s.logf("delete-tag tag=%s", tag)
		return fsyncDir(filepath.Join(s.dir, tagsDir))
	})
}

// GC 回收不被任何标签间接引用的 blob，返回被删除内容的描述符。
// 可达性判定与删除在同一把写锁与排他文件锁下完成：无论本进程还是
// 其他进程，引用仍然有效的内容都不可能被删除。
func (s *Store) GC(ctx context.Context) ([]Descriptor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var removed []Descriptor
	err := s.flock.withLock(true, func() error {
		var err error
		removed, err = s.gcLocked()
		return err
	})
	return removed, err
}
