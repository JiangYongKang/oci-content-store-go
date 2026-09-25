package store

import "errors"

// 失败分类：可区分的哨兵错误。
var (
	// ErrNotFound 请求的内容、标签或清单不存在。
	ErrNotFound = errors.New("store: not found")
	// ErrDigestMismatch 实际内容摘要与声明摘要不一致。
	ErrDigestMismatch = errors.New("store: digest mismatch")
	// ErrSizeMismatch 实际内容长度与声明长度不一致。
	ErrSizeMismatch = errors.New("store: size mismatch")
	// ErrInvalidManifest 清单结构非法（JSON 无法解析或必填字段缺失）。
	ErrInvalidManifest = errors.New("store: invalid manifest")
	// ErrMissingReference 清单引用的内容尚未写入存储。
	ErrMissingReference = errors.New("store: missing reference")
	// ErrInvalidDigest 摘要字符串格式非法。
	ErrInvalidDigest = errors.New("store: invalid digest")
	// ErrInvalidTag 标签名非法。
	ErrInvalidTag = errors.New("store: invalid tag")
)
