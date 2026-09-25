package store

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"strings"
)

// Digest 是内容摘要，形如 "sha256:<64 hex>"。
type Digest string

// DigestOf 计算内容的 sha256 摘要。
func DigestOf(b []byte) Digest {
	sum := sha256.Sum256(b)
	return Digest("sha256:" + hex.EncodeToString(sum[:]))
}

// Validate 校验摘要格式是否合法。
func (d Digest) Validate() error {
	s := string(d)
	rest, ok := strings.CutPrefix(s, "sha256:")
	if !ok || len(rest) != sha256.Size*2 {
		return fmt.Errorf("%w: %q", ErrInvalidDigest, s)
	}
	if _, err := hex.DecodeString(rest); err != nil {
		return fmt.Errorf("%w: %q", ErrInvalidDigest, s)
	}
	return nil
}

// hexPart 返回摘要的十六进制部分（已假定 Validate 通过）。
func (d Digest) hexPart() string {
	return strings.TrimPrefix(string(d), "sha256:")
}

// digestHasher 在流式写入的同时累积 sha256。
type digestHasher struct {
	h hash.Hash
}

func newDigestHasher() *digestHasher {
	return &digestHasher{h: sha256.New()}
}

func (w *digestHasher) Write(p []byte) (int, error) { return w.h.Write(p) }

// Digest 返回当前累积内容的摘要。
func (w *digestHasher) Digest() Digest {
	return Digest("sha256:" + hex.EncodeToString(w.h.Sum(nil)))
}
