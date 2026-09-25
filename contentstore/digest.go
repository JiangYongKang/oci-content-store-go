package contentstore

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// Digest is a content descriptor of the form "<algorithm>:<hex>".
type Digest struct {
	alg string
	hex string
	raw string
}

// String returns the canonical digest string.
func (d Digest) String() string { return d.raw }

// Algorithm returns the digest algorithm.
func (d Digest) Algorithm() string { return d.alg }

// Hex returns the hex-encoded digest value.
func (d Digest) Hex() string { return d.hex }

// IsZero reports whether d is the zero value.
func (d Digest) IsZero() bool { return d.raw == "" }

// supportedAlgorithms is the set of accepted digest algorithms.
//
// The on-disk format reserves the "blobs/<algorithm>/" namespace for future
// algorithms; only sha256 is accepted today.
var supportedAlgorithms = map[string]int{
	"sha256": 32,
}

// ParseDigest parses and validates a digest string. Only sha256 is supported:
// the form must be "sha256:" followed by exactly 64 lowercase hex characters.
func ParseDigest(s string) (Digest, error) {
	alg, val, ok := strings.Cut(s, ":")
	if !ok || alg == "" || val == "" {
		return Digest{}, fmt.Errorf("%w: %q: expected algorithm:hex", ErrInvalidDigest, s)
	}
	wantLen, known := supportedAlgorithms[alg]
	if !known {
		return Digest{}, fmt.Errorf("%w: %q: unsupported algorithm %q", ErrInvalidDigest, s, alg)
	}
	if len(val) != hex.EncodedLen(wantLen) {
		return Digest{}, fmt.Errorf("%w: %q: expected %d hex chars, got %d", ErrInvalidDigest, s, hex.EncodedLen(wantLen), len(val))
	}
	if _, err := hex.DecodeString(val); err != nil {
		return Digest{}, fmt.Errorf("%w: %q: %v", ErrInvalidDigest, s, err)
	}
	if strings.ToLower(val) != val {
		return Digest{}, fmt.Errorf("%w: %q: hex must be lowercase", ErrInvalidDigest, s)
	}
	return Digest{alg: alg, hex: val, raw: s}, nil
}
