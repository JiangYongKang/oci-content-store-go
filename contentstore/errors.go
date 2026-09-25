// Package contentstore implements a local, content-addressed image store
// with tag/manifest references and mark-and-sweep garbage collection.
package contentstore

import "errors"

// Distinguishable failure reasons returned by the store.
var (
	// ErrDigestMismatch means the actual digest of supplied content differs
	// from the digest the caller asked to store it under.
	ErrDigestMismatch = errors.New("contentstore: digest mismatch")
	// ErrSizeMismatch means the actual content length differs from declared.
	ErrSizeMismatch = errors.New("contentstore: size mismatch")
	// ErrInvalidDigest means the digest string is malformed or unsupported.
	ErrInvalidDigest = errors.New("contentstore: invalid digest")
	// ErrInvalidContent means the content is structurally illegal
	// (e.g. a manifest that cannot be parsed or fails validation).
	ErrInvalidContent = errors.New("contentstore: invalid content")
	// ErrNotFound means no committed content exists for the digest/tag.
	ErrNotFound = errors.New("contentstore: not found")
	// ErrInvalidTag means the tag name is empty or otherwise illegal.
	ErrInvalidTag = errors.New("contentstore: invalid tag")
	// ErrReferencedBlobMissing means a manifest references a config/layer
	// digest that is not present in the store.
	ErrReferencedBlobMissing = errors.New("contentstore: referenced blob missing")
)
