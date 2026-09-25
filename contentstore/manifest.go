package contentstore

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Manifest is the stored image manifest: one config and zero or more layers,
// each referenced by digest.
type Manifest struct {
	Config Digest
	Layers []Digest
}

// wireDescriptor is the OCI-style descriptor shape used in manifest JSON.
type wireDescriptor struct {
	MediaType string `json:"mediaType,omitempty"`
	Digest    string `json:"digest"`
	Size      int64  `json:"size,omitempty"`
}

// wireManifest is the on-the-wire manifest representation.
type wireManifest struct {
	SchemaVersion int              `json:"schemaVersion,omitempty"`
	MediaType     string           `json:"mediaType,omitempty"`
	Config        wireDescriptor   `json:"config"`
	Layers        []wireDescriptor `json:"layers"`
}

// parseManifest parses manifest JSON and validates its structure: JSON must be
// well-formed, contain no unknown fields, and every referenced digest must be
// syntactically valid. Existence of referenced blobs is checked at commit time.
func parseManifest(data []byte) (Manifest, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return Manifest{}, fmt.Errorf("%w: empty manifest", ErrInvalidContent)
	}
	var w wireManifest
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&w); err != nil {
		return Manifest{}, fmt.Errorf("%w: manifest JSON: %v", ErrInvalidContent, err)
	}
	if dec.More() {
		return Manifest{}, fmt.Errorf("%w: manifest JSON: trailing data", ErrInvalidContent)
	}
	cfg, err := ParseDigest(w.Config.Digest)
	if err != nil {
		return Manifest{}, fmt.Errorf("%w: config digest: %v", ErrInvalidContent, err)
	}
	m := Manifest{Config: cfg, Layers: make([]Digest, 0, len(w.Layers))}
	seen := map[string]bool{}
	for i, ld := range w.Layers {
		d, err := ParseDigest(ld.Digest)
		if err != nil {
			return Manifest{}, fmt.Errorf("%w: layers[%d] digest: %v", ErrInvalidContent, i, err)
		}
		if d.raw == cfg.raw {
			return Manifest{}, fmt.Errorf("%w: layers[%d] duplicates config digest", ErrInvalidContent, i)
		}
		if seen[d.raw] {
			return Manifest{}, fmt.Errorf("%w: layers[%d]: duplicate layer digest %s", ErrInvalidContent, i, d.raw)
		}
		seen[d.raw] = true
		m.Layers = append(m.Layers, d)
	}
	return m, nil
}

// manifestJSON serializes the manifest in a canonical compact form. Content
// stored through PutManifest is stored under the digest of this form.
func manifestJSON(m Manifest) []byte {
	w := wireManifest{
		SchemaVersion: 2,
		Config:        wireDescriptor{Digest: m.Config.raw},
		Layers:        make([]wireDescriptor, 0, len(m.Layers)),
	}
	for _, d := range m.Layers {
		w.Layers = append(w.Layers, wireDescriptor{Digest: d.raw})
	}
	b, err := json.Marshal(&w)
	if err != nil {
		panic(err) // Manifest only contains validated digests; cannot fail.
	}
	return b
}
