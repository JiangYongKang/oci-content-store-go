package store

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Descriptor 描述一份内容：媒体类型、摘要与长度。
type Descriptor struct {
	MediaType string `json:"mediaType"`
	Digest    Digest `json:"digest"`
	Size      int64  `json:"size"`
}

// Manifest 是镜像清单：一个配置描述符加若干层描述符。
type Manifest struct {
	SchemaVersion int          `json:"schemaVersion"`
	MediaType     string       `json:"mediaType,omitempty"`
	Config        Descriptor   `json:"config"`
	Layers        []Descriptor `json:"layers"`
}

// validateDescriptor 校验单个描述符：摘要格式合法且长度非负。
func validateDescriptor(name string, d Descriptor) error {
	if d.Size < 0 {
		return fmt.Errorf("%w: %s has negative size %d", ErrInvalidManifest, name, d.Size)
	}
	if string(d.Digest) == "" {
		return fmt.Errorf("%w: %s has empty digest", ErrInvalidManifest, name)
	}
	if err := d.Digest.Validate(); err != nil {
		return fmt.Errorf("%w: %s: %v", ErrInvalidManifest, name, err)
	}
	return nil
}

// validate 校验清单结构：版本、配置与每一层都必须合法。
func (m *Manifest) validate() error {
	if m.SchemaVersion != 2 {
		return fmt.Errorf("%w: schemaVersion must be 2, got %d", ErrInvalidManifest, m.SchemaVersion)
	}
	if err := validateDescriptor("config", m.Config); err != nil {
		return err
	}
	if m.Layers == nil {
		return fmt.Errorf("%w: layers is required", ErrInvalidManifest)
	}
	for i, l := range m.Layers {
		if err := validateDescriptor(fmt.Sprintf("layers[%d]", i), l); err != nil {
			return err
		}
	}
	return nil
}

// decodeManifest 解析并校验清单字节：JSON 非法或结构非法都归类为
// ErrInvalidManifest，使调用方能与摘要类错误区分。
func decodeManifest(b []byte) (Manifest, error) {
	var m Manifest
	dec := json.NewDecoder(bytes.NewReader(b))
	if err := dec.Decode(&m); err != nil {
		return Manifest{}, fmt.Errorf("%w: bad json: %v", ErrInvalidManifest, err)
	}
	if dec.More() {
		return Manifest{}, fmt.Errorf("%w: trailing data after manifest", ErrInvalidManifest)
	}
	if err := m.validate(); err != nil {
		return Manifest{}, err
	}
	return m, nil
}

// references 返回清单引用的所有描述符（配置 + 各层，去重保留首次出现）。
func (m *Manifest) references() []Descriptor {
	seen := make(map[Digest]struct{}, len(m.Layers)+1)
	refs := make([]Descriptor, 0, len(m.Layers)+1)
	add := func(d Descriptor) {
		if _, ok := seen[d.Digest]; ok {
			return
		}
		seen[d.Digest] = struct{}{}
		refs = append(refs, d)
	}
	add(m.Config)
	for _, l := range m.Layers {
		add(l)
	}
	return refs
}
