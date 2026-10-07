package config

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// UpdateFile applies fn to the root mapping node of the YAML file at path
// and writes the result back atomically (temporary file + rename), so an
// interrupted write can never leave a half-written configuration. Before
// the rename the current content is copied to <path>.bak-panel — a
// rolling backup of the last state before each panel change. Comments,
// anchors and unrelated sections are preserved: the document is edited
// as a YAML node tree, never re-marshaled from structs.
func UpdateFile(path string, fn func(root *yaml.Node) error) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %q: %w", path, err)
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("parse %q: %w", path, err)
	}
	root := &doc
	if root.Kind == yaml.DocumentNode {
		if len(root.Content) == 0 {
			return fmt.Errorf("parse %q: empty document", path)
		}
		root = root.Content[0]
	}
	if root.Kind != yaml.MappingNode {
		return fmt.Errorf("parse %q: document root is not a mapping", path)
	}
	if err := fn(root); err != nil {
		return err
	}
	out, err := yaml.Marshal(&doc)
	if err != nil {
		return fmt.Errorf("encode %q: %w", path, err)
	}

	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat %q: %w", path, err)
	}
	if info.Size() > maxConfigFileBytes {
		return fmt.Errorf("config %q: size %d bytes exceeds the maximum of %d bytes", path, info.Size(), maxConfigFileBytes)
	}
	if _, err := os.Stat(path + backupSuffix); errors.Is(err, fs.ErrNotExist) {
		_ = copyFile(path, path+backupSuffix)
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, out, info.Mode().Perm()); err != nil {
		return fmt.Errorf("write %q: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("replace %q: %w", path, err)
	}
	return nil
}

// backupSuffix names the rolling backup kept before each panel write.
const backupSuffix = ".bak-panel"

func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0o600)
}

// SetScalarPath sets the value at the dotted mapping path (e.g.
// "web.force_local_tiles" or "meshtastic.channel_alerts") under root,
// creating missing mappings along the way. A non-mapping node anywhere
// on the path is an error.
func SetScalarPath(root *yaml.Node, path string, value *yaml.Node) error {
	parts := strings.Split(path, ".")
	cur := root
	for i, key := range parts {
		idx := -1
		for j := 0; j+1 < len(cur.Content); j += 2 {
			if cur.Content[j].Value == key {
				idx = j
				break
			}
		}
		if i == len(parts)-1 {
			if idx >= 0 {
				if cur.Content[idx+1].Kind == yaml.MappingNode {
					return fmt.Errorf("config path %q: %q is a mapping, not a scalar", path, key)
				}
				cur.Content[idx+1] = value
			} else {
				cur.Content = append(cur.Content, keyNode(key), value)
			}
			return nil
		}
		if idx < 0 {
			m := mappingNode()
			cur.Content = append(cur.Content, keyNode(key), m)
			cur = m
			continue
		}
		if cur.Content[idx+1].Kind != yaml.MappingNode {
			return fmt.Errorf("config path %q: %q is not a mapping", path, key)
		}
		cur = cur.Content[idx+1]
	}
	return nil
}

// keyNode builds a plain scalar node usable as a mapping key.
func keyNode(key string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}
}

// mappingNode builds an empty mapping node.
func mappingNode() *yaml.Node {
	return &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
}

// BoolScalar builds a boolean scalar node.
func BoolScalar(v bool) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: fmt.Sprintf("%v", v)}
}

// StringScalar builds a string scalar node. Multi-line strings are
// rendered as YAML block literals, single-line strings plainly.
func StringScalar(v string) *yaml.Node {
	n := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: v}
	if strings.Contains(v, "\n") {
		n.Style = yaml.LiteralStyle
	}
	return n
}
