package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Instance is one entry of a top-level list in the configuration file
// ("sources", "actions", "outputs"), as the Config page sees it: the
// position it occupies, its identity and the poll interval it declares.
// The position is what a ScalarEdit path needs, because an operator
// addresses one instance by its place in the file.
type Instance struct {
	// Index is the position in the YAML list, for "sources[N]...".
	Index int
	// ID is the instance's "id" key.
	ID string
	// Type is the instance's "type" key.
	Type string
	// PollInterval is config.poll_interval; HasPoll reports whether the
	// instance declares the key at all.
	PollInterval time.Duration
	HasPoll      bool
}

// ConfigPath returns the dotted path of the instance's config mapping,
// ready to be extended with a key ("sources[2].config").
func (in Instance) ConfigPath(section string) string {
	return fmt.Sprintf("%s[%d].config", section, in.Index)
}

// Instances reads one top-level list from the configuration file. It is a
// read-only probe used for rendering: the panel shows the operator the
// instances that are really in the file, not the ones the running
// process happens to hold.
func Instances(path, section string) ([]Instance, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if len(doc.Content) == 0 {
		return nil, nil
	}
	root := doc.Content[0]
	if root.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("%s: the document is not a mapping", path)
	}
	idx := mappingKeyIndex(root, section)
	if idx < 0 {
		return nil, nil
	}
	seq := root.Content[idx+1]
	if seq.Kind != yaml.SequenceNode {
		return nil, fmt.Errorf("%s: section %q is not a list", path, section)
	}
	out := make([]Instance, 0, len(seq.Content))
	for i, item := range seq.Content {
		if item.Kind != yaml.MappingNode {
			continue
		}
		in := Instance{Index: i, ID: scalarOf(item, "id"), Type: scalarOf(item, "type")}
		if cfgIdx := mappingKeyIndex(item, "config"); cfgIdx >= 0 {
			cfg := item.Content[cfgIdx+1]
			if cfg.Kind == yaml.MappingNode {
				if v, ok := scalarNodeOf(cfg, "poll_interval"); ok {
					if d, err := time.ParseDuration(v); err == nil {
						in.PollInterval, in.HasPoll = d, true
					}
				}
			}
		}
		out = append(out, in)
	}
	return out, nil
}

// scalarOf returns a string scalar of a mapping node ("" when absent).
func scalarOf(m *yaml.Node, key string) string {
	v, _ := scalarNodeOf(m, key)
	return v
}

// scalarNodeOf returns a scalar value of a mapping node; ok is false when
// the key is absent or holds a structured value.
func scalarNodeOf(m *yaml.Node, key string) (string, bool) {
	idx := mappingKeyIndex(m, key)
	if idx < 0 {
		return "", false
	}
	v := m.Content[idx+1]
	if v.Kind != yaml.ScalarNode || v.Tag == "!!null" {
		return "", false
	}
	return v.Value, true
}

// ScalarAt reads one scalar value by dotted path, list entries included
// ("aprs.radius_km", "actions[1].config.hazards_interval"). It is the
// read-only probe the Config page uses to show what the file holds.
func ScalarAt(path, dotted string) (string, bool) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", false
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil || len(doc.Content) == 0 {
		return "", false
	}
	node := doc.Content[0]
	segs := strings.Split(dotted, ".")
	for i, seg := range segs {
		if node.Kind != yaml.MappingNode {
			return "", false
		}
		name, item, indexed := splitIndex(seg)
		if i == len(segs)-1 {
			if indexed {
				return "", false
			}
			return scalarNodeOf(node, name)
		}
		idx := mappingKeyIndex(node, name)
		if idx < 0 {
			return "", false
		}
		node = node.Content[idx+1]
		if indexed {
			if node.Kind != yaml.SequenceNode || item < 0 || item >= len(node.Content) {
				return "", false
			}
			node = node.Content[item]
		}
	}
	return "", false
}
