package config

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// ScalarEdit is one targeted panel change: the value at the dotted
// mapping path (e.g. "web.force_local_tiles") becomes the given scalar.
type ScalarEdit struct {
	Path  string
	Value *yaml.Node
}

// UpdateFile applies the requested scalar edits DIRECTLY to the file
// bytes: values of existing keys are replaced in place (their line
// ranges come from the parsed node tree), missing keys are appended at
// the end of their section. Edits are applied sequentially, re-parsing
// after each step, so a batch creating a new section (e.g. all ten
// mqtt_publish flags at once) nests every key under one header.
// Everything else — comments, alignment, blank lines, anchors — stays
// byte-identical, so the panel behaves like an editor of just these
// settings and never reformats the operator's file. The write is atomic
// (temporary file + rename) with a rolling backup <path>.bak-panel
// before the first change.
func UpdateFile(path string, edits []ScalarEdit) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %q: %w", path, err)
	}
	current := data
	for i, e := range edits {
		out, err := applyEdit(current, e)
		if err != nil {
			return fmt.Errorf("edit %d (%s): %w", i, e.Path, err)
		}
		current = []byte(out)
	}

	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat %q: %w", path, err)
	}
	if _, err := os.Stat(path + backupSuffix); errors.Is(err, fs.ErrNotExist) {
		_ = copyFile(path, path+backupSuffix)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, current, info.Mode().Perm()); err != nil {
		return fmt.Errorf("write %q: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("replace %q: %w", path, err)
	}
	return nil
}

// applyEdit computes and applies one edit against the given content and
// returns the new content.
func applyEdit(data []byte, e ScalarEdit) (string, error) {
	lines := strings.Split(string(data), "\n")
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return "", fmt.Errorf("parse: %w", err)
	}
	root := &doc
	if root.Kind == yaml.DocumentNode {
		if len(root.Content) == 0 {
			return "", fmt.Errorf("parse: empty document")
		}
		root = root.Content[0]
	}
	if root.Kind != yaml.MappingNode {
		return "", fmt.Errorf("parse: document root is not a mapping")
	}
	indent := detectIndent(data)
	p, err := buildPatch(root, e, indent, lines)
	if err != nil {
		return "", err
	}
	return applyPatches(lines, []linePatch{p})
}

// linePatch replaces the line range [start, end) with text (no trailing
// newline). start == end means a pure insertion at that line index.
type linePatch struct {
	start, end int
	text       string
}

func applyPatches(lines []string, ps []linePatch) (string, error) {
	var b strings.Builder
	prev := 0
	for _, p := range ps {
		if p.start < prev || p.start > len(lines) || p.end < p.start || p.end > len(lines) {
			return "", fmt.Errorf("patch range [%d,%d) out of order or bounds", p.start, p.end)
		}
		for i := prev; i < p.start; i++ {
			b.WriteString(lines[i])
			b.WriteByte('\n')
		}
		if p.text != "" {
			b.WriteString(p.text)
			b.WriteByte('\n')
		}
		prev = p.end
	}
	for i := prev; i < len(lines); i++ {
		b.WriteString(lines[i])
		if i < len(lines)-1 {
			b.WriteByte('\n')
		}
	}
	return b.String(), nil
}

// buildPatch computes the replacement/insertion for one edit.
func buildPatch(root *yaml.Node, e ScalarEdit, indent int, lines []string) (linePatch, error) {
	parts := strings.Split(e.Path, ".")
	parent, key := root, parts[len(parts)-1]
	for i, seg := range parts[:len(parts)-1] {
		next, idx := childMapping(parent, seg)
		if idx < 0 {
			// The section does not exist yet: append the whole missing
			// tail (section + nested keys) at the end of the current
			// parent mapping.
			anchor := mappingEndLine(parent, lines)
			text := renderNested(parts[i:], e.Value, indent, childIndent(parent, indent))
			return linePatch{start: anchor, end: anchor, text: text}, nil
		}
		if next.Kind != yaml.MappingNode {
			return linePatch{}, fmt.Errorf("section %q is not a mapping", seg)
		}
		parent = next
	}

	if idx := mappingKeyIndex(parent, key); idx >= 0 {
		return replacePatch(parent, idx, key, e.Value, indent, lines), nil
	}
	// Missing key: append after the parent's last child.
	anchor := mappingEndLine(parent, lines)
	rendered := renderKeyed(key, e.Value, indent, childIndent(parent, indent))
	return linePatch{start: anchor, end: anchor, text: rendered}, nil
}

// childMapping finds a key segment inside the mapping and returns its
// value node and pair index (-1 when absent).
func childMapping(m *yaml.Node, key string) (*yaml.Node, int) {
	idx := mappingKeyIndex(m, key)
	if idx < 0 {
		return nil, -1
	}
	return m.Content[idx+1], idx
}

// mappingKeyIndex returns the pair index of key inside the mapping
// (-1 when absent).
func mappingKeyIndex(m *yaml.Node, key string) int {
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return i
		}
	}
	return -1
}

// childIndent is the column at which the parent's children start
// (0-based), falling back to the document indent.
func childIndent(m *yaml.Node, indent int) int {
	if len(m.Content) >= 2 && m.Content[0].Kind == yaml.ScalarNode && m.Content[0].Column > 0 {
		return m.Content[0].Column - 1
	}
	if m.Column > 0 {
		return m.Column - 1
	}
	return indent
}

// mappingEndLine is the 0-based line AFTER the parent's last child
// (the insertion anchor for new keys).
func mappingEndLine(m *yaml.Node, lines []string) int {
	if len(m.Content) == 0 {
		if m.Line > 0 {
			return m.Line // right after the "section:" header line
		}
		return len(lines)
	}
	last := m.Content[len(m.Content)-1]
	end := nodeEndLine(last)
	if end > len(lines) {
		return len(lines)
	}
	return end
}

// nodeEndLine is the 0-based line after the node's last source line
// (recursively for mappings/sequences).
func nodeEndLine(n *yaml.Node) int {
	if n.Kind == yaml.ScalarNode && (n.Style == yaml.LiteralStyle || n.Style == yaml.FoldedStyle) {
		return n.Line + strings.Count(n.Value, "\n") + 1
	}
	if n.Kind == yaml.ScalarNode {
		return n.Line
	}
	end := n.Line
	for _, c := range n.Content {
		if e := nodeEndLine(c); e > end {
			end = e
		}
	}
	return end
}

// replacePatch replaces the value of an existing key in place.
func replacePatch(parent *yaml.Node, pairIdx int, key string, value *yaml.Node, indent int, lines []string) linePatch {
	v := parent.Content[pairIdx+1]
	keyLine := v.Line - 1
	if v.Kind == yaml.ScalarNode && (v.Style == yaml.LiteralStyle || v.Style == yaml.FoldedStyle) {
		// Multi-line block: replace the header line and the content.
		return linePatch{
			start: keyLine,
			end:   nodeEndLine(v),
			text:  renderKeyed(key, value, indent, v.Column-1),
		}
	}
	// Inline scalar: rebuild just this line, preserving any trailing
	// inline comment. renderKeyed carries the original indentation
	// itself (keyIndent = the old leading whitespace).
	line := lines[keyLine]
	lead := line[:len(line)-len(strings.TrimLeft(line, " \t"))]
	comment := v.LineComment
	if comment == "" && v.Column <= len(line) {
		if i := strings.LastIndex(line[v.Column-1:], " #"); i >= 0 {
			comment = line[v.Column-1+i+1:]
		}
	}
	text := renderKeyed(key, value, indent, len(lead))
	if comment != "" {
		text += " " + comment
	}
	return linePatch{start: keyLine, end: keyLine + 1, text: text}
}

// renderKeyed renders "key: value" with value content indented by
// keyIndent (the key's own column) for block scalars.
func renderKeyed(key string, value *yaml.Node, indent, keyIndent int) string {
	return strings.Repeat(" ", keyIndent) + key + ": " + renderValue(value, indent, keyIndent)
}

// renderNested renders a chain of nested keys ending in value, e.g.
// "meshtastic:" + newline + "  station_alerts: true" for a missing
// section. base is the column of the first key.
func renderNested(segments []string, value *yaml.Node, indent, base int) string {
	var b strings.Builder
	for i, k := range segments {
		b.WriteString(strings.Repeat(" ", base+indent*i) + k + ":")
		if i < len(segments)-1 {
			b.WriteString("\n")
		}
	}
	lastIndent := base + indent*(len(segments)-1)
	b.WriteString(" " + renderValue(value, indent, lastIndent))
	return b.String()
}

// renderValue renders just the value part (no key prefix); block scalars
// indent their content by keyIndent+indent.
func renderValue(value *yaml.Node, indent, keyIndent int) string {
	if value.Kind != yaml.ScalarNode {
		return scalarString(value)
	}
	s := value.Value
	if value.Tag == "!!bool" {
		return s
	}
	if strings.Contains(s, "\n") {
		var b strings.Builder
		b.WriteString("|")
		for _, ln := range strings.Split(strings.TrimSuffix(s, "\n"), "\n") {
			b.WriteString("\n" + strings.Repeat(" ", keyIndent+indent) + ln)
		}
		return b.String()
	}
	if needsQuoting(s) {
		var buf bytes.Buffer
		enc := yaml.NewEncoder(&buf)
		_ = enc.Encode(value)
		_ = enc.Close()
		return strings.TrimSuffix(buf.String(), "\n")
	}
	if s == "" {
		return `""`
	}
	return s
}

// scalarString renders any non-string scalar node (bools are handled by
// the caller; this covers numbers etc.).
func scalarString(n *yaml.Node) string {
	if n.Value == "" {
		return `""`
	}
	return n.Value
}

// needsQuoting reports whether a plain YAML rendering of s would be
// ambiguous or broken.
func needsQuoting(s string) bool {
	if s == "" || strings.TrimSpace(s) != s {
		return true
	}
	switch strings.ToLower(s) {
	case "true", "false", "null", "yes", "no", "on", "off", "~":
		return true
	}
	for _, r := range s {
		switch r {
		case ':', '#', '{', '}', '[', ']', ',', '&', '*', '!', '|', '>', '\'', '"', '%', '@', '`':
			return true
		}
	}
	return false
}

// backupSuffix names the rolling backup kept before each panel write.
const backupSuffix = ".bak-panel"

// detectIndent finds the document's mapping indentation (the indentation
// of the first nested key line; 2 for the canonical configs, 4 for the
// yaml.v3 default). Comments and list items are skipped.
func detectIndent(data []byte) int {
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimLeft(line, " ")
		if trimmed == "" || trimmed[0] == '#' || trimmed[0] == '-' {
			continue
		}
		if n := len(line) - len(trimmed); n > 0 {
			return n
		}
	}
	return 4
}

func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0o600)
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
