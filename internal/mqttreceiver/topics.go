package mqttreceiver

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
)

// slug matches WarnFlux's topic segment validation.
var slugRe = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

var hashRe = regexp.MustCompile(`^[0-9a-f]{64}$`)

// TopicKind classifies a parsed MQTT topic.
type TopicKind int

const (
	TopicUnknown TopicKind = iota
	TopicEvents
	TopicStatus
	TopicActive
	TopicInfo
)

// ParsedTopic is the result of strict topic parsing.
type ParsedTopic struct {
	Kind TopicKind

	// active
	Source string
	Hash   string

	// info
	InfoSource string
	ProducerID string
	Key        string
	InfoKind   string
}

// TopicHash returns the lowercase hex SHA-256 over the raw event key bytes,
// matching WarnFlux's active topic ID derivation.
func TopicHash(eventKey string) string {
	sum := sha256.Sum256([]byte(eventKey))
	return hex.EncodeToString(sum[:])
}

// ParseTopic strictly parses a topic against the configured prefix.
// Malformed or unexpected topics yield TopicUnknown and are ignored.
func ParseTopic(prefix, topic string) ParsedTopic {
	rest, ok := strings.CutPrefix(topic, prefix+"/")
	if !ok {
		return ParsedTopic{Kind: TopicUnknown}
	}
	switch rest {
	case "events":
		return ParsedTopic{Kind: TopicEvents}
	case "status":
		return ParsedTopic{Kind: TopicStatus}
	}

	if active, ok := strings.CutPrefix(rest, "active/"); ok {
		source, hash, found := strings.Cut(active, "/")
		if !found || !slugRe.MatchString(source) || !hashRe.MatchString(hash) {
			return ParsedTopic{Kind: TopicUnknown}
		}
		return ParsedTopic{Kind: TopicActive, Source: source, Hash: hash}
	}

	if info, ok := strings.CutPrefix(rest, "info/"); ok {
		segs := strings.Split(info, "/")
		if len(segs) != 4 {
			return ParsedTopic{Kind: TopicUnknown}
		}
		source, producer, key, kind := segs[0], segs[1], segs[2], segs[3]
		if !slugRe.MatchString(source) || !slugRe.MatchString(producer) ||
			!slugRe.MatchString(key) || !slugRe.MatchString(kind) {
			return ParsedTopic{Kind: TopicUnknown}
		}
		return ParsedTopic{
			Kind:       TopicInfo,
			InfoSource: source,
			ProducerID: producer,
			Key:        key,
			InfoKind:   kind,
		}
	}
	return ParsedTopic{Kind: TopicUnknown}
}

// Subscriptions returns the MQTT subscriptions for a WarnFlux-mode
// receiver prefix (QoS 1).
func Subscriptions(prefix string) []string {
	return []string{
		prefix + "/active/#",
		prefix + "/info/#",
		prefix + "/status",
		prefix + "/events",
	}
}
