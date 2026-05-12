package events

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// DedupKey returns a deterministic key suitable for consumer-side deduplication.
//
// Shape: "<eventType>:<partitionKey>:<sha256(eventType+":"+partitionKey+":"+canonicalJSON(payload))>"
//
// Two emissions with the same event type, partition key, and payload produce
// the same key. Consumers SHOULD drop duplicates seen within a recent window
// (e.g. last 1000 ids, or last 60 seconds) — the key itself doesn't encode a
// TTL.
//
// The prefix is included for human-readability when scanning a stream; the
// SHA-256 suffix carries the entropy.
func DedupKey(eventType EventType, partitionKey string, payload any) string {
	canonical, err := canonicalJSON(payload)
	if err != nil {
		// Falling back to a non-canonical encoding here would defeat the
		// determinism guarantee; emit an explicit error marker instead so a
		// human notices the bad payload.
		canonical = []byte(fmt.Sprintf(`{"_canonical_json_error":%q}`, err.Error()))
	}
	h := sha256.New()
	h.Write([]byte(string(eventType)))
	h.Write([]byte{':'})
	h.Write([]byte(partitionKey))
	h.Write([]byte{':'})
	h.Write(canonical)
	return fmt.Sprintf("%s:%s:%s", eventType, partitionKey, hex.EncodeToString(h.Sum(nil)))
}

// canonicalJSON encodes value with object keys sorted recursively. encoding/json
// already sorts struct-tag keys alphabetically, but maps in payloads (e.g.
// IssueUpdatedPayload.Before/After) are not deterministic — we round-trip
// through map[string]any and sort.
func canonicalJSON(v any) ([]byte, error) {
	// Marshal then unmarshal into a generic shape so we can re-marshal with
	// sorted keys. This is O(n) and only runs at emit time, so cost is
	// negligible.
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	var generic any
	if err := json.Unmarshal(raw, &generic); err != nil {
		return nil, fmt.Errorf("unmarshal generic: %w", err)
	}
	return marshalSorted(generic)
}

func marshalSorted(v any) ([]byte, error) {
	switch x := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		buf := []byte{'{'}
		for i, k := range keys {
			if i > 0 {
				buf = append(buf, ',')
			}
			kb, err := json.Marshal(k)
			if err != nil {
				return nil, err
			}
			buf = append(buf, kb...)
			buf = append(buf, ':')
			vb, err := marshalSorted(x[k])
			if err != nil {
				return nil, err
			}
			buf = append(buf, vb...)
		}
		return append(buf, '}'), nil
	case []any:
		buf := []byte{'['}
		for i, item := range x {
			if i > 0 {
				buf = append(buf, ',')
			}
			vb, err := marshalSorted(item)
			if err != nil {
				return nil, err
			}
			buf = append(buf, vb...)
		}
		return append(buf, ']'), nil
	default:
		return json.Marshal(v)
	}
}
