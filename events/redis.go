package events

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Sink is the destination for emitted envelopes. The Sink interface allows
// tests to swap a NoopSink or a recording fake without depending on Redis.
type Sink interface {
	Emit(ctx context.Context, env *EventEnvelope) error
	Close() error
}

// NoopSink swallows every emission. Returned when BEADS_EVENT_SINK is unset.
// Close is safe to call even on a zero NoopSink.
type NoopSink struct{}

func (NoopSink) Emit(context.Context, *EventEnvelope) error { return nil }
func (NoopSink) Close() error                               { return nil }

// RedisSink writes envelopes to Redis Streams. Streams are sharded across
// events:0..events:<NumStreams-1> by hash(partition_key); each stream is
// length-bounded to MaxStreamLen entries via XADD MAXLEN ~.
type RedisSink struct {
	client     *redis.Client
	streamBase string
	numStreams int
	maxLen     int64
}

// SinkConfig tweaks the RedisSink. Zero values use sensible defaults
// (events:, 8 shards, 10000-entry cap).
type SinkConfig struct {
	StreamBase   string // default "events:"
	NumStreams   int    // default 8; must be positive
	MaxStreamLen int64  // default 10000; 0 disables MAXLEN trim
}

const (
	defaultStreamBase   = "events:"
	defaultNumStreams   = 8
	defaultMaxStreamLen = 10000
)

// NewSink parses redisURL and returns either a *RedisSink or a NoopSink.
// An empty string yields a NoopSink with no error; this is the load-bearing
// path for users who haven't opted into event emission.
//
// The redisURL must be a redis:// URL (or rediss:// for TLS). Invalid URLs
// return an error rather than silently degrading; if the caller wants
// best-effort, they should check the error and fall back to NoopSink at the
// call site.
func NewSink(redisURL string) (Sink, error) {
	return NewSinkWithConfig(redisURL, SinkConfig{})
}

// NewSinkWithConfig is NewSink with explicit knobs. Used by tests.
func NewSinkWithConfig(redisURL string, cfg SinkConfig) (Sink, error) {
	if strings.TrimSpace(redisURL) == "" {
		return NoopSink{}, nil
	}
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse BEADS_EVENT_SINK URL %q: %w", redisURL, err)
	}
	client := redis.NewClient(opts)
	if cfg.StreamBase == "" {
		cfg.StreamBase = defaultStreamBase
	}
	if cfg.NumStreams <= 0 {
		cfg.NumStreams = defaultNumStreams
	}
	if cfg.MaxStreamLen == 0 {
		cfg.MaxStreamLen = defaultMaxStreamLen
	}
	return &RedisSink{
		client:     client,
		streamBase: cfg.StreamBase,
		numStreams: cfg.NumStreams,
		maxLen:     cfg.MaxStreamLen,
	}, nil
}

// NewSinkFromEnv constructs a Sink using the BEADS_EVENT_SINK env var. This is
// the canonical entry point for cmd/bd handlers.
func NewSinkFromEnv() (Sink, error) {
	return NewSink(os.Getenv("BEADS_EVENT_SINK"))
}

// StreamFor returns the target stream name for a given partition key. Exposed
// for tests and replay tooling that need to enumerate the active shards.
func (s *RedisSink) StreamFor(partitionKey string) string {
	sum := sha256.Sum256([]byte(partitionKey))
	// Take the first 8 bytes as a uint64, mod numStreams.
	idx := int(binary.BigEndian.Uint64(sum[:8]) % uint64(s.numStreams))
	return fmt.Sprintf("%s%d", s.streamBase, idx)
}

// Emit writes the envelope to its target stream. Envelopes are flattened to
// XADD fields — scalar envelope fields stay as strings, the payload is
// JSON-encoded into a single "payload" field. This shape is easy to inspect
// with redis-cli XRANGE and round-trips cleanly through Replay.
func (s *RedisSink) Emit(ctx context.Context, env *EventEnvelope) error {
	if env == nil {
		return fmt.Errorf("nil envelope")
	}
	stream := s.StreamFor(env.PartitionKey)

	payloadJSON, err := json.Marshal(env.Payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	actorJSON, err := json.Marshal(env.Actor)
	if err != nil {
		return fmt.Errorf("marshal actor: %w", err)
	}

	values := map[string]any{
		"event_id":       env.EventID,
		"event_type":     string(env.EventType),
		"schema_version": env.SchemaVersion,
		"occurred_at":    env.OccurredAt.UTC().Format(time.RFC3339Nano),
		"emitted_at":     env.EmittedAt.UTC().Format(time.RFC3339Nano),
		"partition_key":  env.PartitionKey,
		"source":         env.Source,
		"dedup_key":      env.DedupKey,
		"correlation_id": env.CorrelationID,
		"actor":          string(actorJSON),
		"payload":        string(payloadJSON),
	}
	if env.CausationID != nil {
		values["causation_id"] = *env.CausationID
	}

	args := &redis.XAddArgs{
		Stream: stream,
		Values: values,
	}
	if s.maxLen > 0 {
		args.MaxLen = s.maxLen
		args.Approx = true
	}
	if err := s.client.XAdd(ctx, args).Err(); err != nil {
		return fmt.Errorf("XADD to %s: %w", stream, err)
	}
	return nil
}

// Close releases the underlying Redis client.
func (s *RedisSink) Close() error {
	if s.client == nil {
		return nil
	}
	return s.client.Close()
}

// Streams enumerates the configured shard names. Useful for Replay / Tail
// which need to walk every shard.
func (s *RedisSink) Streams() []string {
	out := make([]string, s.numStreams)
	for i := 0; i < s.numStreams; i++ {
		out[i] = fmt.Sprintf("%s%d", s.streamBase, i)
	}
	return out
}

// Client exposes the underlying *redis.Client for advanced operations (Replay,
// Tail) that need raw stream commands. Callers MUST NOT close it.
func (s *RedisSink) Client() *redis.Client {
	return s.client
}

// EnvelopeFromXMessage rebuilds an envelope from a Redis XMessage's Values map.
// The inverse of Emit's flattening; used by Replay.
func EnvelopeFromXMessage(msg redis.XMessage) (*EventEnvelope, error) {
	env := &EventEnvelope{}
	get := func(k string) string {
		if v, ok := msg.Values[k]; ok {
			if s, ok := v.(string); ok {
				return s
			}
		}
		return ""
	}
	env.EventID = get("event_id")
	env.EventType = EventType(get("event_type"))
	env.PartitionKey = get("partition_key")
	env.Source = get("source")
	env.DedupKey = get("dedup_key")
	env.CorrelationID = get("correlation_id")
	if c := get("causation_id"); c != "" {
		env.CausationID = &c
	}
	if v := get("schema_version"); v != "" {
		var n int
		_, _ = fmt.Sscanf(v, "%d", &n)
		env.SchemaVersion = n
	}
	if t, err := time.Parse(time.RFC3339Nano, get("occurred_at")); err == nil {
		env.OccurredAt = t
	}
	if t, err := time.Parse(time.RFC3339Nano, get("emitted_at")); err == nil {
		env.EmittedAt = t
	}
	if actorStr := get("actor"); actorStr != "" {
		_ = json.Unmarshal([]byte(actorStr), &env.Actor)
	}
	if payloadStr := get("payload"); payloadStr != "" {
		var p any
		if err := json.Unmarshal([]byte(payloadStr), &p); err == nil {
			env.Payload = p
		}
	}
	return env, nil
}
