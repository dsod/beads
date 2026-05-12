package events

import (
	"crypto/rand"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
)

// Actor identifies who or what caused an event. Kind is "human" | "role" | "system";
// ID is a stable identifier within that kind (e.g. "human:dsod",
// "role:strategist:run-abc").
type Actor struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// EventEnvelope is the wire format for every event emitted to the sink.
//
// Two timestamps:
//   - OccurredAt is the source-of-truth time (the mutation's effective time);
//     callers can override via WithOccurredAt for replays.
//   - EmittedAt is when the envelope was wrapped, always wall-clock at construct
//     time.
//
// PartitionKey controls Redis stream routing (events are sharded across
// events:<0..7> by hash(partition_key)). For all issue events the convention
// is "issue:<id>"; downstream consumers can fan out per-stream.
//
// DedupKey is deterministic — see dedup.go. Consumers SHOULD ignore duplicate
// dedup_keys within a short window.
//
// CausationID/CorrelationID form a lightweight tracing layer:
//   - CorrelationID groups all events from one logical workflow (one CLI
//     invocation, one agent task).
//   - CausationID points to the upstream event that triggered this one
//     (nil for user-initiated events).
type EventEnvelope struct {
	EventID       string    `json:"event_id"`
	EventType     EventType `json:"event_type"`
	SchemaVersion int       `json:"schema_version"`
	OccurredAt    time.Time `json:"occurred_at"`
	EmittedAt     time.Time `json:"emitted_at"`
	PartitionKey  string    `json:"partition_key"`
	Source        string    `json:"source"`
	DedupKey      string    `json:"dedup_key"`
	CausationID   *string   `json:"causation_id"`
	CorrelationID string    `json:"correlation_id"`
	Actor         Actor     `json:"actor"`
	Payload       any       `json:"payload"`
}

// Source is the constant value placed in EventEnvelope.Source for this fork.
// It distinguishes events emitted by the bd wrapper from events emitted by
// downstream agents/orchestrators.
const Source = "beads-wrapper"

// Option mutates an envelope during construction. Use WithCausation,
// WithCorrelation, WithActor, or WithOccurredAt rather than fiddling with
// envelope fields directly.
type Option func(*EventEnvelope)

// WithCausation sets the causation_id (id of the event that caused this one).
func WithCausation(id string) Option {
	return func(e *EventEnvelope) { e.CausationID = &id }
}

// WithCorrelation overrides the auto-generated correlation_id. Use this to
// group multiple emissions from one CLI invocation under a shared id.
func WithCorrelation(id string) Option {
	return func(e *EventEnvelope) { e.CorrelationID = id }
}

// WithActor overrides the default actor (which is derived from BEADS_ACTOR or
// `git config user.name`).
func WithActor(a Actor) Option {
	return func(e *EventEnvelope) { e.Actor = a }
}

// WithOccurredAt overrides the source-of-truth timestamp. Useful when replaying
// historical events or backfilling.
func WithOccurredAt(t time.Time) Option {
	return func(e *EventEnvelope) { e.OccurredAt = t }
}

// NewEnvelope constructs a fully-populated envelope. EventID, EmittedAt,
// OccurredAt, CorrelationID, DedupKey, Actor, Source, and SchemaVersion all get
// sensible defaults; pass Options to override.
//
// NewEnvelope is total — it never returns an error. ULID generation failures
// (vanishingly rare; require entropy exhaustion) fall back to a timestamp-only
// id, which is still globally unique enough for this fork.
func NewEnvelope(eventType EventType, partitionKey string, payload any, opts ...Option) *EventEnvelope {
	now := time.Now().UTC()
	env := &EventEnvelope{
		EventID:       newULID(),
		EventType:     eventType,
		SchemaVersion: SchemaVersion,
		OccurredAt:    now,
		EmittedAt:     now,
		PartitionKey:  partitionKey,
		Source:        Source,
		CorrelationID: newULID(),
		Actor:         defaultActor(),
		Payload:       payload,
	}
	for _, opt := range opts {
		opt(env)
	}
	env.DedupKey = DedupKey(eventType, partitionKey, payload)
	return env
}

// newULID returns a monotonic ULID using crypto/rand entropy. The
// `ulid.MonotonicEntropy` wrapper avoids same-millisecond collisions when
// envelopes are constructed in tight loops (e.g. update emitting an updated +
// status_changed + label_added bundle).
var ulidEntropy = ulid.Monotonic(rand.Reader, 0)

func newULID() string {
	id, err := ulid.New(ulid.Timestamp(time.Now()), ulidEntropy)
	if err != nil {
		// crypto/rand never errors on Linux/macOS in practice; fall back to a
		// timestamp-only ULID so we still emit something sortable.
		return ulid.MustNew(ulid.Now(), nil).String()
	}
	return id.String()
}

// defaultActor mirrors getActorWithGit() in cmd/bd: prefer BEADS_ACTOR, then
// `git config user.name`, then $USER, then "unknown". Kept in sync intentionally
// — the events package can't import cmd/bd.
func defaultActor() Actor {
	id := envOrEmpty("BEADS_ACTOR")
	if id == "" {
		id = envOrEmpty("BD_ACTOR")
	}
	if id == "" {
		if out, err := exec.Command("git", "config", "user.name").Output(); err == nil {
			if name := strings.TrimSpace(string(out)); name != "" {
				id = name
			}
		}
	}
	if id == "" {
		id = envOrEmpty("USER")
	}
	if id == "" {
		id = "unknown"
	}
	return Actor{Kind: "human", ID: id}
}

func envOrEmpty(key string) string {
	return strings.TrimSpace(os.Getenv(key))
}
