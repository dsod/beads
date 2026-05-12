package events

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEnvelopeDefaults(t *testing.T) {
	t.Setenv("BEADS_ACTOR", "test-actor")
	before := time.Now().Add(-time.Second)

	env := NewEnvelope(IssueCreated, "issue:bd-abc", IssueCreatedPayload{
		IssueID: "bd-abc",
		Title:   "test",
	})

	require.NotNil(t, env)
	assert.Equal(t, IssueCreated, env.EventType)
	assert.Equal(t, SchemaVersion, env.SchemaVersion)
	assert.Equal(t, "issue:bd-abc", env.PartitionKey)
	assert.Equal(t, Source, env.Source)
	assert.NotEmpty(t, env.EventID)
	assert.NotEmpty(t, env.CorrelationID)
	assert.NotEmpty(t, env.DedupKey)
	assert.Nil(t, env.CausationID)
	assert.Equal(t, "human", env.Actor.Kind)
	assert.Equal(t, "test-actor", env.Actor.ID)
	assert.True(t, env.OccurredAt.After(before))
	assert.True(t, env.EmittedAt.After(before))
}

func TestNewEnvelopeOptions(t *testing.T) {
	causeID := "01HX0000000000000000000000"
	corrID := "01HX0000000000000000000001"
	occurred := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	env := NewEnvelope(IssueClosed, "issue:bd-xyz", IssueClosedPayload{
		IssueID: "bd-xyz",
		Reason:  "done",
	},
		WithCausation(causeID),
		WithCorrelation(corrID),
		WithActor(Actor{Kind: "role", ID: "role:strategist:run-1"}),
		WithOccurredAt(occurred),
	)

	require.NotNil(t, env.CausationID)
	assert.Equal(t, causeID, *env.CausationID)
	assert.Equal(t, corrID, env.CorrelationID)
	assert.Equal(t, "role", env.Actor.Kind)
	assert.Equal(t, "role:strategist:run-1", env.Actor.ID)
	assert.True(t, env.OccurredAt.Equal(occurred))
}

func TestDefaultActorFallback(t *testing.T) {
	// Force the env-var-based path; we can't mock git config portably.
	t.Setenv("BEADS_ACTOR", "")
	t.Setenv("BD_ACTOR", "fallback-actor")
	a := defaultActor()
	assert.Equal(t, "human", a.Kind)
	// Either git config wins (if set in the test environment) or BD_ACTOR
	// wins — both are correct. The "unknown" final fallback is the bug
	// we'd catch.
	assert.NotEmpty(t, a.ID)
	assert.NotEqual(t, "unknown", a.ID)
}

func TestEventIDsMonotonic(t *testing.T) {
	// ULIDs generated back-to-back within a single millisecond should still
	// be lexicographically increasing thanks to the monotonic entropy source.
	a := newULID()
	b := newULID()
	c := newULID()
	assert.Less(t, a, b)
	assert.Less(t, b, c)
}
