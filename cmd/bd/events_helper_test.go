package main

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/steveyegge/beads/events"
	"github.com/steveyegge/beads/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetEventSinkUnsetReturnsNoop(t *testing.T) {
	resetEventSinkForTest()
	t.Setenv("BEADS_EVENT_SINK", "")

	sink, corr := getEventSink()
	_, ok := sink.(events.NoopSink)
	assert.True(t, ok, "expected NoopSink, got %T", sink)
	assert.NotEmpty(t, corr, "correlation id should still be assigned for noop sinks")
}

func TestEmitEventRoundTripThroughHelper(t *testing.T) {
	resetEventSinkForTest()
	mr := miniredis.RunT(t)
	t.Setenv("BEADS_EVENT_SINK", "redis://"+mr.Addr())
	t.Setenv("BEADS_ACTOR", "test-agent")

	emitEvent(context.Background(), events.IssueCreated, issuePartition("bd-1"), events.IssueCreatedPayload{
		IssueID:        "bd-1",
		Type:           "task",
		Title:          "hello",
		Labels:         []string{"l1"},
		CreatedByActor: "test-agent",
	})

	sink, corr := getEventSink()
	require.NotEmpty(t, corr)
	rs, ok := sink.(*events.RedisSink)
	require.True(t, ok, "expected *RedisSink, got %T", sink)

	dest := &testRecordingSink{}
	n, err := events.Replay(context.Background(), rs, "-", dest)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	require.Len(t, dest.envs, 1)
	got := dest.envs[0]
	assert.Equal(t, events.IssueCreated, got.EventType)
	assert.Equal(t, "issue:bd-1", got.PartitionKey)
	assert.Equal(t, corr, got.CorrelationID, "all helper emissions share the run correlation id")
}

func TestEmitUpdateEventsBundlesStatusAndLabels(t *testing.T) {
	resetEventSinkForTest()
	mr := miniredis.RunT(t)
	t.Setenv("BEADS_EVENT_SINK", "redis://"+mr.Addr())
	t.Setenv("BEADS_ACTOR", "alice")

	before := &types.Issue{
		ID:       "bd-1",
		Title:    "x",
		Status:   types.StatusOpen,
		Priority: 2,
		Labels:   []string{"old"},
	}
	after := &types.Issue{
		ID:       "bd-1",
		Title:    "x",
		Status:   types.StatusInProgress,
		Priority: 1,
		Labels:   []string{"new"},
	}

	emitUpdateEvents(context.Background(), "bd-1", before, after,
		map[string]interface{}{"status": "in_progress", "priority": 1},
		[]string{"new"}, []string{"old"}, nil, false)

	sink, _ := getEventSink()
	rs, ok := sink.(*events.RedisSink)
	require.True(t, ok)

	dest := &testRecordingSink{}
	_, err := events.Replay(context.Background(), rs, "-", dest)
	require.NoError(t, err)

	counts := map[events.EventType]int{}
	for _, env := range dest.envs {
		counts[env.EventType]++
	}
	assert.GreaterOrEqual(t, counts[events.IssueStatusChanged], 1, "expected status_changed event")
	assert.GreaterOrEqual(t, counts[events.IssueLabelAdded], 1, "expected label_added event")
	assert.GreaterOrEqual(t, counts[events.IssueLabelRemoved], 1, "expected label_removed event")
	assert.GreaterOrEqual(t, counts[events.IssueUpdated], 1, "expected issue.updated event")
}

func TestEmitUpdateEventsClaim(t *testing.T) {
	resetEventSinkForTest()
	mr := miniredis.RunT(t)
	t.Setenv("BEADS_EVENT_SINK", "redis://"+mr.Addr())
	t.Setenv("BEADS_ACTOR", "bob")

	before := &types.Issue{ID: "bd-2", Status: types.StatusOpen}
	after := &types.Issue{ID: "bd-2", Status: types.StatusInProgress, Assignee: "bob"}

	emitUpdateEvents(context.Background(), "bd-2", before, after,
		map[string]interface{}{}, nil, nil, nil, true)

	sink, _ := getEventSink()
	rs := sink.(*events.RedisSink)
	dest := &testRecordingSink{}
	_, err := events.Replay(context.Background(), rs, "-", dest)
	require.NoError(t, err)

	var sawClaimed, sawStatus bool
	for _, env := range dest.envs {
		if env.EventType == events.IssueClaimed {
			sawClaimed = true
		}
		if env.EventType == events.IssueStatusChanged {
			sawStatus = true
		}
	}
	assert.True(t, sawClaimed, "expected issue.claimed")
	assert.True(t, sawStatus, "expected issue.status_changed alongside claim")
}

func TestDetectCommentBodyKind(t *testing.T) {
	cases := []struct {
		body string
		want string
	}{
		{"[role:strategist] handoff", "envelope"},
		{"hello\n\n## Plan\n- step 1", "plan"},
		{"## Review\nLooks good", "review"},
		{"just a comment", "other"},
		{"", "other"},
	}
	for _, c := range cases {
		got := detectCommentBodyKind(c.body)
		assert.Equal(t, c.want, got, "body=%q", c.body)
	}
}

// testRecordingSink captures envelopes for assertion. Distinct from
// events/redis_test.go's recordingSink to keep test packages decoupled.
type testRecordingSink struct {
	envs []*events.EventEnvelope
}

func (r *testRecordingSink) Emit(_ context.Context, env *events.EventEnvelope) error {
	r.envs = append(r.envs, env)
	return nil
}

func (r *testRecordingSink) Close() error { return nil }
