package events

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingSink captures every emit for assertion. Used to verify replay.
type recordingSink struct {
	mu   sync.Mutex
	envs []*EventEnvelope
}

func (r *recordingSink) Emit(_ context.Context, env *EventEnvelope) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.envs = append(r.envs, env)
	return nil
}

func (r *recordingSink) Close() error { return nil }

func (r *recordingSink) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.envs)
}

func newMiniredisSink(t *testing.T) (*RedisSink, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	sink, err := NewSinkWithConfig("redis://"+mr.Addr(), SinkConfig{NumStreams: 4, MaxStreamLen: 1000})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sink.Close() })
	rs, ok := sink.(*RedisSink)
	require.True(t, ok, "expected *RedisSink, got %T", sink)
	return rs, mr
}

func TestNewSinkEmptyURLReturnsNoop(t *testing.T) {
	s, err := NewSink("")
	require.NoError(t, err)
	_, ok := s.(NoopSink)
	assert.True(t, ok, "expected NoopSink for empty URL, got %T", s)
}

func TestNewSinkInvalidURLErrors(t *testing.T) {
	s, err := NewSink("not-a-url")
	assert.Error(t, err)
	assert.Nil(t, s)
}

func TestRedisSinkEmitRoundTrip(t *testing.T) {
	sink, _ := newMiniredisSink(t)
	ctx := context.Background()

	envs := []*EventEnvelope{
		NewEnvelope(IssueCreated, "issue:bd-1", IssueCreatedPayload{IssueID: "bd-1", Title: "one"}),
		NewEnvelope(IssueClaimed, "issue:bd-1", IssueClaimedPayload{IssueID: "bd-1", ClaimedByActor: "alice"}),
		NewEnvelope(IssueClosed, "issue:bd-1", IssueClosedPayload{IssueID: "bd-1", Reason: "done"}),
		NewEnvelope(IssueCreated, "issue:bd-2", IssueCreatedPayload{IssueID: "bd-2", Title: "two"}),
		NewEnvelope(IssueCreated, "issue:bd-3", IssueCreatedPayload{IssueID: "bd-3", Title: "three"}),
	}
	for _, env := range envs {
		require.NoError(t, sink.Emit(ctx, env))
	}

	// Walk every shard and reconstruct the set of emitted envelopes.
	client := sink.Client()
	totalSeen := 0
	for _, stream := range sink.Streams() {
		msgs, err := client.XRange(ctx, stream, "-", "+").Result()
		require.NoError(t, err)
		totalSeen += len(msgs)
		for _, msg := range msgs {
			env, err := EnvelopeFromXMessage(msg)
			require.NoError(t, err)
			assert.NotEmpty(t, env.EventID)
			assert.NotEmpty(t, env.DedupKey)
			assert.Equal(t, Source, env.Source)
			assert.True(t, strings.HasPrefix(env.PartitionKey, "issue:"))
		}
	}
	assert.Equal(t, len(envs), totalSeen)
}

func TestStreamForIsStableAcrossKeys(t *testing.T) {
	sink, _ := newMiniredisSink(t)
	// Same key → same shard, every time. Different keys → distribute across
	// shards (don't all land on shard 0).
	keys := []string{"issue:bd-1", "issue:bd-2", "issue:bd-3", "issue:bd-4", "issue:bd-5"}
	shardCounts := map[string]int{}
	for _, k := range keys {
		s1 := sink.StreamFor(k)
		s2 := sink.StreamFor(k)
		assert.Equal(t, s1, s2, "stream for %q must be stable", k)
		shardCounts[s1]++
	}
	// With 5 keys hashed into 4 shards, we expect at least 2 distinct shards
	// in use. A 100% single-shard outcome would indicate a broken hash.
	assert.GreaterOrEqual(t, len(shardCounts), 2)
}

func TestReplay(t *testing.T) {
	source, _ := newMiniredisSink(t)
	ctx := context.Background()

	originals := []*EventEnvelope{
		NewEnvelope(IssueCreated, "issue:bd-1", IssueCreatedPayload{IssueID: "bd-1"}),
		NewEnvelope(IssueLabelAdded, "issue:bd-1", IssueLabelAddedPayload{IssueID: "bd-1", Label: "p1"}),
		NewEnvelope(IssueClosed, "issue:bd-1", IssueClosedPayload{IssueID: "bd-1", Reason: "done"}),
	}
	for _, env := range originals {
		require.NoError(t, source.Emit(ctx, env))
	}

	dest := &recordingSink{}
	n, err := Replay(ctx, source, "-", dest)
	require.NoError(t, err)
	assert.Equal(t, len(originals), n)
	assert.Equal(t, len(originals), dest.count())
}

func TestNoopSinkAlwaysSucceeds(t *testing.T) {
	var s NoopSink
	require.NoError(t, s.Emit(context.Background(), nil))
	require.NoError(t, s.Close())
}

// TestEmitErrorOnNilEnvelope confirms the defensive check at the top of Emit.
func TestEmitErrorOnNilEnvelope(t *testing.T) {
	sink, _ := newMiniredisSink(t)
	err := sink.Emit(context.Background(), nil)
	assert.Error(t, err)
}

// TestRedisSinkClientClose ensures Close is idempotent and doesn't panic.
func TestRedisSinkClientClose(t *testing.T) {
	sink, _ := newMiniredisSink(t)
	require.NoError(t, sink.Close())
	// Second close should also not panic; go-redis returns an error here,
	// which is acceptable.
	_ = sink.Close()
}

// Compile-time check: *redis.Client is what we expect.
var _ Sink = (*RedisSink)(nil)
var _ Sink = NoopSink{}
var _ = redis.Nil
