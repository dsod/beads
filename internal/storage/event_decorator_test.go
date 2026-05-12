package storage_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/steveyegge/beads/events"
	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

// recordingSink captures every emitted envelope so assertions can introspect
// the bundle a single mutation produced.
type recordingSink struct {
	mu   sync.Mutex
	envs []*events.EventEnvelope
}

func (r *recordingSink) Emit(_ context.Context, env *events.EventEnvelope) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.envs = append(r.envs, env)
	return nil
}

func (r *recordingSink) Close() error { return nil }

func (r *recordingSink) byType() map[events.EventType]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := map[events.EventType]int{}
	for _, e := range r.envs {
		out[e.EventType]++
	}
	return out
}

func (r *recordingSink) snapshot() []*events.EventEnvelope {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]*events.EventEnvelope, len(r.envs))
	copy(cp, r.envs)
	return cp
}

// fakeStore implements just the DoltStorage methods we exercise in the
// decorator tests. The embedded storage.DoltStorage is intentionally nil — any
// call to an unimplemented method panics, which we treat as a signal that
// the test reached a code path we didn't intend.
type fakeStore struct {
	storage.DoltStorage // nil embed — unimplemented methods panic

	issues map[string]*types.Issue
	failOn map[string]bool

	createdIDs     []string
	updatedIDs     []string
	closedIDs      []string
	claimedIDs     []string
	addedLabels    []string
	removedLabels  []string
	addedDeps      []*types.Dependency
	removedDeps    []string
	commentBodies  []string
	reopenedIDs    []string
	typeChangedIDs []string
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		issues: map[string]*types.Issue{},
		failOn: map[string]bool{},
	}
}

func (s *fakeStore) clone(id string) *types.Issue {
	src, ok := s.issues[id]
	if !ok {
		return nil
	}
	cp := *src
	if src.Labels != nil {
		cp.Labels = append([]string(nil), src.Labels...)
	}
	return &cp
}

func (s *fakeStore) GetIssue(_ context.Context, id string) (*types.Issue, error) {
	if i := s.clone(id); i != nil {
		return i, nil
	}
	return nil, storage.ErrNotFound
}

func (s *fakeStore) CreateIssue(_ context.Context, issue *types.Issue, _ string) error {
	if s.failOn["create"] {
		return assertErr("create")
	}
	cp := *issue
	if issue.Labels != nil {
		cp.Labels = append([]string(nil), issue.Labels...)
	}
	s.issues[issue.ID] = &cp
	s.createdIDs = append(s.createdIDs, issue.ID)
	return nil
}

func (s *fakeStore) UpdateIssue(_ context.Context, id string, updates map[string]interface{}, _ string) error {
	if s.failOn["update:"+id] {
		return assertErr("update")
	}
	cur, ok := s.issues[id]
	if !ok {
		return storage.ErrNotFound
	}
	if v, ok := updates["status"].(string); ok {
		cur.Status = types.Status(v)
	}
	if v, ok := updates["priority"].(int); ok {
		cur.Priority = v
	}
	if v, ok := updates["assignee"].(string); ok {
		cur.Assignee = v
	}
	// Test-only knob: simulate a label-rewriting update so the decorator's
	// before/after diff sees the change. Real SQL handles labels in a
	// separate join; we just need observable post-state here.
	if v, ok := updates["_set_labels_for_test"].([]string); ok {
		cur.Labels = append([]string(nil), v...)
	}
	s.updatedIDs = append(s.updatedIDs, id)
	return nil
}

func (s *fakeStore) CloseIssue(_ context.Context, id string, reason, _, _ string) error {
	if s.failOn["close:"+id] {
		return assertErr("close")
	}
	cur, ok := s.issues[id]
	if !ok {
		return storage.ErrNotFound
	}
	cur.Status = types.StatusClosed
	cur.CloseReason = reason
	s.closedIDs = append(s.closedIDs, id)
	return nil
}

func (s *fakeStore) ReopenIssue(_ context.Context, id, _, _ string) error {
	cur, ok := s.issues[id]
	if !ok {
		return storage.ErrNotFound
	}
	cur.Status = types.StatusOpen
	s.reopenedIDs = append(s.reopenedIDs, id)
	return nil
}

func (s *fakeStore) UpdateIssueType(_ context.Context, id, issueType, _ string) error {
	cur, ok := s.issues[id]
	if !ok {
		return storage.ErrNotFound
	}
	cur.IssueType = types.IssueType(issueType)
	s.typeChangedIDs = append(s.typeChangedIDs, id)
	return nil
}

func (s *fakeStore) ClaimIssue(_ context.Context, id, actor string) error {
	cur, ok := s.issues[id]
	if !ok {
		return storage.ErrNotFound
	}
	cur.Status = types.StatusInProgress
	cur.Assignee = actor
	s.claimedIDs = append(s.claimedIDs, id)
	return nil
}

func (s *fakeStore) AddDependency(_ context.Context, dep *types.Dependency, _ string) error {
	s.addedDeps = append(s.addedDeps, dep)
	return nil
}

func (s *fakeStore) RemoveDependency(_ context.Context, issueID, dependsOnID, _ string) error {
	s.removedDeps = append(s.removedDeps, issueID+"->"+dependsOnID)
	return nil
}

func (s *fakeStore) AddLabel(_ context.Context, issueID, label, _ string) error {
	cur, ok := s.issues[issueID]
	if !ok {
		return storage.ErrNotFound
	}
	cur.Labels = append(cur.Labels, label)
	s.addedLabels = append(s.addedLabels, issueID+":"+label)
	return nil
}

func (s *fakeStore) RemoveLabel(_ context.Context, issueID, label, _ string) error {
	cur, ok := s.issues[issueID]
	if !ok {
		return storage.ErrNotFound
	}
	out := cur.Labels[:0]
	for _, l := range cur.Labels {
		if l != label {
			out = append(out, l)
		}
	}
	cur.Labels = out
	s.removedLabels = append(s.removedLabels, issueID+":"+label)
	return nil
}

func (s *fakeStore) AddIssueComment(_ context.Context, issueID, _, text string) (*types.Comment, error) {
	s.commentBodies = append(s.commentBodies, text)
	return &types.Comment{ID: "c-" + issueID, Text: text}, nil
}

type assertErr string

func (e assertErr) Error() string { return "fake: " + string(e) }

// ── Tests ──────────────────────────────────────────────────────────

func wrap(t *testing.T, store storage.DoltStorage) (*storage.EventEmittingStore, *recordingSink) {
	t.Helper()
	sink := &recordingSink{}
	return storage.NewEventEmittingStore(store, sink, "test-correlation"), sink
}

func seed(s *fakeStore, issue *types.Issue) {
	cp := *issue
	if issue.Labels != nil {
		cp.Labels = append([]string(nil), issue.Labels...)
	}
	s.issues[issue.ID] = &cp
}

func TestEventEmittingStore_CreateIssueEmitsCreated(t *testing.T) {
	inner := newFakeStore()
	dec, sink := wrap(t, inner)

	err := dec.CreateIssue(context.Background(), &types.Issue{
		ID:        "bd-1",
		Title:     "hello",
		IssueType: types.IssueType("task"),
		Labels:    []string{"a"},
	}, "alice")
	require.NoError(t, err)

	envs := sink.snapshot()
	require.Len(t, envs, 1)
	assert.Equal(t, events.IssueCreated, envs[0].EventType)
	assert.Equal(t, "issue:bd-1", envs[0].PartitionKey)
	assert.Equal(t, "test-correlation", envs[0].CorrelationID)
	payload, ok := envs[0].Payload.(events.IssueCreatedPayload)
	require.True(t, ok)
	assert.Equal(t, "bd-1", payload.IssueID)
	assert.Equal(t, []string{"a"}, payload.Labels)
}

func TestEventEmittingStore_CreateIssueFailureSuppressesEvent(t *testing.T) {
	inner := newFakeStore()
	inner.failOn["create"] = true
	dec, sink := wrap(t, inner)

	err := dec.CreateIssue(context.Background(), &types.Issue{ID: "bd-2"}, "alice")
	assert.Error(t, err)
	assert.Empty(t, sink.snapshot(), "no event when inner mutation fails")
}

func TestEventEmittingStore_UpdateBundlesStatusAndUpdated(t *testing.T) {
	inner := newFakeStore()
	seed(inner, &types.Issue{ID: "bd-3", Status: types.StatusOpen, Priority: 2, Labels: []string{"a"}})
	dec, sink := wrap(t, inner)

	err := dec.UpdateIssue(context.Background(), "bd-3", map[string]interface{}{
		"status":   "in_progress",
		"priority": 1,
	}, "alice")
	require.NoError(t, err)

	counts := sink.byType()
	assert.Equal(t, 1, counts[events.IssueStatusChanged], "transition emits status_changed")
	assert.Equal(t, 1, counts[events.IssueUpdated], "catch-all updated also emits")
}

func TestEventEmittingStore_UpdateLabelDiffEmitsAddedAndRemoved(t *testing.T) {
	inner := newFakeStore()
	seed(inner, &types.Issue{ID: "bd-4", Status: types.StatusOpen, Labels: []string{"old"}})
	dec, sink := wrap(t, inner)

	err := dec.UpdateIssue(context.Background(), "bd-4", map[string]interface{}{
		"assignee":             "alice",
		"_set_labels_for_test": []string{"new"},
	}, "alice")
	require.NoError(t, err)

	counts := sink.byType()
	assert.Equal(t, 1, counts[events.IssueLabelAdded])
	assert.Equal(t, 1, counts[events.IssueLabelRemoved])
}

func TestEventEmittingStore_CloseEmitsClosedAndStatusChanged(t *testing.T) {
	inner := newFakeStore()
	seed(inner, &types.Issue{ID: "bd-5", Status: types.StatusInProgress})
	dec, sink := wrap(t, inner)

	err := dec.CloseIssue(context.Background(), "bd-5", "done", "alice", "sess-1")
	require.NoError(t, err)

	counts := sink.byType()
	assert.Equal(t, 1, counts[events.IssueClosed])
	assert.Equal(t, 1, counts[events.IssueStatusChanged])
	for _, env := range sink.snapshot() {
		if env.EventType == events.IssueStatusChanged {
			p, ok := env.Payload.(events.IssueStatusChangedPayload)
			require.True(t, ok)
			assert.Equal(t, "in_progress", p.From)
			assert.Equal(t, "closed", p.To)
		}
	}
}

func TestEventEmittingStore_CloseFailureSuppressesEvents(t *testing.T) {
	inner := newFakeStore()
	seed(inner, &types.Issue{ID: "bd-6", Status: types.StatusOpen})
	inner.failOn["close:bd-6"] = true
	dec, sink := wrap(t, inner)

	err := dec.CloseIssue(context.Background(), "bd-6", "x", "alice", "")
	assert.Error(t, err)
	assert.Empty(t, sink.snapshot())
}

// TestEventEmittingStore_AutoCloseMoleculeBypassEmits documents the
// load-bearing improvement over the CLI-handler approach: when
// autoCloseCompletedMolecule (cmd/bd/close.go) calls activeStore.CloseIssue
// directly, the decorator on activeStore still fires issue.closed +
// issue.status_changed. The CLI-handler emit point used to skip this path.
func TestEventEmittingStore_AutoCloseMoleculeBypassEmits(t *testing.T) {
	inner := newFakeStore()
	seed(inner, &types.Issue{ID: "bd-mol", Status: types.StatusOpen, IssueType: types.IssueType("molecule")})
	dec, sink := wrap(t, inner)

	// Simulate the auto-close call path: a non-CLI caller invokes
	// store.CloseIssue directly. The decorator should still emit.
	err := dec.CloseIssue(context.Background(), "bd-mol", "all steps complete", "alice", "")
	require.NoError(t, err)

	counts := sink.byType()
	assert.Equal(t, 1, counts[events.IssueClosed], "auto-close path now emits issue.closed")
	assert.Equal(t, 1, counts[events.IssueStatusChanged])
}

func TestEventEmittingStore_ClaimEmitsClaimedAndStatusChanged(t *testing.T) {
	inner := newFakeStore()
	seed(inner, &types.Issue{ID: "bd-7", Status: types.StatusOpen})
	dec, sink := wrap(t, inner)

	err := dec.ClaimIssue(context.Background(), "bd-7", "bob")
	require.NoError(t, err)

	counts := sink.byType()
	assert.Equal(t, 1, counts[events.IssueClaimed])
	assert.Equal(t, 1, counts[events.IssueStatusChanged])
	for _, env := range sink.snapshot() {
		if env.EventType == events.IssueClaimed {
			p, ok := env.Payload.(events.IssueClaimedPayload)
			require.True(t, ok)
			assert.Equal(t, "bob", p.ClaimedByActor)
			assert.WithinDuration(t, time.Now().UTC().Add(24*time.Hour), p.LeaseExpiresAt, time.Minute)
		}
	}
}

func TestEventEmittingStore_AddDependencyEmitsUpdatedDependencies(t *testing.T) {
	inner := newFakeStore()
	dec, sink := wrap(t, inner)

	err := dec.AddDependency(context.Background(), &types.Dependency{
		IssueID:     "bd-a",
		DependsOnID: "bd-b",
		Type:        types.DependencyType("blocks"),
	}, "alice")
	require.NoError(t, err)

	envs := sink.snapshot()
	require.Len(t, envs, 1)
	assert.Equal(t, events.IssueUpdated, envs[0].EventType)
	assert.Equal(t, "issue:bd-a", envs[0].PartitionKey)
	p, ok := envs[0].Payload.(events.IssueUpdatedPayload)
	require.True(t, ok)
	assert.Equal(t, []string{"dependencies"}, p.ChangedFields)
}

func TestEventEmittingStore_AddCommentEmitsBodyKind(t *testing.T) {
	inner := newFakeStore()
	dec, sink := wrap(t, inner)

	_, err := dec.AddIssueComment(context.Background(), "bd-8", "alice", "[role:strategist] hello")
	require.NoError(t, err)

	envs := sink.snapshot()
	require.Len(t, envs, 1)
	assert.Equal(t, events.IssueCommentAdded, envs[0].EventType)
	p, ok := envs[0].Payload.(events.IssueCommentAddedPayload)
	require.True(t, ok)
	assert.Equal(t, "envelope", p.BodyKind)
	assert.Equal(t, "c-bd-8", p.CommentID)
}

func TestEventEmittingStore_BodyKindTable(t *testing.T) {
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
		inner := newFakeStore()
		dec, sink := wrap(t, inner)
		_, err := dec.AddIssueComment(context.Background(), "bd-x", "alice", c.body)
		require.NoError(t, err)
		envs := sink.snapshot()
		require.Len(t, envs, 1)
		p := envs[0].Payload.(events.IssueCommentAddedPayload)
		assert.Equal(t, c.want, p.BodyKind, "body=%q", c.body)
	}
}

func TestEventEmittingStore_NilSinkIsZeroCost(t *testing.T) {
	inner := newFakeStore()
	dec := storage.NewEventEmittingStore(inner, nil, "")

	err := dec.CreateIssue(context.Background(), &types.Issue{ID: "bd-9"}, "alice")
	require.NoError(t, err)
	// No sink, no panic, no emit. The fact that we got here without
	// crashing on nil-sink emit is the assertion.
}

func TestEventEmittingStore_NoopSinkSuppresses(t *testing.T) {
	inner := newFakeStore()
	dec := storage.NewEventEmittingStore(inner, events.NoopSink{}, "")

	err := dec.CreateIssue(context.Background(), &types.Issue{ID: "bd-10"}, "alice")
	require.NoError(t, err)
	// NoopSink never records — and the decorator short-circuits before
	// constructing payloads, so even malformed inputs don't blow up.
}

func TestEventEmittingStore_UnwrapEventStore(t *testing.T) {
	inner := newFakeStore()
	dec := storage.NewEventEmittingStore(inner, &recordingSink{}, "")
	assert.Same(t, inner, storage.UnwrapEventStore(dec))

	// Pass-through for non-decorator stores.
	var s storage.DoltStorage = inner
	assert.Same(t, inner, storage.UnwrapEventStore(s))
}
