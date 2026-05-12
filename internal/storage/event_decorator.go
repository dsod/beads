// Package storage — event_decorator.go
//
// EventEmittingStore is a decorator around DoltStorage that publishes
// envelopes to an events.Sink after every successful state-mutating
// operation. It mirrors HookFiringStore's shape so future mutation
// methods are caught automatically by either chain.
//
// Wrap order: events outermost (after hooks). If a hook rejects a
// change inside HookFiringStore, no event is emitted — that's the
// correct semantics for "observation of state changes that DID
// happen". The CLI assembles the chain as
//
//	EventEmittingStore(HookFiringStore(rawStore))
//
// in cmd/bd/store_factory.go.
//
// Transaction support: mutations inside RunInTransaction accumulate
// pending events and only fire on successful commit. Rollback drops
// the queue.
//
// Zero-cost path: when sink is nil (BEADS_EVENT_SINK unset), every
// emit method short-circuits before computing payloads.
package storage

import (
	"context"
	"strings"
	"time"

	"github.com/steveyegge/beads/events"
	"github.com/steveyegge/beads/internal/types"
)

// EventEmittingStore wraps a DoltStorage and emits events.* envelopes
// after successful mutations. Non-mutating methods pass through via the
// embedded DoltStorage.
type EventEmittingStore struct {
	DoltStorage             // embed for passthrough of non-overridden methods
	inner       DoltStorage // the real (or already-wrapped) store
	sink        events.Sink
	corrID      string // correlation id shared by every emit from this wrapper
}

// NewEventEmittingStore wraps store with automatic event emission.
// When sink is nil, no events are emitted (the decorator is still
// constructed so wrap-call sites don't need to branch). corrID groups
// every emit from this wrapper under a single correlation id; pass an
// empty string to auto-generate one.
func NewEventEmittingStore(store DoltStorage, sink events.Sink, corrID string) *EventEmittingStore {
	if corrID == "" {
		corrID = generateEventCorrelationID()
	}
	return &EventEmittingStore{
		DoltStorage: store,
		inner:       store,
		sink:        sink,
		corrID:      corrID,
	}
}

// Inner returns the underlying store. Useful for callers that need to
// reach the concrete store for optional-interface assertions (StoreLocator,
// BackupStore, etc.).
func (e *EventEmittingStore) Inner() DoltStorage { return e.inner }

// UnwrapEventStore returns the underlying store if s is an
// EventEmittingStore, otherwise returns s unchanged. Pair with UnwrapStore
// when reaching for concrete-store-only methods.
func UnwrapEventStore(s DoltStorage) DoltStorage {
	if e, ok := s.(*EventEmittingStore); ok {
		return e.inner
	}
	return s
}

// ── Issue mutations ────────────────────────────────────────────────

func (e *EventEmittingStore) CreateIssue(ctx context.Context, issue *types.Issue, actor string) error {
	if err := e.inner.CreateIssue(ctx, issue, actor); err != nil {
		return err
	}
	e.emitCreated(ctx, issue)
	return nil
}

func (e *EventEmittingStore) CreateIssues(ctx context.Context, issues []*types.Issue, actor string) error {
	if err := e.inner.CreateIssues(ctx, issues, actor); err != nil {
		return err
	}
	for _, issue := range issues {
		e.emitCreated(ctx, issue)
	}
	return nil
}

func (e *EventEmittingStore) UpdateIssue(ctx context.Context, id string, updates map[string]interface{}, actor string) error {
	before, _ := e.inner.GetIssue(ctx, id)
	if err := e.inner.UpdateIssue(ctx, id, updates, actor); err != nil {
		return err
	}
	after, _ := e.inner.GetIssue(ctx, id)
	e.emitUpdated(ctx, id, before, after, updates)
	return nil
}

func (e *EventEmittingStore) ReopenIssue(ctx context.Context, id string, reason string, actor string) error {
	before, _ := e.inner.GetIssue(ctx, id)
	if err := e.inner.ReopenIssue(ctx, id, reason, actor); err != nil {
		return err
	}
	after, _ := e.inner.GetIssue(ctx, id)
	fromStatus := ""
	if before != nil {
		fromStatus = string(before.Status)
	}
	toStatus := string(types.StatusOpen)
	if after != nil {
		toStatus = string(after.Status)
	}
	e.emitStatusChanged(ctx, id, fromStatus, toStatus, nil)
	e.emitUpdated(ctx, id, before, after, map[string]interface{}{"status": toStatus})
	return nil
}

func (e *EventEmittingStore) UpdateIssueType(ctx context.Context, id string, issueType string, actor string) error {
	before, _ := e.inner.GetIssue(ctx, id)
	if err := e.inner.UpdateIssueType(ctx, id, issueType, actor); err != nil {
		return err
	}
	after, _ := e.inner.GetIssue(ctx, id)
	e.emitUpdated(ctx, id, before, after, map[string]interface{}{"issue_type": issueType})
	return nil
}

func (e *EventEmittingStore) CloseIssue(ctx context.Context, id string, reason string, actor string, session string) error {
	before, _ := e.inner.GetIssue(ctx, id)
	if err := e.inner.CloseIssue(ctx, id, reason, actor, session); err != nil {
		return err
	}
	fromStatus := "open"
	if before != nil {
		fromStatus = string(before.Status)
	}
	e.emitClosed(ctx, id, reason, actor)
	e.emitStatusChanged(ctx, id, fromStatus, string(types.StatusClosed), nil)
	return nil
}

// ── Claim mutations ─────────────────────────────────────────────────

func (e *EventEmittingStore) ClaimIssue(ctx context.Context, id string, actor string) error {
	before, _ := e.inner.GetIssue(ctx, id)
	if err := e.inner.ClaimIssue(ctx, id, actor); err != nil {
		return err
	}
	fromStatus := ""
	if before != nil {
		fromStatus = string(before.Status)
	}
	e.emitClaimed(ctx, id, actor)
	claimedBy := actor
	e.emitStatusChanged(ctx, id, fromStatus, string(types.StatusInProgress), &claimedBy)
	return nil
}

// ── Dependency mutations ────────────────────────────────────────────

func (e *EventEmittingStore) AddDependency(ctx context.Context, dep *types.Dependency, actor string) error {
	if err := e.inner.AddDependency(ctx, dep, actor); err != nil {
		return err
	}
	e.emitDependencyAdded(ctx, dep)
	return nil
}

func (e *EventEmittingStore) RemoveDependency(ctx context.Context, issueID, dependsOnID string, actor string) error {
	if err := e.inner.RemoveDependency(ctx, issueID, dependsOnID, actor); err != nil {
		return err
	}
	e.emitDependencyRemoved(ctx, issueID, dependsOnID)
	return nil
}

// ── Label mutations ─────────────────────────────────────────────────

func (e *EventEmittingStore) AddLabel(ctx context.Context, issueID, label, actor string) error {
	if err := e.inner.AddLabel(ctx, issueID, label, actor); err != nil {
		return err
	}
	after, _ := e.inner.GetIssue(ctx, issueID)
	var allLabels []string
	if after != nil {
		allLabels = after.Labels
	}
	e.emitLabelAdded(ctx, issueID, label, allLabels)
	return nil
}

func (e *EventEmittingStore) RemoveLabel(ctx context.Context, issueID, label, actor string) error {
	if err := e.inner.RemoveLabel(ctx, issueID, label, actor); err != nil {
		return err
	}
	after, _ := e.inner.GetIssue(ctx, issueID)
	var allLabels []string
	if after != nil {
		allLabels = after.Labels
	}
	e.emitLabelRemoved(ctx, issueID, label, allLabels)
	return nil
}

// ── Comment mutations ───────────────────────────────────────────────

func (e *EventEmittingStore) AddIssueComment(ctx context.Context, issueID, author, text string) (*types.Comment, error) {
	comment, err := e.inner.AddIssueComment(ctx, issueID, author, text)
	if err != nil {
		return nil, err
	}
	if comment != nil {
		e.emitCommentAdded(ctx, issueID, comment.ID, author, text)
	}
	return comment, nil
}

// ── Transaction support ─────────────────────────────────────────────

// RunInTransaction wraps the inner store's transaction with event
// tracking. Mutations inside the transaction accumulate pending
// envelopes and only fire after the transaction commits successfully.
// Rollback drops the queue.
func (e *EventEmittingStore) RunInTransaction(ctx context.Context, commitMsg string, fn func(tx Transaction) error) error {
	var tracked *eventTrackingTransaction
	err := e.inner.RunInTransaction(ctx, commitMsg, func(tx Transaction) error {
		tracked = &eventTrackingTransaction{Transaction: tx}
		return fn(tracked)
	})
	if err != nil || tracked == nil {
		return err
	}
	for _, p := range tracked.pending {
		e.emit(ctx, p.eventType, p.partitionKey, p.payload)
	}
	return nil
}

// ── Internal emit helpers ───────────────────────────────────────────

func (e *EventEmittingStore) shouldEmit() bool {
	if e == nil || e.sink == nil {
		return false
	}
	if _, isNoop := e.sink.(events.NoopSink); isNoop {
		return false
	}
	return true
}

func (e *EventEmittingStore) emit(ctx context.Context, eventType events.EventType, partitionKey string, payload any) {
	if !e.shouldEmit() {
		return
	}
	env := events.NewEnvelope(eventType, partitionKey, payload, events.WithCorrelation(e.corrID))
	_ = e.sink.Emit(ctx, env)
}

func issuePartitionKey(id string) string { return "issue:" + id }

func (e *EventEmittingStore) emitCreated(ctx context.Context, issue *types.Issue) {
	if issue == nil {
		return
	}
	// Dependencies aren't loaded by default on issue.Dependencies — when the
	// caller stamped a parent-child link before persisting we surface it,
	// otherwise the parent-child dep will arrive as a separate
	// IssueUpdated{ChangedFields:["dependencies"]} event from
	// emitDependencyAdded.
	var parentEpicPtr *string
	for _, dep := range issue.Dependencies {
		if dep != nil && dep.Type == types.DepParentChild {
			p := dep.DependsOnID
			parentEpicPtr = &p
			break
		}
	}
	e.emit(ctx, events.IssueCreated, issuePartitionKey(issue.ID), events.IssueCreatedPayload{
		IssueID:        issue.ID,
		Type:           string(issue.IssueType),
		Title:          issue.Title,
		Labels:         issue.Labels,
		ParentEpicID:   parentEpicPtr,
		CreatedByActor: defaultActorID(),
	})
}

func (e *EventEmittingStore) emitUpdated(ctx context.Context, id string, before, after *types.Issue, updates map[string]interface{}) {
	if !e.shouldEmit() {
		return
	}
	// Detect specific status / label changes first so consumers can route
	// on those event types without re-parsing the catch-all payload.
	if before != nil && after != nil && before.Status != after.Status {
		e.emitStatusChanged(ctx, id, string(before.Status), string(after.Status), nil)
	}
	if before != nil && after != nil {
		emitLabelDiff(ctx, e, id, before.Labels, after.Labels)
	}

	changedFields := make([]string, 0, len(updates))
	for k := range updates {
		if k == "" {
			continue
		}
		changedFields = append(changedFields, k)
	}
	if len(changedFields) == 0 {
		return
	}
	beforeMap := map[string]any{}
	afterMap := map[string]any{}
	populateIssueFieldMap(beforeMap, before, changedFields)
	populateIssueFieldMap(afterMap, after, changedFields)
	e.emit(ctx, events.IssueUpdated, issuePartitionKey(id), events.IssueUpdatedPayload{
		IssueID:       id,
		ChangedFields: changedFields,
		Before:        beforeMap,
		After:         afterMap,
	})
}

func (e *EventEmittingStore) emitStatusChanged(ctx context.Context, id, from, to string, claimedBy *string) {
	e.emit(ctx, events.IssueStatusChanged, issuePartitionKey(id), events.IssueStatusChangedPayload{
		IssueID:   id,
		From:      from,
		To:        to,
		ClaimedBy: claimedBy,
	})
}

func (e *EventEmittingStore) emitClaimed(ctx context.Context, id, actor string) {
	e.emit(ctx, events.IssueClaimed, issuePartitionKey(id), events.IssueClaimedPayload{
		IssueID:        id,
		ClaimedByActor: actor,
		// bd has no native lease TTL; 24h is a placeholder. Consumers
		// that need a server-authoritative expiry should ignore this.
		LeaseExpiresAt: time.Now().UTC().Add(24 * time.Hour),
	})
}

func (e *EventEmittingStore) emitClosed(ctx context.Context, id, reason, actor string) {
	e.emit(ctx, events.IssueClosed, issuePartitionKey(id), events.IssueClosedPayload{
		IssueID:       id,
		Reason:        reason,
		ClosedByActor: actor,
	})
}

func (e *EventEmittingStore) emitLabelAdded(ctx context.Context, id, label string, allLabels []string) {
	e.emit(ctx, events.IssueLabelAdded, issuePartitionKey(id), events.IssueLabelAddedPayload{
		IssueID:   id,
		Label:     label,
		AllLabels: allLabels,
	})
}

func (e *EventEmittingStore) emitLabelRemoved(ctx context.Context, id, label string, allLabels []string) {
	e.emit(ctx, events.IssueLabelRemoved, issuePartitionKey(id), events.IssueLabelRemovedPayload{
		IssueID:   id,
		Label:     label,
		AllLabels: allLabels,
	})
}

func (e *EventEmittingStore) emitCommentAdded(ctx context.Context, issueID, commentID, author, body string) {
	e.emit(ctx, events.IssueCommentAdded, issuePartitionKey(issueID), events.IssueCommentAddedPayload{
		IssueID:     issueID,
		CommentID:   commentID,
		AuthorActor: author,
		BodyKind:    detectCommentBodyKind(body),
	})
}

func (e *EventEmittingStore) emitDependencyAdded(ctx context.Context, dep *types.Dependency) {
	if dep == nil {
		return
	}
	e.emit(ctx, events.IssueUpdated, issuePartitionKey(dep.IssueID), events.IssueUpdatedPayload{
		IssueID:       dep.IssueID,
		ChangedFields: []string{"dependencies"},
		Before:        map[string]any{},
		After: map[string]any{
			"dependencies": map[string]any{
				"added": []map[string]string{{
					"depends_on": dep.DependsOnID,
					"type":       string(dep.Type),
				}},
			},
		},
	})
}

func (e *EventEmittingStore) emitDependencyRemoved(ctx context.Context, issueID, dependsOnID string) {
	e.emit(ctx, events.IssueUpdated, issuePartitionKey(issueID), events.IssueUpdatedPayload{
		IssueID:       issueID,
		ChangedFields: []string{"dependencies"},
		Before: map[string]any{
			"dependencies": map[string]any{
				"removed": []map[string]string{{
					"depends_on": dependsOnID,
				}},
			},
		},
		After: map[string]any{},
	})
}

// emitLabelDiff is a free function so the tx-tracking transaction can
// reuse the same diffing logic without depending on the wrapper instance.
func emitLabelDiff(ctx context.Context, e *EventEmittingStore, id string, before, after []string) {
	prev := make(map[string]struct{}, len(before))
	for _, l := range before {
		prev[l] = struct{}{}
	}
	next := make(map[string]struct{}, len(after))
	for _, l := range after {
		next[l] = struct{}{}
	}
	for l := range next {
		if _, kept := prev[l]; !kept {
			e.emitLabelAdded(ctx, id, l, after)
		}
	}
	for l := range prev {
		if _, kept := next[l]; !kept {
			e.emitLabelRemoved(ctx, id, l, after)
		}
	}
}

// detectCommentBodyKind classifies a comment body for the BodyKind field.
// Cheap heuristics — goal is routing, not parsing.
func detectCommentBodyKind(body string) string {
	trimmed := strings.TrimSpace(body)
	if strings.HasPrefix(trimmed, "[role:") {
		return "envelope"
	}
	if strings.Contains(trimmed, "## Plan") {
		return "plan"
	}
	if strings.Contains(trimmed, "## Review") {
		return "review"
	}
	return "other"
}

// populateIssueFieldMap copies the named fields from issue into dest.
// Mirrors the helper in cmd/bd/events_helper.go pre-decorator; lifted
// here so the storage layer owns the logic.
func populateIssueFieldMap(dest map[string]any, issue *types.Issue, fields []string) {
	if issue == nil {
		return
	}
	for _, f := range fields {
		switch f {
		case "status":
			dest[f] = string(issue.Status)
		case "priority":
			dest[f] = issue.Priority
		case "title":
			dest[f] = issue.Title
		case "assignee":
			dest[f] = issue.Assignee
		case "description":
			dest[f] = issue.Description
		case "design":
			dest[f] = issue.Design
		case "notes":
			dest[f] = issue.Notes
		case "acceptance_criteria":
			dest[f] = issue.AcceptanceCriteria
		case "external_ref":
			if issue.ExternalRef != nil {
				dest[f] = *issue.ExternalRef
			}
		case "spec_id":
			dest[f] = issue.SpecID
		case "estimated_minutes":
			if issue.EstimatedMinutes != nil {
				dest[f] = *issue.EstimatedMinutes
			}
		case "issue_type":
			dest[f] = string(issue.IssueType)
		}
	}
}

// generateEventCorrelationID returns a ULID-shaped id by piggy-backing on
// events.NewEnvelope's auto-assigned correlation field. Keeps the helper
// free of redundant ulid wiring.
func generateEventCorrelationID() string {
	env := events.NewEnvelope(events.EventType("internal.correlation_seed"), "internal", nil)
	return env.CorrelationID
}

// defaultActorID mirrors events.defaultActor (BEADS_ACTOR → git config →
// USER → unknown) but returns just the id string. Used in IssueCreatedPayload
// where we need the actor on the payload itself, not just on the envelope.
func defaultActorID() string {
	return events.NewEnvelope(events.EventType("internal.actor_probe"), "internal", nil).Actor.ID
}

// ── Event tracking transaction ──────────────────────────────────────

// pendingEvent records an event to fire after transaction commit.
type pendingEvent struct {
	eventType    events.EventType
	partitionKey string
	payload      any
}

// eventTrackingTransaction wraps a Transaction, accumulating events as
// mutations land. The wrapper fires them only after RunInTransaction
// returns nil from the inner store.
type eventTrackingTransaction struct {
	Transaction
	pending []pendingEvent
}

func (t *eventTrackingTransaction) queue(eventType events.EventType, partitionKey string, payload any) {
	t.pending = append(t.pending, pendingEvent{eventType, partitionKey, payload})
}

func (t *eventTrackingTransaction) CreateIssue(ctx context.Context, issue *types.Issue, actor string) error {
	if err := t.Transaction.CreateIssue(ctx, issue, actor); err != nil {
		return err
	}
	if issue != nil {
		var parentEpicPtr *string
		for _, dep := range issue.Dependencies {
			if dep.Type == types.DepParentChild {
				p := dep.DependsOnID
				parentEpicPtr = &p
				break
			}
		}
		t.queue(events.IssueCreated, issuePartitionKey(issue.ID), events.IssueCreatedPayload{
			IssueID:        issue.ID,
			Type:           string(issue.IssueType),
			Title:          issue.Title,
			Labels:         issue.Labels,
			ParentEpicID:   parentEpicPtr,
			CreatedByActor: defaultActorID(),
		})
	}
	return nil
}

func (t *eventTrackingTransaction) CreateIssues(ctx context.Context, issues []*types.Issue, actor string) error {
	if err := t.Transaction.CreateIssues(ctx, issues, actor); err != nil {
		return err
	}
	for _, issue := range issues {
		if issue == nil {
			continue
		}
		var parentEpicPtr *string
		for _, dep := range issue.Dependencies {
			if dep.Type == types.DepParentChild {
				p := dep.DependsOnID
				parentEpicPtr = &p
				break
			}
		}
		t.queue(events.IssueCreated, issuePartitionKey(issue.ID), events.IssueCreatedPayload{
			IssueID:        issue.ID,
			Type:           string(issue.IssueType),
			Title:          issue.Title,
			Labels:         issue.Labels,
			ParentEpicID:   parentEpicPtr,
			CreatedByActor: defaultActorID(),
		})
	}
	return nil
}

func (t *eventTrackingTransaction) UpdateIssue(ctx context.Context, id string, updates map[string]interface{}, actor string) error {
	before, _ := t.Transaction.GetIssue(ctx, id)
	if err := t.Transaction.UpdateIssue(ctx, id, updates, actor); err != nil {
		return err
	}
	after, _ := t.Transaction.GetIssue(ctx, id)
	if before != nil && after != nil && before.Status != after.Status {
		t.queue(events.IssueStatusChanged, issuePartitionKey(id), events.IssueStatusChangedPayload{
			IssueID: id,
			From:    string(before.Status),
			To:      string(after.Status),
		})
	}
	if before != nil && after != nil {
		queueLabelDiff(t, id, before.Labels, after.Labels)
	}
	changedFields := make([]string, 0, len(updates))
	for k := range updates {
		if k == "" {
			continue
		}
		changedFields = append(changedFields, k)
	}
	if len(changedFields) > 0 {
		beforeMap := map[string]any{}
		afterMap := map[string]any{}
		populateIssueFieldMap(beforeMap, before, changedFields)
		populateIssueFieldMap(afterMap, after, changedFields)
		t.queue(events.IssueUpdated, issuePartitionKey(id), events.IssueUpdatedPayload{
			IssueID:       id,
			ChangedFields: changedFields,
			Before:        beforeMap,
			After:         afterMap,
		})
	}
	return nil
}

func (t *eventTrackingTransaction) CloseIssue(ctx context.Context, id string, reason string, actor string, session string) error {
	before, _ := t.Transaction.GetIssue(ctx, id)
	if err := t.Transaction.CloseIssue(ctx, id, reason, actor, session); err != nil {
		return err
	}
	fromStatus := "open"
	if before != nil {
		fromStatus = string(before.Status)
	}
	t.queue(events.IssueClosed, issuePartitionKey(id), events.IssueClosedPayload{
		IssueID:       id,
		Reason:        reason,
		ClosedByActor: actor,
	})
	t.queue(events.IssueStatusChanged, issuePartitionKey(id), events.IssueStatusChangedPayload{
		IssueID: id,
		From:    fromStatus,
		To:      string(types.StatusClosed),
	})
	return nil
}

func (t *eventTrackingTransaction) AddDependency(ctx context.Context, dep *types.Dependency, actor string) error {
	return t.AddDependencyWithOptions(ctx, dep, actor, DependencyAddOptions{})
}

func (t *eventTrackingTransaction) AddDependencyWithOptions(ctx context.Context, dep *types.Dependency, actor string, opts DependencyAddOptions) error {
	if err := t.Transaction.AddDependencyWithOptions(ctx, dep, actor, opts); err != nil {
		return err
	}
	if dep == nil {
		return nil
	}
	t.queue(events.IssueUpdated, issuePartitionKey(dep.IssueID), events.IssueUpdatedPayload{
		IssueID:       dep.IssueID,
		ChangedFields: []string{"dependencies"},
		Before:        map[string]any{},
		After: map[string]any{
			"dependencies": map[string]any{
				"added": []map[string]string{{
					"depends_on": dep.DependsOnID,
					"type":       string(dep.Type),
				}},
			},
		},
	})
	return nil
}

func (t *eventTrackingTransaction) RemoveDependency(ctx context.Context, issueID, dependsOnID string, actor string) error {
	if err := t.Transaction.RemoveDependency(ctx, issueID, dependsOnID, actor); err != nil {
		return err
	}
	t.queue(events.IssueUpdated, issuePartitionKey(issueID), events.IssueUpdatedPayload{
		IssueID:       issueID,
		ChangedFields: []string{"dependencies"},
		Before: map[string]any{
			"dependencies": map[string]any{
				"removed": []map[string]string{{
					"depends_on": dependsOnID,
				}},
			},
		},
		After: map[string]any{},
	})
	return nil
}

func (t *eventTrackingTransaction) AddLabel(ctx context.Context, issueID, label, actor string) error {
	if err := t.Transaction.AddLabel(ctx, issueID, label, actor); err != nil {
		return err
	}
	after, _ := t.Transaction.GetIssue(ctx, issueID)
	var allLabels []string
	if after != nil {
		allLabels = after.Labels
	}
	t.queue(events.IssueLabelAdded, issuePartitionKey(issueID), events.IssueLabelAddedPayload{
		IssueID:   issueID,
		Label:     label,
		AllLabels: allLabels,
	})
	return nil
}

func (t *eventTrackingTransaction) RemoveLabel(ctx context.Context, issueID, label, actor string) error {
	if err := t.Transaction.RemoveLabel(ctx, issueID, label, actor); err != nil {
		return err
	}
	after, _ := t.Transaction.GetIssue(ctx, issueID)
	var allLabels []string
	if after != nil {
		allLabels = after.Labels
	}
	t.queue(events.IssueLabelRemoved, issuePartitionKey(issueID), events.IssueLabelRemovedPayload{
		IssueID:   issueID,
		Label:     label,
		AllLabels: allLabels,
	})
	return nil
}

func (t *eventTrackingTransaction) AddComment(ctx context.Context, issueID, actor, comment string) error {
	if err := t.Transaction.AddComment(ctx, issueID, actor, comment); err != nil {
		return err
	}
	// Transaction's AddComment doesn't return the comment id; fall back
	// to the empty string so consumers still get the routing kind.
	t.queue(events.IssueCommentAdded, issuePartitionKey(issueID), events.IssueCommentAddedPayload{
		IssueID:     issueID,
		CommentID:   "",
		AuthorActor: actor,
		BodyKind:    detectCommentBodyKind(comment),
	})
	return nil
}

func queueLabelDiff(t *eventTrackingTransaction, id string, before, after []string) {
	prev := make(map[string]struct{}, len(before))
	for _, l := range before {
		prev[l] = struct{}{}
	}
	next := make(map[string]struct{}, len(after))
	for _, l := range after {
		next[l] = struct{}{}
	}
	for l := range next {
		if _, kept := prev[l]; !kept {
			t.queue(events.IssueLabelAdded, issuePartitionKey(id), events.IssueLabelAddedPayload{
				IssueID:   id,
				Label:     l,
				AllLabels: after,
			})
		}
	}
	for l := range prev {
		if _, kept := next[l]; !kept {
			t.queue(events.IssueLabelRemoved, issuePartitionKey(id), events.IssueLabelRemovedPayload{
				IssueID:   id,
				Label:     l,
				AllLabels: after,
			})
		}
	}
}

// Ensure compile-time interface satisfaction.
var _ DoltStorage = (*EventEmittingStore)(nil)
var _ Transaction = (*eventTrackingTransaction)(nil)
