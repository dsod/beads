package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/steveyegge/beads/events"
	"github.com/steveyegge/beads/internal/types"
)

// eventSinkOnce + eventSink hold a process-wide sink for one CLI invocation.
// Constructed lazily on first emit; closed via closeEventSink in PersistentPostRun.
var (
	eventSinkOnce sync.Once
	eventSink     events.Sink
	eventSinkErr  error
	// eventCorrelationID groups all emissions from this invocation under a
	// single id so downstream consumers can stitch back the bundle.
	eventCorrelationID string
)

// getEventSink returns the cached sink for this CLI invocation. On first call
// it parses BEADS_EVENT_SINK; if unset, returns events.NoopSink with no error.
//
// The sink is process-wide: events emitted later in the run reuse the same
// Redis client and the same correlation id.
func getEventSink() (events.Sink, string) {
	eventSinkOnce.Do(func() {
		eventSink, eventSinkErr = events.NewSinkFromEnv()
		if eventSinkErr != nil {
			// Don't block the user — fall back to NoopSink but record the
			// error for one-time stderr surfacing.
			fmt.Fprintf(os.Stderr, "warning: BEADS_EVENT_SINK invalid (%v); events disabled\n", eventSinkErr)
			eventSink = events.NoopSink{}
		}
		// Generate a correlation id for the whole CLI invocation.
		eventCorrelationID = generateCorrelationID()
	})
	return eventSink, eventCorrelationID
}

// closeEventSink releases the underlying Redis client. Safe to call from
// PersistentPostRun even when emit was never invoked.
func closeEventSink() {
	if eventSink != nil {
		_ = eventSink.Close()
	}
}

// resetEventSinkForTest reinitialises the package-level sink state. Tests use
// this between subtests because the sync.Once would otherwise cache the first
// test's sink decision across the whole package.
func resetEventSinkForTest() {
	if eventSink != nil {
		_ = eventSink.Close()
	}
	eventSinkOnce = sync.Once{}
	eventSink = nil
	eventSinkErr = nil
	eventCorrelationID = ""
}

// generateCorrelationID returns a ULID-shaped id. We can't import events.newULID
// (unexported), so we just create a throwaway envelope and steal its
// correlation id — this keeps the helper free of redundant ulid wiring.
func generateCorrelationID() string {
	env := events.NewEnvelope(events.EventType("internal.correlation_seed"), "internal", nil)
	return env.CorrelationID
}

// emitEvent constructs an envelope and emits it. Failures are logged at debug
// level and never block the CLI — events are best-effort.
//
// Every emission inherits the run-wide correlation id so consumers can stitch
// the bundle from one CLI invocation back together.
func emitEvent(ctx context.Context, eventType events.EventType, partitionKey string, payload any) {
	sink, correlationID := getEventSink()
	if _, isNoop := sink.(events.NoopSink); isNoop {
		return
	}
	env := events.NewEnvelope(eventType, partitionKey, payload, events.WithCorrelation(correlationID))
	if err := sink.Emit(ctx, env); err != nil {
		fmt.Fprintf(os.Stderr, "warning: event emit failed (%s): %v\n", eventType, err)
	}
}

// detectCommentBodyKind classifies a comment body into one of the four buckets
// used by IssueCommentAddedPayload.BodyKind. Cheap heuristics — the goal is
// routing, not parsing.
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

// issuePartition returns the canonical partition key for an issue event.
func issuePartition(id string) string {
	return "issue:" + id
}

// emitUpdateEvents fans out the events corresponding to one issue's update bundle.
//
//   - When claim succeeded: emit issue.claimed + (since claim transitions to
//     in_progress) issue.status_changed.
//   - When regularUpdates includes "status": emit issue.status_changed.
//   - For each addLabels entry: issue.label_added. For each removeLabels entry:
//     issue.label_removed. setLabels are treated as a remove-then-add bundle.
//   - Finally, emit issue.updated covering all changed fields (status included
//     for reconstructability; the dedicated event is for routing convenience,
//     not exclusion).
//
// before is the pre-update issue snapshot; after is the post-update re-fetch.
// Either may be nil — guards everywhere.
func emitUpdateEvents(
	ctx context.Context,
	resolvedID string,
	before, after *types.Issue,
	regularUpdates map[string]interface{},
	addLabels, removeLabels, setLabels []string,
	claimFlag bool,
) {
	partition := issuePartition(resolvedID)
	actorID := getActorWithGit()

	// Claim event: only fires when --claim was set (the storage layer would
	// have errored before reaching here if the claim failed).
	if claimFlag {
		emitEvent(ctx, events.IssueClaimed, partition, events.IssueClaimedPayload{
			IssueID:        resolvedID,
			ClaimedByActor: actorID,
			// bd has no native lease TTL; 24h is a placeholder for consumers
			// that need an upper bound. See events/types.go IssueClaimedPayload.
			LeaseExpiresAt: time.Now().UTC().Add(24 * time.Hour),
		})
		// A successful claim transitions open→in_progress. Emit the dedicated
		// status_changed event for downstream routers.
		fromStatus := ""
		if before != nil {
			fromStatus = string(before.Status)
		}
		claimedBy := actorID
		emitEvent(ctx, events.IssueStatusChanged, partition, events.IssueStatusChangedPayload{
			IssueID:   resolvedID,
			From:      fromStatus,
			To:        string(types.StatusInProgress),
			ClaimedBy: &claimedBy,
		})
	}

	// Explicit status updates via --status.
	if newStatus, ok := regularUpdates["status"].(string); ok && !claimFlag {
		fromStatus := ""
		if before != nil {
			fromStatus = string(before.Status)
		}
		emitEvent(ctx, events.IssueStatusChanged, partition, events.IssueStatusChangedPayload{
			IssueID: resolvedID,
			From:    fromStatus,
			To:      newStatus,
		})
	}

	// Final label set: prefer the actually-stored set when available.
	var finalLabels []string
	if after != nil {
		finalLabels = after.Labels
	}

	// label_added: from --add-label or --set-labels (new entries only).
	for _, lbl := range addLabels {
		emitEvent(ctx, events.IssueLabelAdded, partition, events.IssueLabelAddedPayload{
			IssueID:   resolvedID,
			Label:     lbl,
			AllLabels: finalLabels,
		})
	}
	// label_removed: from --remove-label.
	for _, lbl := range removeLabels {
		emitEvent(ctx, events.IssueLabelRemoved, partition, events.IssueLabelRemovedPayload{
			IssueID:   resolvedID,
			Label:     lbl,
			AllLabels: finalLabels,
		})
	}
	// --set-labels: emit one label_added per entry not previously present and
	// one label_removed per entry that disappeared. Without the previous label
	// set we can't be precise, so when before is nil we emit an
	// issue.label_added per setLabel as a coarse approximation.
	if len(setLabels) > 0 {
		var prevSet map[string]struct{}
		if before != nil {
			prevSet = make(map[string]struct{}, len(before.Labels))
			for _, l := range before.Labels {
				prevSet[l] = struct{}{}
			}
		}
		nextSet := make(map[string]struct{}, len(setLabels))
		for _, l := range setLabels {
			nextSet[l] = struct{}{}
			if _, had := prevSet[l]; !had {
				emitEvent(ctx, events.IssueLabelAdded, partition, events.IssueLabelAddedPayload{
					IssueID:   resolvedID,
					Label:     l,
					AllLabels: finalLabels,
				})
			}
		}
		for l := range prevSet {
			if _, kept := nextSet[l]; !kept {
				emitEvent(ctx, events.IssueLabelRemoved, partition, events.IssueLabelRemovedPayload{
					IssueID:   resolvedID,
					Label:     l,
					AllLabels: finalLabels,
				})
			}
		}
	}

	// issue.updated: catch-all summarizing the diff. Skipped when nothing
	// substantive changed (e.g. a bare --claim with no other updates) since
	// the dedicated event already covers it.
	changedFields := make([]string, 0, len(regularUpdates))
	for k := range regularUpdates {
		changedFields = append(changedFields, k)
	}
	if len(changedFields) == 0 {
		return
	}
	beforeMap := map[string]any{}
	afterMap := map[string]any{}
	if before != nil {
		populateIssueFieldMap(beforeMap, before, changedFields)
	}
	if after != nil {
		populateIssueFieldMap(afterMap, after, changedFields)
	}
	emitEvent(ctx, events.IssueUpdated, partition, events.IssueUpdatedPayload{
		IssueID:       resolvedID,
		ChangedFields: changedFields,
		Before:        beforeMap,
		After:         afterMap,
	})
}

// populateIssueFieldMap copies the named fields from issue into dest. Used to
// build the before/after maps in IssueUpdatedPayload without dumping the whole
// struct.
func populateIssueFieldMap(dest map[string]any, issue *types.Issue, fields []string) {
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
		default:
			// Fall through silently — non-load-bearing fields don't need
			// per-case extraction.
		}
	}
}
