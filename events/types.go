// Package events emits structured envelopes to Redis Streams when state-mutating
// bd CLI handlers run. When BEADS_EVENT_SINK is unset, all emission is a no-op.
//
// The package is intentionally separate from internal/audit (which writes to a
// local audit log) and from internal/storage hooks (which fire after the SQL
// transaction). Events here are a best-effort outbound notification stream for
// downstream consumers (orchestrators, dashboards, replay tooling); they do not
// gate or replace existing audit trails.
package events

import "time"

// EventType identifies the kind of state change being emitted. The taxonomy is
// intentionally narrow — only issue-lifecycle events live here. PR / role /
// epic events come from other systems.
type EventType string

const (
	IssueCreated       EventType = "issue.created"
	IssueUpdated       EventType = "issue.updated"
	IssueLabelAdded    EventType = "issue.label_added"
	IssueLabelRemoved  EventType = "issue.label_removed"
	IssueStatusChanged EventType = "issue.status_changed"
	IssueClaimed       EventType = "issue.claimed"
	IssueClosed        EventType = "issue.closed"
	IssueCommentAdded  EventType = "issue.comment_added"
)

// SchemaVersion is incremented when the envelope shape or any payload shape
// changes in a non-backward-compatible way.
const SchemaVersion = 1

// IssueCreatedPayload is emitted after a new issue is persisted. The CreatedByActor
// duplicates Envelope.Actor.ID for consumers that only inspect the payload.
type IssueCreatedPayload struct {
	IssueID        string   `json:"issue_id"`
	Type           string   `json:"type"`
	Title          string   `json:"title"`
	Labels         []string `json:"labels"`
	ParentEpicID   *string  `json:"parent_epic_id,omitempty"`
	CreatedByActor string   `json:"created_by_actor"`
}

// IssueUpdatedPayload is emitted on any field-level update. ChangedFields lists
// the field names that mutated; Before/After hold the values for each. Fields
// vary across updates, so map[string]any is the honest representation here —
// modeling every union of update shapes would be churn for zero consumer value.
type IssueUpdatedPayload struct {
	IssueID       string         `json:"issue_id"`
	ChangedFields []string       `json:"changed_fields"`
	Before        map[string]any `json:"before"`
	After         map[string]any `json:"after"`
}

// IssueLabelAddedPayload reports a single label-add. AllLabels is the full set
// after the change, so consumers can re-materialize state without a follow-up
// query.
type IssueLabelAddedPayload struct {
	IssueID   string   `json:"issue_id"`
	Label     string   `json:"label"`
	AllLabels []string `json:"all_labels"`
}

// IssueLabelRemovedPayload mirrors IssueLabelAddedPayload for removals.
type IssueLabelRemovedPayload struct {
	IssueID   string   `json:"issue_id"`
	Label     string   `json:"label"`
	AllLabels []string `json:"all_labels"`
}

// IssueStatusChangedPayload is emitted whenever issues.status transitions. The
// ClaimedBy pointer captures who claimed the issue when To == "in_progress" via
// a claim, allowing downstream lease-trackers to attribute the transition.
type IssueStatusChangedPayload struct {
	IssueID   string  `json:"issue_id"`
	From      string  `json:"from"`
	To        string  `json:"to"`
	ClaimedBy *string `json:"claimed_by,omitempty"`
}

// IssueClaimedPayload is emitted alongside IssueStatusChanged when a claim
// happens. LeaseExpiresAt is synthesized at emit time (bd has no native lease
// TTL today); consumers that genuinely need a server-authoritative expiry
// should ignore this field until the storage layer grows one.
type IssueClaimedPayload struct {
	IssueID        string    `json:"issue_id"`
	ClaimedByActor string    `json:"claimed_by_actor"`
	LeaseExpiresAt time.Time `json:"lease_expires_at"`
}

// IssueClosedPayload is emitted after a successful close. Reason mirrors the
// CLI's --reason flag (or "Closed" when unset).
type IssueClosedPayload struct {
	IssueID       string `json:"issue_id"`
	Reason        string `json:"reason"`
	ClosedByActor string `json:"closed_by_actor"`
}

// IssueCommentAddedPayload reports a new comment. BodyKind is heuristic:
//
//   - "envelope" when the body starts with "[role:" (agent handoff envelope)
//   - "plan" when the body contains a "## Plan" header
//   - "review" when the body contains a "## Review" header
//   - "other" otherwise
//
// The kind is detected by the emit site so consumers can route without
// re-parsing the body.
type IssueCommentAddedPayload struct {
	IssueID     string `json:"issue_id"`
	CommentID   string `json:"comment_id"`
	AuthorActor string `json:"author_actor"`
	BodyKind    string `json:"body_kind"`
}
