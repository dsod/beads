package events

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDedupKeyDeterministic(t *testing.T) {
	payload := IssueCreatedPayload{
		IssueID:        "bd-abc",
		Type:           "task",
		Title:          "test",
		Labels:         []string{"a", "b"},
		CreatedByActor: "alice",
	}
	a := DedupKey(IssueCreated, "issue:bd-abc", payload)
	b := DedupKey(IssueCreated, "issue:bd-abc", payload)
	assert.Equal(t, a, b)
	assert.True(t, strings.HasPrefix(a, "issue.created:issue:bd-abc:"))
}

func TestDedupKeyDiffersByEventType(t *testing.T) {
	payload := IssueCreatedPayload{IssueID: "bd-abc", Title: "x"}
	a := DedupKey(IssueCreated, "issue:bd-abc", payload)
	b := DedupKey(IssueUpdated, "issue:bd-abc", payload)
	assert.NotEqual(t, a, b)
}

func TestDedupKeyDiffersByPartition(t *testing.T) {
	payload := IssueCreatedPayload{Title: "x"}
	a := DedupKey(IssueCreated, "issue:bd-abc", payload)
	b := DedupKey(IssueCreated, "issue:bd-xyz", payload)
	assert.NotEqual(t, a, b)
}

func TestDedupKeyDiffersByPayload(t *testing.T) {
	a := DedupKey(IssueCreated, "issue:bd-abc", IssueCreatedPayload{Title: "x"})
	b := DedupKey(IssueCreated, "issue:bd-abc", IssueCreatedPayload{Title: "y"})
	assert.NotEqual(t, a, b)
}

func TestDedupKeyStableAcrossMapOrdering(t *testing.T) {
	// map[string]any uses Go's randomised iteration order. canonicalJSON must
	// neutralise that, or two semantically identical updates would produce
	// different keys.
	p1 := IssueUpdatedPayload{
		IssueID:       "bd-abc",
		ChangedFields: []string{"status", "priority"},
		Before:        map[string]any{"status": "open", "priority": 2},
		After:         map[string]any{"status": "in_progress", "priority": 1},
	}
	p2 := IssueUpdatedPayload{
		IssueID:       "bd-abc",
		ChangedFields: []string{"status", "priority"},
		Before:        map[string]any{"priority": 2, "status": "open"},
		After:         map[string]any{"priority": 1, "status": "in_progress"},
	}
	a := DedupKey(IssueUpdated, "issue:bd-abc", p1)
	b := DedupKey(IssueUpdated, "issue:bd-abc", p2)
	assert.Equal(t, a, b)
}
