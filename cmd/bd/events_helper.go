package main

import (
	"fmt"
	"os"
	"sync"

	"github.com/steveyegge/beads/events"
	"github.com/steveyegge/beads/internal/storage"
)

// eventSinkOnce + eventSink hold a process-wide sink for one CLI invocation.
// Constructed lazily on first wrap; closed via closeEventSink in
// PersistentPostRun.
var (
	eventSinkOnce sync.Once
	eventSink     events.Sink
	eventSinkErr  error
	// eventCorrelationID groups every emission from this invocation under a
	// single id so downstream consumers can stitch the bundle back together.
	eventCorrelationID string
)

// getEventSink returns the cached sink for this CLI invocation. First call
// parses BEADS_EVENT_SINK; if unset, returns events.NoopSink with no error.
//
// The sink is process-wide: every wrapped store in this run shares the same
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
// (unexported), so we create a throwaway envelope and reuse its correlation id —
// keeps the helper free of redundant ulid wiring.
func generateCorrelationID() string {
	env := events.NewEnvelope(events.EventType("internal.correlation_seed"), "internal", nil)
	return env.CorrelationID
}

// wrapStoreWithEvents wraps the given store with storage.EventEmittingStore
// when BEADS_EVENT_SINK is configured (Redis), or returns store unchanged
// otherwise. The wrapped store emits events.* envelopes after every successful
// mutation, including paths that bypass CLI handlers (auto-close molecule,
// bulk dep transactions, routed stores opened by routing.DetectUserRole).
//
// Multi-rig consumers get correct routing for free: each rig's store is
// opened via newDoltStoreFromConfig and wrapped here, so emissions land on
// whichever sink the rig's process is configured for.
func wrapStoreWithEvents(s storage.DoltStorage) storage.DoltStorage {
	if s == nil {
		return s
	}
	sink, corrID := getEventSink()
	if _, isNoop := sink.(events.NoopSink); isNoop {
		return s
	}
	return storage.NewEventEmittingStore(s, sink, corrID)
}
