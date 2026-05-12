package events

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Replay reads events from Redis Streams starting at fromEventID and re-emits
// them to dest. Each shard is walked with XRANGE; events are visited in
// per-shard order. Cross-shard ordering is NOT guaranteed — consumers needing
// global ordering should sort by EventID (ULIDs are lexicographically sortable
// by time).
//
// fromEventID is interpreted as a Redis stream id ("123-0") OR the literal "-"
// meaning "from the start of every shard". Returns the count of events emitted.
//
// dest receives every envelope, including the source's own. Pointing dest back
// at the same source will produce an infinite loop — callers are expected to
// route to a distinct sink (or a NoopSink for dry-run).
func Replay(ctx context.Context, source *RedisSink, fromEventID string, dest Sink) (int, error) {
	if source == nil {
		return 0, fmt.Errorf("nil source sink")
	}
	if dest == nil {
		return 0, fmt.Errorf("nil destination sink")
	}
	if fromEventID == "" {
		fromEventID = "-"
	}

	client := source.Client()
	total := 0
	for _, stream := range source.Streams() {
		msgs, err := client.XRange(ctx, stream, fromEventID, "+").Result()
		if err != nil {
			return total, fmt.Errorf("XRANGE %s: %w", stream, err)
		}
		for _, msg := range msgs {
			env, err := EnvelopeFromXMessage(msg)
			if err != nil {
				return total, fmt.Errorf("decode %s/%s: %w", stream, msg.ID, err)
			}
			if err := dest.Emit(ctx, env); err != nil {
				return total, fmt.Errorf("emit %s/%s: %w", stream, msg.ID, err)
			}
			total++
		}
	}
	return total, nil
}

// Tail blocks reading new messages from every shard, invoking handler for each.
// Returns when ctx is canceled, when handler returns a non-nil error, or when
// the underlying XREAD fails irrecoverably. Used by `bd events tail`.
//
// Tail uses XREAD BLOCK with a 5s timeout so context cancellation is observed
// promptly without polling tighter than necessary.
func Tail(ctx context.Context, source *RedisSink, handler func(*EventEnvelope) error) error {
	if source == nil {
		return fmt.Errorf("nil source sink")
	}
	if handler == nil {
		return fmt.Errorf("nil handler")
	}
	client := source.Client()
	streams := source.Streams()
	// Start tailing at "$" — only events emitted after this call.
	lastIDs := make(map[string]string, len(streams))
	for _, s := range streams {
		lastIDs[s] = "$"
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		args := make([]string, 0, len(streams)*2)
		args = append(args, streams...)
		for _, s := range streams {
			args = append(args, lastIDs[s])
		}
		res, err := client.XRead(ctx, &redis.XReadArgs{
			Streams: args,
			Block:   5_000_000_000, // 5s in nanoseconds
			Count:   100,
		}).Result()
		if err != nil {
			if err == redis.Nil {
				continue
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("XREAD: %w", err)
		}
		for _, stream := range res {
			for _, msg := range stream.Messages {
				env, decErr := EnvelopeFromXMessage(msg)
				if decErr != nil {
					return fmt.Errorf("decode %s/%s: %w", stream.Stream, msg.ID, decErr)
				}
				if hErr := handler(env); hErr != nil {
					return hErr
				}
				lastIDs[stream.Stream] = msg.ID
			}
		}
	}
}
