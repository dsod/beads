package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/steveyegge/beads/events"
)

var eventsCmd = &cobra.Command{
	Use:   "events",
	Short: "Inspect the bd event stream (replay / tail)",
	Long: `Read events emitted by bd to its Redis Streams sink.

The sink is configured via BEADS_EVENT_SINK. When unset, both subcommands
exit immediately with a hint — there's nothing to inspect.`,
}

var eventsReplayCmd = &cobra.Command{
	Use:   "replay <from-event-id>",
	Short: "Print events from the configured sink starting at <from-event-id>",
	Long: `Read every shard via XRANGE starting at <from-event-id> ("-" for the
beginning) and print each envelope as one line of JSON to stdout.

Cross-shard ordering is NOT guaranteed; sort by event_id (ULIDs are
lexicographically sortable by time) if a global order is required.`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		from := "-"
		if len(args) == 1 {
			from = args[0]
		}
		source, err := requireRedisSink()
		if err != nil {
			return err
		}
		defer func() { _ = source.Close() }()

		out := newJSONLineSink(os.Stdout)
		n, err := events.Replay(cmd.Context(), source, from, out)
		if err != nil {
			return fmt.Errorf("replay: %w", err)
		}
		fmt.Fprintf(os.Stderr, "Replayed %d event(s)\n", n)
		return nil
	},
}

var eventsTailCmd = &cobra.Command{
	Use:   "tail",
	Short: "Follow the event sink, printing new envelopes as they arrive",
	Long: `Block on XREAD across every shard and print each new envelope as JSON.

Exits when interrupted (Ctrl-C / SIGTERM) or when the sink connection drops.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		source, err := requireRedisSink()
		if err != nil {
			return err
		}
		defer func() { _ = source.Close() }()

		ctx, cancel := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer cancel()

		err = events.Tail(ctx, source, func(env *events.EventEnvelope) error {
			return writeJSONLine(os.Stdout, env)
		})
		if err != nil && err != context.Canceled && ctx.Err() == nil {
			return fmt.Errorf("tail: %w", err)
		}
		return nil
	},
}

// requireRedisSink returns the configured Redis sink or an error if
// BEADS_EVENT_SINK is unset/invalid. Bypasses the cached process-wide sink so
// `bd events replay` runs in a fresh client without contention with the rest
// of the CLI.
func requireRedisSink() (*events.RedisSink, error) {
	url := os.Getenv("BEADS_EVENT_SINK")
	if url == "" {
		return nil, fmt.Errorf("BEADS_EVENT_SINK is not set; nothing to replay/tail")
	}
	sink, err := events.NewSink(url)
	if err != nil {
		return nil, err
	}
	rs, ok := sink.(*events.RedisSink)
	if !ok {
		return nil, fmt.Errorf("BEADS_EVENT_SINK did not yield a Redis sink (got %T)", sink)
	}
	return rs, nil
}

// jsonLineSink writes envelopes as newline-delimited JSON. It implements
// events.Sink so it can be the destination of Replay.
type jsonLineSink struct {
	out *os.File
}

func newJSONLineSink(out *os.File) *jsonLineSink { return &jsonLineSink{out: out} }

func (s *jsonLineSink) Emit(_ context.Context, env *events.EventEnvelope) error {
	return writeJSONLine(s.out, env)
}

func (s *jsonLineSink) Close() error { return nil }

func writeJSONLine(out *os.File, env *events.EventEnvelope) error {
	b, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}
	b = append(b, '\n')
	_, err = out.Write(b)
	return err
}

func init() {
	eventsCmd.AddCommand(eventsReplayCmd)
	eventsCmd.AddCommand(eventsTailCmd)
	rootCmd.AddCommand(eventsCmd)
}
