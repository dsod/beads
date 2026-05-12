---
id: events
title: bd events
slug: /cli-reference/events
sidebar_position: 999
---

<!-- AUTO-GENERATED: do not edit manually -->
Generated from `bd help --doc events`

## bd events

Read events emitted by bd to its Redis Streams sink.

The sink is configured via BEADS_EVENT_SINK. When unset, both subcommands
exit immediately with a hint — there's nothing to inspect.

```
bd events
```

### bd events replay

Read every shard via XRANGE starting at &lt;from-event-id&gt; ("-" for the
beginning) and print each envelope as one line of JSON to stdout.

Cross-shard ordering is NOT guaranteed; sort by event_id (ULIDs are
lexicographically sortable by time) if a global order is required.

```
bd events replay <from-event-id>
```

### bd events tail

Block on XREAD across every shard and print each new envelope as JSON.

Exits when interrupted (Ctrl-C / SIGTERM) or when the sink connection drops.

```
bd events tail
```
