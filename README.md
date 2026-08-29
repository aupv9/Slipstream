# Slipstream

Change data capture that stays small and stays correct. One static Go binary,
one small Postgres for coordination. No Kafka, no Zookeeper or etcd, no JVM,
no Debezium.

Sources: **PostgreSQL**, **MySQL** and **MongoDB**.
Sinks: generic through an in-process Go interface, with a webhook, a
Postgres/warehouse upsert writer, NATS, Kafka and stdout built in.

## What "correct" means here

Delivery is **at-least-once with idempotent sinks** — deliberately, not as a
shortcut. Exactly-once end to end needs a transactional sink or two-phase
commit, and that complexity is the opposite of what this project is for.
Instead the guarantees are a handful of invariants that can each be tested:

| Invariant | How it is held |
|---|---|
| **One reader per source** | A pipeline runs on ≥2 instances but only the lease holder opens the replication slot / binlog / change stream. |
| **No two leaders** | Leader election is a single atomic `UPDATE` on the `leases` row; Postgres serializes it, so a second winner is impossible. |
| **Per-row order preserved** | One active reader, events emitted in source-log order, no parallelism inside a pipeline. |
| **Snapshot and stream do not overlap or gap** | The initial snapshot runs inside the exact MVCC snapshot exported by the replication slot, so every row is either snapshotted or streamed — never both, never neither. |
| **Replays cannot corrupt** | Every event carries a `position`; sinks either upsert on the primary key or dedupe on `(source_id, table, position)`. |
| **Nothing is dropped** | Failed writes retry with capped backoff, forever by default. A sink may opt into dead-lettering, which parks what it cannot deliver in the control plane rather than discarding it. |
| **TRUNCATE is not silently lost** | A truncate is delivered as its own event; sinks clear the target instead of keeping rows the source no longer has. |
| **A table cannot be half-configured** | A table listed in the config but missing from the publication stops the pipeline, instead of looking replicated while never being captured. |
| **A stale leader cannot rewind progress** | Offset and cursor writes are fenced on still holding the lease; a zombie leader gets `ErrLeaseLost` and stops. |
| **An interrupted snapshot is never mistaken for a complete one** | `snapshot_state` records the snapshot phase; an offset written mid-snapshot is discarded and the snapshot is taken again. |
| **The source only releases logs we truly have** | The position acknowledged back to the source is the *slowest* sink's, never the read-ahead position. |

## Architecture

```
 PostgreSQL / MySQL / MongoDB
 (logical replication / binlog+GTID / change stream)
              │
              ▼
   ┌──────────────────────┐   lease + offset   ┌──────────────────────────┐
   │  Instance — LEADER   │◄──────────────────►│  Control plane            │
   │  • source reader     │                    │  (one small Postgres)     │
   │  • sink router       │                    │   offsets                 │
   └──────────┬───────────┘                    │   leases                  │
              │ ChangeEvent                    │   sink_cursor             │
              ▼                                └────────────▲──────────────┘
   ┌───────────────────────────┐                            │ heartbeat
   │ Sinks (in-process, Go)    │              ┌─────────────┴────────────┐
   │ webhook / pgupsert / …    │              │ Instance — STANDBY        │
   └───────────────────────────┘              │ (watches the lease only)  │
                                              └───────────────────────────┘
```

Each sink has its own bounded queue and its own cursor, so a slow sink does not
hold up a fast one — it only applies backpressure once its queue fills. The
pipeline offset (the resume point, and what gets acknowledged to the source) is
the slowest sink's position.

## Quick start

```bash
# 1. A source with logical WAL and a replication-capable role.
#    postgresql.conf: wal_level = logical
psql "$SRC_DSN" -c "CREATE TABLE customers (id bigint PRIMARY KEY, name text)"

# 2. The control plane: any small Postgres of its own.
go build -o bin/slipstream ./cmd/slipstream
bin/slipstream migrate -config slipstream.yaml     # or: bin/slipstream schema

# 3. Run it. Start the same config on a second host for HA.
bin/slipstream run -config slipstream.yaml
```

Start from [`configs/slipstream.example.yaml`](configs/slipstream.example.yaml);
every field is documented there. `${ENV}` references are expanded on load, so
DSNs and tokens stay out of the file.

```
slipstream run     -config FILE [-log-level debug|info|warn|error] [-log-format text|json]
slipstream migrate -config FILE     # apply the control-plane schema
slipstream schema                   # print the DDL
slipstream version
```

## How the tricky parts work

**Initial snapshot.** This is where CDC implementations usually leak or
duplicate rows. Slipstream creates the replication slot first, with snapshot
export, which returns a snapshot name bound to the exact LSN where the slot
starts decoding. It then opens a `REPEATABLE READ READ ONLY` transaction, makes
`SET TRANSACTION SNAPSHOT` its first statement, and reads the tables inside
that view. Everything committed at or before that LSN is in the snapshot;
everything after arrives on the stream. No `FLUSH TABLES WITH READ LOCK`, no
guessing, no overlap window.

MySQL takes the same idea to a server with no exportable snapshot. It records
`@@GLOBAL.gtid_executed` **before** opening `START TRANSACTION WITH CONSISTENT
SNAPSHOT`, never after. A transaction committing between the two then shows up
in both the snapshot and the replayed stream — a duplicate, which idempotent
sinks absorb. The reverse order would produce a gap instead: a transaction
inside the recorded GTID set but not in the snapshot's view is skipped by the
stream and never delivered at all. Given a choice between duplicating and
losing, this design always duplicates, and that is also why it needs no `FLUSH
TABLES WITH READ LOCK`.

MongoDB does the same thing again: record the cluster time from the server,
snapshot the collections, then open the change stream at exactly that time with
`startAtOperationTime`. After the first event the driver's own resume token
takes over as the position, since that is the resume unit MongoDB itself
guarantees.

MySQL positions are GTID sets, never file+offset: a file name and byte offset
mean nothing after a rotation or a failover. Events are stamped with the set as
it stood *before* their own transaction, so a process that dies mid-transaction
resumes by replaying that transaction whole instead of skipping its remainder.

**Leader election.** One statement, no coordination service:

```sql
UPDATE leases
   SET holder = $1, expires_at = now() + make_interval(secs => $3)
 WHERE pipeline_id = $2
   AND (holder = $1 OR expires_at < now());
```

`RowsAffected() == 1` means you are the leader. The holder renews every
`lease_renew`; failover takes at most `lease_ttl`. On a clean stop the holder
expires its own lease so the standby starts immediately.

**Interrupted snapshots.** Sinks accept snapshot rows as they arrive, so the
pipeline offset advances to the slot's consistent point while the initial
snapshot is still running. An instance killed midway therefore leaves an offset
that *looks* perfectly resumable but covers only the rows already read.
Resuming from it would stream forward and never deliver the rest — a green
pipeline, permanently missing data. So `snapshot_state` records the phase, and a
run only trusts an offset once the snapshot is marked `done`; otherwise it drops
the leftover slot and snapshots again. A redundant snapshot costs time, a
skipped one costs data.

One residual gap to know about: if a row is deleted at the source between an
aborted snapshot and its replacement, the sink can keep a stale copy — the row
is in neither the new snapshot nor the new stream. For an append-mostly table
this is harmless; otherwise truncate the target before letting a re-bootstrap
run. The proper fix is chunked snapshots with watermarks, which is on the
roadmap, not in this version.

**Failover replay.** After a crash the successor resumes from the stored
offset, which is the slowest sink's committed position — so the last few events
before the crash are delivered again. That is the at-least-once window, and it
is exactly why sinks must be idempotent.

**Poison events.** By default a rejected batch is retried forever: nothing is
lost, but one event a sink will never accept (a column the target lacks, a row
it considers invalid) stalls that sink until someone intervenes — loudly, in the
logs. A sink can instead be given `on_failure: dead_letter` with
`max_attempts`, and then failures are isolated per event: the batch is retried
one event at a time, only the ones that still fail are written to
`dead_letters`, and the rest of the stream keeps moving. If that write fails
too, the run stops rather than advancing — losing the delivery and the record of
it at once would be silent data loss.

**Unchanged TOASTed values.** Postgres omits large unchanged column values from
the WAL record. Slipstream omits those keys from the event rather than sending
`null`, and the upsert sink updates only the columns present, so a wide row is
never blanked by an update that did not touch it.

## Watching it run

Set `observability.addr` and the process serves:

| Endpoint | What it is for |
|---|---|
| `/metrics` | Prometheus text format, no client library and no extra dependency. |
| `/healthz` | Liveness. Always 200 while the process is up. |
| `/readyz` | Readiness, plus the instance's role. A standby answers 200: it is *meant* to be idle, and taking it out of service would defeat the point of running one. |

The metric worth alerting on is `slipstream_source_lag_bytes`: how much log the
source is still holding because the slowest sink has not accepted it yet. It
comes from the server's own reported position versus what has been
acknowledged, so it costs no extra query. Also worth watching:
`slipstream_dead_lettered_total` (anything above zero means events were parked
rather than delivered), `slipstream_leader` (two instances reporting 1 for one
pipeline would mean split brain), and `slipstream_sink_queue_depth` (which sink
is applying the backpressure).

## Operating notes

- **A replication slot pins WAL.** If a pipeline stops for good, drop its slot
  (`SELECT pg_drop_replication_slot('...')`) or the source disk will fill.
  While it runs, WAL is released only up to what the slowest sink has accepted.
- **Lag** is visible in the control plane: compare `sink_cursor.position` per
  sink against `offsets.position` and the source's current LSN.
- **Deletes need a before image.** Set `REPLICA IDENTITY` (default is the
  primary key) so key columns are logged; the upsert sink reports a clear error
  if they are missing.
- **`pgupsert` requires configured keys** per table. Without them it refuses to
  write, because a keyless upsert duplicates rows on every replay.
- **Adding a table to a running pipeline** is refused by default: it cannot be
  snapshotted consistently against a slot that already exists. Give the table
  its own pipeline, re-bootstrap this one, or set `auto_add_tables: true` to
  accept streaming-only capture for it.
- **Check `dead_letters`** if a sink uses `on_failure: dead_letter`. Rows there
  are events that were deliberately not delivered.
- **Renaming `pipeline.id` or `source.id`** starts a new slot and a new dedupe
  key space: everything is re-delivered.
- **A pipeline that logs `bootstrapping from scratch`** is re-reading the whole
  source because its last snapshot never finished. Expect the initial load
  again; check `snapshot_state` for when it started and completed.

## Status

| Piece | State |
|---|---|
| Control plane: offsets, leases, per-sink cursors, lease fencing | done, integration-tested |
| Leader/standby runner with heartbeat and handover | done, failover-tested |
| Postgres reader: consistent snapshot + logical streaming + WAL ack | done, integration-tested |
| Snapshot crash safety (`snapshot_state`, forced re-bootstrap) | done, regression-tested |
| Sink router: independent queues, cursors, retry, slowest-sink offset | done, unit-tested |
| Sinks: webhook, pgupsert, NATS, Kafka, stdout | done; all but Kafka integration-tested against a real server |
| Metrics, health and readiness endpoints | done, tested |
| TRUNCATE propagation | done, integration-tested |
| Publication reconcile (refuse silent table drift) | done, integration-tested |
| Dead-letter queue with per-event isolation | done, unit-tested |
| MySQL reader (binlog + GTID, snapshot, DDL-aware schema cache) | done, integration-tested |
| MongoDB reader (change stream, snapshot, resume tokens) | done; mapping unit-tested locally, server tests run in CI |

Deliberately **not** built: exactly-once/2PC, parallel chunked snapshots for
very large tables (a single-transaction snapshot is enough well past most
workloads; revisit at tens of GB), out-of-process/multi-language sink plugins
(a stdio-JSON sink mode can be added if another language is ever needed), a
schema registry or Avro (JSON is easier to debug; Protobuf later if bandwidth
matters), and Oracle/SQL Server/Db2.

## Tests

```bash
make test           # unit tests; integration tests skip without DSNs

make test-integration \
  CP_DSN=postgres://postgres@127.0.0.1:5432/slipstream_cp \
  SRC_DSN=postgres://postgres@127.0.0.1:5432/app
```

The integration tests are the ones that matter, because they check the
guarantees against a real server:

- `internal/controlplane` — one winner out of 16 racing instances, takeover
  after expiry, and a stale leader being fenced out of both offsets and
  cursors.
- `internal/source/postgres` — 20 000 preloaded rows snapshotted while a writer
  keeps committing, asserting every committed row is delivered exactly once
  across the snapshot/stream boundary; plus resume-from-offset and
  update/delete images.

Two end-to-end checks run the real binary across a process kill:

```bash
# Leader SIGKILLed mid-stream: the standby takes over and the mirror ends up
# matching the source row for row.
CP_DSN=... SRC_DSN=... MIRROR_DSN=... scripts/verify-failover.sh

# Leader SIGKILLed mid-snapshot with a committed offset in place: the restart
# must re-snapshot and recover every row rather than streaming on from the
# partial offset.
CP_DSN=... SRC_DSN=... MIRROR_DSN=... scripts/verify-snapshot-crash.sh
```

CI (`.github/workflows/ci.yaml`) runs gofmt, vet, the unit tests, then the
integration tests and both scripts against a real PostgreSQL 16.

## Layout

```
cmd/slipstream          CLI: run, migrate, schema, version
internal/cdc            the normalized ChangeEvent
internal/config         YAML config, defaults, validation
internal/controlplane   offsets, leases, sink cursors, schema
internal/pipeline       leader/standby runner, sink router, factories
internal/source         Reader interface + postgres, mysql, mongo
internal/sink           Sink interface + webhook, pgupsert, stdout
deploy/                 docker compose stack, systemd unit
scripts/                failover verification
```

Adding a sink means implementing `sink.Sink` and one case in
`internal/pipeline/factory.go`; nothing in the read path changes. Adding a
source means implementing `source.Reader` the same way.
