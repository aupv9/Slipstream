# Slipstream

Change data capture that stays small and stays correct. One static Go binary,
one small Postgres for coordination. No Kafka, no Zookeeper or etcd, no JVM,
no Debezium.

Sources: **PostgreSQL** (implemented), **MySQL** and **MongoDB** (interfaces in
place, readers pending — see [Status](#status)).
Sinks: generic through an in-process Go interface, with a webhook, a
Postgres/warehouse upsert writer and stdout built in.

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
| **Nothing is dropped** | Failed writes retry with capped backoff, forever. A broken sink stalls loudly instead of losing data quietly. |
| **A stale leader cannot rewind progress** | Offset and cursor writes are fenced on still holding the lease; a zombie leader gets `ErrLeaseLost` and stops. |
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

For MySQL the equivalent will be reading `@@GLOBAL.gtid_executed` as the first
statement of a `REPEATABLE READ` transaction and streaming from that GTID set;
for MongoDB, recording `clusterTime` before the snapshot and opening the change
stream with `startAtOperationTime`. Both are documented in their packages.

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

**Failover replay.** After a crash the successor resumes from the stored
offset, which is the slowest sink's committed position — so the last few events
before the crash are delivered again. That is the at-least-once window, and it
is exactly why sinks must be idempotent.

**Unchanged TOASTed values.** Postgres omits large unchanged column values from
the WAL record. Slipstream omits those keys from the event rather than sending
`null`, and the upsert sink updates only the columns present, so a wide row is
never blanked by an update that did not touch it.

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
- **Renaming `pipeline.id` or `source.id`** starts a new slot and a new dedupe
  key space: everything is re-delivered.

## Status

| Piece | State |
|---|---|
| Control plane: offsets, leases, per-sink cursors, lease fencing | done, integration-tested |
| Leader/standby runner with heartbeat and handover | done, failover-tested |
| Postgres reader: consistent snapshot + logical streaming + WAL ack | done, integration-tested |
| Sink router: independent queues, cursors, retry, slowest-sink offset | done, unit-tested |
| Sinks: webhook, pgupsert, stdout | done |
| MySQL reader (binlog + GTID) | interface in place, reader pending |
| MongoDB reader (change stream) | interface in place, reader pending |

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

End-to-end failover, including a mid-stream `SIGKILL` of the leader and a
row-for-row comparison of source and mirror afterwards:

```bash
CP_DSN=... SRC_DSN=... MIRROR_DSN=... scripts/verify-failover.sh
```

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
