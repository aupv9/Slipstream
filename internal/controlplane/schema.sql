-- Slipstream control plane.
--
-- Three tables, one small Postgres. No etcd, no Zookeeper: leader election is
-- a single atomic UPDATE against `leases`, which a Postgres transaction already
-- serializes for us.

CREATE TABLE IF NOT EXISTS offsets (
    pipeline_id text        PRIMARY KEY,
    position    text        NOT NULL,
    commit_ts   timestamptz,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE offsets IS
    'Safe resume point per pipeline: the position every configured sink has already accepted.';

CREATE TABLE IF NOT EXISTS leases (
    pipeline_id text        PRIMARY KEY,
    holder      text        NOT NULL DEFAULT '',
    expires_at  timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE leases IS
    'Leader election. Exactly one instance per pipeline may hold an unexpired lease, so exactly one reader is attached to the source.';

CREATE TABLE IF NOT EXISTS sink_cursor (
    pipeline_id text        NOT NULL,
    sink_name   text        NOT NULL,
    position    text        NOT NULL,
    seq         bigint      NOT NULL DEFAULT 0,
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_id, sink_name)
);

COMMENT ON TABLE sink_cursor IS
    'Per-sink progress. Sinks advance independently; the slowest one defines the pipeline offset, so a slow sink never causes data loss and never blocks a fast one beyond its queue.';

CREATE TABLE IF NOT EXISTS snapshot_state (
    pipeline_id    text        PRIMARY KEY,
    phase          text        NOT NULL CHECK (phase IN ('running', 'done')),
    slot_name      text        NOT NULL DEFAULT '',
    consistent_lsn text        NOT NULL DEFAULT '',
    started_at     timestamptz NOT NULL DEFAULT now(),
    completed_at   timestamptz
);

-- Added after the first release; idempotent so upgrades need no separate step.
ALTER TABLE snapshot_state ADD COLUMN IF NOT EXISTS mode text NOT NULL DEFAULT 'single';
ALTER TABLE snapshot_state ADD COLUMN IF NOT EXISTS chunk_table text NOT NULL DEFAULT '';
ALTER TABLE snapshot_state ADD COLUMN IF NOT EXISTS chunk_key jsonb;

COMMENT ON TABLE snapshot_state IS
    'Whether the initial snapshot finished, how it was taken, and how far a chunked one got. A single-transaction snapshot interrupted midway leaves an offset covering only part of the data, so it must never be resumed from; a chunked snapshot streams from the start and is resumable, which is what chunk_table and chunk_key record.';

CREATE TABLE IF NOT EXISTS dead_letters (
    id          bigserial   PRIMARY KEY,
    pipeline_id text        NOT NULL,
    sink_name   text        NOT NULL,
    position    text        NOT NULL,
    attempts    integer     NOT NULL,
    error       text        NOT NULL,
    event       jsonb       NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE dead_letters IS
    'Events a sink rejected up to its attempt limit, parked here so one poison event cannot stall the pipeline forever. Only written by sinks configured with on_failure = dead_letter.';

CREATE INDEX IF NOT EXISTS dead_letters_pipeline_idx ON dead_letters (pipeline_id, created_at DESC);
