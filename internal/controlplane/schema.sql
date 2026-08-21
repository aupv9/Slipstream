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
