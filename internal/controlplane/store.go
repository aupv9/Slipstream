// Package controlplane is the only stateful dependency Slipstream has: one
// small Postgres holding offsets, leases and per-sink cursors.
package controlplane

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrLeaseLost is returned by writes that are fenced because the caller is no
// longer the pipeline's leader. A leader that was paused long enough for its
// lease to expire must never overwrite the progress of its successor.
var ErrLeaseLost = errors.New("controlplane: lease no longer held")

// Store wraps the control-plane connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// Open connects to the control-plane database.
func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("controlplane: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("controlplane: ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }

// Migrate creates the control-plane schema if it is missing. The DDL is
// idempotent, so every instance can run it on startup.
func (s *Store) Migrate(ctx context.Context, ddl string) error {
	if _, err := s.pool.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("controlplane: migrate: %w", err)
	}
	return nil
}

// AcquireOrRenew tries to become (or stay) the leader of a pipeline. It is a
// single UPDATE, so two instances cannot both win: the row is locked by the
// first transaction to reach it, and the loser's WHERE clause no longer holds.
//
// It returns true when this instance holds the lease for another ttl.
func (s *Store) AcquireOrRenew(ctx context.Context, pipelineID, holder string, ttl time.Duration) (bool, error) {
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO leases (pipeline_id, holder, expires_at)
		 VALUES ($1, '', now())
		 ON CONFLICT (pipeline_id) DO NOTHING`, pipelineID); err != nil {
		return false, fmt.Errorf("controlplane: ensure lease row: %w", err)
	}

	tag, err := s.pool.Exec(ctx,
		`UPDATE leases
		    SET holder = $1, expires_at = now() + make_interval(secs => $3)
		  WHERE pipeline_id = $2
		    AND (holder = $1 OR expires_at < now())`,
		holder, pipelineID, ttl.Seconds())
	if err != nil {
		return false, fmt.Errorf("controlplane: acquire lease: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// Release expires our own lease so a standby can take over immediately instead
// of waiting out the TTL. Best effort: a failure here only costs one TTL.
func (s *Store) Release(ctx context.Context, pipelineID, holder string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE leases SET expires_at = now() WHERE pipeline_id = $1 AND holder = $2`,
		pipelineID, holder)
	if err != nil {
		return fmt.Errorf("controlplane: release lease: %w", err)
	}
	return nil
}

// LeaseHolder reports who currently holds an unexpired lease, if anyone.
func (s *Store) LeaseHolder(ctx context.Context, pipelineID string) (string, error) {
	var holder string
	err := s.pool.QueryRow(ctx,
		`SELECT holder FROM leases WHERE pipeline_id = $1 AND expires_at > now()`,
		pipelineID).Scan(&holder)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("controlplane: read lease: %w", err)
	}
	return holder, nil
}

// LoadOffset returns the pipeline's resume position. The bool is false when no
// offset has been stored yet, which is the reader's signal to take an initial
// snapshot.
func (s *Store) LoadOffset(ctx context.Context, pipelineID string) (string, bool, error) {
	var pos string
	err := s.pool.QueryRow(ctx,
		`SELECT position FROM offsets WHERE pipeline_id = $1`, pipelineID).Scan(&pos)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("controlplane: load offset: %w", err)
	}
	return pos, true, nil
}

// SaveOffset stores the pipeline resume point, fenced on still holding the
// lease. Returns ErrLeaseLost if we do not, which the caller must treat as
// "stop reading the source immediately".
func (s *Store) SaveOffset(ctx context.Context, pipelineID, holder, position string, commitTS time.Time) error {
	var ts any
	if !commitTS.IsZero() {
		ts = commitTS
	}
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO offsets (pipeline_id, position, commit_ts, updated_at)
		 SELECT $1, $3, $4, now()
		   FROM leases
		  WHERE pipeline_id = $1 AND holder = $2 AND expires_at > now()
		 ON CONFLICT (pipeline_id) DO UPDATE
		    SET position = excluded.position,
		        commit_ts = excluded.commit_ts,
		        updated_at = now()`,
		pipelineID, holder, position, ts)
	if err != nil {
		return fmt.Errorf("controlplane: save offset: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// SinkCursor is one sink's progress within a pipeline.
type SinkCursor struct {
	Sink     string
	Position string
	Seq      int64
}

// LoadSinkCursors returns the stored progress of every sink of a pipeline.
func (s *Store) LoadSinkCursors(ctx context.Context, pipelineID string) ([]SinkCursor, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT sink_name, position, seq FROM sink_cursor WHERE pipeline_id = $1 ORDER BY sink_name`,
		pipelineID)
	if err != nil {
		return nil, fmt.Errorf("controlplane: load sink cursors: %w", err)
	}
	defer rows.Close()

	var out []SinkCursor
	for rows.Next() {
		var c SinkCursor
		if err := rows.Scan(&c.Sink, &c.Position, &c.Seq); err != nil {
			return nil, fmt.Errorf("controlplane: scan sink cursor: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// AdvanceSinkCursor records that a sink has durably accepted everything up to
// position. Also fenced on the lease.
func (s *Store) AdvanceSinkCursor(ctx context.Context, pipelineID, holder, sink, position string, seq int64) error {
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO sink_cursor (pipeline_id, sink_name, position, seq, updated_at)
		 SELECT $1, $3, $4, $5, now()
		   FROM leases
		  WHERE pipeline_id = $1 AND holder = $2 AND expires_at > now()
		 ON CONFLICT (pipeline_id, sink_name) DO UPDATE
		    SET position = excluded.position,
		        seq = excluded.seq,
		        updated_at = now()`,
		pipelineID, holder, sink, position, seq)
	if err != nil {
		return fmt.Errorf("controlplane: advance sink cursor: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Snapshot modes.
const (
	// SnapshotSingle reads every table inside one consistent transaction. It
	// is simple and exact, but an interrupted one cannot be resumed.
	SnapshotSingle = "single"
	// SnapshotChunked reads tables in key ranges interleaved with the stream,
	// so progress survives a crash and no long transaction is held open.
	SnapshotChunked = "chunked"
)

// Snapshot phases.
const (
	// SnapshotRunning means an initial snapshot started but has not finished.
	SnapshotRunning = "running"
	// SnapshotDone means the initial snapshot completed, so the stored offset
	// covers the whole dataset and can be resumed from.
	SnapshotDone = "done"
)

// BeginSnapshot records that an initial snapshot is starting. Any previous
// state for the pipeline is overwritten, because a new bootstrap supersedes it.
func (s *Store) BeginSnapshot(ctx context.Context, pipelineID, holder, slot, mode string) error {
	if mode == "" {
		mode = SnapshotSingle
	}
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO snapshot_state (pipeline_id, phase, slot_name, mode, started_at, completed_at, consistent_lsn, chunk_table, chunk_key)
		 SELECT $1, 'running', $3, $4, now(), NULL, '', '', NULL
		   FROM leases
		  WHERE pipeline_id = $1 AND holder = $2 AND expires_at > now()
		 ON CONFLICT (pipeline_id) DO UPDATE
		    SET phase = 'running',
		        slot_name = excluded.slot_name,
		        mode = excluded.mode,
		        started_at = now(),
		        completed_at = NULL,
		        consistent_lsn = '',
		        chunk_table = '',
		        chunk_key = NULL`,
		pipelineID, holder, slot, mode)
	if err != nil {
		return fmt.Errorf("controlplane: begin snapshot: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// SaveChunkProgress records how far a chunked snapshot has read. This is what
// lets an interrupted chunked snapshot pick up where it left off instead of
// starting over.
func (s *Store) SaveChunkProgress(ctx context.Context, pipelineID, holder, table string, key []byte) error {
	var keyArg any
	if len(key) > 0 {
		keyArg = string(key)
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE snapshot_state
		    SET chunk_table = $3, chunk_key = $4::jsonb
		  WHERE pipeline_id = $1
		    AND EXISTS (SELECT 1 FROM leases
		                 WHERE pipeline_id = $1 AND holder = $2 AND expires_at > now())`,
		pipelineID, holder, table, keyArg)
	if err != nil {
		return fmt.Errorf("controlplane: save chunk progress: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// CompleteSnapshot marks the snapshot finished. Only after this does the
// pipeline offset describe the complete dataset.
func (s *Store) CompleteSnapshot(ctx context.Context, pipelineID, holder, consistentLSN string) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE snapshot_state
		    SET phase = 'done', completed_at = now(), consistent_lsn = $3
		  WHERE pipeline_id = $1
		    AND EXISTS (SELECT 1 FROM leases
		                 WHERE pipeline_id = $1 AND holder = $2 AND expires_at > now())`,
		pipelineID, holder, consistentLSN)
	if err != nil {
		return fmt.Errorf("controlplane: complete snapshot: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// SnapshotState is what the control plane knows about a pipeline's snapshot.
type SnapshotState struct {
	Phase      string
	Mode       string
	ChunkTable string
	ChunkKey   []byte
}

// LoadSnapshotState reports the recorded snapshot state. found is false when no
// snapshot has ever been recorded for the pipeline.
func (s *Store) LoadSnapshotState(ctx context.Context, pipelineID string) (SnapshotState, bool, error) {
	var (
		st  SnapshotState
		key *string
	)
	err := s.pool.QueryRow(ctx,
		`SELECT phase, mode, chunk_table, chunk_key::text
		   FROM snapshot_state WHERE pipeline_id = $1`, pipelineID).
		Scan(&st.Phase, &st.Mode, &st.ChunkTable, &key)
	if errors.Is(err, pgx.ErrNoRows) {
		return SnapshotState{}, false, nil
	}
	if err != nil {
		return SnapshotState{}, false, fmt.Errorf("controlplane: read snapshot state: %w", err)
	}
	if key != nil {
		st.ChunkKey = []byte(*key)
	}
	return st, true, nil
}

// DeadLetter parks an event a sink kept rejecting. The pipeline advances past
// it, so this is the one place where an event is deliberately not delivered —
// it is only reachable for sinks explicitly configured with
// on_failure = dead_letter.
func (s *Store) DeadLetter(ctx context.Context, pipelineID, holder, sinkName, position string, attempts int, cause string, payload []byte) error {
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO dead_letters (pipeline_id, sink_name, position, attempts, error, event)
		 SELECT $1, $3, $4, $5, $6, $7::jsonb
		   FROM leases
		  WHERE pipeline_id = $1 AND holder = $2 AND expires_at > now()`,
		pipelineID, holder, sinkName, position, attempts, cause, string(payload))
	if err != nil {
		return fmt.Errorf("controlplane: dead letter: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}
