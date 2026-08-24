// Package pgupsert mirrors rows into a Postgres (or Postgres-compatible
// warehouse) table by primary key.
//
// This sink is idempotent by construction: applying the same event twice
// produces the same row, so a replay after failover is harmless without any
// dedupe bookkeeping.
package pgupsert

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aupv9/slipstream/internal/cdc"
	"github.com/aupv9/slipstream/internal/config"
)

// Sink writes upserts and deletes to a target database.
type Sink struct {
	name string
	cfg  config.PGUpsertSink
	pool *pgxpool.Pool
}

// New connects to the target database.
func New(ctx context.Context, name string, cfg config.PGUpsertSink) (*Sink, error) {
	if cfg.DSN == "" {
		return nil, fmt.Errorf("pgupsert sink %q: dsn is required", name)
	}
	pool, err := pgxpool.New(ctx, cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("pgupsert sink %q: connect: %w", name, err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pgupsert sink %q: ping: %w", name, err)
	}
	return &Sink{name: name, cfg: cfg, pool: pool}, nil
}

// Name identifies the sink.
func (s *Sink) Name() string { return s.name }

// Close releases the pool.
func (s *Sink) Close() error {
	s.pool.Close()
	return nil
}

// Write applies the batch in one transaction, one statement per event in
// source order. Statements are pipelined with pgx.Batch, so the batch costs a
// single round trip while still being applied in order — per-row ordering is
// what keeps the mirrored row correct when the same row changes twice inside
// one batch.
func (s *Sink) Write(ctx context.Context, batch []cdc.ChangeEvent) error {
	if len(batch) == 0 {
		return nil
	}

	pgBatch := &pgx.Batch{}
	for _, ev := range batch {
		sql, args, err := s.statement(ev)
		if err != nil {
			return err
		}
		pgBatch.Queue(sql, args...)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgupsert sink %q: begin: %w", s.name, err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	if err := tx.SendBatch(ctx, pgBatch).Close(); err != nil {
		return fmt.Errorf("pgupsert sink %q: apply batch: %w", s.name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pgupsert sink %q: commit: %w", s.name, err)
	}
	return nil
}

func (s *Sink) statement(ev cdc.ChangeEvent) (string, []any, error) {
	target := s.target(ev)

	// TRUNCATE needs no key: every row goes. DELETE rather than TRUNCATE so it
	// composes with the rest of the batch in one transaction, and so it is
	// harmless to replay.
	if ev.Op == cdc.OpTruncate {
		if s.cfg.SoftDelete {
			return fmt.Sprintf("UPDATE %s SET %s = now() WHERE %s IS NULL",
				target, quoteIdent(s.cfg.DeletedCol), quoteIdent(s.cfg.DeletedCol)), nil, nil
		}
		return "DELETE FROM " + target, nil, nil
	}

	keys, err := s.keysFor(ev)
	if err != nil {
		return "", nil, err
	}
	if ev.Op == cdc.OpDelete {
		return s.deleteStatement(ev, target, keys)
	}
	return s.upsertStatement(ev, target, keys)
}

func (s *Sink) upsertStatement(ev cdc.ChangeEvent, target string, keys []string) (string, []any, error) {
	if len(ev.After) == 0 {
		return "", nil, fmt.Errorf("pgupsert sink %q: %s event on %s has no after image", s.name, ev.Op, ev.Qualified())
	}

	// Sorted column order keeps the SQL text stable across batches so the
	// server can reuse its plan cache.
	cols := make([]string, 0, len(ev.After))
	for c := range ev.After {
		cols = append(cols, c)
	}
	sort.Strings(cols)

	for _, k := range keys {
		if _, ok := ev.After[k]; !ok {
			return "", nil, fmt.Errorf("pgupsert sink %q: %s on %s is missing key column %q",
				s.name, ev.Op, ev.Qualified(), k)
		}
	}

	placeholders := make([]string, len(cols))
	quotedCols := make([]string, len(cols))
	args := make([]any, len(cols))
	for i, c := range cols {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		quotedCols[i] = quoteIdent(c)
		args[i] = ev.After[c]
	}

	keySet := make(map[string]bool, len(keys))
	quotedKeys := make([]string, len(keys))
	for i, k := range keys {
		keySet[k] = true
		quotedKeys[i] = quoteIdent(k)
	}

	// Only non-key columns present in this event are updated. A column the
	// source omitted (an unchanged TOASTed value) is deliberately left alone
	// rather than overwritten with NULL.
	updates := make([]string, 0, len(cols))
	for _, c := range cols {
		if keySet[c] {
			continue
		}
		updates = append(updates, fmt.Sprintf("%s = excluded.%s", quoteIdent(c), quoteIdent(c)))
	}
	if s.cfg.SoftDelete {
		updates = append(updates, fmt.Sprintf("%s = NULL", quoteIdent(s.cfg.DeletedCol)))
	}

	conflict := "DO NOTHING"
	if len(updates) > 0 {
		conflict = "DO UPDATE SET " + strings.Join(updates, ", ")
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) %s",
		target,
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(quotedKeys, ", "),
		conflict)
	return sql, args, nil
}

func (s *Sink) deleteStatement(ev cdc.ChangeEvent, target string, keys []string) (string, []any, error) {
	if len(ev.Before) == 0 {
		return "", nil, fmt.Errorf("pgupsert sink %q: delete on %s has no before image; "+
			"set REPLICA IDENTITY on the source table so key columns are logged",
			s.name, ev.Qualified())
	}

	where := make([]string, len(keys))
	args := make([]any, len(keys))
	for i, k := range keys {
		v, ok := ev.Before[k]
		if !ok {
			return "", nil, fmt.Errorf("pgupsert sink %q: delete on %s is missing key column %q; "+
				"check REPLICA IDENTITY on the source table",
				s.name, ev.Qualified(), k)
		}
		where[i] = fmt.Sprintf("%s = $%d", quoteIdent(k), i+1)
		args[i] = v
	}

	if s.cfg.SoftDelete {
		return fmt.Sprintf("UPDATE %s SET %s = now() WHERE %s",
			target, quoteIdent(s.cfg.DeletedCol), strings.Join(where, " AND ")), args, nil
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s", target, strings.Join(where, " AND ")), args, nil
}

// keysFor resolves the conflict target. A table with no configured keys is
// rejected rather than written without one: an upsert without a key silently
// duplicates rows on every replay, which is exactly the failure this design
// promises not to have.
func (s *Sink) keysFor(ev cdc.ChangeEvent) ([]string, error) {
	if keys, ok := s.cfg.Keys[ev.Qualified()]; ok && len(keys) > 0 {
		return keys, nil
	}
	if keys, ok := s.cfg.Keys[ev.Table]; ok && len(keys) > 0 {
		return keys, nil
	}
	return nil, fmt.Errorf("pgupsert sink %q: no key columns configured for %s "+
		"(add pgupsert.keys[%q]); refusing to write without an idempotency key",
		s.name, ev.Qualified(), ev.Qualified())
}

func (s *Sink) target(ev cdc.ChangeEvent) string {
	schema := s.cfg.Schema
	if schema == "" {
		schema = ev.Schema
	}
	if schema == "" {
		return quoteIdent(ev.Table)
	}
	return quoteIdent(schema) + "." + quoteIdent(ev.Table)
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
