package pgupsert

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/aupv9/slipstream/internal/cdc"
	"github.com/aupv9/slipstream/internal/config"
)

// These exercise the statements against a real server, where a wrong ON
// CONFLICT clause or a bad quote actually fails.
//
//	SLIPSTREAM_TEST_MIRROR_DSN=postgres://user@host/db go test ./internal/sink/pgupsert/
func mirrorDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("SLIPSTREAM_TEST_MIRROR_DSN")
	if dsn == "" {
		t.Skip("set SLIPSTREAM_TEST_MIRROR_DSN to run pgupsert integration tests")
	}
	return dsn
}

type target struct {
	dsn   string
	table string
	conn  *pgx.Conn
}

func newTarget(t *testing.T, extraCols string) *target {
	t.Helper()
	dsn := mirrorDSN(t)
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	name := fmt.Sprintf("sink_%d", time.Now().UnixNano()%1e9)
	if _, err := conn.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE %s (id bigint PRIMARY KEY, name text, bio text%s)`, name, extraCols)); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+name)
		_ = conn.Close(context.Background())
	})
	return &target{dsn: dsn, table: name, conn: conn}
}

func (tg *target) sink(t *testing.T, mutate func(*config.PGUpsertSink)) *Sink {
	t.Helper()
	cfg := config.PGUpsertSink{
		DSN:        tg.dsn,
		Schema:     "public",
		Keys:       map[string][]string{"public." + tg.table: {"id"}},
		DeletedCol: "_deleted_at",
	}
	if mutate != nil {
		mutate(&cfg)
	}
	s, err := New(context.Background(), "mirror", cfg)
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func (tg *target) event(op cdc.Op, after, before map[string]any) cdc.ChangeEvent {
	return cdc.ChangeEvent{
		SourceID: "src", Schema: "public", Table: tg.table, Op: op,
		After: after, Before: before, Position: "0/1000",
	}
}

func (tg *target) rows(t *testing.T) map[int64]map[string]any {
	t.Helper()
	rows, err := tg.conn.Query(context.Background(),
		fmt.Sprintf("SELECT * FROM %s ORDER BY id", tg.table))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	out := map[int64]map[string]any{}
	fields := rows.FieldDescriptions()
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
		row := map[string]any{}
		for i, f := range fields {
			row[f.Name] = vals[i]
		}
		id, _ := row["id"].(int64)
		out[id] = row
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

// Applying the same batch twice must leave the same rows: this is the property
// the whole at-least-once design rests on.
func TestApplyingABatchTwiceIsIdempotent(t *testing.T) {
	tg := newTarget(t, "")
	s := tg.sink(t, nil)
	ctx := context.Background()

	batch := []cdc.ChangeEvent{
		tg.event(cdc.OpRead, map[string]any{"id": 1, "name": "ada", "bio": "first"}, nil),
		tg.event(cdc.OpCreate, map[string]any{"id": 2, "name": "bob", "bio": "second"}, nil),
		tg.event(cdc.OpUpdate, map[string]any{"id": 1, "name": "ada2", "bio": "first"}, nil),
	}

	for range 3 {
		if err := s.Write(ctx, batch); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	rows := tg.rows(t)
	if len(rows) != 2 {
		t.Fatalf("target holds %d rows after three identical writes, want 2", len(rows))
	}
	if rows[1]["name"] != "ada2" {
		t.Errorf("row 1 name = %v, want the last update to win", rows[1]["name"])
	}
}

// Two changes to one row inside a single batch must land in order, or the
// mirror ends up holding the older value.
func TestOrderWithinABatchIsPreserved(t *testing.T) {
	tg := newTarget(t, "")
	s := tg.sink(t, nil)

	batch := []cdc.ChangeEvent{
		tg.event(cdc.OpCreate, map[string]any{"id": 1, "name": "v1"}, nil),
		tg.event(cdc.OpUpdate, map[string]any{"id": 1, "name": "v2"}, nil),
		tg.event(cdc.OpUpdate, map[string]any{"id": 1, "name": "v3"}, nil),
	}
	if err := s.Write(context.Background(), batch); err != nil {
		t.Fatalf("write: %v", err)
	}

	if got := tg.rows(t)[1]["name"]; got != "v3" {
		t.Errorf("name = %v, want v3 (the last change in the batch)", got)
	}
}

// An update that omits an unchanged TOASTed column must leave it alone, not
// blank it.
func TestOmittedColumnsAreNotBlanked(t *testing.T) {
	tg := newTarget(t, "")
	s := tg.sink(t, nil)
	ctx := context.Background()

	if err := s.Write(ctx, []cdc.ChangeEvent{
		tg.event(cdc.OpCreate, map[string]any{"id": 1, "name": "ada", "bio": "a long biography"}, nil),
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// The reader omits the key entirely for an unchanged TOASTed value.
	if err := s.Write(ctx, []cdc.ChangeEvent{
		tg.event(cdc.OpUpdate, map[string]any{"id": 1, "name": "ada renamed"}, nil),
	}); err != nil {
		t.Fatalf("update: %v", err)
	}

	row := tg.rows(t)[1]
	if row["name"] != "ada renamed" {
		t.Errorf("name = %v, want the update to apply", row["name"])
	}
	if row["bio"] != "a long biography" {
		t.Errorf("bio = %v, want it untouched by an update that did not carry it", row["bio"])
	}
}

func TestDeleteAndTruncateClearRows(t *testing.T) {
	tg := newTarget(t, "")
	s := tg.sink(t, nil)
	ctx := context.Background()

	seed := []cdc.ChangeEvent{
		tg.event(cdc.OpCreate, map[string]any{"id": 1, "name": "a"}, nil),
		tg.event(cdc.OpCreate, map[string]any{"id": 2, "name": "b"}, nil),
		tg.event(cdc.OpCreate, map[string]any{"id": 3, "name": "c"}, nil),
	}
	if err := s.Write(ctx, seed); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := s.Write(ctx, []cdc.ChangeEvent{
		tg.event(cdc.OpDelete, nil, map[string]any{"id": 2, "name": "b"}),
	}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if rows := tg.rows(t); len(rows) != 2 || rows[2] != nil {
		t.Fatalf("after delete the target holds %v", rows)
	}

	// Replaying the delete must stay harmless.
	if err := s.Write(ctx, []cdc.ChangeEvent{
		tg.event(cdc.OpDelete, nil, map[string]any{"id": 2, "name": "b"}),
	}); err != nil {
		t.Fatalf("replayed delete: %v", err)
	}

	if err := s.Write(ctx, []cdc.ChangeEvent{tg.event(cdc.OpTruncate, nil, nil)}); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if rows := tg.rows(t); len(rows) != 0 {
		t.Fatalf("truncate left %d rows behind", len(rows))
	}
}

func TestSoftDeleteStampsAndRevives(t *testing.T) {
	tg := newTarget(t, ", _deleted_at timestamptz")
	s := tg.sink(t, func(c *config.PGUpsertSink) { c.SoftDelete = true })
	ctx := context.Background()

	if err := s.Write(ctx, []cdc.ChangeEvent{
		tg.event(cdc.OpCreate, map[string]any{"id": 1, "name": "a"}, nil),
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := s.Write(ctx, []cdc.ChangeEvent{
		tg.event(cdc.OpDelete, nil, map[string]any{"id": 1}),
	}); err != nil {
		t.Fatalf("soft delete: %v", err)
	}

	row := tg.rows(t)[1]
	if row == nil {
		t.Fatal("a soft delete should keep the row")
	}
	if row["_deleted_at"] == nil {
		t.Error("the row was not stamped as deleted")
	}

	if err := s.Write(ctx, []cdc.ChangeEvent{
		tg.event(cdc.OpCreate, map[string]any{"id": 1, "name": "back"}, nil),
	}); err != nil {
		t.Fatalf("revive: %v", err)
	}
	if got := tg.rows(t)[1]["_deleted_at"]; got != nil {
		t.Errorf("_deleted_at = %v, want the tombstone cleared when the row comes back", got)
	}
}

// A batch must be all-or-nothing: a failure partway through cannot leave the
// mirror holding half of it.
func TestABadEventRollsBackTheWholeBatch(t *testing.T) {
	tg := newTarget(t, "")
	s := tg.sink(t, nil)

	batch := []cdc.ChangeEvent{
		tg.event(cdc.OpCreate, map[string]any{"id": 1, "name": "ok"}, nil),
		tg.event(cdc.OpCreate, map[string]any{"id": 2, "nonexistent_column": "boom"}, nil),
	}
	if err := s.Write(context.Background(), batch); err == nil {
		t.Fatal("writing an unknown column should fail")
	}
	if rows := tg.rows(t); len(rows) != 0 {
		t.Fatalf("a failed batch left %d rows behind; it must roll back entirely", len(rows))
	}
}
