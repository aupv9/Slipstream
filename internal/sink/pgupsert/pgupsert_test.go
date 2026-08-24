package pgupsert

import (
	"strings"
	"testing"

	"github.com/aupv9/slipstream/internal/cdc"
	"github.com/aupv9/slipstream/internal/config"
)

func newTestSink(cfg config.PGUpsertSink) *Sink {
	if cfg.DeletedCol == "" {
		cfg.DeletedCol = "_deleted_at"
	}
	return &Sink{name: "target", cfg: cfg}
}

func keyed() config.PGUpsertSink {
	return config.PGUpsertSink{Keys: map[string][]string{"public.users": {"id"}}}
}

func TestUpsertStatementIsIdempotentOnTheKey(t *testing.T) {
	s := newTestSink(keyed())
	ev := cdc.ChangeEvent{
		Schema: "public", Table: "users", Op: cdc.OpCreate,
		After: map[string]any{"id": 7, "email": "a@b.c", "name": "Ada"},
	}

	sql, args, err := s.statement(ev)
	if err != nil {
		t.Fatalf("statement: %v", err)
	}
	if !strings.Contains(sql, `INSERT INTO "public"."users"`) {
		t.Errorf("unexpected target in %q", sql)
	}
	if !strings.Contains(sql, `ON CONFLICT ("id") DO UPDATE SET`) {
		t.Errorf("upsert is not keyed on the primary key: %q", sql)
	}
	if strings.Contains(sql, `"id" = excluded."id"`) {
		t.Errorf("key column should not be in the update list: %q", sql)
	}
	// Columns are sorted, so args follow email, id, name.
	if len(args) != 3 || args[0] != "a@b.c" || args[1] != 7 || args[2] != "Ada" {
		t.Errorf("args = %v, want deterministic column order", args)
	}
}

// An unchanged TOASTed value is absent from the after image. It must be left
// alone, not overwritten with NULL.
func TestUpsertLeavesOmittedColumnsAlone(t *testing.T) {
	s := newTestSink(keyed())
	ev := cdc.ChangeEvent{
		Schema: "public", Table: "users", Op: cdc.OpUpdate,
		After: map[string]any{"id": 7, "name": "Ada"},
	}

	sql, _, err := s.statement(ev)
	if err != nil {
		t.Fatalf("statement: %v", err)
	}
	if strings.Contains(sql, "email") {
		t.Errorf("statement touches a column the source did not send: %q", sql)
	}
}

func TestDeleteUsesTheBeforeImage(t *testing.T) {
	s := newTestSink(keyed())
	ev := cdc.ChangeEvent{
		Schema: "public", Table: "users", Op: cdc.OpDelete,
		Before: map[string]any{"id": 7, "email": "a@b.c"},
	}

	sql, args, err := s.statement(ev)
	if err != nil {
		t.Fatalf("statement: %v", err)
	}
	if want := `DELETE FROM "public"."users" WHERE "id" = $1`; sql != want {
		t.Errorf("sql = %q, want %q", sql, want)
	}
	if len(args) != 1 || args[0] != 7 {
		t.Errorf("args = %v, want [7]", args)
	}
}

func TestSoftDeleteStampsInsteadOfRemoving(t *testing.T) {
	cfg := keyed()
	cfg.SoftDelete = true
	s := newTestSink(cfg)

	ev := cdc.ChangeEvent{
		Schema: "public", Table: "users", Op: cdc.OpDelete,
		Before: map[string]any{"id": 7},
	}
	sql, _, err := s.statement(ev)
	if err != nil {
		t.Fatalf("statement: %v", err)
	}
	if !strings.HasPrefix(sql, `UPDATE "public"."users" SET "_deleted_at" = now()`) {
		t.Errorf("sql = %q, want a soft delete", sql)
	}

	// A later insert of the same key must clear the tombstone, or the row
	// would stay invisible after being recreated.
	revived, _, err := s.statement(cdc.ChangeEvent{
		Schema: "public", Table: "users", Op: cdc.OpCreate,
		After: map[string]any{"id": 7, "name": "Ada"},
	})
	if err != nil {
		t.Fatalf("statement: %v", err)
	}
	if !strings.Contains(revived, `"_deleted_at" = NULL`) {
		t.Errorf("upsert does not clear the tombstone: %q", revived)
	}
}

// Writing without a key would duplicate rows on every replay, so it must be a
// configuration error rather than a silent bad write.
func TestWithoutConfiguredKeysTheEventIsRejected(t *testing.T) {
	s := newTestSink(config.PGUpsertSink{})
	_, _, err := s.statement(cdc.ChangeEvent{
		Schema: "public", Table: "users", Op: cdc.OpCreate,
		After: map[string]any{"id": 1},
	})
	if err == nil || !strings.Contains(err.Error(), "no key columns configured") {
		t.Fatalf("expected a missing-keys error, got %v", err)
	}
}

func TestDeleteWithoutBeforeImageIsRejectedWithGuidance(t *testing.T) {
	s := newTestSink(keyed())
	_, _, err := s.statement(cdc.ChangeEvent{
		Schema: "public", Table: "users", Op: cdc.OpDelete,
	})
	if err == nil || !strings.Contains(err.Error(), "REPLICA IDENTITY") {
		t.Fatalf("expected REPLICA IDENTITY guidance, got %v", err)
	}
}

func TestTargetSchemaOverride(t *testing.T) {
	cfg := keyed()
	cfg.Schema = "mirror"
	s := newTestSink(cfg)

	sql, _, err := s.statement(cdc.ChangeEvent{
		Schema: "public", Table: "users", Op: cdc.OpCreate,
		After: map[string]any{"id": 1},
	})
	if err != nil {
		t.Fatalf("statement: %v", err)
	}
	if !strings.Contains(sql, `INSERT INTO "mirror"."users"`) {
		t.Errorf("sql = %q, want the configured target schema", sql)
	}
}

func TestQuotingResistsInjectionThroughIdentifiers(t *testing.T) {
	cfg := config.PGUpsertSink{Keys: map[string][]string{`public.we"ird`: {`i"d`}}}
	s := newTestSink(cfg)

	sql, _, err := s.statement(cdc.ChangeEvent{
		Schema: "public", Table: `we"ird`, Op: cdc.OpDelete,
		Before: map[string]any{`i"d`: 1},
	})
	if err != nil {
		t.Fatalf("statement: %v", err)
	}
	if want := `DELETE FROM "public"."we""ird" WHERE "i""d" = $1`; sql != want {
		t.Errorf("sql = %q, want %q", sql, want)
	}
}

// TRUNCATE removes every row, so it must reach the target as a statement of its
// own — and it needs no configured key, since no row is identified.
func TestTruncateClearsTheTarget(t *testing.T) {
	s := newTestSink(config.PGUpsertSink{})
	sql, args, err := s.statement(cdc.ChangeEvent{
		Schema: "public", Table: "users", Op: cdc.OpTruncate,
	})
	if err != nil {
		t.Fatalf("statement: %v", err)
	}
	if want := `DELETE FROM "public"."users"`; sql != want {
		t.Errorf("sql = %q, want %q", sql, want)
	}
	if len(args) != 0 {
		t.Errorf("args = %v, want none", args)
	}
}

func TestTruncateWithSoftDeleteStampsRemainingRows(t *testing.T) {
	cfg := keyed()
	cfg.SoftDelete = true
	s := newTestSink(cfg)

	sql, _, err := s.statement(cdc.ChangeEvent{
		Schema: "public", Table: "users", Op: cdc.OpTruncate,
	})
	if err != nil {
		t.Fatalf("statement: %v", err)
	}
	if want := `UPDATE "public"."users" SET "_deleted_at" = now() WHERE "_deleted_at" IS NULL`; sql != want {
		t.Errorf("sql = %q, want %q", sql, want)
	}
}
