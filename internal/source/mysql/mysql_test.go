package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aupv9/slipstream/internal/cdc"
	"github.com/aupv9/slipstream/internal/config"
	"github.com/aupv9/slipstream/internal/source"
)

// These need a MySQL server with ROW binlog and GTID mode on:
//
//	SLIPSTREAM_TEST_MYSQL_DSN='slip:slip@tcp(127.0.0.1:3306)/app' go test ./internal/source/mysql/
func mysqlDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("SLIPSTREAM_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set SLIPSTREAM_TEST_MYSQL_DSN to run MySQL source tests")
	}
	return dsn
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

type fixture struct {
	dsn      string
	db       *sql.DB
	database string
	table    string
	serverID uint32
}

func newFixture(t *testing.T, name string) *fixture {
	t.Helper()
	dsn := mysqlDSN(t)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}

	var database string
	if err := db.QueryRow(`SELECT DATABASE()`).Scan(&database); err != nil {
		t.Fatalf("current database: %v", err)
	}

	stamp := time.Now().UnixNano() % 1e9
	f := &fixture{
		dsn:      dsn,
		db:       db,
		database: database,
		table:    fmt.Sprintf("slip_%s_%d", name, stamp),
		// Unique per test: MySQL kicks off an existing client with the same id.
		serverID: uint32(100000 + stamp%100000),
	}

	if _, err := db.Exec(fmt.Sprintf(
		"CREATE TABLE %s (id BIGINT PRIMARY KEY, note VARCHAR(255))", f.table)); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec("DROP TABLE IF EXISTS " + f.table)
		_ = db.Close()
	})
	return f
}

func (f *fixture) readerConfig() config.MySQL {
	return config.MySQL{
		DSN:      f.dsn,
		ServerID: f.serverID,
		Tables:   []string{f.database + "." + f.table},
		Snapshot: true,
	}
}

func waitFor(t *testing.T, events <-chan cdc.ChangeEvent, match func(cdc.ChangeEvent) bool) cdc.ChangeEvent {
	t.Helper()
	timeout := time.After(30 * time.Second)
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatal("event stream closed before a matching event arrived")
			}
			if match(ev) {
				return ev
			}
		case <-timeout:
			t.Fatal("timed out waiting for an event")
		}
	}
}

func rowID(ev cdc.ChangeEvent) (int64, bool) {
	values := ev.After
	if len(values) == 0 {
		values = ev.Before
	}
	switch v := values["id"].(type) {
	case int64:
		return v, true
	case int32:
		return int64(v), true
	case int:
		return int64(v), true
	default:
		return 0, false
	}
}

// The same boundary invariant the Postgres reader has: rows committed while the
// snapshot runs must all arrive, by one path or the other. MySQL's ordering
// (read the GTID set, then open the snapshot) admits duplicates but no gaps, so
// the assertion is coverage rather than exclusivity.
func TestSnapshotAndStreamCoverEveryRow(t *testing.T) {
	f := newFixture(t, "boundary")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const preloaded = 5000
	for i := 1; i <= preloaded; i += 500 {
		if _, err := f.db.ExecContext(ctx, fmt.Sprintf(
			"INSERT INTO %s (id, note) SELECT ?, 'preloaded'", f.table), i); err != nil {
			t.Fatalf("preload: %v", err)
		}
		for j := i + 1; j < i+500 && j <= preloaded; j++ {
			if _, err := f.db.ExecContext(ctx, fmt.Sprintf(
				"INSERT INTO %s (id, note) VALUES (?, 'preloaded')", f.table), j); err != nil {
				t.Fatalf("preload: %v", err)
			}
		}
	}

	var (
		wmu     sync.Mutex
		written = map[int64]bool{}
	)
	writeCtx, stopWriter := context.WithCancel(ctx)
	defer stopWriter()
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for id := int64(preloaded + 1); ; id++ {
			if writeCtx.Err() != nil {
				return
			}
			if _, err := f.db.ExecContext(writeCtx, fmt.Sprintf(
				"INSERT INTO %s (id, note) VALUES (?, 'concurrent')", f.table), id); err != nil {
				return
			}
			wmu.Lock()
			written[id] = true
			wmu.Unlock()
			time.Sleep(2 * time.Millisecond)
		}
	}()

	reader := New(f.readerConfig(), "src", quietLogger())
	events := make(chan cdc.ChangeEvent, 8192)
	readCtx, stopReader := context.WithCancel(ctx)
	defer stopReader()
	go func() {
		defer close(events)
		_ = reader.ReadChanges(readCtx, source.ReadRequest{}, events)
	}()

	seen := map[int64]bool{}
	snapshotted, streamed := 0, 0

	writerStopAt := time.Now().Add(3 * time.Second)
	var target int64 = -1
	deadline := time.After(90 * time.Second)

	for {
		if target == -1 && time.Now().After(writerStopAt) {
			stopWriter()
			<-writerDone
			wmu.Lock()
			target = int64(len(written))
			wmu.Unlock()
			if target == 0 {
				t.Fatal("the writer committed nothing; the test would be vacuous")
			}
		}
		if target >= 0 && int64(len(seen)) >= preloaded+target {
			break
		}

		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatalf("reader stopped after %d rows", len(seen))
			}
			if id, ok := rowID(ev); ok {
				switch ev.Op {
				case cdc.OpRead:
					snapshotted++
					seen[id] = true
				case cdc.OpCreate:
					streamed++
					seen[id] = true
				}
			}
		case <-time.After(50 * time.Millisecond):
		case <-deadline:
			t.Fatalf("timed out with %d of %d rows (snapshot=%d stream=%d)",
				len(seen), preloaded+target, snapshotted, streamed)
		}
	}

	if snapshotted == 0 || streamed == 0 {
		t.Fatalf("no boundary was exercised: snapshot=%d stream=%d", snapshotted, streamed)
	}

	var missing []int64
	for id := int64(1); id <= preloaded; id++ {
		if !seen[id] {
			missing = append(missing, id)
		}
	}
	wmu.Lock()
	for id := range written {
		if !seen[id] {
			missing = append(missing, id)
		}
	}
	wmu.Unlock()
	if len(missing) > 0 {
		t.Errorf("%d committed rows were never delivered, e.g. %v", len(missing), missing[:min(len(missing), 10)])
	}
	t.Logf("snapshot=%d stream=%d unique=%d (preloaded=%d concurrent=%d)",
		snapshotted, streamed, len(seen), preloaded, target)
}

func TestStreamsInsertUpdateDelete(t *testing.T) {
	f := newFixture(t, "dml")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := f.db.ExecContext(ctx, fmt.Sprintf(
		"INSERT INTO %s (id, note) VALUES (1, 'first')", f.table)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	reader := New(f.readerConfig(), "src", quietLogger())
	events := make(chan cdc.ChangeEvent, 256)
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go func() {
		defer close(events)
		_ = reader.ReadChanges(runCtx, source.ReadRequest{}, events)
	}()

	snap := waitFor(t, events, func(ev cdc.ChangeEvent) bool { return ev.Op == cdc.OpRead })
	if got := snap.After["note"]; got != "first" {
		t.Errorf("snapshot note = %v (%T), want the string \"first\"", got, got)
	}
	if snap.Table != f.table {
		t.Errorf("snapshot names table %q, want %q", snap.Table, f.table)
	}

	for _, stmt := range []string{
		fmt.Sprintf("INSERT INTO %s (id, note) VALUES (2, 'inserted')", f.table),
		fmt.Sprintf("UPDATE %s SET note = 'updated' WHERE id = 2", f.table),
		fmt.Sprintf("DELETE FROM %s WHERE id = 2", f.table),
	} {
		if _, err := f.db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	ins := waitFor(t, events, func(ev cdc.ChangeEvent) bool { return ev.Op == cdc.OpCreate })
	if got := ins.After["note"]; got != "inserted" {
		t.Errorf("insert note = %v", got)
	}
	if ins.CommitTS.IsZero() {
		t.Error("insert has no commit timestamp")
	}

	upd := waitFor(t, events, func(ev cdc.ChangeEvent) bool { return ev.Op == cdc.OpUpdate })
	if got := upd.After["note"]; got != "updated" {
		t.Errorf("update after = %v, want \"updated\"", got)
	}
	// binlog_row_image=FULL gives a complete before image, which pgoutput does
	// not do by default; a sink can rely on it here.
	if got := upd.Before["note"]; got != "inserted" {
		t.Errorf("update before = %v, want \"inserted\"", got)
	}

	del := waitFor(t, events, func(ev cdc.ChangeEvent) bool { return ev.Op == cdc.OpDelete })
	if id, ok := rowID(del); !ok || id != 2 {
		t.Errorf("delete before image = %v, want id 2", del.Before)
	}
}

// Resuming from a stored GTID set must pick up changes made while nothing was
// reading, and must not re-run the snapshot.
func TestResumeFromStoredGTIDSet(t *testing.T) {
	f := newFixture(t, "resume")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := f.db.ExecContext(ctx, fmt.Sprintf(
		"INSERT INTO %s (id, note) VALUES (1, 'before')", f.table)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	reader := New(f.readerConfig(), "src", quietLogger())
	events := make(chan cdc.ChangeEvent, 256)
	runCtx, stop := context.WithCancel(ctx)
	go func() {
		defer close(events)
		_ = reader.ReadChanges(runCtx, source.ReadRequest{}, events)
	}()

	waitFor(t, events, func(ev cdc.ChangeEvent) bool { return ev.Op == cdc.OpRead })
	if _, err := f.db.ExecContext(ctx, fmt.Sprintf(
		"INSERT INTO %s (id, note) VALUES (2, 'streamed')", f.table)); err != nil {
		t.Fatalf("insert 2: %v", err)
	}
	streamed := waitFor(t, events, func(ev cdc.ChangeEvent) bool { return ev.Op == cdc.OpCreate })
	resumeFrom := streamed.Position
	if resumeFrom == "" {
		t.Fatal("streamed event carries no position")
	}

	stop()
	for range events {
	}
	_ = reader.Close()

	// Changes made while nothing is attached must still be in the binlog.
	if _, err := f.db.ExecContext(ctx, fmt.Sprintf(
		"INSERT INTO %s (id, note) VALUES (3, 'while-down')", f.table)); err != nil {
		t.Fatalf("insert 3: %v", err)
	}

	cfg := f.readerConfig()
	cfg.ServerID = f.serverID + 1
	resumed := New(cfg, "src", quietLogger())
	events2 := make(chan cdc.ChangeEvent, 256)
	runCtx2, stop2 := context.WithCancel(ctx)
	defer stop2()
	go func() {
		defer close(events2)
		_ = resumed.ReadChanges(runCtx2, source.ReadRequest{From: resumeFrom}, events2)
	}()

	seen := map[int64]bool{}
	for !seen[3] {
		ev := waitFor(t, events2, func(cdc.ChangeEvent) bool { return true })
		if ev.Op == cdc.OpRead {
			t.Fatal("resume re-ran the snapshot instead of streaming from the stored GTID set")
		}
		if id, ok := rowID(ev); ok {
			seen[id] = true
		}
	}
}

// A server without ROW logging or GTIDs cannot support correct CDC, so the
// reader must refuse rather than emit partial events.
func TestRefusesUnsuitableServerSettings(t *testing.T) {
	dsn := mysqlDSN(t)
	r := New(config.MySQL{DSN: dsn, ServerID: 999001}, "src", quietLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// The live server is configured correctly, so this documents the check
	// passing; the failure paths are unit-tested through the error strings.
	if err := r.checkServerSettings(ctx, db); err != nil {
		t.Fatalf("a correctly configured server was rejected: %v", err)
	}
}
