package postgres

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/aupv9/slipstream/internal/cdc"
	"github.com/aupv9/slipstream/internal/config"
	"github.com/aupv9/slipstream/internal/source"
)

// These tests need a real Postgres with wal_level=logical. The whole point of
// the snapshot design is how it interacts with the server's own MVCC snapshot
// and WAL, so there is nothing useful to fake here.
//
//	SLIPSTREAM_TEST_SOURCE_DSN=postgres://user@host/db go test ./internal/source/postgres/
func sourceDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("SLIPSTREAM_TEST_SOURCE_DSN")
	if dsn == "" {
		t.Skip("set SLIPSTREAM_TEST_SOURCE_DSN to run Postgres source tests")
	}
	return dsn
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// fixture creates a throwaway table, slot and publication, and tears them all
// down afterwards so repeated runs cannot inherit state.
type fixture struct {
	dsn   string
	table string
	slot  string
	pub   string
	conn  *pgx.Conn
}

func newFixture(t *testing.T, name string) *fixture {
	t.Helper()
	dsn := sourceDSN(t)
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	suffix := fmt.Sprintf("%s_%d", name, time.Now().UnixNano()%1e9)
	f := &fixture{
		dsn:   dsn,
		table: "slip_" + suffix,
		slot:  "slip_slot_" + suffix,
		pub:   "slip_pub_" + suffix,
		conn:  conn,
	}

	if _, err := conn.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE %s (id bigint PRIMARY KEY, note text)`, f.table)); err != nil {
		t.Fatalf("create table: %v", err)
	}

	t.Cleanup(func() {
		ctx := context.Background()
		// Drop the slot first: an undropped slot pins WAL forever.
		if _, err := conn.Exec(ctx, `SELECT pg_drop_replication_slot($1)
			WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, f.slot); err != nil {
			t.Logf("drop slot: %v", err)
		}
		if _, err := conn.Exec(ctx, "DROP PUBLICATION IF EXISTS "+f.pub); err != nil {
			t.Logf("drop publication: %v", err)
		}
		if _, err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+f.table); err != nil {
			t.Logf("drop table: %v", err)
		}
		_ = conn.Close(ctx)
	})
	return f
}

func (f *fixture) readerConfig() config.Postgres {
	return config.Postgres{
		DSN:         f.dsn,
		Slot:        f.slot,
		Publication: f.pub,
		Tables:      []string{"public." + f.table},
		Snapshot:    true,
	}
}

// TestSnapshotAndStreamHaveNoGapAndNoOverlap is the invariant the whole
// bootstrap sequence exists for. While the snapshot runs, rows keep being
// committed. Every row must be delivered exactly once overall: either it is in
// the snapshot (committed at or before the slot's consistent point) or it
// arrives on the stream (committed after it) — never both, never neither.
func TestSnapshotAndStreamHaveNoGapAndNoOverlap(t *testing.T) {
	f := newFixture(t, "boundary")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	const preloaded = 20000
	if _, err := f.conn.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s SELECT g, 'preloaded' FROM generate_series(1, %d) g`, f.table, preloaded)); err != nil {
		t.Fatalf("preload: %v", err)
	}

	// A writer that keeps committing rows across the snapshot boundary.
	writer, err := pgx.Connect(ctx, f.dsn)
	if err != nil {
		t.Fatalf("writer connect: %v", err)
	}
	defer writer.Close(context.Background())

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
			if _, err := writer.Exec(writeCtx, fmt.Sprintf(
				`INSERT INTO %s VALUES ($1, 'concurrent')`, f.table), id); err != nil {
				return
			}
			wmu.Lock()
			written[id] = true
			wmu.Unlock()
			time.Sleep(time.Millisecond)
		}
	}()

	reader := New(f.readerConfig(), "src", quietLogger())
	events := make(chan cdc.ChangeEvent, 4096)
	readCtx, stopReader := context.WithCancel(ctx)
	defer stopReader()

	readErr := make(chan error, 1)
	go func() {
		defer close(events)
		readErr <- reader.ReadChanges(readCtx, source.ReadRequest{}, events)
	}()

	snapshotIDs := map[int64]bool{}
	streamIDs := map[int64]bool{}
	var dupes []string

	// Keep writing for a while after the reader starts, then wait until every
	// committed row has been delivered.
	writerStopAt := time.Now().Add(3 * time.Second)
	var finalTarget int64 = -1
	deadline := time.After(60 * time.Second)

	for {
		if finalTarget == -1 && time.Now().After(writerStopAt) {
			stopWriter()
			<-writerDone
			wmu.Lock()
			finalTarget = int64(len(written))
			wmu.Unlock()
			if finalTarget == 0 {
				t.Fatal("the writer committed nothing; the test would be vacuous")
			}
		}
		if finalTarget >= 0 && int64(len(snapshotIDs)+len(streamIDs)) >= preloaded+finalTarget {
			break
		}

		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatalf("reader stopped early: %v", <-readErr)
			}
			if id, isInsert := insertedID(ev); isInsert {
				switch ev.Op {
				case cdc.OpRead:
					if snapshotIDs[id] {
						dupes = append(dupes, fmt.Sprintf("id %d twice in snapshot", id))
					}
					snapshotIDs[id] = true
				case cdc.OpCreate:
					if streamIDs[id] {
						dupes = append(dupes, fmt.Sprintf("id %d twice on stream", id))
					}
					streamIDs[id] = true
				}
			}
		case <-time.After(50 * time.Millisecond):
			// Poll so the writer stop and the completion check still run when
			// the stream goes briefly quiet.
		case <-deadline:
			t.Fatalf("timed out with %d of %d rows delivered (snapshot=%d stream=%d)",
				len(snapshotIDs)+len(streamIDs), preloaded+finalTarget, len(snapshotIDs), len(streamIDs))
		}
	}
	stopReader()

	if len(dupes) > 0 {
		t.Errorf("duplicate deliveries: %v", dupes[:min(len(dupes), 10)])
	}

	// Neither side may be empty, or the boundary was never actually crossed.
	if len(snapshotIDs) == 0 || len(streamIDs) == 0 {
		t.Fatalf("no overlap window exercised: snapshot=%d stream=%d", len(snapshotIDs), len(streamIDs))
	}

	// No overlap: a row must not be both snapshotted and streamed.
	var overlap []int64
	for id := range streamIDs {
		if snapshotIDs[id] {
			overlap = append(overlap, id)
		}
	}
	if len(overlap) > 0 {
		t.Errorf("%d rows were delivered by both snapshot and stream, e.g. %v",
			len(overlap), overlap[:min(len(overlap), 10)])
	}

	// No gap: every committed row was delivered by exactly one of the two.
	var missing []int64
	for id := int64(1); id <= preloaded; id++ {
		if !snapshotIDs[id] && !streamIDs[id] {
			missing = append(missing, id)
		}
	}
	wmu.Lock()
	for id := range written {
		if !snapshotIDs[id] && !streamIDs[id] {
			missing = append(missing, id)
		}
	}
	wmu.Unlock()
	if len(missing) > 0 {
		t.Errorf("%d committed rows were never delivered, e.g. %v",
			len(missing), missing[:min(len(missing), 10)])
	}

	t.Logf("snapshot=%d stream=%d (preloaded=%d concurrent=%d)",
		len(snapshotIDs), len(streamIDs), preloaded, finalTarget)
}

// TestResumeFromStoredOffsetSkipsTheSnapshot checks the failover path: a
// restart with a stored position must stream on from there instead of
// re-snapshotting, and must not lose changes made while nothing was reading.
func TestResumeFromStoredOffsetSkipsTheSnapshot(t *testing.T) {
	f := newFixture(t, "resume")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := f.conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s VALUES (1, 'before')`, f.table)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// First run: snapshot, then stream one insert.
	reader := New(f.readerConfig(), "src", quietLogger())
	events := make(chan cdc.ChangeEvent, 128)
	runCtx, stop := context.WithCancel(ctx)
	go func() {
		defer close(events)
		_ = reader.ReadChanges(runCtx, source.ReadRequest{}, events)
	}()

	first := waitFor(t, events, func(ev cdc.ChangeEvent) bool { return ev.Op == cdc.OpRead })
	if id, _ := insertedID(first); id != 1 {
		t.Fatalf("snapshot delivered id %v, want 1", id)
	}

	if _, err := f.conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s VALUES (2, 'streamed')`, f.table)); err != nil {
		t.Fatalf("insert 2: %v", err)
	}
	streamed := waitFor(t, events, func(ev cdc.ChangeEvent) bool { return ev.Op == cdc.OpCreate })
	resumeFrom := streamed.Position
	if resumeFrom == "" {
		t.Fatal("streamed event carries no position, so nothing could be resumed from")
	}

	stop()
	for range events {
	}
	if err := reader.Close(); err != nil {
		t.Logf("close: %v", err)
	}

	// Changes made while no reader is attached must still be in the slot.
	if _, err := f.conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s VALUES (3, 'while-down')`, f.table)); err != nil {
		t.Fatalf("insert 3: %v", err)
	}

	// Second run: resume from the stored position.
	resumed := New(f.readerConfig(), "src", quietLogger())
	events2 := make(chan cdc.ChangeEvent, 128)
	runCtx2, stop2 := context.WithCancel(ctx)
	defer stop2()
	go func() {
		defer close(events2)
		_ = resumed.ReadChanges(runCtx2, source.ReadRequest{From: resumeFrom}, events2)
	}()

	seen := map[int64]bool{}
	for {
		ev := waitFor(t, events2, func(cdc.ChangeEvent) bool { return true })
		if ev.Op == cdc.OpRead {
			t.Fatal("resume re-ran the snapshot instead of streaming from the stored offset")
		}
		if id, ok := insertedID(ev); ok {
			seen[id] = true
		}
		if seen[3] {
			break
		}
	}
	if !seen[3] {
		t.Fatal("the change made while the reader was down was lost")
	}
}

// TestStreamsUpdatesAndDeletesWithKeyImages checks that a sink has what it
// needs to apply an update or delete: an after image for updates, and the key
// columns in the before image for deletes.
func TestStreamsUpdatesAndDeletesWithKeyImages(t *testing.T) {
	f := newFixture(t, "dml")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := f.conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s VALUES (1, 'first')`, f.table)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	reader := New(f.readerConfig(), "src", quietLogger())
	events := make(chan cdc.ChangeEvent, 128)
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go func() {
		defer close(events)
		_ = reader.ReadChanges(runCtx, source.ReadRequest{}, events)
	}()

	waitFor(t, events, func(ev cdc.ChangeEvent) bool { return ev.Op == cdc.OpRead })

	for _, stmt := range []string{
		fmt.Sprintf(`UPDATE %s SET note = 'updated' WHERE id = 1`, f.table),
		fmt.Sprintf(`DELETE FROM %s WHERE id = 1`, f.table),
	} {
		if _, err := f.conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	upd := waitFor(t, events, func(ev cdc.ChangeEvent) bool { return ev.Op == cdc.OpUpdate })
	if upd.Table != f.table || upd.Schema != "public" {
		t.Errorf("update event names %s.%s", upd.Schema, upd.Table)
	}
	if got := upd.After["note"]; got != "updated" {
		t.Errorf("update after image note = %v, want \"updated\"", got)
	}
	if upd.CommitTS.IsZero() {
		t.Error("update event has no commit timestamp")
	}

	del := waitFor(t, events, func(ev cdc.ChangeEvent) bool { return ev.Op == cdc.OpDelete })
	if del.Before == nil {
		t.Fatal("delete event has no before image, so a sink cannot locate the row")
	}
	if got := del.Before["id"]; fmt.Sprint(got) != "1" {
		t.Errorf("delete before image id = %v, want 1", got)
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

// insertedID pulls the row id out of an insert-like event.
func insertedID(ev cdc.ChangeEvent) (int64, bool) {
	if ev.Op != cdc.OpRead && ev.Op != cdc.OpCreate {
		return 0, false
	}
	raw, ok := ev.After["id"]
	if !ok {
		return 0, false
	}
	switch v := raw.(type) {
	case int64:
		return v, true
	case int32:
		return int64(v), true
	case int:
		return int64(v), true
	default:
		var n int64
		if _, err := fmt.Sscan(fmt.Sprint(raw), &n); err == nil {
			return n, true
		}
		return 0, false
	}
}

// recordingHooks captures the snapshot lifecycle a reader reports. The reader
// calls it from its own goroutine, so access is guarded.
type recordingHooks struct {
	mu        sync.Mutex
	started   int
	completed int
	position  string
}

func (h *recordingHooks) SnapshotStarted(context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.started++
	return nil
}

func (h *recordingHooks) SnapshotCompleted(_ context.Context, position string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.completed++
	h.position = position
	return nil
}

func (h *recordingHooks) state() (started, completed int, position string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.started, h.completed, h.position
}

// awaitCompletion waits for the reader to report the snapshot finished, which
// happens just after the last row is emitted.
func (h *recordingHooks) awaitCompletion(t *testing.T) string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, completed, position := h.state()
		if completed > 0 {
			return position
		}
		if time.Now().After(deadline) {
			t.Fatal("the reader never reported the snapshot as complete")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestInterruptedSnapshotIsRedoneFromScratch is the regression test for a real
// data-loss bug: sinks accept snapshot rows as they arrive, so the offset
// advances to the slot's consistent point while the snapshot is still running.
// An instance that dies midway leaves an offset that looks resumable but only
// covers the rows already read. Resuming from it streamed forward and silently
// never delivered the rest — the pipeline looked healthy while missing data
// permanently. A run told to bootstrap must therefore drop the leftover slot
// and read every row again.
func TestInterruptedSnapshotIsRedoneFromScratch(t *testing.T) {
	f := newFixture(t, "interrupted")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	const rows = 5000
	if _, err := f.conn.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s SELECT g, 'row-' || g FROM generate_series(1, %d) g`, f.table, rows)); err != nil {
		t.Fatalf("preload: %v", err)
	}

	// First attempt: die partway through the snapshot.
	first := New(f.readerConfig(), "src", quietLogger())
	events := make(chan cdc.ChangeEvent, 8192)
	runCtx, kill := context.WithCancel(ctx)
	hooks1 := &recordingHooks{}
	go func() {
		defer close(events)
		_ = first.ReadChanges(runCtx, source.ReadRequest{Hooks: hooks1}, events)
	}()

	partial := 0
	for partial < 100 {
		if _, ok := insertedID(waitFor(t, events, func(ev cdc.ChangeEvent) bool { return ev.Op == cdc.OpRead })); ok {
			partial++
		}
	}
	kill()
	for range events {
	}
	_ = first.Close()

	started, completed, _ := hooks1.state()
	if started != 1 {
		t.Fatalf("snapshot start was recorded %d times, want 1", started)
	}
	if completed != 0 {
		t.Fatal("an interrupted snapshot must not be recorded as complete")
	}

	// The leftover slot proves the interrupted attempt got that far.
	var slots int
	if err := f.conn.QueryRow(ctx,
		`SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1`, f.slot).Scan(&slots); err != nil {
		t.Fatalf("count slots: %v", err)
	}
	if slots != 1 {
		t.Fatalf("expected the interrupted attempt to leave its slot behind, found %d", slots)
	}

	// Second attempt: an offset exists, but the snapshot never completed, so
	// the pipeline forces a bootstrap.
	second := New(f.readerConfig(), "src", quietLogger())
	events2 := make(chan cdc.ChangeEvent, 8192)
	runCtx2, stop2 := context.WithCancel(ctx)
	defer stop2()
	hooks2 := &recordingHooks{}
	go func() {
		defer close(events2)
		_ = second.ReadChanges(runCtx2, source.ReadRequest{
			From:           "0/1",
			ForceBootstrap: true,
			Hooks:          hooks2,
		}, events2)
	}()

	seen := map[int64]bool{}
	deadline := time.After(60 * time.Second)
	for len(seen) < rows {
		select {
		case ev, ok := <-events2:
			if !ok {
				t.Fatalf("reader stopped after %d of %d rows", len(seen), rows)
			}
			if ev.Op != cdc.OpRead {
				continue
			}
			if id, ok := insertedID(ev); ok {
				seen[id] = true
			}
		case <-deadline:
			t.Fatalf("only %d of %d rows were re-snapshotted", len(seen), rows)
		}
	}

	for id := int64(1); id <= rows; id++ {
		if !seen[id] {
			t.Fatalf("row %d was never re-delivered", id)
		}
	}
	if position := hooks2.awaitCompletion(t); position == "" {
		t.Error("completion did not report the position streaming starts from")
	}
	if _, completed, _ := hooks2.state(); completed != 1 {
		t.Errorf("snapshot completion recorded %d times, want 1", completed)
	}
}

// A dropped TRUNCATE leaves sinks holding every row the source just discarded,
// so it must be delivered as its own event.
func TestTruncateIsDelivered(t *testing.T) {
	f := newFixture(t, "truncate")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := f.conn.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s SELECT g, 'row-' || g FROM generate_series(1, 5) g`, f.table)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	reader := New(f.readerConfig(), "src", quietLogger())
	events := make(chan cdc.ChangeEvent, 128)
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go func() {
		defer close(events)
		_ = reader.ReadChanges(runCtx, source.ReadRequest{}, events)
	}()

	waitFor(t, events, func(ev cdc.ChangeEvent) bool { return ev.Op == cdc.OpRead })

	if _, err := f.conn.Exec(ctx, "TRUNCATE "+f.table); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	ev := waitFor(t, events, func(ev cdc.ChangeEvent) bool { return ev.Op == cdc.OpTruncate })
	if ev.Table != f.table || ev.Schema != "public" {
		t.Errorf("truncate event names %s.%s, want public.%s", ev.Schema, ev.Table, f.table)
	}
	if ev.Before != nil || ev.After != nil {
		t.Error("a truncate event carries no row images")
	}
	if ev.Position == "" {
		t.Error("truncate event has no position")
	}
}

// A table added to the config after the publication exists used to be ignored
// in silence: the operator believes it replicates when it does not.
func TestConfiguredTableMissingFromPublicationIsRefused(t *testing.T) {
	f := newFixture(t, "drift")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// First run creates the publication covering only the fixture table.
	reader := New(f.readerConfig(), "src", quietLogger())
	events := make(chan cdc.ChangeEvent, 16)
	runCtx, stop := context.WithCancel(ctx)
	go func() {
		defer close(events)
		_ = reader.ReadChanges(runCtx, source.ReadRequest{}, events)
	}()
	waitForStreaming(t, f, ctx)
	stop()
	for range events {
	}
	_ = reader.Close()

	// A second table appears in the config later.
	late := f.table + "_late"
	if _, err := f.conn.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE %s (id bigint PRIMARY KEY, note text)`, late)); err != nil {
		t.Fatalf("create late table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = f.conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+late)
	})
	if _, err := f.conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s VALUES (1, 'existing')`, late)); err != nil {
		t.Fatalf("seed late table: %v", err)
	}

	cfg := f.readerConfig()
	cfg.Tables = append(cfg.Tables, "public."+late)

	strict := New(cfg, "src", quietLogger())
	err := strict.ReadChanges(ctx, source.ReadRequest{From: "0/1"}, make(chan cdc.ChangeEvent, 1))
	if err == nil {
		t.Fatal("a table missing from the publication must stop the pipeline, not be ignored")
	}
	if !strings.Contains(err.Error(), late) || !strings.Contains(err.Error(), "auto_add_tables") {
		t.Fatalf("the error should name the table and the opt-in flag, got: %v", err)
	}

	// With the opt-in, the table is added and captured streaming-only.
	cfg.AutoAddTables = true
	lenient := New(cfg, "src", quietLogger())
	events2 := make(chan cdc.ChangeEvent, 128)
	runCtx2, stop2 := context.WithCancel(ctx)
	defer stop2()
	go func() {
		defer close(events2)
		_ = lenient.ReadChanges(runCtx2, source.ReadRequest{From: "0/1"}, events2)
	}()
	waitForStreaming(t, f, ctx)

	if _, err := f.conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s VALUES (2, 'after-add')`, late)); err != nil {
		t.Fatalf("insert into late table: %v", err)
	}
	ev := waitFor(t, events2, func(ev cdc.ChangeEvent) bool {
		return ev.Table == late && ev.Op == cdc.OpCreate
	})
	if got := ev.After["note"]; got != "after-add" {
		t.Errorf("late table event = %v, want the inserted row", got)
	}

	// Row 1 was written before the table joined the publication, so it is
	// deliberately not delivered: that is what the warning is about.
	select {
	case stray := <-events2:
		if stray.Table == late {
			if note, _ := stray.After["note"].(string); note == "existing" {
				t.Error("a pre-existing row appeared without a snapshot; that should not be possible")
			}
		}
	case <-time.After(500 * time.Millisecond):
	}
}

// waitForStreaming waits until the reader has an active slot, which means it
// finished bootstrapping and is consuming the stream.
func waitForStreaming(t *testing.T, f *fixture, ctx context.Context) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var active bool
		err := f.conn.QueryRow(ctx,
			`SELECT active FROM pg_replication_slots WHERE slot_name = $1`, f.slot).Scan(&active)
		if err == nil && active {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the reader never started streaming (last err: %v)", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
