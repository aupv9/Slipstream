package postgres

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aupv9/slipstream/internal/cdc"
	"github.com/aupv9/slipstream/internal/config"
	"github.com/aupv9/slipstream/internal/source"
)

func (f *fixture) chunkedConfig(size int) config.Postgres {
	cfg := f.readerConfig()
	cfg.SnapshotMode = config.SnapshotChunked
	cfg.ChunkSize = size
	return cfg
}

// A chunked snapshot must deliver every row exactly as a single-transaction one
// would, while writes keep landing. Rows the stream overtakes are dropped from
// their chunk, so the newer streamed version is what a sink ends up with.
func TestChunkedSnapshotCoversEveryRowWhileWritesContinue(t *testing.T) {
	f := newFixture(t, "chunked")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const preloaded = 20000
	if _, err := f.conn.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s SELECT g, 'preloaded' FROM generate_series(1, %d) g`, f.table, preloaded)); err != nil {
		t.Fatalf("preload: %v", err)
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
			// Insert a new row and update an old one, so the run exercises both
			// new rows and rows the stream overtakes mid-chunk.
			if _, err := f.conn.Exec(writeCtx, fmt.Sprintf(
				`INSERT INTO %s VALUES ($1, 'concurrent')`, f.table), id); err != nil {
				return
			}
			if _, err := f.conn.Exec(writeCtx, fmt.Sprintf(
				`UPDATE %s SET note = 'touched' WHERE id = $1`, f.table), (id%preloaded)+1); err != nil {
				return
			}
			wmu.Lock()
			written[id] = true
			wmu.Unlock()
			time.Sleep(time.Millisecond)
		}
	}()

	hooks := &recordingHooks{}
	reader := New(f.chunkedConfig(2000), "src", quietLogger())
	events := make(chan cdc.ChangeEvent, 8192)
	readCtx, stopReader := context.WithCancel(ctx)
	defer stopReader()
	go func() {
		defer close(events)
		_ = reader.ReadChanges(readCtx, source.ReadRequest{Hooks: hooks}, events)
	}()

	// A row the stream overtakes mid-chunk is deliberately dropped from its
	// chunk and arrives as an update instead, so an update counts as delivered
	// too: what matters is that the row's current state reaches the sink.
	seen := map[int64]bool{}
	snapshotted, streamed, updated := 0, 0, 0
	writerStopAt := time.Now().Add(4 * time.Second)
	var target int64 = -1
	deadline := time.After(90 * time.Second)

	for {
		if target == -1 && time.Now().After(writerStopAt) {
			stopWriter()
			<-writerDone
			wmu.Lock()
			target = int64(len(written))
			wmu.Unlock()
		}
		if target >= 0 && int64(len(seen)) >= preloaded+target {
			break
		}

		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatalf("reader stopped after %d rows", len(seen))
			}
			if id, ok := anyRowID(ev); ok {
				switch ev.Op {
				case cdc.OpRead:
					snapshotted++
					seen[id] = true
				case cdc.OpCreate:
					streamed++
					seen[id] = true
				case cdc.OpUpdate:
					updated++
					seen[id] = true
				}
			}
		case <-time.After(50 * time.Millisecond):
		case <-deadline:
			t.Fatalf("timed out with %d of %d rows (snapshot=%d stream=%d update=%d)",
				len(seen), preloaded+target, snapshotted, streamed, updated)
		}
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
		t.Errorf("%d rows were never delivered, e.g. %v", len(missing), missing[:min(len(missing), 10)])
	}

	if _, _, mode := hooks.state3(); mode != config.SnapshotChunked {
		t.Errorf("snapshot mode recorded as %q, want %q", mode, config.SnapshotChunked)
	}
	// Chunk progress is what makes a restart resumable, so it must be recorded
	// per chunk rather than only at the end.
	if got := len(hooks.progress()); got < 2 {
		t.Errorf("recorded %d chunk checkpoints, want one per chunk", got)
	}
	t.Logf("snapshot=%d stream=%d update=%d unique=%d checkpoints=%d",
		snapshotted, streamed, updated, len(seen), len(hooks.progress()))
}

// The whole point of chunking: an interrupted snapshot continues from its last
// chunk instead of starting over, and still ends up with every row.
func TestChunkedSnapshotResumesFromItsLastChunk(t *testing.T) {
	f := newFixture(t, "chunkresume")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const preloaded = 20000
	if _, err := f.conn.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s SELECT g, 'preloaded' FROM generate_series(1, %d) g`, f.table, preloaded)); err != nil {
		t.Fatalf("preload: %v", err)
	}

	// First run: stop after a couple of chunks.
	hooks1 := &recordingHooks{}
	first := New(f.chunkedConfig(1000), "src", quietLogger())
	events := make(chan cdc.ChangeEvent, 8192)
	runCtx, kill := context.WithCancel(ctx)

	go func() {
		defer close(events)
		_ = first.ReadChanges(runCtx, source.ReadRequest{Hooks: hooks1}, events)
	}()

	seen := map[int64]bool{}
	var lastPosition string
	for len(hooks1.progress()) < 3 {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatal("reader stopped early")
			}
			if id, ok := anyRowID(ev); ok && (ev.Op == cdc.OpRead || ev.Op == cdc.OpCreate) {
				seen[id] = true
			}
			if ev.Position != "" {
				lastPosition = ev.Position
			}
		case <-time.After(60 * time.Second):
			t.Fatalf("only %d chunks completed", len(hooks1.progress()))
		}
	}
	kill()
	// Keep counting while draining: the reader runs ahead of this loop, so
	// rows it already delivered must not be mistaken for rows never sent.
	for ev := range events {
		if id, ok := anyRowID(ev); ok && (ev.Op == cdc.OpRead || ev.Op == cdc.OpCreate) {
			seen[id] = true
		}
	}
	_ = first.Close()

	progress := hooks1.progress()
	checkpoint := progress[len(progress)-1]
	if checkpoint.key == "" || checkpoint.key == "null" {
		t.Fatalf("chunk checkpoint carries no key: %+v", checkpoint)
	}
	if _, completed, _ := hooks1.state3(); completed != 0 {
		t.Fatal("an interrupted chunked snapshot must not be recorded as complete")
	}
	partial := len(seen)
	if partial >= preloaded {
		t.Fatalf("the snapshot finished before the interruption (%d rows); lower the chunk size", partial)
	}

	// Second run: resume, which must continue the snapshot rather than redo it.
	hooks2 := &recordingHooks{}
	second := New(f.chunkedConfig(1000), "src", quietLogger())
	events2 := make(chan cdc.ChangeEvent, 8192)
	runCtx2, stop2 := context.WithCancel(ctx)
	defer stop2()
	go func() {
		defer close(events2)
		_ = second.ReadChanges(runCtx2, source.ReadRequest{
			From:  lastPosition,
			Hooks: hooks2,
			ResumeSnapshot: &source.SnapshotResume{
				Table: checkpoint.table,
				Key:   []byte(checkpoint.key),
			},
		}, events2)
	}()

	deadline := time.After(60 * time.Second)
	for len(seen) < preloaded {
		select {
		case ev, ok := <-events2:
			if !ok {
				t.Fatalf("reader stopped with %d of %d rows", len(seen), preloaded)
			}
			if id, ok := anyRowID(ev); ok && (ev.Op == cdc.OpRead || ev.Op == cdc.OpCreate) {
				seen[id] = true
			}
		case <-deadline:
			t.Fatalf("resume delivered %d of %d rows", len(seen), preloaded)
		}
	}

	for id := int64(1); id <= preloaded; id++ {
		if !seen[id] {
			t.Fatalf("row %d was never delivered across the two runs", id)
		}
	}
	// The table is only known to be finished once a chunk comes back short, so
	// completion lands just after the last row.
	waitDeadline := time.Now().Add(30 * time.Second)
	for {
		_, completed, _ := hooks2.state3()
		if completed == 1 {
			break
		}
		if completed > 1 {
			t.Fatalf("the resumed snapshot recorded %d completions, want exactly 1", completed)
		}
		if time.Now().After(waitDeadline) {
			t.Fatal("the resumed snapshot never recorded completion")
		}
		select {
		case <-events2:
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Logf("first run delivered %d rows, resume completed the remaining %d", partial, preloaded-partial)
}

// A table with no primary key cannot be paged through, and that must be said
// plainly rather than discovered as missing data later.
func TestChunkedSnapshotRefusesTablesWithoutAPrimaryKey(t *testing.T) {
	f := newFixture(t, "nopk")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	nopk := f.table + "_nopk"
	if _, err := f.conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (id bigint, note text)`, nopk)); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = f.conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+nopk)
	})

	cfg := f.chunkedConfig(100)
	cfg.Tables = append(cfg.Tables, "public."+nopk)

	reader := New(cfg, "src", quietLogger())
	err := reader.ReadChanges(ctx, source.ReadRequest{Hooks: &recordingHooks{}}, make(chan cdc.ChangeEvent, 1))
	if err == nil {
		t.Fatal("expected chunked mode to refuse a table with no primary key")
	}
	if got := err.Error(); !contains(got, "primary key") || !contains(got, "snapshot_mode") {
		t.Errorf("the error should name the problem and the way out, got: %v", err)
	}
}

// anyRowID pulls the row id from whichever image an event carries.
func anyRowID(ev cdc.ChangeEvent) (int64, bool) {
	values := ev.After
	if len(values) == 0 {
		values = ev.Before
	}
	raw, ok := values["id"]
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

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (len(sub) == 0 || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
