package controlplane

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

// These tests need a real Postgres: the guarantees under test are Postgres'
// own (a single UPDATE is atomic, so two instances cannot both win a lease),
// and a fake would prove nothing.
//
//	SLIPSTREAM_TEST_CP_DSN=postgres://user@host/db go test ./internal/controlplane/
func testStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("SLIPSTREAM_TEST_CP_DSN")
	if dsn == "" {
		t.Skip("set SLIPSTREAM_TEST_CP_DSN to run control-plane tests")
	}

	ctx := context.Background()
	store, err := Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := store.Migrate(ctx, Schema); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(store.Close)
	return store
}

// cleanup removes a pipeline's rows so reruns start clean.
func cleanup(t *testing.T, s *Store, pipeline string) {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		for _, table := range []string{"offsets", "leases", "sink_cursor"} {
			if _, err := s.pool.Exec(ctx, "DELETE FROM "+table+" WHERE pipeline_id = $1", pipeline); err != nil {
				t.Logf("cleanup %s: %v", table, err)
			}
		}
	})
}

func TestOnlyOneInstanceCanHoldTheLease(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	pipeline := "test-exclusive"
	cleanup(t, s, pipeline)

	first, err := s.AcquireOrRenew(ctx, pipeline, "inst-a", 30*time.Second)
	if err != nil {
		t.Fatalf("acquire a: %v", err)
	}
	if !first {
		t.Fatal("inst-a should have taken a free lease")
	}

	second, err := s.AcquireOrRenew(ctx, pipeline, "inst-b", 30*time.Second)
	if err != nil {
		t.Fatalf("acquire b: %v", err)
	}
	if second {
		t.Fatal("inst-b took a lease that inst-a already holds: split brain")
	}

	// The holder may renew freely.
	again, err := s.AcquireOrRenew(ctx, pipeline, "inst-a", 30*time.Second)
	if err != nil || !again {
		t.Fatalf("holder could not renew: ok=%v err=%v", again, err)
	}

	holder, err := s.LeaseHolder(ctx, pipeline)
	if err != nil {
		t.Fatalf("lease holder: %v", err)
	}
	if holder != "inst-a" {
		t.Fatalf("holder = %q, want inst-a", holder)
	}
}

func TestLeaseIsTakenOverAfterItExpires(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	pipeline := "test-expiry"
	cleanup(t, s, pipeline)

	if ok, err := s.AcquireOrRenew(ctx, pipeline, "inst-a", 300*time.Millisecond); err != nil || !ok {
		t.Fatalf("acquire a: ok=%v err=%v", ok, err)
	}
	if ok, _ := s.AcquireOrRenew(ctx, pipeline, "inst-b", time.Second); ok {
		t.Fatal("inst-b took an unexpired lease")
	}

	time.Sleep(400 * time.Millisecond)

	ok, err := s.AcquireOrRenew(ctx, pipeline, "inst-b", time.Second)
	if err != nil {
		t.Fatalf("acquire b: %v", err)
	}
	if !ok {
		t.Fatal("standby did not take over an expired lease")
	}
}

func TestReleaseHandsOverImmediately(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	pipeline := "test-release"
	cleanup(t, s, pipeline)

	if ok, err := s.AcquireOrRenew(ctx, pipeline, "inst-a", time.Minute); err != nil || !ok {
		t.Fatalf("acquire a: ok=%v err=%v", ok, err)
	}
	// A release by a non-holder must not expire someone else's lease.
	if err := s.Release(ctx, pipeline, "inst-b"); err != nil {
		t.Fatalf("release by non-holder: %v", err)
	}
	if ok, _ := s.AcquireOrRenew(ctx, pipeline, "inst-b", time.Minute); ok {
		t.Fatal("a non-holder's release expired the lease")
	}

	if err := s.Release(ctx, pipeline, "inst-a"); err != nil {
		t.Fatalf("release: %v", err)
	}
	ok, err := s.AcquireOrRenew(ctx, pipeline, "inst-b", time.Minute)
	if err != nil {
		t.Fatalf("acquire b: %v", err)
	}
	if !ok {
		t.Fatal("standby did not take over a released lease")
	}
}

func TestConcurrentAcquireElectsExactlyOneLeader(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	pipeline := "test-race"
	cleanup(t, s, pipeline)

	const instances = 16
	results := make(chan bool, instances)
	start := make(chan struct{})
	for i := range instances {
		go func(i int) {
			<-start
			ok, err := s.AcquireOrRenew(ctx, pipeline, string(rune('a'+i)), 30*time.Second)
			if err != nil {
				t.Errorf("acquire: %v", err)
			}
			results <- ok
		}(i)
	}
	close(start)

	won := 0
	for range instances {
		if <-results {
			won++
		}
	}
	if won != 1 {
		t.Fatalf("%d instances won the lease, want exactly 1", won)
	}
}

func TestOffsetWritesAreFencedByTheLease(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	pipeline := "test-fencing"
	cleanup(t, s, pipeline)

	if ok, err := s.AcquireOrRenew(ctx, pipeline, "old-leader", 300*time.Millisecond); err != nil || !ok {
		t.Fatalf("acquire: ok=%v err=%v", ok, err)
	}
	if err := s.SaveOffset(ctx, pipeline, "old-leader", "0/1000", time.Now()); err != nil {
		t.Fatalf("save offset as leader: %v", err)
	}

	// The old leader stalls, its lease expires, a standby takes over and makes
	// progress.
	time.Sleep(400 * time.Millisecond)
	if ok, err := s.AcquireOrRenew(ctx, pipeline, "new-leader", time.Minute); err != nil || !ok {
		t.Fatalf("takeover: ok=%v err=%v", ok, err)
	}
	if err := s.SaveOffset(ctx, pipeline, "new-leader", "0/2000", time.Now()); err != nil {
		t.Fatalf("save offset as new leader: %v", err)
	}

	// The zombie must not be able to rewind the offset.
	err := s.SaveOffset(ctx, pipeline, "old-leader", "0/1500", time.Now())
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale leader wrote an offset: err=%v", err)
	}
	if err := s.AdvanceSinkCursor(ctx, pipeline, "old-leader", "hook", "0/1500", 5); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale leader advanced a sink cursor: err=%v", err)
	}

	pos, found, err := s.LoadOffset(ctx, pipeline)
	if err != nil || !found {
		t.Fatalf("load offset: found=%v err=%v", found, err)
	}
	if pos != "0/2000" {
		t.Fatalf("offset = %q, want the new leader's 0/2000", pos)
	}
}

func TestOffsetAndSinkCursorRoundTrip(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	pipeline := "test-roundtrip"
	cleanup(t, s, pipeline)

	if _, found, err := s.LoadOffset(ctx, pipeline); err != nil || found {
		t.Fatalf("a fresh pipeline must have no offset: found=%v err=%v", found, err)
	}
	if ok, err := s.AcquireOrRenew(ctx, pipeline, "inst", time.Minute); err != nil || !ok {
		t.Fatalf("acquire: ok=%v err=%v", ok, err)
	}

	if err := s.SaveOffset(ctx, pipeline, "inst", "0/ABCD", time.Now()); err != nil {
		t.Fatalf("save offset: %v", err)
	}
	if err := s.SaveOffset(ctx, pipeline, "inst", "0/BCDE", time.Time{}); err != nil {
		t.Fatalf("save offset without commit_ts: %v", err)
	}
	pos, found, err := s.LoadOffset(ctx, pipeline)
	if err != nil || !found || pos != "0/BCDE" {
		t.Fatalf("offset = %q found=%v err=%v", pos, found, err)
	}

	for _, c := range []SinkCursor{{Sink: "hook", Position: "0/BCDE", Seq: 9}, {Sink: "mirror", Position: "0/ABCD", Seq: 4}} {
		if err := s.AdvanceSinkCursor(ctx, pipeline, "inst", c.Sink, c.Position, c.Seq); err != nil {
			t.Fatalf("advance %s: %v", c.Sink, err)
		}
	}
	cursors, err := s.LoadSinkCursors(ctx, pipeline)
	if err != nil {
		t.Fatalf("load cursors: %v", err)
	}
	if len(cursors) != 2 {
		t.Fatalf("got %d cursors, want 2", len(cursors))
	}
	if cursors[0].Sink != "hook" || cursors[0].Seq != 9 || cursors[1].Position != "0/ABCD" {
		t.Fatalf("unexpected cursors: %+v", cursors)
	}
}
