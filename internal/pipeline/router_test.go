package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aupv9/slipstream/internal/cdc"
	"github.com/aupv9/slipstream/internal/config"
	"github.com/aupv9/slipstream/internal/sink"
)

// fakeStore records what the router commits.
type fakeStore struct {
	mu       sync.Mutex
	offsets  []string
	cursors  map[string]string
	parked   []parkedEvent
	parkFail error
}

type parkedEvent struct {
	sink     string
	position string
	attempts int
	cause    string
}

func newFakeStore() *fakeStore {
	return &fakeStore{cursors: map[string]string{}}
}

func (f *fakeStore) SaveOffset(_ context.Context, _, _, position string, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.offsets = append(f.offsets, position)
	return nil
}

func (f *fakeStore) AdvanceSinkCursor(_ context.Context, _, _, sinkName, position string, _ int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cursors[sinkName] = position
	return nil
}

func (f *fakeStore) DeadLetter(_ context.Context, _, _, sinkName, position string, attempts int, cause string, _ []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.parkFail != nil {
		return f.parkFail
	}
	f.parked = append(f.parked, parkedEvent{sink: sinkName, position: position, attempts: attempts, cause: cause})
	return nil
}

func (f *fakeStore) deadLettered() []parkedEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]parkedEvent(nil), f.parked...)
}

func (f *fakeStore) lastOffset() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.offsets) == 0 {
		return ""
	}
	return f.offsets[len(f.offsets)-1]
}

func (f *fakeStore) cursor(name string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cursors[name]
}

// fakeSink records the events it accepts and can be made to fail or block.
type fakeSink struct {
	name string

	mu       sync.Mutex
	got      []cdc.ChangeEvent
	attempts int

	failFirst int           // fail this many Write calls before succeeding
	delay     time.Duration // per-write delay, to make a sink the slow one
	gate      chan struct{} // if set, each Write waits for a token
	rejectID  int           // if non-zero, any batch containing this id fails
}

func (s *fakeSink) Name() string { return s.name }

func (s *fakeSink) Write(ctx context.Context, batch []cdc.ChangeEvent) error {
	if s.gate != nil {
		select {
		case <-s.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.delay > 0 {
		select {
		case <-time.After(s.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	if s.attempts <= s.failFirst {
		return fmt.Errorf("sink %s: synthetic failure %d", s.name, s.attempts)
	}
	if s.rejectID != 0 {
		for _, ev := range batch {
			if id, ok := ev.After["id"].(int); ok && id == s.rejectID {
				return fmt.Errorf("sink %s: refusing id %d", s.name, s.rejectID)
			}
		}
	}
	s.got = append(s.got, batch...)
	return nil
}

func (s *fakeSink) Close() error { return nil }

func (s *fakeSink) events() []cdc.ChangeEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]cdc.ChangeEvent(nil), s.got...)
}

func (s *fakeSink) writeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

func sinkCfg(name string) config.SinkConfig {
	return config.SinkConfig{
		Name:           name,
		Type:           "fake",
		QueueSize:      64,
		BatchMaxEvents: 10,
		BatchMaxWait:   config.Duration(5 * time.Millisecond),
		RetryInitial:   config.Duration(time.Millisecond),
		RetryMax:       config.Duration(5 * time.Millisecond),
	}
}

func events(n int) []cdc.ChangeEvent {
	out := make([]cdc.ChangeEvent, n)
	for i := range out {
		out[i] = cdc.ChangeEvent{
			SourceID: "src",
			Schema:   "public",
			Table:    "t",
			Op:       cdc.OpCreate,
			After:    map[string]any{"id": i + 1},
			Position: fmt.Sprintf("0/%04X", i+1),
		}
	}
	return out
}

func feedAndRun(t *testing.T, r *Router, in []cdc.ChangeEvent) error {
	t.Helper()
	ch := make(chan cdc.ChangeEvent, len(in))
	for _, ev := range in {
		ch <- ev
	}
	close(ch)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return r.Run(ctx, ch)
}

func TestRouterDeliversEveryEventToEverySinkInOrder(t *testing.T) {
	store := newFakeStore()
	a := &fakeSink{name: "a"}
	b := &fakeSink{name: "b"}
	cfgs := []config.SinkConfig{sinkCfg("a"), sinkCfg("b")}

	r := NewRouter("p1", "inst1", store, cfgs, []sink.Sink{a, b},
		5*time.Millisecond, nil, testLogger())

	in := events(25)
	if err := feedAndRun(t, r, in); err != nil {
		t.Fatalf("router returned: %v", err)
	}

	for _, s := range []*fakeSink{a, b} {
		got := s.events()
		if len(got) != len(in) {
			t.Fatalf("sink %s got %d events, want %d", s.name, len(got), len(in))
		}
		for i := range got {
			if got[i].Position != in[i].Position {
				t.Fatalf("sink %s event %d out of order: got %s want %s",
					s.name, i, got[i].Position, in[i].Position)
			}
		}
	}

	if want := in[len(in)-1].Position; store.lastOffset() != want {
		t.Fatalf("committed offset %q, want %q", store.lastOffset(), want)
	}
}

// The offset that may be forgotten is the slowest sink's position, never the
// fastest one's: promoting the fast sink would lose data for the slow one on
// failover.
func TestRouterCommitsSlowestSinkPosition(t *testing.T) {
	store := newFakeStore()
	fast := &fakeSink{name: "fast"}
	slow := &fakeSink{name: "slow", gate: make(chan struct{}, 1)}
	cfgs := []config.SinkConfig{sinkCfg("fast"), sinkCfg("slow")}

	// One batch at a time for the slow sink.
	cfgs[1].BatchMaxEvents = 1
	cfgs[0].BatchMaxEvents = 1

	r := NewRouter("p1", "inst1", store, cfgs, []sink.Sink{fast, slow},
		5*time.Millisecond, nil, testLogger())

	in := events(5)
	ch := make(chan cdc.ChangeEvent, len(in))
	for _, ev := range in {
		ch <- ev
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- r.Run(ctx, ch) }()

	// Let the slow sink accept exactly the first two events.
	slow.gate <- struct{}{}
	slow.gate <- struct{}{}

	deadline := time.After(5 * time.Second)
	for {
		if store.cursor("fast") == in[len(in)-1].Position && store.cursor("slow") == in[1].Position {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("cursors did not settle: fast=%q slow=%q offset=%q",
				store.cursor("fast"), store.cursor("slow"), store.lastOffset())
		case <-time.After(5 * time.Millisecond):
		}
	}

	if got, want := store.lastOffset(), in[1].Position; got != want {
		t.Fatalf("offset %q follows the fast sink; want the slow sink's %q", got, want)
	}

	// Drain the rest so the run can finish.
	go func() {
		for range in {
			slow.gate <- struct{}{}
		}
	}()
	close(ch)
	if err := <-done; err != nil {
		t.Fatalf("router returned: %v", err)
	}
}

// A failing sink must be retried rather than skipped: at-least-once means no
// batch is ever dropped.
func TestRouterRetriesFailedWritesAndDoesNotAdvancePastThem(t *testing.T) {
	store := newFakeStore()
	flaky := &fakeSink{name: "flaky", failFirst: 3}
	cfgs := []config.SinkConfig{sinkCfg("flaky")}
	cfgs[0].BatchMaxEvents = 100

	r := NewRouter("p1", "inst1", store, cfgs, []sink.Sink{flaky},
		5*time.Millisecond, nil, testLogger())

	in := events(4)
	if err := feedAndRun(t, r, in); err != nil {
		t.Fatalf("router returned: %v", err)
	}

	if got := flaky.writeCount(); got < 4 {
		t.Fatalf("sink was called %d times, want at least 4 (3 failures + 1 success)", got)
	}
	if got := len(flaky.events()); got != len(in) {
		t.Fatalf("sink accepted %d events after retries, want %d", got, len(in))
	}
	if want := in[len(in)-1].Position; store.lastOffset() != want {
		t.Fatalf("committed offset %q, want %q", store.lastOffset(), want)
	}
}

// Nothing may be committed before every sink has accepted something: the
// offset is the resume point, so an early commit would silently skip events.
func TestRouterCommitsNothingWhileASinkHasAcceptedNothing(t *testing.T) {
	store := newFakeStore()
	ok := &fakeSink{name: "ok"}
	stuck := &fakeSink{name: "stuck", gate: make(chan struct{})}
	cfgs := []config.SinkConfig{sinkCfg("ok"), sinkCfg("stuck")}

	r := NewRouter("p1", "inst1", store, cfgs, []sink.Sink{ok, stuck},
		2*time.Millisecond, nil, testLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	ch := make(chan cdc.ChangeEvent, 4)
	for _, ev := range events(4) {
		ch <- ev
	}

	if err := r.Run(ctx, ch); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("router returned: %v", err)
	}
	if got := store.lastOffset(); got != "" {
		t.Fatalf("committed offset %q while one sink was stuck; want no commit", got)
	}
}

// The acker must only ever see the committed (slowest) position, because the
// source releases its log up to whatever we acknowledge.
func TestRouterAcksOnlyCommittedPositions(t *testing.T) {
	store := newFakeStore()
	s := &fakeSink{name: "a"}
	acker := &recordingAcker{}

	r := NewRouter("p1", "inst1", store, []config.SinkConfig{sinkCfg("a")},
		[]sink.Sink{s}, 5*time.Millisecond, acker, testLogger())

	in := events(3)
	if err := feedAndRun(t, r, in); err != nil {
		t.Fatalf("router returned: %v", err)
	}

	acked := acker.positions()
	if len(acked) == 0 {
		t.Fatal("nothing was acknowledged to the source")
	}
	if last := acked[len(acked)-1]; last != in[len(in)-1].Position {
		t.Fatalf("last ack %q, want %q", last, in[len(in)-1].Position)
	}
}

type recordingAcker struct {
	mu   sync.Mutex
	seen []string
}

func (a *recordingAcker) Ack(position string) {
	a.mu.Lock()
	a.seen = append(a.seen, position)
	a.mu.Unlock()
}

func (a *recordingAcker) positions() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.seen...)
}

// A sink configured to dead-letter must park what it cannot deliver and let the
// pipeline move on, rather than stalling behind one poison event.
func TestRouterDeadLettersAfterMaxAttempts(t *testing.T) {
	store := newFakeStore()
	broken := &fakeSink{name: "broken", failFirst: 1 << 30} // never accepts anything
	cfg := sinkCfg("broken")
	cfg.BatchMaxEvents = 1
	cfg.OnFailure = config.OnFailureDeadLetter
	cfg.MaxAttempts = 2

	r := NewRouter("p1", "inst1", store, []config.SinkConfig{cfg}, []sink.Sink{broken},
		5*time.Millisecond, nil, testLogger())

	in := events(3)
	if err := feedAndRun(t, r, in); err != nil {
		t.Fatalf("router returned: %v", err)
	}

	parked := store.deadLettered()
	if len(parked) != len(in) {
		t.Fatalf("parked %d events, want %d", len(parked), len(in))
	}
	for i, p := range parked {
		if p.position != in[i].Position {
			t.Errorf("parked[%d] position = %q, want %q", i, p.position, in[i].Position)
		}
		if p.attempts != 2 {
			t.Errorf("parked[%d] attempts = %d, want the configured limit 2", i, p.attempts)
		}
		if p.cause == "" {
			t.Errorf("parked[%d] recorded no cause", i)
		}
	}
	// The pipeline must have advanced past them, not stalled.
	if want := in[len(in)-1].Position; store.lastOffset() != want {
		t.Fatalf("offset = %q, want %q: dead-lettering should let the pipeline advance",
			store.lastOffset(), want)
	}
}

// One bad event in a batch must not take the good ones down with it.
func TestRouterDeadLetterIsolatesOnlyTheBadEvent(t *testing.T) {
	store := newFakeStore()
	picky := &fakeSink{name: "picky", rejectID: 3}
	cfg := sinkCfg("picky")
	cfg.BatchMaxEvents = 100
	cfg.OnFailure = config.OnFailureDeadLetter
	cfg.MaxAttempts = 2

	r := NewRouter("p1", "inst1", store, []config.SinkConfig{cfg}, []sink.Sink{picky},
		5*time.Millisecond, nil, testLogger())

	in := events(5)
	if err := feedAndRun(t, r, in); err != nil {
		t.Fatalf("router returned: %v", err)
	}

	parked := store.deadLettered()
	if len(parked) != 1 {
		t.Fatalf("parked %d events, want exactly the one the sink refuses: %+v", len(parked), parked)
	}
	if parked[0].position != in[2].Position {
		t.Errorf("parked the wrong event: %q, want %q", parked[0].position, in[2].Position)
	}

	delivered := picky.events()
	if len(delivered) != len(in)-1 {
		t.Fatalf("sink accepted %d events, want %d (all but the rejected one)", len(delivered), len(in)-1)
	}
	for _, ev := range delivered {
		if id, _ := ev.After["id"].(int); id == 3 {
			t.Fatal("the rejected event was delivered after all")
		}
	}
}

// If the dead-letter write itself fails, the pipeline must not advance: losing
// both the delivery and the record of it would be silent data loss.
func TestRouterDoesNotAdvanceWhenParkingFails(t *testing.T) {
	store := newFakeStore()
	store.parkFail = errors.New("control plane unavailable")
	broken := &fakeSink{name: "broken", failFirst: 1 << 30}
	cfg := sinkCfg("broken")
	cfg.BatchMaxEvents = 1
	cfg.OnFailure = config.OnFailureDeadLetter
	cfg.MaxAttempts = 1

	r := NewRouter("p1", "inst1", store, []config.SinkConfig{cfg}, []sink.Sink{broken},
		5*time.Millisecond, nil, testLogger())

	err := feedAndRun(t, r, events(2))
	if err == nil {
		t.Fatal("expected the run to fail so the events are replayed after a restart")
	}
	if store.lastOffset() != "" {
		t.Fatalf("offset advanced to %q despite the parking failure", store.lastOffset())
	}
}
