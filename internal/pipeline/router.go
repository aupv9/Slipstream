// Package pipeline owns leadership, fan-out to sinks, and offset commits.
package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/aupv9/slipstream/internal/cdc"
	"github.com/aupv9/slipstream/internal/config"
	"github.com/aupv9/slipstream/internal/controlplane"
	"github.com/aupv9/slipstream/internal/sink"
	"github.com/aupv9/slipstream/internal/source"
)

// progressStore is the slice of the control plane the router depends on.
// Keeping it an interface lets the fan-out and commit logic be tested without
// a database.
type progressStore interface {
	SaveOffset(ctx context.Context, pipelineID, holder, position string, commitTS time.Time) error
	AdvanceSinkCursor(ctx context.Context, pipelineID, holder, sinkName, position string, seq int64) error
}

// Router fans one ordered event stream out to every configured sink.
//
// Each sink has its own bounded queue and its own cursor, so a slow sink does
// not hold up a fast one — it only applies backpressure once its queue is
// full. The position that is safe to forget (stored as the pipeline offset and
// acknowledged to the source) is the *slowest* sink's position, never an
// average and never the read-ahead position.
type Router struct {
	pipelineID string
	holder     string
	store      progressStore
	log        *slog.Logger
	interval   time.Duration
	acker      source.Acker

	workers []*worker

	mu         sync.Mutex
	positions  map[int64]string // seq -> source position, pruned on commit
	lastOffset string
}

// NewRouter wires a router for one pipeline run.
func NewRouter(pipelineID, holder string, store progressStore, cfgs []config.SinkConfig, sinks []sink.Sink, interval time.Duration, acker source.Acker, log *slog.Logger) *Router {
	r := &Router{
		pipelineID: pipelineID,
		holder:     holder,
		store:      store,
		log:        log.With("component", "router"),
		interval:   interval,
		acker:      acker,
		positions:  make(map[int64]string),
	}
	for i, s := range sinks {
		r.workers = append(r.workers, newWorker(cfgs[i], s, log))
	}
	return r
}

type seqEvent struct {
	seq int64
	ev  cdc.ChangeEvent
}

// Run consumes in until it is closed or ctx is cancelled.
func (r *Router) Run(ctx context.Context, in <-chan cdc.ChangeEvent) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	g, gctx := errgroup.WithContext(runCtx)
	for _, w := range r.workers {
		g.Go(func() error { return w.run(gctx) })
	}
	g.Go(func() error { return r.feed(gctx, in) })

	stop := make(chan struct{})
	commitDone := make(chan struct{})
	var commitErr error
	go func() {
		defer close(commitDone)
		commitErr = r.commitLoop(runCtx, stop)
		if commitErr != nil {
			// Losing the fence means another instance is the leader now; stop
			// reading the source immediately.
			cancel()
		}
	}()

	ingestErr := g.Wait()
	close(stop)
	<-commitDone

	if commitErr == nil {
		// A last commit on clean shutdown keeps the replay window small. The
		// context is detached because ctx is usually already cancelled here.
		final, fcancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		if err := r.commitOnce(final); err != nil && !errors.Is(err, controlplane.ErrLeaseLost) {
			r.log.Warn("final commit failed; events already delivered will be replayed", "err", err)
		}
		fcancel()
	}

	if ingestErr != nil {
		return ingestErr
	}
	return commitErr
}

// feed stamps each event with a monotonic sequence number and hands it to every
// worker. Sending blocks when a queue is full, which is how the slowest sink
// throttles the reader.
func (r *Router) feed(ctx context.Context, in <-chan cdc.ChangeEvent) error {
	defer func() {
		for _, w := range r.workers {
			close(w.queue)
		}
	}()

	var seq int64
	for {
		select {
		case ev, ok := <-in:
			if !ok {
				return nil
			}
			seq++
			r.recordPosition(seq, ev.Position)
			for _, w := range r.workers {
				select {
				case w.queue <- seqEvent{seq: seq, ev: ev}:
				case <-ctx.Done():
					return nil
				}
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (r *Router) recordPosition(seq int64, position string) {
	r.mu.Lock()
	r.positions[seq] = position
	r.mu.Unlock()
}

func (r *Router) commitLoop(ctx context.Context, stop <-chan struct{}) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := r.commitOnce(ctx); err != nil {
				if errors.Is(err, controlplane.ErrLeaseLost) {
					r.log.Error("lease lost; stopping pipeline", "err", err)
					return err
				}
				// A transient control-plane error only delays the commit; the
				// next tick retries and worst case we replay from an older
				// offset after a restart.
				r.log.Warn("commit failed", "err", err)
			}
		case <-stop:
			return nil
		case <-ctx.Done():
			return nil
		}
	}
}

// commitOnce persists every sink cursor, then promotes the slowest one to the
// pipeline offset and acknowledges it to the source.
func (r *Router) commitOnce(ctx context.Context) error {
	var (
		minSeq int64 = -1
		minPos string
		idle   bool
	)
	for _, w := range r.workers {
		// Persist progress for every sink, idle ones included, so lag stays
		// observable in the control plane.
		if changed, cseq, cpos := w.pendingCursor(); changed {
			if err := r.store.AdvanceSinkCursor(ctx, r.pipelineID, r.holder, w.sink.Name(), cpos, cseq); err != nil {
				return err
			}
			w.cursorPersisted(cseq)
		}

		seq, pos := w.progress()
		if seq == 0 {
			// This sink has not durably accepted anything yet, so nothing is
			// safe to forget.
			idle = true
			continue
		}
		if minSeq == -1 || seq < minSeq {
			minSeq, minPos = seq, pos
		}
	}
	if idle || minSeq <= 0 {
		return nil
	}

	if minPos != "" && minPos != r.lastOffset {
		if err := r.store.SaveOffset(ctx, r.pipelineID, r.holder, minPos, time.Time{}); err != nil {
			return err
		}
		r.lastOffset = minPos
		if r.acker != nil {
			r.acker.Ack(minPos)
		}
		r.log.Debug("offset committed", "position", minPos, "seq", minSeq)
	}

	r.prune(minSeq)
	return nil
}

func (r *Router) prune(upTo int64) {
	r.mu.Lock()
	for seq := range r.positions {
		if seq <= upTo {
			delete(r.positions, seq)
		}
	}
	r.mu.Unlock()
}

// worker drives one sink.
type worker struct {
	cfg   config.SinkConfig
	sink  sink.Sink
	queue chan seqEvent
	log   *slog.Logger

	mu           sync.Mutex
	doneSeq      int64
	donePos      string
	persistedSeq int64
}

func newWorker(cfg config.SinkConfig, s sink.Sink, log *slog.Logger) *worker {
	return &worker{
		cfg:   cfg,
		sink:  s,
		queue: make(chan seqEvent, cfg.QueueSize),
		log:   log.With("sink", s.Name()),
	}
}

func (w *worker) progress() (int64, string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.doneSeq, w.donePos
}

func (w *worker) pendingCursor() (bool, int64, string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.doneSeq > w.persistedSeq, w.doneSeq, w.donePos
}

func (w *worker) cursorPersisted(seq int64) {
	w.mu.Lock()
	if seq > w.persistedSeq {
		w.persistedSeq = seq
	}
	w.mu.Unlock()
}

func (w *worker) setProgress(seq int64, pos string) {
	w.mu.Lock()
	if seq > w.doneSeq {
		w.doneSeq, w.donePos = seq, pos
	}
	w.mu.Unlock()
}

// run batches from the queue and writes until the queue is closed or ctx ends.
func (w *worker) run(ctx context.Context) error {
	batch := make([]cdc.ChangeEvent, 0, w.cfg.BatchMaxEvents)

	for {
		batch = batch[:0]

		// Block for the first event of the batch.
		first, ok := recv(ctx, w.queue)
		if !ok {
			return nil
		}
		batch = append(batch, first.ev)
		lastSeq := first.seq

		deadline := time.After(w.cfg.BatchMaxWait.D())
		closed := false
	fill:
		for len(batch) < w.cfg.BatchMaxEvents {
			select {
			case se, more := <-w.queue:
				if !more {
					closed = true
					break fill
				}
				batch = append(batch, se.ev)
				lastSeq = se.seq
			case <-deadline:
				break fill
			case <-ctx.Done():
				return nil
			}
		}

		if err := w.flush(ctx, batch, lastSeq); err != nil {
			return err
		}
		if closed {
			return nil
		}
	}
}

// flush writes a batch, retrying with capped exponential backoff. It never
// gives up: dropping a batch would break the at-least-once guarantee, so a
// permanently broken sink stalls its own pipeline loudly instead of losing
// data silently.
func (w *worker) flush(ctx context.Context, batch []cdc.ChangeEvent, lastSeq int64) error {
	if len(batch) == 0 {
		return nil
	}
	lastPos := batch[len(batch)-1].Position
	delay := w.cfg.RetryInitial.D()

	for attempt := 1; ; attempt++ {
		err := w.sink.Write(ctx, batch)
		if err == nil {
			w.setProgress(lastSeq, lastPos)
			w.log.Debug("batch written", "events", len(batch), "seq", lastSeq)
			return nil
		}
		if ctx.Err() != nil {
			// Shutting down: this batch was never acknowledged, so it will be
			// replayed after resume.
			return nil
		}
		w.log.Error("sink write failed; retrying", "attempt", attempt, "events", len(batch), "backoff", delay, "err", err)

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil
		}
		if delay = delay * 2; delay > w.cfg.RetryMax.D() {
			delay = w.cfg.RetryMax.D()
		}
	}
}

func recv(ctx context.Context, q <-chan seqEvent) (seqEvent, bool) {
	select {
	case se, ok := <-q:
		return se, ok
	case <-ctx.Done():
		return seqEvent{}, false
	}
}
