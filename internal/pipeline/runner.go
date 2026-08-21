package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/aupv9/slipstream/internal/cdc"
	"github.com/aupv9/slipstream/internal/config"
	"github.com/aupv9/slipstream/internal/controlplane"
	"github.com/aupv9/slipstream/internal/sink"
	"github.com/aupv9/slipstream/internal/source"
)

// eventBuffer is the handover buffer between reader and router. It only needs
// to absorb jitter; the real buffering is each sink's own queue.
const eventBuffer = 1024

// Runner is one process: it competes for the pipeline's lease and, while it
// holds it, runs the source reader and the sink fan-out. Run two or more of
// these against the same control plane and the pipeline is highly available.
type Runner struct {
	cfg   *config.Config
	store *controlplane.Store
	log   *slog.Logger
}

// NewRunner builds a runner.
func NewRunner(cfg *config.Config, store *controlplane.Store, log *slog.Logger) *Runner {
	return &Runner{
		cfg:   cfg,
		store: store,
		log:   log.With("pipeline", cfg.Pipeline.ID, "instance", cfg.InstanceID),
	}
}

// Run blocks until ctx is cancelled, alternating between standby and leader.
func (r *Runner) Run(ctx context.Context) error {
	renew := r.cfg.ControlPlane.LeaseRenew.D()
	standby := false

	for {
		if ctx.Err() != nil {
			return nil
		}

		leader, err := r.store.AcquireOrRenew(ctx, r.cfg.Pipeline.ID, r.cfg.InstanceID, r.cfg.ControlPlane.LeaseTTL.D())
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			r.log.Error("lease acquisition failed", "err", err)
			if !sleep(ctx, renew) {
				return nil
			}
			continue
		}

		if !leader {
			if !standby {
				holder, _ := r.store.LeaseHolder(ctx, r.cfg.Pipeline.ID)
				r.log.Info("standby: another instance holds the lease", "holder", holder)
				standby = true
			}
			if !sleep(ctx, renew) {
				return nil
			}
			continue
		}

		standby = false
		r.log.Info("acquired lease; becoming leader")
		if err := r.lead(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			r.log.Error("pipeline stopped", "err", err)
		}
		if ctx.Err() != nil {
			return nil
		}
		// Back off before contending again so a persistently failing instance
		// lets a healthy standby take over.
		if !sleep(ctx, renew) {
			return nil
		}
	}
}

// lead runs the pipeline while heartbeating the lease. Losing the lease
// cancels the run, so the source connection is dropped before the successor
// opens its own.
func (r *Runner) lead(ctx context.Context) error {
	leadCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		r.heartbeat(leadCtx, cancel)
	}()

	err := r.runPipeline(leadCtx)

	cancel()
	<-hbDone

	// Expire our own lease so a standby can take over immediately instead of
	// waiting out the TTL. Guarded by holder, so it cannot expire someone
	// else's lease.
	release, rcancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	if rerr := r.store.Release(release, r.cfg.Pipeline.ID, r.cfg.InstanceID); rerr != nil {
		r.log.Warn("could not release lease; standby will wait out the TTL", "err", rerr)
	}
	rcancel()

	return err
}

// heartbeat renews the lease. It tolerates transient control-plane errors up
// to the TTL, but cancels the run immediately if another instance has taken
// the lease.
func (r *Runner) heartbeat(ctx context.Context, cancel context.CancelFunc) {
	ttl := r.cfg.ControlPlane.LeaseTTL.D()
	renew := r.cfg.ControlPlane.LeaseRenew.D()
	ticker := time.NewTicker(renew)
	defer ticker.Stop()

	lastOK := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		held, err := r.store.AcquireOrRenew(ctx, r.cfg.Pipeline.ID, r.cfg.InstanceID, ttl)
		switch {
		case err != nil:
			if ctx.Err() != nil {
				return
			}
			r.log.Warn("lease renewal failed", "err", err, "since_last_ok", time.Since(lastOK))
			if time.Since(lastOK) >= ttl {
				r.log.Error("lease has expired; stopping pipeline to avoid two readers")
				cancel()
				return
			}
		case !held:
			r.log.Error("lease taken by another instance; stopping pipeline")
			cancel()
			return
		default:
			lastOK = time.Now()
		}
	}
}

// runPipeline reads from the source into the router until ctx ends.
func (r *Runner) runPipeline(ctx context.Context) error {
	reader, err := buildReader(r.cfg.Pipeline.Source, r.log)
	if err != nil {
		return err
	}
	defer func() { _ = reader.Close() }()

	sinks, err := buildSinks(ctx, r.cfg.Pipeline.Sinks)
	if err != nil {
		return err
	}
	defer closeSinks(sinks, r.log)

	from, found, err := r.store.LoadOffset(ctx, r.cfg.Pipeline.ID)
	if err != nil {
		return err
	}
	if found {
		r.log.Info("resuming", "position", from)
	} else {
		r.log.Info("no stored offset; the reader will take an initial snapshot")
	}

	if cursors, cerr := r.store.LoadSinkCursors(ctx, r.cfg.Pipeline.ID); cerr == nil {
		for _, c := range cursors {
			r.log.Info("previous sink progress", "sink", c.Sink, "position", c.Position)
		}
	}

	var acker source.Acker
	if a, ok := reader.(source.Acker); ok {
		acker = a
	}

	router := NewRouter(r.cfg.Pipeline.ID, r.cfg.InstanceID, r.store,
		r.cfg.Pipeline.Sinks, sinks, r.cfg.Pipeline.CommitInterval.D(), acker, r.log)

	events := make(chan cdc.ChangeEvent, eventBuffer)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(events)
		rerr := reader.ReadChanges(gctx, from, events)
		if rerr != nil && !errors.Is(rerr, context.Canceled) {
			return rerr
		}
		return nil
	})
	g.Go(func() error { return router.Run(gctx, events) })

	return g.Wait()
}

func closeSinks(sinks []sink.Sink, log *slog.Logger) {
	for _, s := range sinks {
		if err := s.Close(); err != nil {
			log.Warn("closing sink failed", "sink", s.Name(), "err", err)
		}
	}
}

// sleep waits for d, reporting false if ctx ended first.
func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}
