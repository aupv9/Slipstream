package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/aupv9/slipstream/internal/cdc"
	"github.com/aupv9/slipstream/internal/config"
	"github.com/aupv9/slipstream/internal/controlplane"
	"github.com/aupv9/slipstream/internal/observability"
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
	instanceID string
	cp         config.ControlPlane
	pipeline   config.Pipeline
	store      *controlplane.Store
	metrics    *observability.Registry
	health     *observability.Health
	log        *slog.Logger
}

// NewRunner builds a runner for one pipeline. metrics and health may be nil
// when the endpoints are disabled.
func NewRunner(instanceID string, cp config.ControlPlane, pipeline config.Pipeline, store *controlplane.Store, metrics *observability.Registry, health *observability.Health, log *slog.Logger) *Runner {
	if health == nil {
		health = &observability.Health{}
	}
	return &Runner{
		instanceID: instanceID,
		cp:         cp,
		pipeline:   pipeline,
		store:      store,
		metrics:    metrics,
		health:     health,
		log:        log.With("pipeline", pipeline.ID, "instance", instanceID),
	}
}

// pipelineLabels are the metric labels for this runner.
func (r *Runner) pipelineLabels() []string {
	return []string{"pipeline", r.pipeline.ID, "instance", r.instanceID}
}

// setRole publishes whether this instance is the leader.
func (r *Runner) setRole(leader bool) {
	r.health.SetRole(r.pipeline.ID, leader)
	var v int64
	if leader {
		v = 1
	}
	r.metrics.Set(observability.MetricLeader, r.pipelineLabels(), v)
}

// Run blocks until ctx is cancelled, alternating between standby and leader.
func (r *Runner) Run(ctx context.Context) error {
	renew := r.cp.LeaseRenew.D()
	standby := false

	for {
		if ctx.Err() != nil {
			return nil
		}

		leader, err := r.store.AcquireOrRenew(ctx, r.pipeline.ID, r.instanceID, r.cp.LeaseTTL.D())
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
			r.setRole(false)
			if !standby {
				holder, _ := r.store.LeaseHolder(ctx, r.pipeline.ID)
				r.log.Info("standby: another instance holds the lease", "holder", holder)
				standby = true
			}
			if !sleep(ctx, renew) {
				return nil
			}
			continue
		}

		standby = false
		r.setRole(true)
		r.log.Info("acquired lease; becoming leader")
		err = r.lead(ctx)
		r.setRole(false)
		r.health.SetStreaming(r.pipeline.ID, false)
		r.health.SetError(r.pipeline.ID, err)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			r.metrics.Add(observability.MetricPipelineRestarts, r.pipelineLabels(), 1)
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
	if rerr := r.store.Release(release, r.pipeline.ID, r.instanceID); rerr != nil {
		r.log.Warn("could not release lease; standby will wait out the TTL", "err", rerr)
	}
	rcancel()

	return err
}

// heartbeat renews the lease. It tolerates transient control-plane errors up
// to the TTL, but cancels the run immediately if another instance has taken
// the lease.
func (r *Runner) heartbeat(ctx context.Context, cancel context.CancelFunc) {
	ttl := r.cp.LeaseTTL.D()
	renew := r.cp.LeaseRenew.D()
	ticker := time.NewTicker(renew)
	defer ticker.Stop()

	lastOK := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		held, err := r.store.AcquireOrRenew(ctx, r.pipeline.ID, r.instanceID, ttl)
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

// resumeDecision is where a run starts from, and why.
type resumeDecision struct {
	From           string
	ForceBootstrap bool
	Reason         string
	// ResumeSnapshot continues an interrupted chunked snapshot.
	ResumeSnapshot *source.SnapshotResume
}

// planResume decides whether a stored offset may be resumed from.
//
// An offset written during a single-transaction snapshot is not trustworthy.
// Sinks accept snapshot rows as they arrive, so the offset advances to the
// slot's consistent point while the snapshot is still running; an instance that
// dies midway leaves an offset that looks perfectly resumable but only covers
// the part of the snapshot already read. Resuming from it streams on and the
// unread rows are never delivered — a healthy-looking pipeline, permanently
// missing data. When in doubt, snapshot again: a redundant snapshot costs time,
// a skipped one costs data.
//
// A chunked snapshot is different, and better. It streams from the very
// beginning and reads key ranges alongside the stream, so the offset is
// meaningful the whole way through and the snapshot picks up from its last
// chunk instead of starting over.
func planResume(offset string, offsetFound bool, state controlplane.SnapshotState, stateFound bool) resumeDecision {
	switch {
	case !stateFound:
		return resumeDecision{
			From:           offset,
			ForceBootstrap: true,
			Reason:         "no completed snapshot is recorded for this pipeline",
		}

	case state.Phase == controlplane.SnapshotRunning && state.Mode == controlplane.SnapshotChunked && offsetFound:
		return resumeDecision{
			From:   offset,
			Reason: "continuing an interrupted chunked snapshot from its last chunk",
			ResumeSnapshot: &source.SnapshotResume{
				Table: state.ChunkTable,
				Key:   state.ChunkKey,
			},
		}

	case state.Phase == controlplane.SnapshotRunning:
		return resumeDecision{
			From:           offset,
			ForceBootstrap: true,
			Reason:         "the previous snapshot was interrupted, so any stored offset is partial",
		}

	case !offsetFound:
		return resumeDecision{
			ForceBootstrap: true,
			Reason:         "the snapshot is marked complete but no offset was ever committed",
		}

	default:
		return resumeDecision{From: offset, Reason: "resuming from the committed offset"}
	}
}

// snapshotHooks persists the snapshot lifecycle to the control plane. Writes
// are fenced on the lease, so a leader that has been superseded cannot mark a
// snapshot complete.
type snapshotHooks struct {
	store      *controlplane.Store
	pipelineID string
	holder     string
	capture    string
	metrics    *observability.Registry
	labels     []string
	log        *slog.Logger
}

func (h *snapshotHooks) SnapshotStarted(ctx context.Context, mode string) error {
	h.log.Info("snapshot starting", "capture", h.capture, "mode", mode)
	h.metrics.Set(observability.MetricSnapshotRunning, h.labels, 1)
	return h.store.BeginSnapshot(ctx, h.pipelineID, h.holder, h.capture, mode)
}

// SnapshotChunkDone persists how far a chunked snapshot has read, which is what
// makes it resumable after a crash.
func (h *snapshotHooks) SnapshotChunkDone(ctx context.Context, table string, key []byte) error {
	h.log.Debug("snapshot chunk complete", "table", table)
	return h.store.SaveChunkProgress(ctx, h.pipelineID, h.holder, table, key)
}

func (h *snapshotHooks) SnapshotCompleted(ctx context.Context, position string) error {
	h.log.Info("snapshot complete", "position", position)
	h.metrics.Set(observability.MetricSnapshotRunning, h.labels, 0)
	return h.store.CompleteSnapshot(ctx, h.pipelineID, h.holder, position)
}

// captureID names the source-side capture object, for diagnostics in
// snapshot_state.
func captureID(cfg config.Source) string {
	switch cfg.Type {
	case "postgres", "postgresql":
		return cfg.Postgres.Slot
	case "mysql":
		return fmt.Sprintf("server_id=%d", cfg.MySQL.ServerID)
	case "mongodb", "mongo":
		return cfg.MongoDB.Database
	default:
		return ""
	}
}

// runPipeline reads from the source into the router until ctx ends.
func (r *Runner) runPipeline(ctx context.Context) error {
	reader, err := buildReader(r.pipeline.Source, r.log)
	if err != nil {
		return err
	}
	defer func() { _ = reader.Close() }()

	sinks, err := buildSinks(ctx, r.pipeline.Sinks, r.log)
	if err != nil {
		return err
	}
	defer closeSinks(sinks, r.log)

	offset, offsetFound, err := r.store.LoadOffset(ctx, r.pipeline.ID)
	if err != nil {
		return err
	}
	state, stateFound, err := r.store.LoadSnapshotState(ctx, r.pipeline.ID)
	if err != nil {
		return err
	}

	plan := planResume(offset, offsetFound, state, stateFound)
	switch {
	case plan.ForceBootstrap:
		r.log.Warn("bootstrapping from scratch", "reason", plan.Reason, "stored_offset", offset)
	case plan.ResumeSnapshot != nil:
		r.log.Info("resuming", "position", plan.From, "reason", plan.Reason,
			"snapshot_table", plan.ResumeSnapshot.Table)
	default:
		r.log.Info("resuming", "position", plan.From)
	}

	if cursors, cerr := r.store.LoadSinkCursors(ctx, r.pipeline.ID); cerr == nil {
		for _, c := range cursors {
			r.log.Info("previous sink progress", "sink", c.Sink, "position", c.Position)
		}
	}

	var acker source.Acker
	if a, ok := reader.(source.Acker); ok {
		acker = a
	}

	var lag source.LagReporter
	if l, ok := reader.(source.LagReporter); ok {
		lag = l
	}

	router := NewRouter(RouterOptions{
		PipelineID: r.pipeline.ID,
		Holder:     r.instanceID,
		Store:      r.store,
		SinkConfig: r.pipeline.Sinks,
		Sinks:      sinks,
		Interval:   r.pipeline.CommitInterval.D(),
		Acker:      acker,
		Lag:        lag,
		Metrics:    r.metrics,
		Log:        r.log,
	})

	req := source.ReadRequest{
		From:           plan.From,
		ForceBootstrap: plan.ForceBootstrap,
		ResumeSnapshot: plan.ResumeSnapshot,
		Hooks: &snapshotHooks{
			store:      r.store,
			pipelineID: r.pipeline.ID,
			holder:     r.instanceID,
			capture:    captureID(r.pipeline.Source),
			metrics:    r.metrics,
			labels:     []string{"pipeline", r.pipeline.ID},
			log:        r.log,
		},
	}

	events := make(chan cdc.ChangeEvent, eventBuffer)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(events)
		defer r.health.SetStreaming(r.pipeline.ID, false)
		r.health.SetStreaming(r.pipeline.ID, true)
		rerr := reader.ReadChanges(gctx, req, events)
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
