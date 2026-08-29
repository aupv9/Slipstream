// Package mongo reads changes from a MongoDB change stream.
//
// Change streams are the supported interface for this, so there is no oplog
// parsing here: the server hands back a resume token with every event, and that
// token is exactly the resume unit this pipeline needs. Change streams require
// a replica set or a sharded cluster; a standalone mongod has no oplog to read.
package mongo

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/aupv9/slipstream/internal/cdc"
	"github.com/aupv9/slipstream/internal/config"
	"github.com/aupv9/slipstream/internal/controlplane"
	"github.com/aupv9/slipstream/internal/source"
)

// Reader captures changes from one MongoDB deployment.
type Reader struct {
	cfg      config.MongoDB
	sourceID string
	log      *slog.Logger

	client *mongo.Client
}

// New builds a MongoDB reader. Nothing connects until ReadChanges runs.
func New(cfg config.MongoDB, sourceID string, log *slog.Logger) *Reader {
	return &Reader{cfg: cfg, sourceID: sourceID, log: log.With("source", "mongodb")}
}

// Name identifies the reader.
func (r *Reader) Name() string { return "mongodb" }

// Close disconnects the client.
func (r *Reader) Close() error {
	if r.client == nil {
		return nil
	}
	err := r.client.Disconnect(context.Background())
	r.client = nil
	if err != nil {
		return fmt.Errorf("mongodb: disconnect: %w", err)
	}
	return nil
}

// ReadChanges resumes from req.From, or bootstraps, then follows the change
// stream until ctx is cancelled.
func (r *Reader) ReadChanges(ctx context.Context, req source.ReadRequest, out chan<- cdc.ChangeEvent) error {
	if r.cfg.URI == "" {
		return fmt.Errorf("mongodb: uri is required")
	}
	if r.cfg.Database == "" {
		return fmt.Errorf("mongodb: database is required")
	}

	client, err := mongo.Connect(options.Client().ApplyURI(r.cfg.URI))
	if err != nil {
		return fmt.Errorf("mongodb: connect: %w", err)
	}
	r.client = client
	defer r.Close()

	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("mongodb: ping: %w", err)
	}

	var start position
	if req.From != "" && !req.ForceBootstrap {
		if start, err = parsePosition(req.From); err != nil {
			return err
		}
		r.log.Info("resuming from stored offset", "position", req.From)
	} else {
		if req.From != "" {
			r.log.Warn("discarding the stored offset and snapshotting again",
				"offset", req.From,
				"reason", "the previous snapshot did not complete, so this offset covers only part of the data")
		}
		if start, err = r.bootstrap(ctx, req, out); err != nil {
			return err
		}
	}

	return r.stream(ctx, start, out)
}

// bootstrap records the cluster time, then snapshots the collections.
//
// As with MySQL, the time is recorded *before* the snapshot reads anything. A
// write landing between the two is then delivered twice — once from the
// snapshot, once from the stream — and idempotent sinks absorb that. Recording
// it afterwards would instead skip writes that happened during the snapshot but
// before the recorded time, which is a gap nothing can repair.
func (r *Reader) bootstrap(ctx context.Context, req source.ReadRequest, out chan<- cdc.ChangeEvent) (position, error) {
	if req.Hooks != nil {
		if err := req.Hooks.SnapshotStarted(ctx, controlplane.SnapshotSingle); err != nil {
			return position{}, fmt.Errorf("mongodb: record snapshot start: %w", err)
		}
	}

	ts, err := r.operationTime(ctx)
	if err != nil {
		return position{}, err
	}
	start := position{Timestamp: &ts}
	r.log.Info("recorded starting cluster time", "t", ts.T, "i", ts.I)

	if r.cfg.Snapshot {
		if err := r.snapshot(ctx, start.String(), out); err != nil {
			return position{}, err
		}
	} else {
		r.log.Info("snapshot disabled; streaming only from the recorded cluster time")
	}

	if req.Hooks != nil {
		if err := req.Hooks.SnapshotCompleted(ctx, start.String()); err != nil {
			return position{}, fmt.Errorf("mongodb: record snapshot completion: %w", err)
		}
	}
	return start, nil
}

// operationTime asks the server for the cluster time the stream can start from.
func (r *Reader) operationTime(ctx context.Context) (bson.Timestamp, error) {
	var raw bson.Raw
	err := r.client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&raw)
	if err != nil {
		return bson.Timestamp{}, fmt.Errorf("mongodb: hello: %w", err)
	}

	if v, lookupErr := raw.LookupErr("operationTime"); lookupErr == nil {
		if t, i, ok := v.TimestampOK(); ok {
			return bson.Timestamp{T: t, I: i}, nil
		}
	}
	// Fall back to the gossiped cluster time on deployments that do not report
	// operationTime for this command.
	if v, lookupErr := raw.LookupErr("$clusterTime", "clusterTime"); lookupErr == nil {
		if t, i, ok := v.TimestampOK(); ok {
			return bson.Timestamp{T: t, I: i}, nil
		}
	}
	return bson.Timestamp{}, fmt.Errorf("mongodb: server reported no operationTime; " +
		"change streams need a replica set or sharded cluster")
}

// snapshot reads the configured collections.
func (r *Reader) snapshot(ctx context.Context, position string, out chan<- cdc.ChangeEvent) error {
	db := r.client.Database(r.cfg.Database)

	names := r.cfg.Collections
	if len(names) == 0 {
		listed, err := db.ListCollectionNames(ctx, bson.D{})
		if err != nil {
			return fmt.Errorf("mongodb: list collections: %w", err)
		}
		names = listed
	}
	if len(names) == 0 {
		return fmt.Errorf("mongodb: database %s has no collections to capture", r.cfg.Database)
	}

	for _, name := range names {
		n, err := r.snapshotCollection(ctx, db, name, position, out)
		if err != nil {
			return err
		}
		r.log.Info("snapshot complete", "collection", name, "documents", n)
	}
	return nil
}

func (r *Reader) snapshotCollection(ctx context.Context, db *mongo.Database, name, position string, out chan<- cdc.ChangeEvent) (int64, error) {
	cursor, err := db.Collection(name).Find(ctx, bson.D{})
	if err != nil {
		return 0, fmt.Errorf("mongodb: snapshot %s: %w", name, err)
	}
	defer cursor.Close(ctx)

	var count int64
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return count, fmt.Errorf("mongodb: snapshot %s: decode: %w", name, err)
		}
		ev := cdc.ChangeEvent{
			SourceID: r.sourceID,
			Schema:   r.cfg.Database,
			Table:    name,
			Op:       cdc.OpRead,
			After:    normalizeDoc(doc),
			Position: position,
		}
		if err := emit(ctx, out, ev); err != nil {
			return count, err
		}
		count++
	}
	if err := cursor.Err(); err != nil {
		return count, fmt.Errorf("mongodb: snapshot %s: %w", name, err)
	}
	return count, nil
}

// stream follows the change stream, stamping each event with its own resume
// token. Every change event is independently resumable, so a token is a safe
// commit point on its own.
func (r *Reader) stream(ctx context.Context, start position, out chan<- cdc.ChangeEvent) error {
	opts := options.ChangeStream()
	switch {
	case start.Token != "":
		opts.SetResumeAfter(bson.D{{Key: "_data", Value: start.Token}})
	case start.Timestamp != nil:
		opts.SetStartAtOperationTime(start.Timestamp)
	}

	full := options.UpdateLookup
	if r.cfg.FullDocument != "" {
		full = options.FullDocument(r.cfg.FullDocument)
	}
	// Without updateLookup an update carries only the changed fields, which is
	// not enough for a sink that upserts whole documents.
	opts.SetFullDocument(full)

	db := r.client.Database(r.cfg.Database)
	pipeline := mongo.Pipeline{}
	if len(r.cfg.Collections) > 0 {
		pipeline = mongo.Pipeline{{{Key: "$match", Value: bson.D{
			{Key: "ns.coll", Value: bson.D{{Key: "$in", Value: r.cfg.Collections}}},
		}}}}
	}

	cs, err := db.Watch(ctx, pipeline, opts)
	if err != nil {
		return fmt.Errorf("mongodb: open change stream: %w", err)
	}
	defer cs.Close(context.Background())
	r.log.Info("streaming", "database", r.cfg.Database, "from", start.String())

	for cs.Next(ctx) {
		var raw bson.Raw
		if err := cs.Decode(&raw); err != nil {
			return fmt.Errorf("mongodb: decode change event: %w", err)
		}
		ev, ok, err := r.toChangeEvent(raw)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if err := emit(ctx, out, ev); err != nil {
			return err
		}
	}

	if err := cs.Err(); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("mongodb: change stream: %w", err)
	}
	return nil
}

func emit(ctx context.Context, out chan<- cdc.ChangeEvent, ev cdc.ChangeEvent) error {
	select {
	case out <- ev:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// position is either a resume token (from the stream) or a cluster time (from
// the snapshot). Keeping them distinguishable in one string means the stored
// offset says how it should be resumed.
type position struct {
	Token     string
	Timestamp *bson.Timestamp
}

func (p position) String() string {
	if p.Token != "" {
		return "token:" + p.Token
	}
	if p.Timestamp != nil {
		return fmt.Sprintf("ts:%d,%d", p.Timestamp.T, p.Timestamp.I)
	}
	return ""
}

func parsePosition(s string) (position, error) {
	switch {
	case strings.HasPrefix(s, "token:"):
		return position{Token: strings.TrimPrefix(s, "token:")}, nil

	case strings.HasPrefix(s, "ts:"):
		parts := strings.SplitN(strings.TrimPrefix(s, "ts:"), ",", 2)
		if len(parts) != 2 {
			return position{}, fmt.Errorf("mongodb: malformed timestamp offset %q", s)
		}
		t, err := strconv.ParseUint(parts[0], 10, 32)
		if err != nil {
			return position{}, fmt.Errorf("mongodb: malformed timestamp offset %q: %w", s, err)
		}
		i, err := strconv.ParseUint(parts[1], 10, 32)
		if err != nil {
			return position{}, fmt.Errorf("mongodb: malformed timestamp offset %q: %w", s, err)
		}
		ts := bson.Timestamp{T: uint32(t), I: uint32(i)}
		return position{Timestamp: &ts}, nil

	default:
		return position{}, fmt.Errorf("mongodb: unrecognised offset %q, want a token: or ts: prefix", s)
	}
}
