// Package mongo will read a change stream with the official mongo-driver.
//
// Roadmap step 4. Declared now so the wiring exists; the snapshot technique is
// recorded here before it is implemented:
//
//   - Read the current clusterTime (e.g. from a hello/ping response) *before*
//     touching the collections.
//   - Snapshot the collections, emitting cdc.OpRead events positioned at that
//     clusterTime.
//   - Open the change stream with startAtOperationTime set to exactly that
//     clusterTime, so streaming picks up precisely where the snapshot's view
//     ends.
//
// After the first event, the driver's resume token replaces clusterTime as the
// stored position: it is the resume unit MongoDB itself guarantees.
package mongo

import (
	"context"
	"log/slog"

	"github.com/aupv9/slipstream/internal/cdc"
	"github.com/aupv9/slipstream/internal/config"
	"github.com/aupv9/slipstream/internal/source"
)

// Reader is the MongoDB change-stream reader.
type Reader struct {
	cfg      config.MongoDB
	sourceID string
	log      *slog.Logger
}

// New builds a MongoDB reader.
func New(cfg config.MongoDB, sourceID string, log *slog.Logger) *Reader {
	return &Reader{cfg: cfg, sourceID: sourceID, log: log.With("source", "mongodb")}
}

// Name identifies the reader.
func (r *Reader) Name() string { return "mongodb" }

// ReadChanges is not implemented yet.
func (r *Reader) ReadChanges(context.Context, string, chan<- cdc.ChangeEvent) error {
	return &source.ErrNotImplemented{
		Source: "mongodb",
		Reason: "change-stream reader is roadmap step 4; see the package comment for the clusterTime snapshot plan",
	}
}

// Close is a no-op until the reader holds connections.
func (r *Reader) Close() error { return nil }
