// Package source defines the reader contract every capture implementation
// satisfies. Adding a source means adding one file here; nothing in the sink
// path changes.
package source

import (
	"context"
	"fmt"

	"github.com/aupv9/slipstream/internal/cdc"
)

// Reader streams normalized changes out of one database.
//
// Exactly one Reader per pipeline is ever active at a time — the instance
// holding the lease — because replication slots, binlog connections and change
// streams should each have a single consumer. That single-reader rule is also
// what preserves per-row ordering: events are emitted in source-log order and
// never processed in parallel within a pipeline.
type Reader interface {
	// Name identifies the implementation in logs.
	Name() string

	// ReadChanges emits events into out, resuming after `from`. An empty
	// `from` means "no offset stored yet": the reader takes an initial
	// snapshot pinned to the exact position where streaming begins, so no row
	// is missed or duplicated at the boundary.
	//
	// It blocks until ctx is cancelled (returning nil or ctx.Err()) or an
	// unrecoverable error occurs. Sending on `out` may block, which is how
	// sink backpressure reaches the source.
	ReadChanges(ctx context.Context, from string, out chan<- cdc.ChangeEvent) error

	// Close releases connections.
	Close() error
}

// Acker is implemented by readers that must tell the server how far we have
// durably progressed, so it can release log segments it no longer needs to
// keep (WAL for Postgres, binlog for MySQL). The pipeline calls Ack with the
// slowest sink's position, never with the read-ahead position.
type Acker interface {
	Ack(position string)
}

// ErrNotImplemented is returned by readers that are declared but not yet
// built.
type ErrNotImplemented struct {
	Source string
	Reason string
}

func (e *ErrNotImplemented) Error() string {
	return fmt.Sprintf("source %s: not implemented yet: %s", e.Source, e.Reason)
}
