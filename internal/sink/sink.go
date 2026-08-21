// Package sink defines the write contract. A sink is an in-process Go
// implementation compiled into the single binary — "generic" in the sense that
// adding one requires no change to any reader, without paying for an
// out-of-process plugin protocol.
package sink

import (
	"context"

	"github.com/aupv9/slipstream/internal/cdc"
)

// Sink accepts batches of change events.
//
// Delivery is at-least-once: after a failover the reader may replay the last
// events it had already emitted, and a retried batch may be written twice.
// Every implementation is therefore required to be idempotent, either by
// upserting on the row's primary key or by deduplicating on
// cdc.ChangeEvent.IdempotencyKey().
//
// Write must return an error rather than partially succeeding silently; the
// pipeline retries with backoff and never drops a batch.
type Sink interface {
	// Name matches the configured sink name and keys its cursor row.
	Name() string
	// Write delivers a batch. Events arrive in source-log order.
	Write(ctx context.Context, batch []cdc.ChangeEvent) error
	// Close flushes and releases resources.
	Close() error
}
