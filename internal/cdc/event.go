// Package cdc defines the normalized change event that every source reader
// produces and every sink consumes. Keeping this type source-agnostic is what
// lets a new sink be added without touching any reader, and vice versa.
package cdc

import (
	"fmt"
	"time"
)

// Op is the kind of change an event describes.
type Op string

const (
	// OpCreate is an insert observed on the change stream.
	OpCreate Op = "c"
	// OpUpdate is an update observed on the change stream.
	OpUpdate Op = "u"
	// OpDelete is a delete observed on the change stream.
	OpDelete Op = "d"
	// OpRead is a row produced by the initial snapshot, not by the log.
	OpRead Op = "r"
	// OpTruncate is a TRUNCATE of the whole relation. It carries no row
	// images: every row is gone. A sink that ignores it keeps rows the source
	// no longer has.
	OpTruncate Op = "t"
)

// ChangeEvent is one normalized row-level change.
//
// Position is the source's own resume unit: an LSN for Postgres, a GTID (or
// file+offset) for MySQL, a resume token for MongoDB. It is opaque to
// everything except the reader that produced it — the pipeline only ever
// stores it and hands it back on resume, and sinks only use it as part of a
// dedupe key. Because a resumed reader may replay the last few events it had
// already emitted, sinks must treat (SourceID, Schema, Table, Position) as an
// idempotency key, or be naturally idempotent by upserting on primary key.
type ChangeEvent struct {
	SourceID string         `json:"source_id"`
	Schema   string         `json:"schema"`
	Table    string         `json:"table"`
	Op       Op             `json:"op"`
	Before   map[string]any `json:"before,omitempty"`
	After    map[string]any `json:"after,omitempty"`
	Position string         `json:"position"`
	CommitTS time.Time      `json:"commit_ts"`
}

// Qualified is the "schema.table" name of the changed relation.
func (e ChangeEvent) Qualified() string {
	if e.Schema == "" {
		return e.Table
	}
	return e.Schema + "." + e.Table
}

// IdempotencyKey is the key a sink should use to recognize a replayed event.
func (e ChangeEvent) IdempotencyKey() string {
	return fmt.Sprintf("%s|%s|%s", e.SourceID, e.Qualified(), e.Position)
}
