// Package mysql will read row-based binlog events with
// github.com/go-mysql-org/go-mysql.
//
// Roadmap step 4. The reader is declared here so the pipeline wiring and
// config surface are already in place, and so the snapshot technique is
// recorded before it is implemented:
//
//   - Open a REPEATABLE READ transaction. Its *first* statement reads
//     @@GLOBAL.gtid_executed, which pins the exact GTID set the transaction's
//     view corresponds to.
//   - Snapshot the tables inside that same transaction, emitting cdc.OpRead
//     events positioned at that GTID set.
//   - Start streaming the binlog from exactly that GTID set.
//
// That sequence needs no FLUSH TABLES WITH READ LOCK — no global lock, and no
// gap or overlap at the snapshot/stream boundary. GTID mode is strongly
// preferred over file+offset positions, which break on binlog rotation and
// failover.
package mysql

import (
	"context"
	"log/slog"

	"github.com/aupv9/slipstream/internal/cdc"
	"github.com/aupv9/slipstream/internal/config"
	"github.com/aupv9/slipstream/internal/source"
)

// Reader is the MySQL binlog reader.
type Reader struct {
	cfg      config.MySQL
	sourceID string
	log      *slog.Logger
}

// New builds a MySQL reader.
func New(cfg config.MySQL, sourceID string, log *slog.Logger) *Reader {
	return &Reader{cfg: cfg, sourceID: sourceID, log: log.With("source", "mysql")}
}

// Name identifies the reader.
func (r *Reader) Name() string { return "mysql" }

// ReadChanges is not implemented yet.
func (r *Reader) ReadChanges(context.Context, string, chan<- cdc.ChangeEvent) error {
	return &source.ErrNotImplemented{
		Source: "mysql",
		Reason: "binlog reader is roadmap step 4; see the package comment for the GTID snapshot plan",
	}
}

// Close is a no-op until the reader holds connections.
func (r *Reader) Close() error { return nil }
