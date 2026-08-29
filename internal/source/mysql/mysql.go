// Package mysql reads row-based binlog events with go-mysql, using GTIDs as
// the resume unit.
//
// GTIDs are preferred over file+offset positions because a file name and byte
// offset are only meaningful on one server: they break on binlog rotation and
// become actively wrong after a failover to a replica. A GTID set survives
// both.
package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql" // database/sql driver for the snapshot

	"github.com/aupv9/slipstream/internal/cdc"
	"github.com/aupv9/slipstream/internal/config"
	"github.com/aupv9/slipstream/internal/controlplane"
	"github.com/aupv9/slipstream/internal/source"
)

// Reader captures changes from one MySQL server.
type Reader struct {
	cfg      config.MySQL
	sourceID string
	log      *slog.Logger

	syncer *replication.BinlogSyncer

	// columns caches column names and types per table, since a TableMapEvent
	// carries neither.
	mu      sync.RWMutex
	columns map[string]*tableSchema
}

type tableSchema struct {
	names  []string
	binary []bool
}

// New builds a MySQL reader. Nothing connects until ReadChanges runs.
func New(cfg config.MySQL, sourceID string, log *slog.Logger) *Reader {
	return &Reader{
		cfg:      cfg,
		sourceID: sourceID,
		log:      log.With("source", "mysql"),
		columns:  make(map[string]*tableSchema),
	}
}

// Name identifies the reader.
func (r *Reader) Name() string { return "mysql" }

// Close stops the binlog syncer.
func (r *Reader) Close() error {
	if r.syncer != nil {
		r.syncer.Close()
		r.syncer = nil
	}
	return nil
}

// ReadChanges resumes from req.From, or bootstraps, then streams the binlog.
func (r *Reader) ReadChanges(ctx context.Context, req source.ReadRequest, out chan<- cdc.ChangeEvent) error {
	if r.cfg.DSN == "" {
		return fmt.Errorf("mysql: dsn is required")
	}
	if r.cfg.ServerID == 0 {
		return fmt.Errorf("mysql: server_id is required and must be unique in the replication topology")
	}

	db, err := sql.Open("mysql", r.cfg.DSN)
	if err != nil {
		return fmt.Errorf("mysql: open: %w", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("mysql: connect: %w", err)
	}
	if err := r.checkServerSettings(ctx, db); err != nil {
		return err
	}

	var startSet string
	if req.From != "" && !req.ForceBootstrap {
		startSet = req.From
		r.log.Info("resuming from stored offset", "gtid_set", startSet)
	} else {
		if req.From != "" {
			r.log.Warn("discarding the stored offset and snapshotting again",
				"offset", req.From,
				"reason", "the previous snapshot did not complete, so this offset covers only part of the data")
		}
		if startSet, err = r.bootstrap(ctx, db, req, out); err != nil {
			return err
		}
	}

	return r.stream(ctx, db, startSet, out)
}

// checkServerSettings fails early on a server that cannot support correct CDC,
// rather than silently producing partial events later.
func (r *Reader) checkServerSettings(ctx context.Context, db *sql.DB) error {
	var format, rowImage, gtidMode string
	if err := db.QueryRowContext(ctx,
		`SELECT @@binlog_format, @@binlog_row_image, @@gtid_mode`).Scan(&format, &rowImage, &gtidMode); err != nil {
		return fmt.Errorf("mysql: read server settings: %w", err)
	}

	if !strings.EqualFold(format, "ROW") {
		return fmt.Errorf("mysql: binlog_format is %q, need ROW: statement-based logging cannot be turned back into row changes", format)
	}
	if !strings.EqualFold(gtidMode, "ON") {
		return fmt.Errorf("mysql: gtid_mode is %q, need ON: file+offset positions break on rotation and failover", gtidMode)
	}
	if !strings.EqualFold(rowImage, "FULL") {
		// MINIMAL omits unchanged columns from the before image, so deletes
		// and updates arrive without the columns a sink needs.
		r.log.Warn("binlog_row_image is not FULL; before images will be incomplete",
			"binlog_row_image", rowImage)
	}
	return nil
}

// bootstrap records the GTID set, then snapshots inside a consistent read.
//
// The ordering matters and is deliberate: the GTID set is read *before*
// opening the snapshot transaction. A transaction committing between the two
// then appears both in the snapshot and in the replayed stream, which is a
// duplicate — and duplicates are what idempotent sinks exist to absorb. The
// reverse order would be a gap instead: a transaction committed after the
// snapshot's view but already inside the recorded GTID set is skipped by the
// stream and never delivered at all.
//
// This is also why no FLUSH TABLES WITH READ LOCK is needed: correctness comes
// from choosing the safe side of the race, not from locking the whole server.
func (r *Reader) bootstrap(ctx context.Context, db *sql.DB, req source.ReadRequest, out chan<- cdc.ChangeEvent) (string, error) {
	if req.Hooks != nil {
		if err := req.Hooks.SnapshotStarted(ctx, controlplane.SnapshotSingle); err != nil {
			return "", fmt.Errorf("mysql: record snapshot start: %w", err)
		}
	}

	var gtidSet string
	if err := db.QueryRowContext(ctx, `SELECT @@GLOBAL.gtid_executed`).Scan(&gtidSet); err != nil {
		return "", fmt.Errorf("mysql: read gtid_executed: %w", err)
	}
	gtidSet = strings.ReplaceAll(gtidSet, "\n", "")
	r.log.Info("recorded starting position", "gtid_set", gtidSet)

	if r.cfg.Snapshot {
		if err := r.snapshot(ctx, db, gtidSet, out); err != nil {
			return "", err
		}
	} else {
		r.log.Info("snapshot disabled; streaming only from the recorded GTID set")
	}

	if req.Hooks != nil {
		if err := req.Hooks.SnapshotCompleted(ctx, gtidSet); err != nil {
			return "", fmt.Errorf("mysql: record snapshot completion: %w", err)
		}
	}
	return gtidSet, nil
}

// snapshot reads every configured table inside one consistent read view.
func (r *Reader) snapshot(ctx context.Context, db *sql.DB, position string, out chan<- cdc.ChangeEvent) error {
	tables, err := r.snapshotTables(ctx, db)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return fmt.Errorf("mysql: no tables to capture; set source.mysql.tables")
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("mysql: snapshot connection: %w", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, `SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ`); err != nil {
		return fmt.Errorf("mysql: set isolation level: %w", err)
	}
	// WITH CONSISTENT SNAPSHOT pins one InnoDB read view for the whole
	// transaction, so every table is read as of the same instant.
	if _, err := conn.ExecContext(ctx, `START TRANSACTION WITH CONSISTENT SNAPSHOT`); err != nil {
		return fmt.Errorf("mysql: start consistent snapshot: %w", err)
	}
	defer func() {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
	}()

	for _, t := range tables {
		n, err := r.snapshotTable(ctx, conn, t, position, out)
		if err != nil {
			return err
		}
		r.log.Info("snapshot complete", "table", t.String(), "rows", n)
	}
	return nil
}

func (r *Reader) snapshotTable(ctx context.Context, conn *sql.Conn, t table, position string, out chan<- cdc.ChangeEvent) (int64, error) {
	rows, err := conn.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s", t.quoted()))
	if err != nil {
		return 0, fmt.Errorf("mysql: snapshot %s: %w", t, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return 0, fmt.Errorf("mysql: snapshot %s: columns: %w", t, err)
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		return 0, fmt.Errorf("mysql: snapshot %s: column types: %w", t, err)
	}

	var count int64
	for rows.Next() {
		holders := make([]any, len(cols))
		for i := range holders {
			holders[i] = new(any)
		}
		if err := rows.Scan(holders...); err != nil {
			return count, fmt.Errorf("mysql: snapshot %s: scan: %w", t, err)
		}

		after := make(map[string]any, len(cols))
		for i, name := range cols {
			v := *(holders[i].(*any))
			after[name] = normalizeSQLValue(v, types[i])
		}

		ev := cdc.ChangeEvent{
			SourceID: r.sourceID,
			Schema:   t.Schema,
			Table:    t.Name,
			Op:       cdc.OpRead,
			After:    after,
			Position: position,
		}
		if err := emit(ctx, out, ev); err != nil {
			return count, err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("mysql: snapshot %s: %w", t, err)
	}
	return count, nil
}

// normalizeSQLValue turns driver values into something a sink can use. MySQL
// hands back []byte for most text, which would marshal to base64.
func normalizeSQLValue(v any, ct *sql.ColumnType) any {
	b, ok := v.([]byte)
	if !ok {
		return v
	}
	if isBinaryType(ct.DatabaseTypeName()) {
		return b
	}
	return string(b)
}

func isBinaryType(name string) bool {
	switch strings.ToUpper(name) {
	case "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "BINARY", "VARBINARY", "GEOMETRY":
		return true
	}
	return false
}
