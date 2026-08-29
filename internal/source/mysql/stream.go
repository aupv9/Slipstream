package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	gomysqldriver "github.com/go-sql-driver/mysql"

	"github.com/aupv9/slipstream/internal/cdc"
)

// stream follows the binlog from the given GTID set.
//
// Positions are deliberately one transaction behind: every event carries the
// GTID set as it stood *before* the transaction it belongs to. If the process
// dies halfway through emitting a transaction, the stored offset still points
// before it, so the resume replays that transaction whole. Stamping events with
// the set including their own transaction would let a partial transaction be
// committed as complete, and the rest of its rows would never be delivered.
func (r *Reader) stream(ctx context.Context, db *sql.DB, startSet string, out chan<- cdc.ChangeEvent) error {
	host, port, user, password, err := parseDSN(r.cfg.DSN)
	if err != nil {
		return err
	}

	gset, err := mysql.ParseGTIDSet(mysql.MySQLFlavor, startSet)
	if err != nil {
		return fmt.Errorf("mysql: stored offset %q is not a GTID set: %w", startSet, err)
	}

	heartbeat := r.cfg.Heartbeat.D()
	if heartbeat == 0 {
		heartbeat = 30 * time.Second
	}

	r.syncer = replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID:        r.cfg.ServerID,
		Flavor:          mysql.MySQLFlavor,
		Host:            host,
		Port:            port,
		User:            user,
		Password:        password,
		HeartbeatPeriod: heartbeat,
		Logger:          r.log,
	})
	defer r.Close()

	streamer, err := r.syncer.StartSyncGTID(gset)
	if err != nil {
		return fmt.Errorf("mysql: start replication at %s: %w", startSet, err)
	}
	r.log.Info("streaming", "from", startSet, "server_id", r.cfg.ServerID)

	// committed is the position events are stamped with: everything durably
	// before the transaction currently being decoded.
	committed := gset.Clone()
	var pendingGTID string

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("mysql: receive: %w", err)
		}

		switch e := ev.Event.(type) {
		case *replication.GTIDEvent:
			next, gerr := e.GTIDNext()
			if gerr != nil {
				return fmt.Errorf("mysql: decode gtid: %w", gerr)
			}
			pendingGTID = next.String()

		case *replication.RowsEvent:
			if err := r.handleRows(ctx, db, ev.Header, e, committed.String(), out); err != nil {
				return err
			}

		case *replication.XIDEvent:
			// Transaction committed: only now may the position advance past it.
			if pendingGTID != "" {
				if err := committed.Update(pendingGTID); err != nil {
					return fmt.Errorf("mysql: advance gtid set with %q: %w", pendingGTID, err)
				}
				pendingGTID = ""
			}

		case *replication.QueryEvent:
			// DDL. The cached column list may now be wrong, so drop it and let
			// the next row event look it up again.
			r.invalidateSchema(string(e.Schema), string(e.Query))
			if pendingGTID != "" && isTransactionBoundary(string(e.Query)) {
				if err := committed.Update(pendingGTID); err != nil {
					return fmt.Errorf("mysql: advance gtid set with %q: %w", pendingGTID, err)
				}
				pendingGTID = ""
			}
		}
	}
}

// handleRows turns one row event into change events.
func (r *Reader) handleRows(ctx context.Context, db *sql.DB, header *replication.EventHeader, e *replication.RowsEvent, position string, out chan<- cdc.ChangeEvent) error {
	schema := string(e.Table.Schema)
	name := string(e.Table.Table)
	if !r.captures(schema, name) {
		return nil
	}

	cols, err := r.schemaFor(ctx, db, schema, name)
	if err != nil {
		return err
	}
	commitTS := time.Unix(int64(header.Timestamp), 0).UTC()

	switch header.EventType {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		for _, row := range e.Rows {
			ev := r.event(schema, name, cdc.OpCreate, nil, cols.toMap(row), position, commitTS)
			if err := emit(ctx, out, ev); err != nil {
				return err
			}
		}

	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		for _, row := range e.Rows {
			ev := r.event(schema, name, cdc.OpDelete, cols.toMap(row), nil, position, commitTS)
			if err := emit(ctx, out, ev); err != nil {
				return err
			}
		}

	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		// Updates arrive as before/after pairs.
		for i := 0; i+1 < len(e.Rows); i += 2 {
			ev := r.event(schema, name, cdc.OpUpdate,
				cols.toMap(e.Rows[i]), cols.toMap(e.Rows[i+1]), position, commitTS)
			if err := emit(ctx, out, ev); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Reader) event(schema, name string, op cdc.Op, before, after map[string]any, position string, commitTS time.Time) cdc.ChangeEvent {
	return cdc.ChangeEvent{
		SourceID: r.sourceID,
		Schema:   schema,
		Table:    name,
		Op:       op,
		Before:   before,
		After:    after,
		Position: position,
		CommitTS: commitTS,
	}
}

// toMap pairs a binlog row with its column names.
func (t *tableSchema) toMap(row []any) map[string]any {
	out := make(map[string]any, len(t.names))
	for i, name := range t.names {
		if i >= len(row) {
			break
		}
		out[name] = normalizeBinlogValue(row[i], i < len(t.binary) && t.binary[i])
	}
	return out
}

// normalizeBinlogValue converts go-mysql's decoded values into sink-friendly
// ones. Text columns arrive as []byte and would otherwise marshal to base64.
func normalizeBinlogValue(v any, binary bool) any {
	b, ok := v.([]byte)
	if !ok {
		return v
	}
	if binary {
		return b
	}
	return string(b)
}

// captures reports whether a table is in scope.
func (r *Reader) captures(schema, name string) bool {
	if len(r.cfg.Tables) == 0 {
		return true
	}
	qualified := schema + "." + name
	for _, t := range r.cfg.Tables {
		if strings.EqualFold(t, qualified) {
			return true
		}
	}
	return false
}

// schemaFor returns the column names for a table, looking them up once. A
// TableMapEvent carries column types but no names, so they have to come from
// information_schema.
func (r *Reader) schemaFor(ctx context.Context, db *sql.DB, schema, name string) (*tableSchema, error) {
	key := schema + "." + name

	r.mu.RLock()
	cached, ok := r.columns[key]
	r.mu.RUnlock()
	if ok {
		return cached, nil
	}

	rows, err := db.QueryContext(ctx,
		`SELECT column_name, data_type
		   FROM information_schema.columns
		  WHERE table_schema = ? AND table_name = ?
		  ORDER BY ordinal_position`, schema, name)
	if err != nil {
		return nil, fmt.Errorf("mysql: read columns of %s: %w", key, err)
	}
	defer rows.Close()

	ts := &tableSchema{}
	for rows.Next() {
		var col, dataType string
		if err := rows.Scan(&col, &dataType); err != nil {
			return nil, fmt.Errorf("mysql: scan columns of %s: %w", key, err)
		}
		ts.names = append(ts.names, col)
		ts.binary = append(ts.binary, isBinaryType(dataType))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mysql: read columns of %s: %w", key, err)
	}
	if len(ts.names) == 0 {
		return nil, fmt.Errorf("mysql: table %s has no columns in information_schema; was it dropped?", key)
	}

	r.mu.Lock()
	r.columns[key] = ts
	r.mu.Unlock()
	return ts, nil
}

// invalidateSchema forgets cached columns after DDL, so an added or dropped
// column does not silently shift every value one place.
func (r *Reader) invalidateSchema(schema, query string) {
	upper := strings.ToUpper(query)
	if !strings.Contains(upper, "ALTER") && !strings.Contains(upper, "DROP") &&
		!strings.Contains(upper, "CREATE") && !strings.Contains(upper, "RENAME") {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if schema == "" {
		r.columns = make(map[string]*tableSchema)
		return
	}
	for key := range r.columns {
		if strings.HasPrefix(key, schema+".") {
			delete(r.columns, key)
		}
	}
}

// isTransactionBoundary reports whether a QueryEvent ends a transaction. DDL is
// its own implicit transaction and produces no XIDEvent.
func isTransactionBoundary(query string) bool {
	q := strings.ToUpper(strings.TrimSpace(query))
	if q == "COMMIT" {
		return true
	}
	// Any DDL commits implicitly.
	for _, prefix := range []string{"ALTER", "CREATE", "DROP", "RENAME", "TRUNCATE"} {
		if strings.HasPrefix(q, prefix) {
			return true
		}
	}
	return false
}

// table is a schema-qualified table name.
type table struct {
	Schema string
	Name   string
}

func (t table) String() string { return t.Schema + "." + t.Name }

func (t table) quoted() string {
	return quoteIdent(t.Schema) + "." + quoteIdent(t.Name)
}

func quoteIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

// snapshotTables resolves the configured tables, defaulting to every base table
// in the DSN's database.
func (r *Reader) snapshotTables(ctx context.Context, db *sql.DB) ([]table, error) {
	if len(r.cfg.Tables) > 0 {
		out := make([]table, 0, len(r.cfg.Tables))
		for _, name := range r.cfg.Tables {
			parts := strings.SplitN(name, ".", 2)
			if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
				return nil, fmt.Errorf("mysql: invalid table name %q, want database.table", name)
			}
			out = append(out, table{Schema: parts[0], Name: parts[1]})
		}
		return out, nil
	}

	var dbName string
	if err := db.QueryRowContext(ctx, `SELECT DATABASE()`).Scan(&dbName); err != nil || dbName == "" {
		return nil, fmt.Errorf("mysql: no tables configured and the DSN names no database")
	}

	rows, err := db.QueryContext(ctx,
		`SELECT table_name FROM information_schema.tables
		  WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name`, dbName)
	if err != nil {
		return nil, fmt.Errorf("mysql: list tables: %w", err)
	}
	defer rows.Close()

	var out []table
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("mysql: scan table name: %w", err)
		}
		out = append(out, table{Schema: dbName, Name: name})
	}
	return out, rows.Err()
}

// parseDSN pulls the connection parts out of a go-sql-driver DSN, since the
// binlog syncer takes them separately.
func parseDSN(dsn string) (host string, port uint16, user, password string, err error) {
	cfg, perr := gomysqldriver.ParseDSN(dsn)
	if perr != nil {
		return "", 0, "", "", fmt.Errorf("mysql: parse dsn: %w", perr)
	}
	if cfg.Net != "tcp" {
		return "", 0, "", "", fmt.Errorf("mysql: replication needs a tcp dsn, got %q", cfg.Net)
	}

	hostPart, portPart, serr := splitHostPort(cfg.Addr)
	if serr != nil {
		return "", 0, "", "", serr
	}
	return hostPart, portPart, cfg.User, cfg.Passwd, nil
}

func splitHostPort(addr string) (string, uint16, error) {
	idx := strings.LastIndex(addr, ":")
	if idx < 0 {
		return addr, 3306, nil
	}
	var port uint16
	if _, err := fmt.Sscanf(addr[idx+1:], "%d", &port); err != nil {
		return "", 0, fmt.Errorf("mysql: invalid port in %q: %w", addr, err)
	}
	return addr[:idx], port, nil
}

func emit(ctx context.Context, out chan<- cdc.ChangeEvent, ev cdc.ChangeEvent) error {
	select {
	case out <- ev:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
