package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	"github.com/aupv9/slipstream/internal/cdc"
	"github.com/aupv9/slipstream/internal/source"
)

// watermarkPrefix is the logical decoding message prefix the chunker uses.
const watermarkPrefix = "slipstream"

// chunker takes the initial snapshot in primary-key ranges, interleaved with
// the stream, using the watermark scheme from the DBLog paper.
//
// For each chunk it emits a low watermark into the WAL, reads the chunk, then
// emits a high watermark. Any row changed between those two marks arrives on
// the stream anyway, so it is removed from the chunk before the chunk is
// emitted — the stream's version is the newer one and must win. The result
// needs no long-running transaction and no exported snapshot, which is what
// makes it both crash-resumable and safe for tables too large to read in one
// go.
//
// Postgres can emit those watermarks itself with pg_logical_emit_message, so
// unlike the usual implementations this needs no signal table in the source
// database.
type chunker struct {
	reader  *Reader
	admin   *pgx.Conn
	hooks   source.SnapshotHooks
	size    int
	log     *slog.Logger
	tables  []chunkTable
	current int

	state      chunkState
	id         string
	buffered   []cdc.ChangeEvent
	changed    map[string]bool
	activeTbl  table
	activeKeys []string
	done       bool
}

type chunkState int

const (
	chunkIdle chunkState = iota
	chunkAwaitingLow
	chunkAwaitingHigh
)

// chunkTable is one table to snapshot, with the key columns used to page
// through it.
type chunkTable struct {
	table   table
	keys    []string
	lastKey []any
	done    bool
}

// newChunker prepares the chunked snapshot, resolving each table's primary key.
func newChunker(ctx context.Context, r *Reader, admin *pgx.Conn, hooks source.SnapshotHooks, resume *source.SnapshotResume, size int) (*chunker, error) {
	tables, err := r.capturedTables(ctx, admin)
	if err != nil {
		return nil, err
	}
	if len(tables) == 0 {
		return nil, fmt.Errorf("postgres: publication %s captures no tables", r.cfg.Publication)
	}

	c := &chunker{
		reader: r,
		admin:  admin,
		hooks:  hooks,
		size:   size,
		log:    r.log.With("snapshot", "chunked"),
	}

	for _, t := range tables {
		keys, err := primaryKey(ctx, admin, t)
		if err != nil {
			return nil, err
		}
		if len(keys) == 0 {
			return nil, fmt.Errorf("postgres: table %s has no primary key, so it cannot be read in key ranges; "+
				"give it a primary key or set snapshot_mode: single", t)
		}
		c.tables = append(c.tables, chunkTable{table: t, keys: keys})
	}

	if resume != nil && resume.Table != "" {
		if err := c.resumeFrom(resume); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// resumeFrom skips the tables already finished and continues the one that was
// in progress from its last key.
func (c *chunker) resumeFrom(resume *source.SnapshotResume) error {
	idx := -1
	for i, t := range c.tables {
		if t.table.String() == resume.Table {
			idx = i
			break
		}
	}
	if idx < 0 {
		c.log.Warn("the table a previous run was snapshotting is no longer captured; starting from the first table",
			"table", resume.Table)
		return nil
	}

	for i := range idx {
		c.tables[i].done = true
	}
	if len(resume.Key) > 0 {
		var key []any
		if err := json.Unmarshal(resume.Key, &key); err != nil {
			return fmt.Errorf("postgres: stored chunk key %q is not decodable: %w", resume.Key, err)
		}
		c.tables[idx].lastKey = key
	}
	c.current = idx
	c.log.Info("resuming the snapshot", "table", resume.Table, "after_key", string(resume.Key))
	return nil
}

// finished reports whether every table has been read.
func (c *chunker) finished() bool { return c.done }

// idle reports whether the chunker is between chunks and can start another.
func (c *chunker) idle() bool { return c.state == chunkIdle && !c.done }

// startChunk emits the low watermark, reads one chunk, and emits the high
// watermark. The rows are held until the high watermark comes back on the
// stream, because only then is it known which of them were overtaken.
func (c *chunker) startChunk(ctx context.Context) error {
	t := c.nextTable()
	if t == nil {
		return c.finish(ctx)
	}

	c.id = fmt.Sprintf("%d", time.Now().UnixNano())
	c.activeTbl = t.table
	c.activeKeys = t.keys

	if err := c.emitWatermark(ctx, "lw"); err != nil {
		return err
	}

	rows, lastKey, err := c.readChunk(ctx, t)
	if err != nil {
		return err
	}
	c.buffered = rows
	t.lastKey = lastKey
	if len(rows) < c.size {
		t.done = true
	}

	if err := c.emitWatermark(ctx, "hw"); err != nil {
		return err
	}

	c.state = chunkAwaitingLow
	c.changed = make(map[string]bool)
	return nil
}

func (c *chunker) nextTable() *chunkTable {
	for i := c.current; i < len(c.tables); i++ {
		if !c.tables[i].done {
			c.current = i
			return &c.tables[i]
		}
	}
	return nil
}

// emitWatermark writes a watermark into the WAL.
//
// The message is transactional so that committing it flushes the WAL and the
// walsender forwards it immediately. A non-transactional message is written but
// not flushed, so on an otherwise idle database it waits for the WAL writer:
// measured here at 201ms per watermark against 0.6ms for a transactional one,
// and every chunk needs two of them.
func (c *chunker) emitWatermark(ctx context.Context, kind string) error {
	_, err := c.admin.Exec(ctx,
		`SELECT pg_logical_emit_message(true, $1, $2)`,
		watermarkPrefix, kind+":"+c.id)
	if err != nil {
		return fmt.Errorf("postgres: emit %s watermark: %w", kind, err)
	}
	return nil
}

// readChunk reads the next key range of a table.
func (c *chunker) readChunk(ctx context.Context, t *chunkTable) ([]cdc.ChangeEvent, []any, error) {
	quotedKeys := make([]string, len(t.keys))
	for i, k := range t.keys {
		quotedKeys[i] = quoteIdent(k)
	}
	order := strings.Join(quotedKeys, ", ")

	sql := fmt.Sprintf("SELECT * FROM %s ORDER BY %s LIMIT %d", t.table.quoted(), order, c.size)
	var args []any
	if len(t.lastKey) > 0 {
		placeholders := make([]string, len(t.keys))
		for i := range t.keys {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		}
		sql = fmt.Sprintf("SELECT * FROM %s WHERE (%s) > (%s) ORDER BY %s LIMIT %d",
			t.table.quoted(), order, strings.Join(placeholders, ", "), order, c.size)
		args = t.lastKey
	}

	rows, err := c.admin.Query(ctx, sql, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("postgres: read chunk of %s: %w", t.table, err)
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	var (
		out     []cdc.ChangeEvent
		lastKey []any
	)
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, nil, fmt.Errorf("postgres: read chunk of %s: %w", t.table, err)
		}
		after := make(map[string]any, len(fields))
		for i, f := range fields {
			if i < len(values) {
				after[f.Name] = values[i]
			}
		}

		lastKey = lastKey[:0]
		for _, k := range t.keys {
			lastKey = append(lastKey, after[k])
		}
		lastKey = append([]any(nil), lastKey...)

		out = append(out, cdc.ChangeEvent{
			SourceID: c.reader.sourceID,
			Schema:   t.table.Schema,
			Table:    t.table.Name,
			Op:       cdc.OpRead,
			After:    after,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("postgres: read chunk of %s: %w", t.table, err)
	}
	return out, lastKey, nil
}

// observe records a row the stream changed while a chunk was being read. Those
// rows are dropped from the chunk: the stream carries a newer version, and
// emitting the older snapshot row after it would move the sink backwards.
func (c *chunker) observe(ev cdc.ChangeEvent) {
	if c.state != chunkAwaitingHigh {
		return
	}
	if ev.Schema != c.activeTbl.Schema || ev.Table != c.activeTbl.Name {
		return
	}
	values := ev.After
	if len(values) == 0 {
		values = ev.Before
	}
	if key, ok := rowKey(values, c.activeKeys); ok {
		c.changed[key] = true
	}
}

// onMessage handles a watermark coming back on the stream.
func (c *chunker) onMessage(ctx context.Context, msg *pglogrepl.LogicalDecodingMessage, xld pglogrepl.XLogData, out chan<- cdc.ChangeEvent) error {
	if msg.Prefix != watermarkPrefix {
		return nil
	}
	content := string(msg.Content)

	switch {
	case content == "lw:"+c.id && c.state == chunkAwaitingLow:
		c.state = chunkAwaitingHigh
		return nil

	case content == "hw:"+c.id && c.state == chunkAwaitingHigh:
		return c.flush(ctx, xld, out)
	}
	return nil
}

// flush emits the chunk, minus the rows the stream overtook.
func (c *chunker) flush(ctx context.Context, xld pglogrepl.XLogData, out chan<- cdc.ChangeEvent) error {
	position := xld.WALStart.String()
	emitted, skipped := 0, 0

	for _, ev := range c.buffered {
		if key, ok := rowKey(ev.After, c.activeKeys); ok && c.changed[key] {
			skipped++
			continue
		}
		ev.Position = position
		if err := emit(ctx, out, ev); err != nil {
			return err
		}
		emitted++
	}

	c.buffered = nil
	c.state = chunkIdle
	c.changed = nil

	t := &c.tables[c.current]
	key, err := json.Marshal(t.lastKey)
	if err != nil {
		return fmt.Errorf("postgres: encode chunk key: %w", err)
	}
	if c.hooks != nil {
		if err := c.hooks.SnapshotChunkDone(ctx, t.table.String(), key); err != nil {
			return fmt.Errorf("postgres: record chunk progress: %w", err)
		}
	}

	c.log.Debug("chunk complete", "table", t.table.String(),
		"emitted", emitted, "superseded_by_stream", skipped, "table_done", t.done)
	if t.done {
		c.log.Info("table snapshot complete", "table", t.table.String())
	}
	return nil
}

// finish marks the whole snapshot done.
func (c *chunker) finish(ctx context.Context) error {
	c.done = true
	if c.hooks != nil {
		if err := c.hooks.SnapshotCompleted(ctx, ""); err != nil {
			return fmt.Errorf("postgres: record snapshot completion: %w", err)
		}
	}
	c.log.Info("chunked snapshot complete", "tables", len(c.tables))
	return nil
}

// rowKey renders a row's key columns into a comparable string.
func rowKey(values map[string]any, keys []string) (string, bool) {
	if len(values) == 0 || len(keys) == 0 {
		return "", false
	}
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		v, ok := values[k]
		if !ok {
			return "", false
		}
		parts = append(parts, fmt.Sprintf("%v", v))
	}
	return strings.Join(parts, "\x00"), true
}

// primaryKey returns a table's primary key columns in index order.
func primaryKey(ctx context.Context, admin *pgx.Conn, t table) ([]string, error) {
	rows, err := admin.Query(ctx,
		`SELECT a.attname
		   FROM pg_index i
		   JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		  WHERE i.indrelid = $1::regclass AND i.indisprimary
		  ORDER BY array_position(i.indkey, a.attnum)`, t.quoted())
	if err != nil {
		return nil, fmt.Errorf("postgres: read primary key of %s: %w", t, err)
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("postgres: read primary key of %s: %w", t, err)
		}
		keys = append(keys, name)
	}
	return keys, rows.Err()
}
