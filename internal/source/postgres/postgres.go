// Package postgres reads changes with Postgres' own logical replication
// protocol via pglogrepl and the built-in pgoutput plugin. No external output
// plugin, no JVM.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/aupv9/slipstream/internal/cdc"
	"github.com/aupv9/slipstream/internal/config"
	"github.com/aupv9/slipstream/internal/controlplane"
	"github.com/aupv9/slipstream/internal/source"
)

// standbyInterval is how often we tell the server which LSN we have flushed.
// Until we do, Postgres must retain that WAL, so this doubles as the knob that
// keeps the slot from pinning the disk.
const standbyInterval = 10 * time.Second

// Reader captures changes from one Postgres database.
type Reader struct {
	cfg      config.Postgres
	sourceID string
	log      *slog.Logger

	repl    *pgconn.PgConn
	rels    map[uint32]*pglogrepl.RelationMessage
	typeMap *pgtype.Map

	// ackLSN is written by the pipeline (Ack) and read by the streaming loop.
	ackLSN atomic.Uint64
	// serverLSN is the furthest position the server has told us about, from
	// keepalives and data messages. The gap to ackLSN is the WAL the server is
	// still holding for us.
	serverLSN atomic.Uint64
	// lastEmitted is the position of the last event handed to the pipeline.
	lastEmitted atomic.Uint64
	// caughtUp is true when every event emitted so far has been acknowledged,
	// which is what makes it safe to acknowledge past uncaptured WAL.
	caughtUp atomic.Bool
}

// New builds a Postgres reader. Nothing is connected until ReadChanges runs.
func New(cfg config.Postgres, sourceID string, log *slog.Logger) *Reader {
	r := &Reader{
		cfg:      cfg,
		sourceID: sourceID,
		log:      log.With("source", "postgres", "slot", cfg.Slot),
		rels:     make(map[uint32]*pglogrepl.RelationMessage),
		typeMap:  pgtype.NewMap(),
	}
	// Nothing emitted yet means nothing outstanding.
	r.caughtUp.Store(true)
	return r
}

// Name identifies the reader.
func (r *Reader) Name() string { return "postgres" }

// Ack records the position the slowest sink has accepted. Only that position
// may be reported to the server: acknowledging read-ahead would let Postgres
// discard WAL we might still need after a crash.
func (r *Reader) Ack(position string) {
	lsn, err := pglogrepl.ParseLSN(position)
	if err != nil {
		return
	}
	// Everything emitted has now been accepted, so the slot no longer needs to
	// hold WAL on account of this pipeline.
	r.caughtUp.Store(uint64(lsn) >= r.lastEmitted.Load())
	advance(&r.ackLSN, uint64(lsn))
}

// advance raises an atomic to v if v is further along.
func advance(target *atomic.Uint64, v uint64) {
	for {
		cur := target.Load()
		if v <= cur {
			return
		}
		if target.CompareAndSwap(cur, v) {
			return
		}
	}
}

// LagBytes reports how much WAL the server is still retaining for this slot.
func (r *Reader) LagBytes() (int64, bool) {
	server := r.serverLSN.Load()
	acked := r.ackLSN.Load()
	if server == 0 || acked == 0 {
		return 0, false
	}
	if server < acked {
		return 0, true
	}
	return int64(server - acked), true
}

// noteServerPosition records the furthest position the server has reported.
func (r *Reader) noteServerPosition(lsn pglogrepl.LSN) {
	advance(&r.serverLSN, uint64(lsn))
}

// noteIdleProgress acknowledges WAL the server has sent that produced nothing
// for us to deliver.
//
// Without this, a slot pins WAL forever whenever the captured tables are quiet
// while the rest of the database is busy: the acknowledged position only moves
// when one of our own events is committed, so the gap — and the disk usage —
// grows without bound. A keepalive is only sent after every data message before
// it, so once everything already emitted has been acknowledged, the position it
// reports is safe to acknowledge too.
func (r *Reader) noteIdleProgress(serverEnd pglogrepl.LSN) {
	if !r.caughtUp.Load() {
		return
	}
	advance(&r.ackLSN, uint64(serverEnd))
}

// Close drops the replication connection.
func (r *Reader) Close() error {
	if r.repl == nil {
		return nil
	}
	err := r.repl.Close(context.Background())
	r.repl = nil
	return err
}

// ReadChanges resumes from req.From, or bootstraps (snapshot + slot creation),
// then streams until ctx is cancelled.
func (r *Reader) ReadChanges(ctx context.Context, req source.ReadRequest, out chan<- cdc.ChangeEvent) error {
	if r.cfg.DSN == "" {
		return errors.New("postgres: dsn is required")
	}

	admin, err := pgx.Connect(ctx, r.cfg.DSN)
	if err != nil {
		return fmt.Errorf("postgres: connect: %w", err)
	}
	defer admin.Close(context.Background())

	if err := r.reconcilePublication(ctx, admin); err != nil {
		return err
	}

	repl, err := r.connectReplication(ctx)
	if err != nil {
		return err
	}
	r.repl = repl
	defer r.Close()

	chunked := r.cfg.SnapshotMode == config.SnapshotChunked && r.cfg.Snapshot

	var startLSN pglogrepl.LSN
	if req.From != "" && !req.ForceBootstrap {
		if startLSN, err = pglogrepl.ParseLSN(req.From); err != nil {
			return fmt.Errorf("postgres: stored offset %q is not an LSN: %w", req.From, err)
		}
		r.log.Info("resuming from stored offset", "lsn", startLSN)
	} else {
		if req.From != "" {
			r.log.Warn("discarding the stored offset and snapshotting again",
				"offset", req.From,
				"reason", "the previous snapshot did not complete, so this offset covers only part of the data")
		}
		if startLSN, err = r.bootstrap(ctx, admin, req, out); err != nil {
			return err
		}
	}

	// A chunked snapshot runs inside the stream loop, so it also has to be set
	// up when resuming an interrupted one.
	var chunk *chunker
	if chunked && !r.snapshotComplete(req) {
		size := r.cfg.ChunkSize
		if size <= 0 {
			size = 10000
		}
		if chunk, err = newChunker(ctx, r, admin, req.Hooks, req.ResumeSnapshot, size); err != nil {
			return err
		}
	}

	r.ackLSN.Store(uint64(startLSN))
	return r.stream(ctx, startLSN, chunk, out)
}

// snapshotComplete reports whether the snapshot is already finished, in which
// case this run only streams.
func (r *Reader) snapshotComplete(req source.ReadRequest) bool {
	// A resume with no snapshot to continue means the previous snapshot
	// finished; a bootstrap always starts one.
	return req.From != "" && !req.ForceBootstrap && req.ResumeSnapshot == nil
}

// connectReplication opens the streaming connection. The replication parameter
// is set on the parsed config rather than by editing the DSN string, so both
// URL and keyword/value DSNs work.
func (r *Reader) connectReplication(ctx context.Context) (*pgconn.PgConn, error) {
	cfg, err := pgconn.ParseConfig(r.cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse dsn: %w", err)
	}
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = map[string]string{}
	}
	cfg.RuntimeParams["replication"] = "database"

	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect replication: %w", err)
	}
	return conn, nil
}

// bootstrap creates the slot and, while its exported snapshot is still valid,
// reads every table inside it.
//
// This is the step that is easy to get wrong. CREATE_REPLICATION_SLOT with
// snapshot export hands back a snapshot name bound to the exact LSN at which
// the slot starts decoding. A REPEATABLE READ transaction that adopts that
// snapshot therefore sees the database as of precisely that LSN: everything
// older is in the snapshot, everything newer arrives on the stream, and the
// boundary has neither a gap nor an overlap. The replication connection must
// stay open the whole time or the exported snapshot is discarded.
//
// A slot left over from an earlier attempt is dropped rather than reused. We
// only reach bootstrap when there is no offset worth resuming from, and an
// exported snapshot can only be taken by a freshly created slot — reusing the
// old one would mean streaming forward without ever reading the rows the
// interrupted snapshot never got to.
func (r *Reader) bootstrap(ctx context.Context, admin *pgx.Conn, req source.ReadRequest, out chan<- cdc.ChangeEvent) (pglogrepl.LSN, error) {
	if err := r.dropSlotIfExists(ctx, admin); err != nil {
		return 0, err
	}

	chunked := r.cfg.SnapshotMode == config.SnapshotChunked && r.cfg.Snapshot
	mode := controlplane.SnapshotSingle
	if chunked {
		mode = controlplane.SnapshotChunked
	}

	// Mark the snapshot as started before anything is emitted, so an instance
	// that dies partway through leaves evidence about its offset: partial for a
	// single-transaction snapshot, resumable for a chunked one.
	if req.Hooks != nil {
		if err := req.Hooks.SnapshotStarted(ctx, mode); err != nil {
			return 0, fmt.Errorf("postgres: record snapshot start: %w", err)
		}
	}

	// A chunked snapshot reads key ranges alongside the stream, so it needs no
	// exported snapshot and no long-lived transaction.
	action, err := snapshotAction(ctx, admin, r.cfg.Snapshot && !chunked)
	if err != nil {
		return 0, err
	}

	slot, err := pglogrepl.CreateReplicationSlot(ctx, r.repl, r.cfg.Slot, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{
			Mode:           pglogrepl.LogicalReplication,
			SnapshotAction: action,
		})
	if err != nil {
		if isDuplicateObject(err) {
			// We just dropped it, so someone else recreated it: another
			// instance believes it is the leader. Stop rather than race it.
			return 0, fmt.Errorf("postgres: slot %s reappeared during bootstrap; "+
				"another instance may be reading this source: %w", r.cfg.Slot, err)
		}
		return 0, fmt.Errorf("postgres: create slot %s: %w", r.cfg.Slot, err)
	}

	consistent, err := pglogrepl.ParseLSN(slot.ConsistentPoint)
	if err != nil {
		return 0, fmt.Errorf("postgres: parse consistent point %q: %w", slot.ConsistentPoint, err)
	}
	r.log.Info("created replication slot", "consistent_point", consistent, "snapshot", slot.SnapshotName)

	switch {
	case chunked:
		// The chunker runs inside the stream loop and reports completion
		// itself, once every table has been read.
		r.log.Info("chunked snapshot will run alongside the stream", "chunk_size", r.cfg.ChunkSize)
		return consistent, nil

	case r.cfg.Snapshot:
		if err := r.snapshot(ctx, admin, slot.SnapshotName, consistent, out); err != nil {
			return 0, err
		}

	default:
		r.log.Info("snapshot disabled; streaming only from the slot's consistent point")
	}

	if req.Hooks != nil {
		if err := req.Hooks.SnapshotCompleted(ctx, consistent.String()); err != nil {
			return 0, fmt.Errorf("postgres: record snapshot completion: %w", err)
		}
	}
	return consistent, nil
}

// dropSlotIfExists removes a slot left behind by an earlier attempt. A slot
// still held open by a dying instance cannot be dropped, so this waits for it
// to go inactive rather than giving up immediately.
func (r *Reader) dropSlotIfExists(ctx context.Context, admin *pgx.Conn) error {
	var activePID *int32
	err := admin.QueryRow(ctx,
		`SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1`, r.cfg.Slot).Scan(&activePID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("postgres: inspect slot %s: %w", r.cfg.Slot, err)
	}

	r.log.Warn("dropping a replication slot left by an earlier attempt",
		"slot", r.cfg.Slot, "held_by_pid", activePID)

	// The previous holder's backend may take a moment to be reaped.
	deadline := time.Now().Add(30 * time.Second)
	for attempt := 1; ; attempt++ {
		_, err := admin.Exec(ctx, `SELECT pg_drop_replication_slot($1)`, r.cfg.Slot)
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("postgres: could not drop slot %s after %d attempts "+
				"(still in use by another reader?): %w", r.cfg.Slot, attempt, err)
		}
		r.log.Warn("slot still in use; retrying the drop", "slot", r.cfg.Slot, "err", err)
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// snapshotAction picks the option syntax the server understands. Postgres 15
// replaced the bare EXPORT_SNAPSHOT keyword with a parenthesized option list
// and still accepts the old form, so we send whichever the server prefers.
func snapshotAction(ctx context.Context, admin *pgx.Conn, wantSnapshot bool) (string, error) {
	var verNum int
	if err := admin.QueryRow(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&verNum); err != nil {
		return "", fmt.Errorf("postgres: read server version: %w", err)
	}
	modern := verNum >= 150000
	switch {
	case wantSnapshot && modern:
		return "(SNAPSHOT 'export')", nil
	case wantSnapshot:
		return "EXPORT_SNAPSHOT", nil
	case modern:
		return "(SNAPSHOT 'nothing')", nil
	default:
		return "NOEXPORT_SNAPSHOT", nil
	}
}

// snapshot reads all captured tables inside the exported snapshot.
func (r *Reader) snapshot(ctx context.Context, admin *pgx.Conn, snapshotName string, at pglogrepl.LSN, out chan<- cdc.ChangeEvent) error {
	tables, err := r.capturedTables(ctx, admin)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return fmt.Errorf("postgres: publication %s captures no tables", r.cfg.Publication)
	}

	tx, err := admin.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return fmt.Errorf("postgres: begin snapshot tx: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	// Must be the first statement in the transaction.
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT %s", quoteLiteral(snapshotName))); err != nil {
		return fmt.Errorf("postgres: adopt exported snapshot: %w", err)
	}

	for _, t := range tables {
		n, err := r.snapshotTable(ctx, tx, t, at, out)
		if err != nil {
			return err
		}
		r.log.Info("snapshot complete", "table", t.String(), "rows", n)
	}
	return tx.Commit(ctx)
}

func (r *Reader) snapshotTable(ctx context.Context, tx pgx.Tx, t table, at pglogrepl.LSN, out chan<- cdc.ChangeEvent) (int64, error) {
	rows, err := tx.Query(ctx, fmt.Sprintf("SELECT * FROM %s", t.quoted()))
	if err != nil {
		return 0, fmt.Errorf("postgres: snapshot %s: %w", t, err)
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	var count int64
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return count, fmt.Errorf("postgres: snapshot %s: read row: %w", t, err)
		}
		after := make(map[string]any, len(fields))
		for i, f := range fields {
			if i < len(values) {
				after[f.Name] = values[i]
			}
		}
		ev := cdc.ChangeEvent{
			SourceID: r.sourceID,
			Schema:   t.Schema,
			Table:    t.Name,
			Op:       cdc.OpRead,
			After:    after,
			Position: at.String(),
			CommitTS: time.Time{},
		}
		if err := emit(ctx, out, ev); err != nil {
			return count, err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("postgres: snapshot %s: %w", t, err)
	}
	return count, nil
}

// stream is the replication loop: decode pgoutput messages, emit row events,
// and periodically report the acknowledged LSN.
func (r *Reader) stream(ctx context.Context, startLSN pglogrepl.LSN, chunk *chunker, out chan<- cdc.ChangeEvent) error {
	args := []string{
		"proto_version '1'",
		fmt.Sprintf("publication_names '%s'", r.cfg.Publication),
	}
	if chunk != nil {
		// The chunker's watermarks travel as logical decoding messages, which
		// pgoutput only forwards when asked.
		args = append(args, "messages 'true'")
	}
	if err := pglogrepl.StartReplication(ctx, r.repl, r.cfg.Slot, startLSN,
		pglogrepl.StartReplicationOptions{PluginArgs: args}); err != nil {
		return fmt.Errorf("postgres: start replication at %s: %w", startLSN, err)
	}
	r.log.Info("streaming", "from", startLSN, "publication", r.cfg.Publication)

	nextStandby := time.Now().Add(standbyInterval)
	var txCommitTS time.Time

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		if time.Now().After(nextStandby) {
			if err := r.sendStandbyUpdate(ctx); err != nil {
				return err
			}
			nextStandby = time.Now().Add(standbyInterval)
		}

		if chunk != nil && chunk.idle() {
			if err := chunk.startChunk(ctx); err != nil {
				return err
			}
		}

		recvCtx, cancel := context.WithDeadline(ctx, nextStandby)
		raw, err := r.repl.ReceiveMessage(recvCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("postgres: receive: %w", err)
		}

		copyData, ok := raw.(*pgproto3.CopyData)
		if !ok {
			// Errors and other protocol messages are not expected mid-stream.
			r.log.Debug("ignoring non-CopyData message", "type", fmt.Sprintf("%T", raw))
			continue
		}
		if len(copyData.Data) == 0 {
			continue
		}

		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			ka, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("postgres: parse keepalive: %w", err)
			}
			r.noteServerPosition(ka.ServerWALEnd)
			r.noteIdleProgress(ka.ServerWALEnd)
			if ka.ReplyRequested {
				if err := r.sendStandbyUpdate(ctx); err != nil {
					return err
				}
				nextStandby = time.Now().Add(standbyInterval)
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("postgres: parse xlogdata: %w", err)
			}
			r.noteServerPosition(xld.ServerWALEnd)
			msg, err := pglogrepl.Parse(xld.WALData)
			if err != nil {
				// Unknown message types (logical decoding messages, stream
				// control in proto v2) are not fatal.
				r.log.Debug("skipping undecodable message", "err", err)
				continue
			}
			if err := r.handle(ctx, msg, xld, &txCommitTS, chunk, out); err != nil {
				return err
			}
		}
	}
}

func (r *Reader) handle(ctx context.Context, msg pglogrepl.Message, xld pglogrepl.XLogData, txCommitTS *time.Time, chunk *chunker, out chan<- cdc.ChangeEvent) error {
	switch m := msg.(type) {
	case *pglogrepl.LogicalDecodingMessage:
		if chunk != nil {
			return chunk.onMessage(ctx, m, xld, out)
		}
		return nil

	case *pglogrepl.RelationMessage:
		r.rels[m.RelationID] = m
		return nil

	case *pglogrepl.BeginMessage:
		*txCommitTS = m.CommitTime
		return nil

	case *pglogrepl.CommitMessage:
		*txCommitTS = m.CommitTime
		return nil

	case *pglogrepl.InsertMessage:
		rel, ok := r.rels[m.RelationID]
		if !ok {
			return fmt.Errorf("postgres: insert for unknown relation %d", m.RelationID)
		}
		return r.emitRow(ctx, chunk, out, r.event(rel, cdc.OpCreate, nil, r.decodeTuple(rel, m.Tuple), xld, *txCommitTS))

	case *pglogrepl.UpdateMessage:
		rel, ok := r.rels[m.RelationID]
		if !ok {
			return fmt.Errorf("postgres: update for unknown relation %d", m.RelationID)
		}
		return r.emitRow(ctx, chunk, out, r.event(rel, cdc.OpUpdate,
			r.decodeTuple(rel, m.OldTuple), r.decodeTuple(rel, m.NewTuple), xld, *txCommitTS))

	case *pglogrepl.DeleteMessage:
		rel, ok := r.rels[m.RelationID]
		if !ok {
			return fmt.Errorf("postgres: delete for unknown relation %d", m.RelationID)
		}
		return r.emitRow(ctx, chunk, out, r.event(rel, cdc.OpDelete, r.decodeTuple(rel, m.OldTuple), nil, xld, *txCommitTS))

	case *pglogrepl.TruncateMessage:
		// One TRUNCATE can name several relations. Dropping this message would
		// leave sinks holding every row the source just discarded.
		for _, id := range m.RelationIDs {
			rel, ok := r.rels[id]
			if !ok {
				return fmt.Errorf("postgres: truncate for unknown relation %d", id)
			}
			if err := r.emitRow(ctx, chunk, out, r.event(rel, cdc.OpTruncate, nil, nil, xld, *txCommitTS)); err != nil {
				return err
			}
		}
		return nil

	default:
		return nil
	}
}

// emitRow sends a streamed change, first letting a running chunk note that this
// row has been overtaken.
func (r *Reader) emitRow(ctx context.Context, chunk *chunker, out chan<- cdc.ChangeEvent, ev cdc.ChangeEvent) error {
	if chunk != nil {
		chunk.observe(ev)
	}
	if lsn, err := pglogrepl.ParseLSN(ev.Position); err == nil {
		advance(&r.lastEmitted, uint64(lsn))
		r.caughtUp.Store(false)
	}
	return emit(ctx, out, ev)
}

func (r *Reader) event(rel *pglogrepl.RelationMessage, op cdc.Op, before, after map[string]any, xld pglogrepl.XLogData, commitTS time.Time) cdc.ChangeEvent {
	// WALStart is where this record begins, which is exactly where a resume
	// would restart decoding, so it is the position worth storing.
	return cdc.ChangeEvent{
		SourceID: r.sourceID,
		Schema:   rel.Namespace,
		Table:    rel.RelationName,
		Op:       op,
		Before:   before,
		After:    after,
		Position: xld.WALStart.String(),
		CommitTS: commitTS,
	}
}

// decodeTuple turns a pgoutput tuple into column name -> Go value.
func (r *Reader) decodeTuple(rel *pglogrepl.RelationMessage, tup *pglogrepl.TupleData) map[string]any {
	if tup == nil {
		return nil
	}
	values := make(map[string]any, len(tup.Columns))
	for i, col := range tup.Columns {
		if i >= len(rel.Columns) {
			break
		}
		name := rel.Columns[i].Name
		switch col.DataType {
		case pglogrepl.TupleDataTypeNull:
			values[name] = nil
		case pglogrepl.TupleDataTypeToast:
			// Unchanged TOASTed value: the server did not send it. Omitting
			// the key is honest; writing nil would look like a real NULL and
			// could wipe the column at the sink.
			continue
		default:
			values[name] = r.decodeValue(rel.Columns[i].DataType, col.Data)
		}
	}
	return values
}

func (r *Reader) decodeValue(oid uint32, data []byte) any {
	if dt, ok := r.typeMap.TypeForOID(oid); ok {
		if v, err := dt.Codec.DecodeValue(r.typeMap, oid, pgtype.TextFormatCode, data); err == nil {
			return v
		}
	}
	return string(data)
}

func (r *Reader) sendStandbyUpdate(ctx context.Context) error {
	lsn := pglogrepl.LSN(r.ackLSN.Load())
	if err := pglogrepl.SendStandbyStatusUpdate(ctx, r.repl,
		pglogrepl.StandbyStatusUpdate{WALWritePosition: lsn}); err != nil {
		return fmt.Errorf("postgres: standby status update: %w", err)
	}
	r.log.Debug("acknowledged", "lsn", lsn)
	return nil
}

// reconcilePublication creates the publication on first run, and on later runs
// checks it still covers the configured tables.
//
// The dangerous case is a table added to the config after the publication
// exists: the old code saw the publication and returned, so pgoutput never
// decoded that table and nothing said so. Silence there means an operator
// believes a table is replicating when it is not.
func (r *Reader) reconcilePublication(ctx context.Context, admin *pgx.Conn) error {
	var exists bool
	if err := admin.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)`,
		r.cfg.Publication).Scan(&exists); err != nil {
		return fmt.Errorf("postgres: check publication: %w", err)
	}
	if !exists {
		return r.createPublication(ctx, admin)
	}
	if len(r.cfg.Tables) == 0 {
		// Nothing declared: the publication's own membership is the source of
		// truth (FOR ALL TABLES, or managed by hand).
		return nil
	}

	published, err := r.capturedTables(ctx, admin)
	if err != nil {
		return err
	}
	have := make(map[string]bool, len(published))
	for _, t := range published {
		have[t.String()] = true
	}

	var missing []table
	for _, name := range r.cfg.Tables {
		t, err := parseTable(name)
		if err != nil {
			return err
		}
		if !have[t.String()] {
			missing = append(missing, t)
		}
		delete(have, t.String())
	}
	for extra := range have {
		r.log.Warn("publication captures a table that is not in the config; leaving it alone",
			"publication", r.cfg.Publication, "table", extra)
	}
	if len(missing) == 0 {
		return nil
	}

	names := make([]string, 0, len(missing))
	quoted := make([]string, 0, len(missing))
	for _, t := range missing {
		names = append(names, t.String())
		quoted = append(quoted, t.quoted())
	}

	if !r.cfg.AutoAddTables {
		return fmt.Errorf("postgres: publication %s does not capture %v. "+
			"Adding a table to an existing publication cannot be snapshotted consistently, "+
			"so this is not done silently. Either give the table its own pipeline (it gets a "+
			"proper snapshot), or re-bootstrap this one by dropping slot %s and its offsets row, "+
			"or set auto_add_tables: true to capture it streaming-only, accepting that rows "+
			"written before now are never delivered",
			r.cfg.Publication, names, r.cfg.Slot)
	}

	if _, err := admin.Exec(ctx, fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s",
		quoteIdent(r.cfg.Publication), strings.Join(quoted, ", "))); err != nil {
		return fmt.Errorf("postgres: add tables %v to publication %s: %w", names, r.cfg.Publication, err)
	}
	r.log.Warn("added tables to the publication without a snapshot; "+
		"rows written before now will not be delivered for them",
		"publication", r.cfg.Publication, "tables", names)
	return nil
}

func (r *Reader) createPublication(ctx context.Context, admin *pgx.Conn) error {
	stmt := fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", quoteIdent(r.cfg.Publication))
	if len(r.cfg.Tables) > 0 {
		quoted := make([]string, 0, len(r.cfg.Tables))
		for _, name := range r.cfg.Tables {
			t, err := parseTable(name)
			if err != nil {
				return err
			}
			quoted = append(quoted, t.quoted())
		}
		stmt = fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s",
			quoteIdent(r.cfg.Publication), strings.Join(quoted, ", "))
	}
	if _, err := admin.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("postgres: create publication %s: %w", r.cfg.Publication, err)
	}
	r.log.Info("created publication", "publication", r.cfg.Publication, "tables", r.cfg.Tables)
	return nil
}

// capturedTables is the authoritative table list: whatever the publication
// actually decodes, so snapshot and stream can never disagree about scope.
func (r *Reader) capturedTables(ctx context.Context, admin *pgx.Conn) ([]table, error) {
	rows, err := admin.Query(ctx,
		`SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = $1 ORDER BY schemaname, tablename`,
		r.cfg.Publication)
	if err != nil {
		return nil, fmt.Errorf("postgres: list publication tables: %w", err)
	}
	defer rows.Close()

	var out []table
	for rows.Next() {
		var t table
		if err := rows.Scan(&t.Schema, &t.Name); err != nil {
			return nil, fmt.Errorf("postgres: scan publication table: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func emit(ctx context.Context, out chan<- cdc.ChangeEvent, ev cdc.ChangeEvent) error {
	select {
	case out <- ev:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// table is a schema-qualified relation name.
type table struct {
	Schema string
	Name   string
}

func (t table) String() string { return t.Schema + "." + t.Name }

func (t table) quoted() string {
	if t.Schema == "" {
		return quoteIdent(t.Name)
	}
	return quoteIdent(t.Schema) + "." + quoteIdent(t.Name)
}

func parseTable(name string) (table, error) {
	parts := strings.SplitN(name, ".", 2)
	if len(parts) == 1 {
		return table{Schema: "public", Name: parts[0]}, nil
	}
	if parts[0] == "" || parts[1] == "" {
		return table{}, fmt.Errorf("postgres: invalid table name %q, want schema.table", name)
	}
	return table{Schema: parts[0], Name: parts[1]}, nil
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func quoteLiteral(s string) string {
	return `'` + strings.ReplaceAll(s, `'`, `''`) + `'`
}

func isDuplicateObject(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "42710" // duplicate_object
	}
	return false
}
