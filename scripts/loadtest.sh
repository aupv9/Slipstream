#!/usr/bin/env bash
#
# Measures what one pipeline actually sustains, so the decision to turn on
# chunked snapshots (or to shard a pipeline) rests on numbers rather than on a
# guess about "large tables".
#
# It reports snapshot throughput, streaming throughput under a steady write
# load, and the worst lag observed — all scraped from the pipeline's own
# /metrics, so the numbers are the ones an operator would see in production.
#
# Usage:
#   CP_DSN=postgres://postgres@127.0.0.1:5432/slipstream_cp \
#   SRC_DSN=postgres://postgres@127.0.0.1:5432/app \
#   ROWS=1000000 WRITERS=4 DURATION=60 scripts/loadtest.sh
#
# SNAPSHOT_MODE=chunked compares the two snapshot strategies on the same data.

set -euo pipefail

CP_DSN=${CP_DSN:?set CP_DSN}
SRC_DSN=${SRC_DSN:?set SRC_DSN}

PIPELINE=${PIPELINE:-loadtest}
SLOT="slipstream_${PIPELINE//-/_}"
TABLE=${TABLE:-loadtest}
ROWS=${ROWS:-500000}
WRITERS=${WRITERS:-2}
DURATION=${DURATION:-30}
SNAPSHOT_MODE=${SNAPSHOT_MODE:-single}
CHUNK_SIZE=${CHUNK_SIZE:-10000}
METRICS_PORT=${METRICS_PORT:-19090}
WORKDIR=$(mktemp -d)

log() { printf '\n=== %s\n' "$*"; }

cleanup() {
  for pid in ${PIDS:-}; do kill -9 "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  psql "$SRC_DSN" -qtc "SELECT pg_drop_replication_slot('$SLOT')
        WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '$SLOT')" >/dev/null 2>&1 || true
  psql "$SRC_DSN" -qc "DROP PUBLICATION IF EXISTS $SLOT" >/dev/null 2>&1 || true
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

# metric reads one counter or gauge out of the exposition text.
metric() {
  curl -s "http://127.0.0.1:${METRICS_PORT}/metrics" \
    | awk -v name="$1" '$0 ~ "^" name "[{ ]" { v=$NF } END { print (v == "" ? 0 : v) }'
}

log "building"
go build -o "$WORKDIR/slipstream" ./cmd/slipstream

log "loading $ROWS rows"
psql "$SRC_DSN" -qc "DROP TABLE IF EXISTS $TABLE" \
  -c "CREATE TABLE $TABLE (id bigint PRIMARY KEY, payload text, updated_at timestamptz DEFAULT now())"
psql "$SRC_DSN" -qtc "SELECT pg_drop_replication_slot('$SLOT')
      WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '$SLOT')" >/dev/null
psql "$SRC_DSN" -qc "DROP PUBLICATION IF EXISTS $SLOT" >/dev/null
for t in offsets leases sink_cursor snapshot_state; do
  psql "$CP_DSN" -qc "DELETE FROM $t WHERE pipeline_id = '$PIPELINE'" >/dev/null 2>&1 || true
done
psql "$SRC_DSN" -qc "INSERT INTO $TABLE SELECT g, repeat(md5(g::text), 4), now() FROM generate_series(1, $ROWS) g"

cat > "$WORKDIR/slipstream.yaml" <<YAML
instance_id: load-1
observability:
  addr: 127.0.0.1:${METRICS_PORT}
control_plane:
  dsn: $CP_DSN
  lease_ttl: 10s
  lease_renew: 3s
  auto_migrate: true
pipeline:
  id: $PIPELINE
  commit_interval: 1s
  source:
    type: postgres
    postgres:
      dsn: $SRC_DSN
      tables: [public.$TABLE]
      snapshot: true
      snapshot_mode: $SNAPSHOT_MODE
      chunk_size: $CHUNK_SIZE
  sinks:
    - name: devnull
      type: stdout
      batch_max_events: 1000
      batch_max_wait: 50ms
YAML

log "snapshot: mode=$SNAPSHOT_MODE rows=$ROWS"
START=$(date +%s.%N)
"$WORKDIR/slipstream" run -config "$WORKDIR/slipstream.yaml" -log-level warn \
  >/dev/null 2>"$WORKDIR/slipstream.log" &
PIDS=$!

for _ in $(seq 1 3000); do
  if curl -sf "http://127.0.0.1:${METRICS_PORT}/healthz" >/dev/null 2>&1; then break; fi
  sleep 0.1
done

while :; do
  read_rows=$(metric slipstream_snapshot_rows_total)
  running=$(metric slipstream_snapshot_running)
  # Awk compares as numbers; the counter is a float in the exposition format.
  if awk -v a="$read_rows" -v b="$ROWS" 'BEGIN { exit !(a >= b) }' && [ "${running%.*}" = "0" ]; then
    break
  fi
  sleep 0.2
done
END=$(date +%s.%N)
SNAP_SECS=$(awk -v s="$START" -v e="$END" 'BEGIN { printf "%.1f", e - s }')
SNAP_RATE=$(awk -v r="$ROWS" -v s="$SNAP_SECS" 'BEGIN { printf "%.0f", r / (s > 0 ? s : 1) }')

log "streaming: ${WRITERS} writers for ${DURATION}s"
WRITER_PIDS=""
for w in $(seq 1 "$WRITERS"); do
  (
    end=$(( $(date +%s) + DURATION ))
    i=$(( ROWS + w * 10000000 ))
    while [ "$(date +%s)" -lt "$end" ]; do
      for _ in $(seq 1 50); do
        i=$(( i + 1 ))
        echo "INSERT INTO $TABLE VALUES ($i, 'live', now());"
      done
      echo "UPDATE $TABLE SET updated_at = now() WHERE id = $(( RANDOM % ROWS + 1 ));"
    done | psql "$SRC_DSN" -q >/dev/null 2>&1
  ) &
  WRITER_PIDS="$WRITER_PIDS $!"
done
PIDS="$PIDS $WRITER_PIDS"

BEFORE=$(metric slipstream_events_read_total)
PEAK_LAG=0
SAMPLES=0
for _ in $(seq 1 "$DURATION"); do
  sleep 1
  lag=$(metric slipstream_source_lag_bytes)
  PEAK_LAG=$(awk -v a="$lag" -v b="$PEAK_LAG" 'BEGIN { print (a > b ? a : b) }')
  SAMPLES=$((SAMPLES + 1))
done
AFTER=$(metric slipstream_events_read_total)

# Let the pipeline drain what the writers left behind.
for _ in $(seq 1 120); do
  lag=$(metric slipstream_source_lag_bytes)
  awk -v a="$lag" 'BEGIN { exit !(a < 1000000) }' && break
  sleep 1
done

STREAM_RATE=$(awk -v a="$AFTER" -v b="$BEFORE" -v d="$DURATION" 'BEGIN { printf "%.0f", (a - b) / d }')
FINAL_LAG=$(metric slipstream_source_lag_bytes)
WRITTEN=$(metric slipstream_events_written_total)
FAILS=$(metric slipstream_write_failures_total)

log "results"
cat <<REPORT
snapshot mode        : $SNAPSHOT_MODE (chunk_size=$CHUNK_SIZE)
rows snapshotted     : $ROWS in ${SNAP_SECS}s  =>  ${SNAP_RATE} rows/s
streaming throughput : ${STREAM_RATE} events/s over ${DURATION}s with ${WRITERS} writers
peak lag             : ${PEAK_LAG} bytes
lag after drain      : ${FINAL_LAG} bytes
events written       : ${WRITTEN}
sink write failures  : ${FAILS}

Reading these numbers:
  * Snapshot rate sets how long a first load takes. Divide your largest table by
    it; if the answer is longer than you are willing to hold a transaction open,
    switch that pipeline to snapshot_mode: chunked.
  * If peak lag keeps climbing rather than settling, the sink is the bottleneck,
    not the source: check slipstream_sink_queue_depth per sink.
  * Any write failures here mean the numbers above are optimistic.
REPORT
