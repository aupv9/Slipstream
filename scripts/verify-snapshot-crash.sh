#!/usr/bin/env bash
#
# Regression check for a real data-loss bug.
#
# Sinks accept snapshot rows as they arrive, so the pipeline offset advances to
# the slot's consistent point while the initial snapshot is still running. An
# instance killed midway used to leave an offset that looked perfectly
# resumable but covered only the rows already read; the next run streamed
# forward from it and the remaining rows were never delivered. The pipeline
# stayed green while permanently missing data.
#
# This script kills the leader mid-snapshot, restarts it, and asserts the sink
# converges to the full row count.
#
# Usage:
#   CP_DSN=postgres://postgres@127.0.0.1:5432/slipstream_cp \
#   SRC_DSN=postgres://postgres@127.0.0.1:5432/app \
#   MIRROR_DSN=postgres://postgres@127.0.0.1:5432/mirror \
#   scripts/verify-snapshot-crash.sh

set -euo pipefail

CP_DSN=${CP_DSN:?set CP_DSN}
SRC_DSN=${SRC_DSN:?set SRC_DSN}
MIRROR_DSN=${MIRROR_DSN:?set MIRROR_DSN}

PIPELINE=${PIPELINE:-snapshot-crash-check}
SLOT="slipstream_${PIPELINE//-/_}"
TABLE=snapcrash
ROWS=${ROWS:-400000}
# Kill once the snapshot is demonstrably underway but nowhere near done, rather
# than after a fixed sleep, so the check does not depend on machine speed.
KILL_AT_ROWS=${KILL_AT_ROWS:-2000}
KILL_TIMEOUT=${KILL_TIMEOUT:-120}
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

log "building"
go build -o "$WORKDIR/slipstream" ./cmd/slipstream

log "preparing $ROWS rows"
psql "$SRC_DSN" -qc "DROP TABLE IF EXISTS $TABLE" \
  -c "CREATE TABLE $TABLE (id bigint PRIMARY KEY, payload text)"
psql "$MIRROR_DSN" -qc "DROP TABLE IF EXISTS $TABLE" \
  -c "CREATE TABLE $TABLE (id bigint PRIMARY KEY, payload text)"
psql "$SRC_DSN" -qtc "SELECT pg_drop_replication_slot('$SLOT')
      WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '$SLOT')" >/dev/null
psql "$SRC_DSN" -qc "DROP PUBLICATION IF EXISTS $SLOT" >/dev/null
for t in offsets leases sink_cursor snapshot_state; do
  psql "$CP_DSN" -qc "DELETE FROM $t WHERE pipeline_id = '$PIPELINE'" >/dev/null 2>&1 || true
done
psql "$SRC_DSN" -qc "INSERT INTO $TABLE SELECT g, md5(g::text) || md5((g + 1)::text) FROM generate_series(1, $ROWS) g"

cat > "$WORKDIR/slipstream.yaml" <<YAML
instance_id: crash-1
control_plane:
  dsn: $CP_DSN
  lease_ttl: 5s
  lease_renew: 1s
  auto_migrate: true
pipeline:
  id: $PIPELINE
  commit_interval: 200ms
  source:
    type: postgres
    postgres:
      dsn: $SRC_DSN
      tables: [public.$TABLE]
      snapshot: true
  sinks:
    - name: mirror
      type: pgupsert
      batch_max_events: 500
      pgupsert:
        dsn: $MIRROR_DSN
        keys:
          public.$TABLE: [id]
YAML

mirror_count() { psql "$MIRROR_DSN" -qtAc "SELECT count(*) FROM $TABLE"; }

log "run 1: killing the leader once the snapshot is underway"
"$WORKDIR/slipstream" run -config "$WORKDIR/slipstream.yaml" -log-level warn >"$WORKDIR/run1.log" 2>&1 &
PIDS=$!

stored_offset() {
  psql "$CP_DSN" -qtAc "SELECT coalesce(position, '') FROM offsets WHERE pipeline_id = '$PIPELINE'"
}

# Wait for the exact state that used to be dangerous: rows delivered, a
# committed offset that looks resumable, and the snapshot far from finished.
killed=no
for _ in $(seq 1 $((KILL_TIMEOUT * 5))); do
  n=$(mirror_count)
  if [ "$n" -ge "$ROWS" ]; then break; fi
  if [ "$n" -ge "$KILL_AT_ROWS" ] && [ -n "$(stored_offset)" ]; then
    kill -9 $PIDS
    killed=yes
    break
  fi
  sleep 0.2
done
wait 2>/dev/null || true

if [ "$killed" != "yes" ]; then
  echo "FAIL: never saw the dangerous state (mirrored $(mirror_count) of $ROWS, offset '$(stored_offset)');"
  echo "      raise ROWS or lower KILL_AT_ROWS so there is a window to hit"
  tail -20 "$WORKDIR/run1.log"
  exit 1
fi

PARTIAL=$(mirror_count)
STORED=$(psql "$CP_DSN" -qtAc "SELECT coalesce(position, '') FROM offsets WHERE pipeline_id = '$PIPELINE'")
PHASE=$(psql "$CP_DSN" -qtAc "SELECT coalesce(phase, 'none') FROM snapshot_state WHERE pipeline_id = '$PIPELINE'")
echo "mirrored so far: $PARTIAL of $ROWS"
echo "offset written:  ${STORED:-none}"
echo "snapshot phase:  ${PHASE:-none}"

if [ "$PARTIAL" -ge "$ROWS" ]; then
  echo "FAIL: the snapshot finished before the kill landed; raise ROWS"
  exit 1
fi
if [ -z "$STORED" ]; then
  echo "FAIL: no offset was committed, so this run does not exercise the bug;"
  echo "      raise KILL_AT_ROWS above one commit interval's worth of rows"
  exit 1
fi
if [ "$PHASE" != "running" ]; then
  echo "FAIL: an interrupted snapshot must be left marked 'running', got '${PHASE:-none}'"
  exit 1
fi

log "run 2: restarting; it must re-snapshot rather than resume the partial offset"
"$WORKDIR/slipstream" run -config "$WORKDIR/slipstream.yaml" -log-level info >"$WORKDIR/run2.log" 2>&1 &
PIDS=$!

for _ in $(seq 1 240); do
  [ "$(mirror_count)" -ge "$ROWS" ] && break
  sleep 1
done

FINAL=$(mirror_count)
PHASE=$(psql "$CP_DSN" -qtAc "SELECT phase FROM snapshot_state WHERE pipeline_id = '$PIPELINE'")
echo "mirrored after restart: $FINAL of $ROWS"
echo "snapshot phase:         $PHASE"

# A live change must still flow, proving the pipeline went on to stream.
psql "$SRC_DSN" -qc "INSERT INTO $TABLE VALUES (-1, 'after-recovery')" >/dev/null
for _ in $(seq 1 30); do
  [ "$(psql "$MIRROR_DSN" -qtAc "SELECT count(*) FROM $TABLE WHERE id = -1")" -eq 1 ] && break
  sleep 0.5
done
LIVE=$(psql "$MIRROR_DSN" -qtAc "SELECT count(*) FROM $TABLE WHERE id = -1")

if [ "$FINAL" -ge "$ROWS" ] && [ "$PHASE" = "done" ] && [ "$LIVE" -eq 1 ]; then
  log "PASS: recovered the full $ROWS rows after a mid-snapshot kill, and streaming continued"
else
  log "FAIL: mirrored=$FINAL want>=$ROWS, phase=$PHASE want done, live_change=$LIVE want 1"
  tail -20 "$WORKDIR/run2.log"
  exit 1
fi
