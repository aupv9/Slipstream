#!/usr/bin/env bash
#
# Roadmap step 3 check: kill the leader mid-stream and prove the standby
# resumes without losing or duplicating data.
#
# It preloads a table, starts two Slipstream instances against one control
# plane, writes continuously while killing the leader, then compares the source
# table with the mirrored table row for row.
#
# Usage:
#   CP_DSN=postgres://postgres@127.0.0.1:5432/slipstream_cp \
#   SRC_DSN=postgres://postgres@127.0.0.1:5432/app \
#   MIRROR_DSN=postgres://postgres@127.0.0.1:5432/mirror \
#   scripts/verify-failover.sh
#
# The source database needs wal_level = logical.

set -euo pipefail

CP_DSN=${CP_DSN:?set CP_DSN}
SRC_DSN=${SRC_DSN:?set SRC_DSN}
MIRROR_DSN=${MIRROR_DSN:?set MIRROR_DSN}

PIPELINE=${PIPELINE:-failover-check}
WORKDIR=$(mktemp -d)
TABLE=customers
PRELOAD=${PRELOAD:-5000}
WRITE_ROWS=${WRITE_ROWS:-600}
WRITE_DELAY=${WRITE_DELAY:-0.02}

log() { printf '\n=== %s\n' "$*"; }

cleanup() {
  for pid in ${PIDS:-}; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  psql "$SRC_DSN" -qtc "SELECT pg_drop_replication_slot('slipstream_${PIPELINE//-/_}')
        WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'slipstream_${PIPELINE//-/_}')" >/dev/null 2>&1 || true
  psql "$SRC_DSN" -qc "DROP PUBLICATION IF EXISTS slipstream_${PIPELINE//-/_}" >/dev/null 2>&1 || true
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

log "building"
go build -o "$WORKDIR/slipstream" ./cmd/slipstream

log "resetting source, mirror and control plane"
psql "$SRC_DSN" -qc "DROP TABLE IF EXISTS $TABLE" \
  -c "CREATE TABLE $TABLE (id bigint PRIMARY KEY, name text, changed_at timestamptz DEFAULT now())"
psql "$MIRROR_DSN" -qc "DROP TABLE IF EXISTS $TABLE" \
  -c "CREATE TABLE $TABLE (id bigint PRIMARY KEY, name text, changed_at timestamptz)"
psql "$SRC_DSN" -qtc "SELECT pg_drop_replication_slot('slipstream_${PIPELINE//-/_}')
      WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'slipstream_${PIPELINE//-/_}')" >/dev/null
psql "$SRC_DSN" -qc "DROP PUBLICATION IF EXISTS slipstream_${PIPELINE//-/_}" >/dev/null
for t in offsets leases sink_cursor; do
  psql "$CP_DSN" -qc "DELETE FROM $t WHERE pipeline_id = '$PIPELINE'" >/dev/null 2>&1 || true
done

psql "$SRC_DSN" -qc "INSERT INTO $TABLE SELECT g, 'preloaded-' || g, now() FROM generate_series(1, $PRELOAD) g"
log "preloaded $PRELOAD rows"

cat > "$WORKDIR/slipstream.yaml" <<YAML
instance_id: \${SLIPSTREAM_INSTANCE_ID}
control_plane:
  dsn: $CP_DSN
  lease_ttl: 4s
  lease_renew: 1s
  auto_migrate: true
pipeline:
  id: $PIPELINE
  commit_interval: 500ms
  source:
    type: postgres
    postgres:
      dsn: $SRC_DSN
      tables: [public.$TABLE]
      snapshot: true
  sinks:
    - name: mirror
      type: pgupsert
      batch_max_events: 200
      batch_max_wait: 100ms
      pgupsert:
        dsn: $MIRROR_DSN
        keys:
          public.$TABLE: [id]
YAML

start_instance() {
  local name=$1
  SLIPSTREAM_INSTANCE_ID=$name "$WORKDIR/slipstream" run \
    -config "$WORKDIR/slipstream.yaml" -log-level info >"$WORKDIR/$name.log" 2>&1 &
  echo $!
}

PID_A=$(start_instance inst-a)
sleep 0.5
PID_B=$(start_instance inst-b)
PIDS="$PID_A $PID_B"
log "started inst-a (pid $PID_A) and inst-b (pid $PID_B)"

mirror_count() { psql "$MIRROR_DSN" -qtAc "SELECT count(*) FROM $TABLE"; }

# Wait for the snapshot to land.
for _ in $(seq 1 120); do
  [ "$(mirror_count)" -ge "$PRELOAD" ] && break
  sleep 0.5
done
log "snapshot mirrored: $(mirror_count) rows"

# Write continuously: inserts, an update and a delete.
(
  for i in $(seq $((PRELOAD + 1)) $((PRELOAD + WRITE_ROWS))); do
    echo "INSERT INTO $TABLE VALUES ($i, 'live-$i', now());"
    if [ $((i % 25)) -eq 0 ]; then
      echo "UPDATE $TABLE SET name = 'updated-$i', changed_at = now() WHERE id = $((i - 10));"
      echo "DELETE FROM $TABLE WHERE id = $((i / 2));"
    fi
    sleep "$WRITE_DELAY"
  done
) | psql "$SRC_DSN" -q >/dev/null &
WRITER=$!
PIDS="$PIDS $WRITER"

sleep 3
LEADER=$(psql "$CP_DSN" -qtAc "SELECT holder FROM leases WHERE pipeline_id = '$PIPELINE' AND expires_at > now()")
log "current leader: ${LEADER:-none}"
if [ -z "$LEADER" ]; then echo "FAIL: no leader was elected"; exit 1; fi

if [ "$LEADER" = "inst-a" ]; then KILL_PID=$PID_A; SURVIVOR=inst-b; else KILL_PID=$PID_B; SURVIVOR=inst-a; fi
log "killing the leader $LEADER (pid $KILL_PID) with SIGKILL"
kill -9 "$KILL_PID"

# The survivor must take the lease over within the TTL.
for _ in $(seq 1 40); do
  NEW=$(psql "$CP_DSN" -qtAc "SELECT holder FROM leases WHERE pipeline_id = '$PIPELINE' AND expires_at > now()")
  [ "$NEW" = "$SURVIVOR" ] && break
  sleep 0.5
done
log "lease holder after failover: ${NEW:-none}"
if [ "$NEW" != "$SURVIVOR" ]; then echo "FAIL: $SURVIVOR did not take over"; exit 1; fi

wait $WRITER 2>/dev/null || true
log "writer finished; waiting for the mirror to converge"

checksum() {
  psql "$1" -qtAc "SELECT count(*) || ':' || coalesce(md5(string_agg(id || '|' || name, ',' ORDER BY id)), '')
                     FROM $TABLE"
}

for _ in $(seq 1 120); do
  SRC=$(checksum "$SRC_DSN")
  DST=$(checksum "$MIRROR_DSN")
  [ "$SRC" = "$DST" ] && break
  sleep 0.5
done

log "state"
psql "$CP_DSN" -c "SELECT pipeline_id, holder, expires_at > now() AS valid FROM leases WHERE pipeline_id = '$PIPELINE'"
psql "$CP_DSN" -c "SELECT pipeline_id, position FROM offsets WHERE pipeline_id = '$PIPELINE'"
psql "$CP_DSN" -c "SELECT sink_name, position, seq FROM sink_cursor WHERE pipeline_id = '$PIPELINE'"
echo "source: $SRC"
echo "mirror: $DST"

if [ "$SRC" = "$DST" ]; then
  log "PASS: the mirror matches the source exactly after killing the leader mid-stream"
else
  log "FAIL: the mirror diverged from the source"
  tail -30 "$WORKDIR/inst-a.log" "$WORKDIR/inst-b.log"
  exit 1
fi
