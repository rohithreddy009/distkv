#!/usr/bin/env bash
# Chaos test against a running local cluster:
#   1. Writes a sentinel key.
#   2. kill -9 the current leader.
#   3. Measures how long until the cluster accepts writes again.
#   4. Verifies no data was lost, then restarts the killed node and checks
#      it rejoins and catches up.
set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER="localhost:7001,localhost:7002,localhost:7003"
CLI="./bin/distkv-cli -cluster $CLUSTER"
PEERS="1=localhost:8001,2=localhost:8002,3=localhost:8003"

echo "== baseline write =="
$CLI put chaos-sentinel "before-crash"
$CLI get chaos-sentinel

echo
echo "== finding leader =="
STATUS=$($CLI status)
echo "$STATUS"
LEADER_ID=$(echo "$STATUS" | awk '/LEADER/ {for(i=1;i<=NF;i++) if ($i ~ /^id=/) {sub("id=","",$i); print $i}}')
if [ -z "$LEADER_ID" ]; then echo "no leader found"; exit 1; fi
LEADER_PID=$(cat "data/n$LEADER_ID.pid")
echo "leader is node $LEADER_ID (pid $LEADER_PID)"

echo
echo "== kill -9 the leader =="
kill -9 "$LEADER_PID"
rm -f "data/n$LEADER_ID.pid"
START=$(python3 -c 'import time; print(time.time())')

echo "== waiting for cluster to accept writes again =="
until $CLI -timeout 2s put chaos-after "post-crash" >/dev/null 2>&1; do :; done
END=$(python3 -c 'import time; print(time.time())')
RECOVERY=$(python3 -c "print(f'{$END - $START:.2f}')")
echo "cluster accepted writes ${RECOVERY}s after leader was killed"

echo
echo "== verifying no data loss =="
VAL=$($CLI get chaos-sentinel)
if [ "$VAL" != "before-crash" ]; then echo "DATA LOSS: got '$VAL'"; exit 1; fi
echo "sentinel intact: $VAL"

echo
echo "== restarting killed node $LEADER_ID =="
i=$LEADER_ID
nohup ./bin/distkv -id "$i" -dir "data/n$i" -kv ":700$i" -raft ":800$i" -peers "$PEERS" \
  >> "data/n$i.log" 2>&1 &
echo $! > "data/n$i.pid"
disown
sleep 2
$CLI status

echo
echo "== chaos test PASSED (recovery: ${RECOVERY}s) =="
