#!/usr/bin/env bash
# Starts a local 3-node DistKV cluster. Data in ./data/, logs in ./data/nX.log.
set -euo pipefail
cd "$(dirname "$0")/.."

PEERS="1=localhost:8001,2=localhost:8002,3=localhost:8003"

mkdir -p data bin
go build -o bin/distkv ./cmd/distkv
go build -o bin/distkv-cli ./cmd/distkv-cli

for i in 1 2 3; do
  if [ -f "data/n$i.pid" ] && kill -0 "$(cat data/n$i.pid)" 2>/dev/null; then
    echo "node $i already running (pid $(cat data/n$i.pid))"
    continue
  fi
  nohup ./bin/distkv -id "$i" -dir "data/n$i" -kv ":700$i" -raft ":800$i" -peers "$PEERS" \
    >> "data/n$i.log" 2>&1 &
  echo $! > "data/n$i.pid"
  disown
  echo "started node $i (pid $!) kv=:700$i raft=:800$i"
done

sleep 1
./bin/distkv-cli -cluster localhost:7001,localhost:7002,localhost:7003 status
