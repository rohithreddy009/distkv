#!/usr/bin/env bash
# Stops all local DistKV nodes started by start-cluster.sh.
set -uo pipefail
cd "$(dirname "$0")/.."

for i in 1 2 3; do
  if [ -f "data/n$i.pid" ]; then
    pid=$(cat "data/n$i.pid")
    if kill "$pid" 2>/dev/null; then
      echo "stopped node $i (pid $pid)"
    fi
    rm -f "data/n$i.pid"
  fi
done
