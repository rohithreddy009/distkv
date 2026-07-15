#!/usr/bin/env bash
# Starts Prometheus + Grafana, scraping local nodes on :9001-:9003.
# Run ./scripts/start-cluster.sh first.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose -f docker-compose.monitoring.yml up -d
echo "Prometheus: http://localhost:9090"
echo "Grafana:    http://localhost:3000  (admin / admin)"
echo "Dashboard:  DistKV -> DistKV Overview"
