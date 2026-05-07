#!/usr/bin/env bash
set -euo pipefail

echo "Waiting for Superset endpoint..."
for i in {1..60}; do
  if curl -fsS "http://localhost:${SUPERSET_PORT:-8088}/health" >/dev/null 2>&1; then
    echo "Superset is ready"
    exit 0
  fi
  sleep 2
done

echo "Superset did not become healthy in time"
exit 1
