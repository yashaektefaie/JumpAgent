#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="${JUMP_SERVER_ROOT:-/srv/jump}"
mkdir -p "$ROOT/logs"

echo "[$(date -Is)] Starting all downloads" | tee -a "$ROOT/logs/download_all.log"

"$(dirname "$0")/download_production.sh" > >(tee -a "$ROOT/logs/download_production.outer.log") 2>&1 &
prod_pid=$!

"$(dirname "$0")/download_zenodo.py" > >(tee -a "$ROOT/logs/download_zenodo.outer.log") 2>&1 &
zen_pid=$!

set +e
wait "$prod_pid"
prod_status=$?
wait "$zen_pid"
zen_status=$?
set -e

echo "[$(date -Is)] production_status=$prod_status zenodo_status=$zen_status" | tee -a "$ROOT/logs/download_all.log"
du -sh "$ROOT/data" | tee -a "$ROOT/logs/download_all.log"
df -h "$ROOT" | tee -a "$ROOT/logs/download_all.log"

exit $(( prod_status != 0 || zen_status != 0 ))
