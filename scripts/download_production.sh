#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="${JUMP_SERVER_ROOT:-/srv/jump}"
DEST="$ROOT/data/jump_production"
LOG="$ROOT/logs/download_production.log"
S3_URI="${JUMP_PRODUCTION_S3_URI:-s3://imaging-platform/projects/cpg0042-chandrasekaran-jump/workspace/publication_data/2025_Chandrasekaran/jump_production_datastore/}"

mkdir -p "$DEST" "$ROOT/logs" "$ROOT/manifests"

{
  echo "[$(date -Is)] Starting JUMP production datastore sync"
  echo "source=$S3_URI"
  echo "dest=$DEST"
  aws s3 sync --no-sign-request --region us-east-1 --only-show-errors "$S3_URI" "$DEST/"
  echo "[$(date -Is)] Finished JUMP production datastore sync"
  du -sh "$DEST" || true
  printf "files=%s\n" "$(find "$DEST" -type f | wc -l)"
} 2>&1 | tee -a "$LOG"
