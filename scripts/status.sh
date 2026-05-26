#!/usr/bin/env bash
set -euo pipefail

ROOT="${JUMP_SERVER_ROOT:-/srv/jump}"

printf "== screens ==\n"
screen -ls || true

printf "\n== active processes ==\n"
ps -ef | grep -E "download|zenodo|curl|aws s3|uvicorn|jump_agent" | grep -v grep || true

printf "\n== data sizes ==\n"
du -sh "$ROOT"/data/* 2>/dev/null || true

printf "\n== zenodo files ==\n"
ls -lh "$ROOT/data/jump_hub_zenodo" 2>/dev/null || true

printf "\n== recent production log ==\n"
tail -n 15 "$ROOT/logs/download_production.log" 2>/dev/null || true

printf "\n== recent zenodo log ==\n"
tail -n 20 "$ROOT/logs/download_zenodo.log" 2>/dev/null || true

printf "\n== service ==\n"
systemctl --no-pager --full status jump-agent-api 2>/dev/null | sed -n '1,18p' || true

printf "\n== disk ==\n"
df -h "$ROOT"
