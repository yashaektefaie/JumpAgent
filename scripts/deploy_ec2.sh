#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="${JUMP_SERVER_ROOT:-/srv/jump}"
REPO_DIR="$ROOT/app/JumpAgent"
VENV="$ROOT/venv"

if [[ ! -d "$REPO_DIR/.git" ]]; then
  echo "Repository not found at $REPO_DIR"
  echo "Clone it first, for example:"
  echo "  git clone git@github.com:yashaektefaie/JumpAgent.git $REPO_DIR"
  exit 1
fi

cd "$REPO_DIR"
git pull --ff-only

python3 -m venv "$VENV"
"$VENV/bin/python" -m pip install --upgrade pip
"$VENV/bin/pip" install -r requirements.txt

if [[ ! -f "$ROOT/api_key" ]]; then
  umask 077
  python3 - <<'PY' > "$ROOT/api_key"
import secrets
print(secrets.token_urlsafe(32))
PY
fi

sudo tee /etc/systemd/system/jump-agent-api.service >/dev/null <<EOF
[Unit]
Description=JUMP Agent API
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=$REPO_DIR
Environment=JUMP_DATA_ROOT=$ROOT/data
EnvironmentFile=-$ROOT/jump-agent.env
ExecStart=/bin/bash -lc 'export JUMP_API_KEY="\$(cat $ROOT/api_key)"; exec $VENV/bin/uvicorn jump_agent_api.app:app --host 127.0.0.1 --port 8000'
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable jump-agent-api
sudo systemctl restart jump-agent-api
sudo systemctl --no-pager --full status jump-agent-api
