#!/usr/bin/env bash
set -Eeuo pipefail

DOMAIN="${JUMP_AGENT_DOMAIN:-jump-agent.net}"
UPSTREAM="${JUMP_AGENT_UPSTREAM:-127.0.0.1:8000}"

if ! command -v caddy >/dev/null 2>&1; then
  if command -v dnf >/dev/null 2>&1; then
    sudo dnf install -y caddy || {
      sudo dnf install -y 'dnf-command(copr)'
      sudo dnf copr enable -y @caddy/caddy
      sudo dnf install -y caddy
    }
  else
    echo "No supported package manager found. Install Caddy first." >&2
    exit 1
  fi
fi

sudo mkdir -p /etc/caddy /var/log/caddy
if id caddy >/dev/null 2>&1; then
  sudo chown caddy:caddy /var/log/caddy
fi

sudo tee /etc/caddy/Caddyfile >/dev/null <<EOF
$DOMAIN {
    encode zstd gzip

    header {
        X-Content-Type-Options nosniff
        Referrer-Policy no-referrer
    }

    reverse_proxy $UPSTREAM

    log {
        output file /var/log/caddy/jump-agent-access.log
        format console
    }
}
EOF

sudo caddy fmt --overwrite /etc/caddy/Caddyfile
sudo systemctl enable --now caddy
sudo systemctl reload caddy || sudo systemctl restart caddy
sudo systemctl --no-pager --full status caddy
