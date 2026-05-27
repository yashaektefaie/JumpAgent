#!/usr/bin/env bash
set -Eeuo pipefail

DOMAIN="${JUMP_AGENT_DOMAIN:-jump-agent.net}"
UPSTREAM="${JUMP_AGENT_UPSTREAM:-127.0.0.1:8000}"
CADDY_VERSION="${CADDY_VERSION:-2.11.3}"

install_caddy_from_github() {
  local arch
  local tmpdir
  case "$(uname -m)" in
    x86_64) arch="amd64" ;;
    aarch64 | arm64) arch="arm64" ;;
    *) echo "Unsupported architecture for Caddy binary: $(uname -m)" >&2; exit 1 ;;
  esac

  tmpdir="$(mktemp -d)"
  trap 'rm -rf "$tmpdir"' RETURN

  local asset="caddy_${CADDY_VERSION}_linux_${arch}.tar.gz"
  local base_url="https://github.com/caddyserver/caddy/releases/download/v${CADDY_VERSION}"
  curl -L --fail -o "$tmpdir/$asset" "$base_url/$asset"
  curl -L --fail -o "$tmpdir/checksums.txt" "$base_url/caddy_${CADDY_VERSION}_checksums.txt"
  grep " $asset\$" "$tmpdir/checksums.txt" > "$tmpdir/checksum.txt"
  (cd "$tmpdir" && sha512sum -c checksum.txt)

  tar -xzf "$tmpdir/$asset" -C "$tmpdir" caddy
  sudo install -m 0755 "$tmpdir/caddy" /usr/local/bin/caddy
  sudo dnf install -y libcap >/dev/null 2>&1 || true
  sudo setcap cap_net_bind_service=+ep /usr/local/bin/caddy || true
}

if ! command -v caddy >/dev/null 2>&1; then
  if command -v dnf >/dev/null 2>&1; then
    sudo dnf install -y caddy || {
      sudo dnf install -y 'dnf-command(copr)'
      sudo dnf copr enable -y @caddy/caddy || install_caddy_from_github
      command -v caddy >/dev/null 2>&1 || sudo dnf install -y caddy || install_caddy_from_github
    }
  else
    install_caddy_from_github
  fi
fi

CADDY_BIN="$(command -v caddy)"

if ! getent passwd caddy >/dev/null 2>&1; then
  sudo useradd --system --home /var/lib/caddy --shell /sbin/nologin caddy
fi

sudo mkdir -p /etc/caddy /var/lib/caddy /var/log/caddy
if id caddy >/dev/null 2>&1; then
  sudo chown caddy:caddy /var/lib/caddy /var/log/caddy
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

if [[ ! -f /etc/systemd/system/caddy.service && ! -f /usr/lib/systemd/system/caddy.service ]]; then
  sudo tee /etc/systemd/system/caddy.service >/dev/null <<EOF
[Unit]
Description=Caddy
Documentation=https://caddyserver.com/docs/
After=network-online.target
Wants=network-online.target

[Service]
Type=notify
User=caddy
Group=caddy
ExecStart=$CADDY_BIN run --environ --config /etc/caddy/Caddyfile
ExecReload=$CADDY_BIN reload --config /etc/caddy/Caddyfile --force
TimeoutStopSec=5s
LimitNOFILE=1048576
PrivateTmp=true
ProtectSystem=full
AmbientCapabilities=CAP_NET_BIND_SERVICE
CapabilityBoundingSet=CAP_NET_BIND_SERVICE
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
EOF
fi

sudo "$CADDY_BIN" fmt --overwrite /etc/caddy/Caddyfile
sudo systemctl daemon-reload
sudo systemctl enable --now caddy
sudo systemctl reload caddy || sudo systemctl restart caddy
sudo systemctl --no-pager --full status caddy
