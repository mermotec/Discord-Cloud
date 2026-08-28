#!/usr/bin/env bash
# @author MermoTEC
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
  echo "Run with: sudo ./install.sh"
  exit 1
fi

APP_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
RUN_USER="${SUDO_USER:-root}"

if [[ ! -f "$APP_DIR/.env" ]]; then
  cp "$APP_DIR/.env.example" "$APP_DIR/.env"
  secret="$(openssl rand -hex 32)"
  sed -i "s/replace-with-output-of-openssl-rand-hex-32/$secret/" "$APP_DIR/.env"
  chown "$RUN_USER":"$RUN_USER" "$APP_DIR/.env"
  chmod 600 "$APP_DIR/.env"
  echo "Created $APP_DIR/.env. Add the bot token, server ID, and dashboard password, then run this installer again."
  exit 1
fi

if grep -Eq 'paste-your-bot-token|replace-with-a-long-password|^DISCORD_GUILD_ID=123456789012345678$' "$APP_DIR/.env"; then
  echo "Finish configuring $APP_DIR/.env before installing."
  exit 1
fi

ensure_setting() { grep -q "^$1=" "$APP_DIR/.env" || printf '%s=%s\n' "$1" "$2" >>"$APP_DIR/.env"; }
ensure_setting DISCORD_CLOUD_DOWNLOAD_CONCURRENCY 3
ensure_setting DISCORD_CLOUD_RETRY_ATTEMPTS 6
ensure_setting DISCORD_CLOUD_LIBRARY_CACHE_SECONDS 30
ensure_setting DISCORD_CLOUD_HARD_MAX_UPLOAD_GB 100
ensure_setting DISCORD_CLOUD_MAX_FILES 5000
chown "$RUN_USER":"$RUN_USER" "$APP_DIR/.env"
chmod 600 "$APP_DIR/.env"

apt-get update
apt-get install -y python3 python3-venv ca-certificates
sudo -u "$RUN_USER" python3 -m venv "$APP_DIR/.venv"
sudo -u "$RUN_USER" "$APP_DIR/.venv/bin/pip" install --upgrade pip
sudo -u "$RUN_USER" "$APP_DIR/.venv/bin/pip" install -r "$APP_DIR/requirements.txt"
test -f "$APP_DIR/assets/cloud-background.webp" || { echo "Missing assets/cloud-background.webp"; exit 1; }
chown "$RUN_USER":"$RUN_USER" "$APP_DIR"
if [[ -f "$APP_DIR/state.json" ]]; then chown "$RUN_USER":"$RUN_USER" "$APP_DIR/state.json"; chmod 600 "$APP_DIR/state.json"; fi
sudo -u "$RUN_USER" "$APP_DIR/.venv/bin/python" "$APP_DIR/app.py" --self-test
install -d -o "$RUN_USER" -g "$RUN_USER" -m 700 /var/tmp/discord-cloud

cat >/etc/systemd/system/discord-cloud.service <<EOF
[Unit]
Description=Discord Cloud
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$RUN_USER
WorkingDirectory=$APP_DIR
ExecStart=$APP_DIR/.venv/bin/python $APP_DIR/app.py
Restart=on-failure
RestartSec=5
TimeoutStopSec=30
Environment=PYTHONUNBUFFERED=1
Environment=PYTHONDONTWRITEBYTECODE=1
UMask=0077
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable discord-cloud
systemctl restart discord-cloud
echo "Discord Cloud v3 installed and restarted. Check: systemctl status discord-cloud"