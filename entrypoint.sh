#!/bin/sh

# Check if auth_token is provided
if [ -z "$OBICO_AUTH_TOKEN" ]; then
    echo "[CRITICAL ERROR] Environment variable OBICO_AUTH_TOKEN is missing!"
    echo "You must provide your auth_token from your Obico account for this to work."
    exit 1
fi

# Dynamically generate the Obico configuration file
cat <<EOF > /app/moonraker-obico.cfg
[server]
url = ${OBICO_URL:-https://app.obico.io}
auth_token = ${OBICO_AUTH_TOKEN}

[moonraker]
host = 127.0.0.1
port = 7125

[logging]
path = /app/logs
level = info
EOF

echo "[PROXY] Starting Elegoo CC2 to Moonraker proxy in the background..."
python main.py &

echo "[OBICO] Starting Moonraker-Obico client..."
cd /app/moonraker-obico
exec python -m moonraker_obico.app -c /app/moonraker-obico.cfg
