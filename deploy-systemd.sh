#!/usr/bin/env bash

set -Eeuo pipefail

APP_DIR="${APP_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
BRANCH="${BRANCH:-main}"
SERVICE_NAME="${SERVICE_NAME:-codex2api}"
INSTALL_DIR="${INSTALL_DIR:-/opt/codex2api}"
GO_BUILD_FLAGS="${GO_BUILD_FLAGS:-}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "[ERROR] missing command: $1" >&2
    exit 1
  }
}

run_root() {
  if [[ ${EUID:-0} -eq 0 ]]; then
    "$@"
  else
    sudo "$@"
  fi
}

echo "[1/7] checking prerequisites..."
require_cmd git
require_cmd go
require_cmd npm
require_cmd systemctl

echo "[2/7] updating source..."
cd "$APP_DIR"
git fetch origin "$BRANCH"
git checkout "$BRANCH"
git pull --ff-only origin "$BRANCH"

echo "[3/7] building frontend (Vue/Vite)..."
pushd "$APP_DIR/frontend" >/dev/null
npm ci --no-audit --fund=false
npm run build
popd >/dev/null

if [[ ! -f "$APP_DIR/frontend/dist/index.html" ]]; then
  echo "[ERROR] frontend build failed: frontend/dist/index.html not found" >&2
  exit 1
fi

echo "[4/7] downloading go modules..."
cd "$APP_DIR"
go mod download

echo "[5/7] building backend..."
if [[ -n "$GO_BUILD_FLAGS" ]]; then
  # shellcheck disable=SC2086
  go build $GO_BUILD_FLAGS -o "$APP_DIR/codex2api" .
else
  go build -o "$APP_DIR/codex2api" .
fi

echo "[6/7] installing binary..."
run_root mkdir -p "$INSTALL_DIR"
run_root install -m 0755 "$APP_DIR/codex2api" "$INSTALL_DIR/codex2api"

echo "[7/7] restarting systemd service..."
run_root systemctl daemon-reload
run_root systemctl restart "$SERVICE_NAME"

if run_root systemctl is-active --quiet "$SERVICE_NAME"; then
  echo "[OK] service is active: $SERVICE_NAME"
  run_root systemctl --no-pager --full status "$SERVICE_NAME" | sed -n '1,20p'
else
  echo "[ERROR] service failed to start: $SERVICE_NAME" >&2
  run_root systemctl --no-pager --full status "$SERVICE_NAME" || true
  run_root journalctl -u "$SERVICE_NAME" -n 100 --no-pager || true
  exit 1
fi

