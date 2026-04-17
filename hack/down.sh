#!/bin/bash

set -e

echo "==> Stopping existing faasd services..."
sudo systemctl disable --now faasd 2>/dev/null || echo "faasd not running"
sudo systemctl disable --now faasd-provider 2>/dev/null || echo "faasd-provider not running"
sudo systemctl disable --now faasd-gateway 2>/dev/null || echo "faasd-gateway not running"
sudo rm -f /etc/default/faasd

echo "==> Flushing old logs..."
sudo journalctl --rotate --vacuum-time=1s -u faasd || true
sudo journalctl --rotate --vacuum-time=1s -u faasd-provider || true
sudo journalctl --rotate --vacuum-time=1s -u faasd-gateway || true

echo "==> Uninstalling faasd services..."
make uninstall

echo "==> Reloading systemd daemon..."
sudo systemctl daemon-reload

echo "==> Cleaning builds..."
make clean
