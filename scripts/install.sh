#!/usr/bin/env bash
# 安装 group-img-saver 到指定目录并注册 systemd（Linux）
set -euo pipefail

DEST="${1:-/opt/group-img-saver}"
SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "==> 安装到 $DEST"
mkdir -p "$DEST"
cp "$SRC/group_img_saver_svc.py" "$DEST/"
cp "$SRC/requirements.txt" "$DEST/"
[ -f "$DEST/config.json" ] || cp "$SRC/config.example.json" "$DEST/config.json"

echo "==> 创建虚拟环境"
python3 -m venv "$DEST/.venv"
"$DEST/.venv/bin/pip" install -q --upgrade pip
"$DEST/.venv/bin/pip" install -q -r "$DEST/requirements.txt"

echo "==> 注册 systemd 单元"
cp "$SRC/systemd/group-img-saver.service" /etc/systemd/system/
systemctl daemon-reload

cat <<MSG

完成。接下来：
  1. 编辑 $DEST/config.json（watch_groups / save_root / onebot_ws）
  2. $DEST/.venv/bin/python $DEST/group_img_saver_svc.py --check-config
  3. systemctl enable --now group-img-saver
  4. journalctl -u group-img-saver -f

MSG
