#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""napcat_health — 探测 NapCat/QQ 登录状态，写入健康快照供本地工具读取。

一次性执行（systemd timer 每 2 分钟跑一次）：
  1. 连 NapCat 的 OneBot WS
  2. 调 get_status  → {online, good}
  3. 调 get_login_info → {user_id, nickname}
  4. 结果原子写入 health.json；状态发生翻转时追加一行到 health.log

判定口径：
  ws_ok     能否连上 6700（NapCat 进程/网络是否在）
  online    OneBot 自报的 online 字段（QQ 是否在线）
  degraded  online=True 但 good=False（半掉线：能连但没完全就绪）
  logged_in user_id > 0（拿到账号信息才算真登录）

退出码：0=健康；1=掉线/异常；2=WS 连不上。
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from pathlib import Path

try:
    import websockets
except ImportError:
    print("[fatal] 缺少 websockets，请在服务 venv 中运行", file=sys.stderr)
    sys.exit(3)

WS_URL = os.environ.get("GIS_HEALTH_WS", "ws://127.0.0.1:6700/")
STATE_DIR = Path(os.environ.get("GIS_HEALTH_DIR", "/opt/group_img_saver"))
SNAPSHOT = STATE_DIR / "health.json"
HISTORY = STATE_DIR / "health.log"
IMAGES_ROOT = Path(os.environ.get("GIS_IMAGES_ROOT", "/root/group_imgs"))


async def call(ws, action: str, params: dict | None = None, timeout: float = 8.0):
    echo = f"h-{action}"
    await ws.send(json.dumps({"action": action, "params": params or {}, "echo": echo}))
    while True:
        raw = await asyncio.wait_for(ws.recv(), timeout)
        try:
            d = json.loads(raw)
        except Exception:
            continue
        if d.get("echo") == echo:
            return d


def last_image_ts() -> tuple[str | None, float | None]:
    """最近一张落盘图片（路径, mtime），用于判断'在线但没动静'。"""
    newest_p, newest_t = None, None
    if not IMAGES_ROOT.is_dir():
        return (None, None)
    for dirpath, _dn, fns in os.walk(IMAGES_ROOT):
        for fn in fns:
            p = os.path.join(dirpath, fn)
            try:
                m = os.path.getmtime(p)
            except OSError:
                continue
            if newest_t is None or m > newest_t:
                newest_p, newest_t = p, m
    return (newest_p, newest_t)


async def probe() -> dict:
    snap = {
        "ts": time.time(),
        "ts_text": time.strftime("%Y-%m-%d %H:%M:%S"),
        "ws_url": WS_URL,
        "ws_ok": False,
        "online": None,
        "good": None,
        "logged_in": False,
        "user_id": None,
        "nickname": None,
        "error": None,
    }
    try:
        async with websockets.connect(WS_URL, max_size=16 * 1024 * 1024,
                                     open_timeout=8, close_timeout=3) as ws:
            snap["ws_ok"] = True
            try:
                st = await call(ws, "get_status")
                data = st.get("data") or {}
                snap["online"] = bool(data.get("online"))
                snap["good"] = bool(data.get("good"))
            except Exception as e:
                snap["error"] = f"get_status: {e!r}"
            try:
                li = await call(ws, "get_login_info")
                data = li.get("data") or {}
                uid = data.get("user_id")
                snap["user_id"] = uid
                snap["nickname"] = data.get("nickname")
                snap["logged_in"] = bool(uid) and int(uid) > 0
            except Exception as e:
                snap["error"] = (snap["error"] or "") + f" get_login_info: {e!r}"
    except Exception as e:
        snap["error"] = f"connect: {e!r}"

    snap["degraded"] = bool(snap["online"]) and snap["good"] is False
    if not snap["ws_ok"]:
        snap["verdict"] = "ws_down"          # NapCat 进程/端口问题
    elif not snap["online"]:
        snap["verdict"] = "offline"          # QQ 掉线（需重新登录）
    elif not snap["logged_in"]:
        snap["verdict"] = "no_login_info"    # 连上但拿不到账号
    elif snap["degraded"]:
        snap["verdict"] = "degraded"         # 半掉线
    else:
        snap["verdict"] = "ok"

    p, t = last_image_ts()
    snap["last_image"] = p
    snap["last_image_ts"] = t
    snap["last_image_age_min"] = round((time.time() - t) / 60, 1) if t else None
    return snap


def write(snap: dict) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    prev_verdict = None
    if SNAPSHOT.exists():
        try:
            prev_verdict = json.loads(SNAPSHOT.read_text(encoding="utf-8")).get("verdict")
        except Exception:
            pass
    tmp = SNAPSHOT.with_suffix(".tmp")
    tmp.write_text(json.dumps(snap, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(SNAPSHOT)

    if prev_verdict != snap["verdict"]:
        line = (f"{snap['ts_text']}\t{prev_verdict or '-'} → {snap['verdict']}\t"
                f"online={snap['online']} good={snap['good']} "
                f"uid={snap['user_id']} nick={snap['nickname']}")
        if snap.get("error"):
            line += f"\terr={snap['error'][:200]}"
        with HISTORY.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
        print(f"[state] {line}")


def main() -> int:
    snap = asyncio.run(probe())
    write(snap)
    print(json.dumps({k: snap[k] for k in
                      ("ts_text", "ws_ok", "online", "good", "logged_in", "user_id",
                       "nickname", "verdict", "last_image_age_min", "error")},
                     ensure_ascii=False))
    if snap["verdict"] == "ok":
        return 0
    return 2 if snap["verdict"] == "ws_down" else 1


if __name__ == "__main__":
    sys.exit(main())
