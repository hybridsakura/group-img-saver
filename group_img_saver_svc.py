#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
group_img_saver_svc — 独立于 AstrBot 的群聊图片归档服务

工作方式：以 OneBot 11 客户端身份连接 NapCat 的 WebSocket 服务端，
实时接收群消息，把指定群的图片（原图）按群号分目录下载到本地；
支持在群里发 /补抓 [条数] 通过 get_group_msg_history 回溯缓存内历史图片。

去重：每次落盘前边下载边算内容哈希（默认 xxh3_128，非加密、15+ GB/s），
写入本地 SQLite 索引。
  - 命中已有哈希 → 不再写新文件，hits+1、last_seen 刷新
  - 若这次来自另一个群 → 用硬链接把已有文件挂到该群目录（零额外字节，目录仍完整）
  - 所有"被跳过的副本"都记录到 duplicates 表（不删除、不处理，留作后续用途）
  - 索引有窗口上限（max_entries）与实测延迟守卫（latency_budget_ms）双重淘汰

依赖：aiohttp、websockets、xxhash（可选；缺失时回退 SHA1）。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

import aiohttp
import websockets

try:
    import xxhash
except ImportError:  # 退化为标准库哈希
    xxhash = None

# ======================== 配置 ========================
__version__ = "2.0.0"


def _parse_args(argv=None):
    import argparse

    ap = argparse.ArgumentParser(
        prog="group-img-saver",
        description="按群号归档 QQ(NapCat/OneBot11) 群聊图片，内容哈希去重，支持历史回溯。",
    )
    ap.add_argument("-c", "--config", default=os.environ.get("GIS_CONFIG", "config.json"),
                    help="配置文件路径（默认 config.json，可用环境变量 GIS_CONFIG 覆盖）")
    ap.add_argument("--check-config", action="store_true",
                    help="只校验并打印配置，不连接 NapCat")
    ap.add_argument("--reindex", action="store_true",
                    help="只把 save_root 下已有文件重建进索引后退出")
    ap.add_argument("-V", "--version", action="version", version=f"%(prog)s {__version__}")
    args, _unknown = ap.parse_known_args(argv)
    return args


ARGS = _parse_args()
CONFIG_PATH = Path(ARGS.config)

DEFAULT_CONFIG = {
    "onebot_ws": "ws://127.0.0.1:6700/",
    "access_token": "",
    # 要监听的群号（字符串）。留空 = 不监听任何群。
    "watch_groups": [],
    "save_root": "./images",
    "backfill_limit": 2000,
    "log_level": "INFO",
    # ---- 去重相关 ----
    "dedup_enabled": True,
    # 内容哈希算法：xxh3_128（默认，128 位，碰撞可忽略）| xxh64 | sha1
    "hash_algo": "xxh3_128",
    "hash_db_path": "./hash_index.db",
    # 索引窗口上限（条）。超出后按最后命中时间淘汰最早的记录
    "max_entries": 200000,
    # 延迟守卫：索引操作平均耗时（EMA）超过该值就淘汰最早 10%
    "latency_budget_ms": 1000,
    # 同一张图在另一个群出现时，用硬链接挂进该群目录（不占额外空间）
    "hardlink_across_groups": True,
    # 启动时把 save_root 下已有文件补录进索引（幂等）
    "reindex_on_start": True,
    # 记录被跳过的重复副本（不删除文件，仅入库备用）
    "record_duplicates": True,
    # xxhash 缺失时是否允许静默回退 sha1。默认 false（fail-closed）：
    # 回退会触发索引迁移重建，运行时台账不可恢复。
    "allow_algo_fallback": False,
}
# =====================================================


def load_config() -> dict:
    cfg = dict(DEFAULT_CONFIG)
    if CONFIG_PATH.exists():
        try:
            cfg.update(json.loads(CONFIG_PATH.read_text(encoding="utf-8")))
        except Exception as e:
            print(f"[warn] 读取配置失败 {CONFIG_PATH}: {e}", file=sys.stderr)
    return cfg


def resolve_hash_algo(cfg: dict, xxhash_mod) -> tuple[str | None, str | None]:
    """决定实际使用的哈希算法。

    返回 (algo, error)。error 非空表示必须中止启动（fail-closed）：
    xxhash 缺失时若静默回退 sha1，会触发索引迁移重建（DROP 表），
    运行时台账（live/link）无法从磁盘重建 → 数据丢失。
    """
    algo = str(cfg.get("hash_algo", "xxh3_128"))
    if algo not in ("xxh3_128", "xxh64", "sha1"):
        return (None, f"未知 hash_algo: {algo}（可选 xxh3_128 / xxh64 / sha1）")
    if algo.startswith("xxh") and xxhash_mod is None:
        if cfg.get("allow_algo_fallback", False):
            return ("sha1", f"xxhash 未安装，按 allow_algo_fallback=true 回退 sha1")
        return (None, (
            f"hash_algo={algo} 需要 xxhash 模块但当前环境未安装。\n"
            f"  已中止启动以避免索引被重建（会丢失 duplicates 台账）。\n"
            f"  处理：pip install xxhash（推荐），"
            f"或显式设置 allow_algo_fallback=true 接受回退 sha1 并重建索引。"))
    return (algo, None)


CFG = load_config()
WATCH = {str(g).strip() for g in CFG.get("watch_groups", []) if str(g).strip()}
SAVE_ROOT = Path(CFG["save_root"])
LOG_LEVEL = str(CFG.get("log_level", "INFO")).upper()
HASH_ALGO, _algo_err = resolve_hash_algo(CFG, xxhash)
if HASH_ALGO is None:
    print(f"[ERROR] {_algo_err}", file=sys.stderr, flush=True)
    sys.exit(2)
if _algo_err:
    print(f"[warn] {_algo_err}", file=sys.stderr, flush=True)


def log(level: str, msg: str) -> None:
    if level == "DEBUG" and LOG_LEVEL != "DEBUG":
        return
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} [{level}] {msg}", flush=True)


def new_hasher():
    """返回具有 update()/hexdigest() 接口的增量哈希对象。"""
    if HASH_ALGO == "xxh3_128":
        return xxhash.xxh3_128()
    if HASH_ALGO == "xxh64":
        return xxhash.xxh64()
    return hashlib.sha1()


# ---------------------- 哈希索引 ----------------------

class HashStore:
    """SQLite 内容哈希索引 + 重复副本台账。

    表：
      hashes     内容哈希 → 首次落盘路径 / 命中次数 / 最后命中时间
      duplicates 被跳过的重复副本（路径、群号、大小、判定时间、来源）
      meta       元信息（schema_version / algo）

    窗口控制：
      - 硬上限 max_entries：超出即按 last_seen 淘汰最旧记录
      - 延迟守卫：操作耗时 EMA 超过 latency_budget_ms 时淘汰最旧 10%
    """

    SCHEMA_VERSION = 2

    DDL = """
    CREATE TABLE IF NOT EXISTS hashes (
        hash       TEXT PRIMARY KEY,
        algo       TEXT NOT NULL,
        size       INTEGER NOT NULL,
        first_path TEXT NOT NULL,
        first_seen REAL NOT NULL,
        last_seen  REAL NOT NULL,
        hits       INTEGER NOT NULL DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_last_seen ON hashes(last_seen);

    CREATE TABLE IF NOT EXISTS duplicates (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        hash        TEXT NOT NULL,
        algo        TEXT NOT NULL,
        path        TEXT NOT NULL,
        group_id    TEXT,
        size        INTEGER,
        kind        TEXT NOT NULL,      -- reindex | live | link
        detected_at REAL NOT NULL,
        UNIQUE(hash, path)
    );
    CREATE INDEX IF NOT EXISTS idx_dup_hash ON duplicates(hash);
    CREATE INDEX IF NOT EXISTS idx_dup_group ON duplicates(group_id);

    CREATE TABLE IF NOT EXISTS meta (
        key   TEXT PRIMARY KEY,
        value TEXT
    );
    """

    def __init__(self, path: str, max_entries: int = 200000,
                 latency_budget_ms: float = 1000.0):
        self.path = path
        self.max_entries = max(1000, int(max_entries))
        self.latency_budget = float(latency_budget_ms) / 1000.0
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._migrate()
        self._ema = 0.0          # 操作耗时指数移动平均（秒）
        self._ops = 0

    # ---- 建表 / 迁移 ----
    HASH_COLS = ["hash", "algo", "size", "first_path", "first_seen", "last_seen", "hits"]

    def _migrate(self) -> None:
        # 先只建 meta，用于读取旧版本信息
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
        cur = self.conn.execute("SELECT value FROM meta WHERE key='schema_version'")
        row = cur.fetchone()
        ver = int(row[0]) if row else 0
        algo_row = self.conn.execute("SELECT value FROM meta WHERE key='algo'").fetchone()
        old_algo = algo_row[0] if algo_row else None

        # 关键：以实际列结构为准，不信 meta。历史上出现过 meta 已写 v2
        # 但表仍是 v1(sha1 列) 的中间态，只看 meta 会漏掉重建。
        exists = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='hashes'").fetchone()
        cols_bad = False
        if exists:
            cols = [r[1] for r in self.conn.execute("PRAGMA table_info(hashes)")]
            cols_bad = cols != self.HASH_COLS

        stale = (ver != self.SCHEMA_VERSION or old_algo != HASH_ALGO or cols_bad)
        if stale and exists:
            log("WARN", f"索引重建：schema={ver}/{old_algo} 列={'v1' if cols_bad else 'ok'}"
                        f" → {self.SCHEMA_VERSION}/{HASH_ALGO}，旧索引将重建")
            # 重建前留一份快照：运行时台账（live/link）无法从磁盘重建，只能靠这份备份找回
            try:
                import shutil
                bak = f"{self.path}.bak.{old_algo or 'v1'}.{time.strftime('%Y%m%d-%H%M%S')}"
                self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                shutil.copy2(self.path, bak)
                log("WARN", f"旧索引已备份到 {bak}")
            except Exception as e:
                log("ERROR", f"索引备份失败（继续重建，但台账不可恢复）: {e!r}")
        if stale:
            self.conn.execute("DROP TABLE IF EXISTS hashes")
            self.conn.execute("DROP TABLE IF EXISTS duplicates")

        self.conn.executescript(self.DDL)

        if stale:
            self.conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('schema_version',?)",
                              (str(self.SCHEMA_VERSION),))
            self.conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('algo',?)",
                              (HASH_ALGO,))

    # ---- 内部：带耗时统计的 execute ----
    def _exec(self, sql: str, params: tuple = ()):
        t0 = time.perf_counter()
        cur = self.conn.execute(sql, params)
        dt = time.perf_counter() - t0
        # EMA，alpha=0.05，避免单次抖动误判
        self._ema = dt if self._ema == 0 else (self._ema * 0.95 + dt * 0.05)
        self._ops += 1
        if self._ops % 500 == 0:
            self._guard()
        return cur

    def _guard(self) -> None:
        """窗口控制：延迟守卫优先，其次硬上限。"""
        if self._ema > self.latency_budget:
            removed = self._evict(max(1, self.count() // 10))
            if removed:
                log("WARN", f"索引操作 EMA {self._ema*1000:.0f}ms 超预算 "
                            f"{self.latency_budget*1000:.0f}ms，淘汰最旧 {removed} 条")
            self._ema = 0.0
        elif self.count() > self.max_entries:
            removed = self._evict(self.count() - self.max_entries)
            if removed:
                log("INFO", f"索引超出窗口上限 {self.max_entries}，淘汰最旧 {removed} 条")

    def _evict(self, n: int) -> int:
        cur = self._exec(
            "DELETE FROM hashes WHERE hash IN "
            "(SELECT hash FROM hashes ORDER BY last_seen ASC LIMIT ?)", (int(n),)
        )
        return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    # ---- 对外接口：索引 ----
    def get(self, digest: str):
        cur = self._exec(
            "SELECT hash, algo, size, first_path, first_seen, last_seen, hits "
            "FROM hashes WHERE hash = ?", (digest,)
        )
        return cur.fetchone()

    def put(self, digest: str, size: int, first_path: str) -> None:
        now = time.time()
        self._exec(
            "INSERT INTO hashes (hash, algo, size, first_path, first_seen, last_seen, hits) "
            "VALUES (?,?,?,?,?,?,0) ON CONFLICT(hash) DO NOTHING",
            (digest, HASH_ALGO, size, first_path, now, now),
        )

    def touch(self, digest: str) -> int:
        """命中：hits+1、刷新 last_seen，返回累计命中次数。"""
        self._exec("UPDATE hashes SET hits = hits + 1, last_seen = ? WHERE hash = ?",
                   (time.time(), digest))
        row = self.get(digest)
        return int(row[6]) if row else 0

    def count(self) -> int:
        return int(self.conn.execute("SELECT COUNT(*) FROM hashes").fetchone()[0])

    # ---- 对外接口：重复副本台账 ----
    def record_duplicate(self, digest: str, path: str, group_id: str | None,
                         size: int, kind: str) -> None:
        """登记一个被跳过的重复副本（不删除文件）。同 (hash,path) 幂等。"""
        if not CFG.get("record_duplicates", True):
            return
        self._exec(
            "INSERT OR IGNORE INTO duplicates "
            "(hash, algo, path, group_id, size, kind, detected_at) VALUES (?,?,?,?,?,?,?)",
            (digest, HASH_ALGO, path, group_id, size, kind, time.time()),
        )

    def stats(self) -> dict:
        row = self.conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(hits),0), COALESCE(SUM(size),0) FROM hashes"
        ).fetchone()
        drow = self.conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(size),0) FROM duplicates"
        ).fetchone()
        return {"entries": row[0], "hits": row[1], "indexed_bytes": row[2],
                "duplicate_rows": drow[0], "duplicate_bytes": drow[1],
                "algo": HASH_ALGO, "ema_ms": round(self._ema * 1000, 3)}

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass


# ---------------------- OneBot 客户端 ----------------------

class OneBotClient:
    """最小 OneBot 11 WS 客户端：收事件 + 调 action（带 echo 匹配）。"""

    def __init__(self, url: str, token: str = ""):
        self.url = url
        self.token = token
        self.ws = None
        self._seq = 0
        self._pending: dict[str, asyncio.Future] = {}

    async def run(self, on_event) -> None:
        headers = {"Authorization": f"Bearer {self.token}"} if self.token else None
        backoff = 1
        while True:
            try:
                log("INFO", f"连接 {self.url} ...")
                async with websockets.connect(
                    self.url,
                    additional_headers=headers,
                    ping_interval=30,
                    ping_timeout=20,
                    max_size=32 * 1024 * 1024,
                ) as ws:
                    self.ws = ws
                    backoff = 1
                    log("INFO", "已连接 NapCat OneBot WS")
                    async for raw in ws:
                        asyncio.create_task(self._dispatch(raw, on_event))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log("WARN", f"连接异常: {e!r}，{backoff}s 后重连")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def _dispatch(self, raw: str, on_event) -> None:
        try:
            data = json.loads(raw)
        except Exception:
            return
        echo = data.get("echo")
        if echo is not None and echo in self._pending:
            fut = self._pending.pop(echo)
            if not fut.done():
                fut.set_result(data)
            return
        if data.get("post_type") == "message":
            try:
                await on_event(data)
            except Exception as e:
                log("ERROR", f"处理消息失败: {e!r}")

    async def call(self, action: str, params: dict | None = None, timeout: float = 30.0):
        if self.ws is None:
            raise RuntimeError("尚未连接")
        self._seq += 1
        echo = f"gis-{self._seq}"
        fut = asyncio.get_event_loop().create_future()
        self._pending[echo] = fut
        await self.ws.send(json.dumps({"action": action, "params": params or {}, "echo": echo}))
        try:
            data = await asyncio.wait_for(fut, timeout=timeout)
        finally:
            self._pending.pop(echo, None)
        if data.get("retcode") not in (0, None):
            raise RuntimeError(f"{action} 失败: {data}")
        return data.get("data")


# ---------------------- 图片落盘 / 去重 ----------------------

def _ext_of(url: str) -> str:
    ext = os.path.splitext(url.split("?")[0])[1].lower()
    return ext if ext in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp") else ".jpg"


async def save_image(sess: aiohttp.ClientSession, url: str, group_dir: Path,
                     stem: str, store: HashStore | None) -> tuple[str, str | None]:
    """下载并落盘（带去重）。返回 (status, 路径)，status ∈ {saved, dup, linked, exists, error}。"""
    tmp = None
    try:
        group_dir.mkdir(parents=True, exist_ok=True)
        dst = group_dir / f"{stem}{_ext_of(url)}"
        if dst.exists():
            return ("exists", str(dst))

        tmp = group_dir / f".{stem}.part"
        hasher = new_hasher()
        size = 0
        async with sess.get(url, timeout=aiohttp.ClientTimeout(total=60)) as r:
            if r.status != 200:
                log("WARN", f"下载失败 HTTP {r.status}: {url[:120]}")
                return ("error", None)
            with open(tmp, "wb") as f:
                async for chunk in r.content.iter_chunked(64 * 1024):
                    hasher.update(chunk)
                    size += len(chunk)
                    f.write(chunk)

        digest = hasher.hexdigest()
        row = store.get(digest) if store else None

        if row is not None:
            # 命中重复：删掉刚下的临时文件，不再占用磁盘
            first_path = row[3]
            hits = store.touch(digest)
            try:
                tmp.unlink()
            except FileNotFoundError:
                pass
            tmp = None
            # 跨群场景：硬链接到本群目录，保持目录完整且零额外字节
            if CFG.get("hardlink_across_groups", True) and os.path.exists(first_path):
                try:
                    if Path(first_path).parent != group_dir:
                        os.link(first_path, dst)
                        store.record_duplicate(digest, str(dst), group_dir.name, size, "link")
                        log("INFO", f"重复(硬链接) {group_dir.name}/{dst.name} ← {first_path} (hits={hits})")
                        return ("linked", str(dst))
                except OSError as e:
                    log("WARN", f"硬链接失败，跳过: {e!r}")
            # 同群重复：只登记台账，不落文件
            store.record_duplicate(digest, str(dst), group_dir.name, size, "live")
            log("INFO", f"重复(跳过) {group_dir.name}/{dst.name} hits={hits} hash={digest[:12]}")
            return ("dup", first_path)

        # 新内容：原子落盘 + 记索引
        tmp.replace(dst)
        tmp = None
        if store:
            store.put(digest, size, str(dst))
        log("INFO", f"保存 {group_dir.name}/{dst.name} ({size} bytes, {HASH_ALGO}={digest[:12]})")
        return ("saved", str(dst))

    except Exception as e:
        log("WARN", f"保存异常 {url[:120]}: {e!r}")
        return ("error", None)
    finally:
        if tmp is not None:
            try:
                tmp.unlink()
            except Exception:
                pass


def seg_name(msg_id, idx: int) -> str:
    return f"msg{msg_id}_{idx}"


# ---------------------- 事件处理 ----------------------

class App:
    def __init__(self, client: OneBotClient, store: HashStore | None = None):
        self.client = client
        self.store = store
        self.session: aiohttp.ClientSession | None = None

    async def start(self):
        self.session = aiohttp.ClientSession()
        for gid in WATCH:
            (SAVE_ROOT / gid).mkdir(parents=True, exist_ok=True)
        log("INFO", f"监听群: {', '.join(sorted(WATCH)) or '(空)'}；保存到 {SAVE_ROOT}")
        if self.store:
            log("INFO", f"哈希索引({HASH_ALGO}): {CFG.get('hash_db_path')} "
                        f"现有 {self.store.count()} 条 (窗口上限 {self.store.max_entries})")
            if CFG.get("reindex_on_start", True):
                added, dups = self.reindex()
                log("INFO", f"存量补录完成：新增索引 {added} 条，登记重复副本 {dups} 条")

    def reindex(self) -> tuple[int, int]:
        """把 save_root 下已有文件补录进索引；同内容的后到者登记为重复副本。

        按 mtime 升序处理，最早出现的文件作为正本。幂等。
        返回 (新增索引数, 新登记重复数)。
        """
        if not self.store:
            return (0, 0)
        added = dups = 0
        files = sorted(
            (p for p in SAVE_ROOT.rglob("*") if p.is_file() and not p.name.startswith(".")),
            key=lambda p: p.stat().st_mtime,
        )
        for p in files:
            try:
                hasher = new_hasher()
                size = 0
                with open(p, "rb") as f:
                    for chunk in iter(lambda: f.read(1 << 20), b""):
                        hasher.update(chunk)
                        size += len(chunk)
                digest = hasher.hexdigest()
                row = self.store.get(digest)
                if row is None:
                    self.store.put(digest, size, str(p))
                    added += 1
                    continue
                # 该哈希已登记：若当前文件就是正本本身（重启后重复扫描），跳过，
                # 否则才是真正的重复副本。
                first_path = row[3]
                if Path(first_path).resolve() == p.resolve():
                    continue
                if not os.path.exists(first_path):
                    # 正本已消失（被移动/删除）：把当前文件接管为正本
                    self.store.conn.execute(
                        "UPDATE hashes SET first_path=?, size=? WHERE hash=?",
                        (str(p), size, digest))
                    continue
                cur = self.store.conn.execute(
                    "INSERT OR IGNORE INTO duplicates "
                    "(hash, algo, path, group_id, size, kind, detected_at) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (digest, HASH_ALGO, str(p), p.parent.name, size, "reindex", time.time()),
                )
                if cur.rowcount > 0:
                    dups += 1
            except Exception as e:
                log("WARN", f"补录失败 {p}: {e!r}")
        return (added, dups)

    async def on_event(self, ev: dict):
        if ev.get("message_type") != "group":
            return
        gid = str(ev.get("group_id", ""))
        if gid not in WATCH:
            return
        message = ev.get("message") or []
        raw = str(ev.get("raw_message") or "").strip()
        msg_id = ev.get("message_id")

        if raw == "/补抓" or raw.startswith("/补抓 "):
            await self.backfill(gid, raw)
            return

        for idx, seg in enumerate(message):
            if seg.get("type") == "image":
                url = (seg.get("data") or {}).get("url")
                if not url:
                    continue
                await save_image(self.session, url, SAVE_ROOT / gid,
                                 seg_name(msg_id, idx), self.store)

    async def backfill(self, gid: str, raw: str):
        limit = CFG.get("backfill_limit", 2000)
        parts = raw.split()
        if len(parts) > 1 and parts[1].isdigit():
            limit = int(parts[1])
        folder = SAVE_ROOT / gid
        folder.mkdir(parents=True, exist_ok=True)
        await self.reply(gid, f"开始回溯本群缓存内历史消息（上限 {limit} 条）…")

        seq, fetched, saved, dup = 0, 0, 0, 0
        while fetched < limit:
            try:
                data = await self.client.call(
                    "get_group_msg_history",
                    {"group_id": int(gid), "message_seq": seq, "count": 20},
                )
            except Exception as e:
                log("WARN", f"get_group_msg_history 失败: {e!r}")
                break
            msgs = (data or {}).get("messages") or []
            if not msgs:
                break
            for m in msgs:
                mid = m.get("message_id")
                for idx, seg in enumerate(m.get("message") or []):
                    if seg.get("type") != "image":
                        continue
                    url = (seg.get("data") or {}).get("url")
                    if not url:
                        continue
                    status, _ = await save_image(self.session, url, folder,
                                                 seg_name(mid, idx), self.store)
                    if status in ("saved", "linked"):
                        saved += 1
                    elif status in ("dup", "exists"):
                        dup += 1
            new_seq = msgs[0].get("message_id", 0)
            if not new_seq or new_seq == seq:
                break
            seq = new_seq
            fetched += len(msgs)

        msg = f"补抓完成：翻了 {fetched} 条消息，新增 {saved} 张，跳过重复 {dup} 张 → {folder}"
        if self.store:
            st = self.store.stats()
            msg += (f"（索引 {st['entries']} 条，累计命中 {st['hits']} 次，"
                    f"重复台账 {st['duplicate_rows']} 条）")
        await self.reply(gid, msg)

    async def reply(self, gid: str, text: str):
        try:
            await self.client.call(
                "send_msg",
                {"message_type": "group", "group_id": int(gid), "message": text},
            )
        except Exception as e:
            log("WARN", f"回复失败: {e!r}")


async def main():
    store = None
    if CFG.get("dedup_enabled", True):
        store = HashStore(CFG["hash_db_path"],
                          CFG.get("max_entries", 200000),
                          CFG.get("latency_budget_ms", 1000))

    if ARGS.check_config:
        st = store.stats() if store else {}
        print(json.dumps({"config_file": str(CONFIG_PATH), "watch_groups": sorted(WATCH),
                          "save_root": str(SAVE_ROOT), "hash_algo": HASH_ALGO,
                          "index": st}, ensure_ascii=False, indent=2))
        return

    client = OneBotClient(CFG["onebot_ws"], CFG.get("access_token", ""))
    app = App(client, store)
    await app.start()

    if ARGS.reindex:
        log("INFO", "仅重建索引模式，退出")
        return

    await client.run(app.on_event)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("INFO", "退出")
