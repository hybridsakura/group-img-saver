#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""group_img_saver_svc 去重 + 重复台账验证（真实 HTTP 下载，非 mock）。

覆盖：
  T1 同群同图二次出现 → 只落 1 个文件，第二次返回 dup，hits=1，重复台账 +1
  T2 不同内容 → 各自落盘
  T3 同图跨群 → 硬链接（inode 相同），台账记 kind=link
  T4 索引窗口上限 → 超限按 last_seen 淘汰最旧
  T5 延迟守卫 → EMA 超预算时淘汰最旧 10%
  T6 哈希算法为 xxh3_128，且 meta 里记录了 algo
  T7 reindex 把存量重复副本登记为 kind=reindex（不删文件）
"""
import asyncio
import http.server
import importlib.util
import os
import shutil
import sys
import tempfile
import threading
from pathlib import Path

ROOT = Path(tempfile.mkdtemp(prefix="gis_test_"))
(ROOT / "imgs").mkdir()
PORT = 18092

IMG_A = os.urandom(2048)
IMG_B = os.urandom(4096)
(ROOT / "imgs" / "a.jpg").write_bytes(IMG_A)
(ROOT / "imgs" / "b.jpg").write_bytes(IMG_B)


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *a, **kw):
        super().__init__(*a, directory=str(ROOT / "imgs"), **kw)

    def log_message(self, *a):
        pass


srv = http.server.ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
threading.Thread(target=srv.serve_forever, daemon=True).start()

os.environ["GIS_CONFIG"] = str(ROOT / "cfg.json")
(ROOT / "cfg.json").write_text('{"save_root": "%s", "watch_groups": ["gA","gB"]}'
                               % (ROOT / "out"))
spec = importlib.util.spec_from_file_location(
    "svc", str(Path(__file__).resolve().parent / "group_img_saver_svc.py"))
svc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(svc)

URL = f"http://127.0.0.1:{PORT}"
results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond), detail))
    print(("PASS  " if cond else "FAIL  ") + name + ("  " + detail if detail else ""), flush=True)


def dg(data: bytes) -> str:
    h = svc.new_hasher()
    h.update(data)
    return h.hexdigest()


async def main():
    import aiohttp
    store = svc.HashStore(str(ROOT / "h.db"), max_entries=1000, latency_budget_ms=1000)
    async with aiohttp.ClientSession() as sess:
        gA = ROOT / "out" / "gA"
        gB = ROOT / "out" / "gB"

        # T6 算法
        check("T6 算法为 xxh3_128", svc.HASH_ALGO == "xxh3_128", f"algo={svc.HASH_ALGO}")
        meta = dict(store.conn.execute("SELECT key,value FROM meta").fetchall())
        check("T6 meta 记录 algo/schema", meta.get("algo") == "xxh3_128"
              and meta.get("schema_version") == "2", f"meta={meta}")

        # T1 同群同图两次
        s1, _ = await svc.save_image(sess, f"{URL}/a.jpg", gA, "msg1_0", store)
        s2, _ = await svc.save_image(sess, f"{URL}/a.jpg", gA, "msg2_0", store)
        files = list(gA.glob("msg*.jpg"))
        row = store.get(dg(IMG_A))
        check("T1 首次落盘", s1 == "saved" and len(files) == 1, f"{s1} files={len(files)}")
        check("T1 二次判重复", s2 == "dup", f"status={s2}")
        check("T1 hits=1", row and row[6] == 1, f"hits={row[6] if row else None}")
        check("T1 未产生第二个文件", len(list(gA.iterdir())) == 1,
              f"dir={[p.name for p in gA.iterdir()]}")
        dup_rows = store.conn.execute(
            "SELECT path, kind FROM duplicates WHERE hash=?", (dg(IMG_A),)).fetchall()
        check("T1 重复已入台账(kind=live)", len(dup_rows) == 1 and dup_rows[0][1] == "live",
              f"rows={dup_rows}")

        # T2 不同内容
        s3, _ = await svc.save_image(sess, f"{URL}/b.jpg", gA, "msg3_0", store)
        check("T2 不同内容各自落盘", s3 == "saved" and len(list(gA.iterdir())) == 2,
              f"{s3} files={len(list(gA.iterdir()))}")

        # T3 同图跨群 → 硬链接
        s4, _ = await svc.save_image(sess, f"{URL}/a.jpg", gB, "msg4_0", store)
        fA = next(gA.glob("msg1_0.jpg"))
        fB = next(gB.glob("msg4_0.jpg"))
        check("T3 跨群返回 linked", s4 == "linked", f"status={s4}")
        check("T3 inode 相同（硬链接）", fA.stat().st_ino == fB.stat().st_ino,
              f"inoA={fA.stat().st_ino} inoB={fB.stat().st_ino}")
        check("T3 内容一致", fA.read_bytes() == fB.read_bytes() == IMG_A)
        link_row = store.conn.execute(
            "SELECT kind FROM duplicates WHERE hash=? AND path=?",
            (dg(IMG_A), str(fB))).fetchone()
        check("T3 台账记 kind=link", link_row and link_row[0] == "link", f"row={link_row}")

        # T4 窗口上限淘汰
        st2 = svc.HashStore(str(ROOT / "h2.db"), max_entries=1000, latency_budget_ms=1000)
        for i in range(1005):
            st2.put(f"{i:032x}", 10, f"/tmp/x{i}")
        st2._guard()
        check("T4 超上限被淘汰到 1000 以内", st2.count() <= 1000, f"count={st2.count()}")
        check("T4 淘汰的是最旧（首条已消失）", st2.get(f"{0:032x}") is None)
        check("T4 最新记录仍在", st2.get(f"{1004:032x}") is not None)

        # T5 延迟守卫
        st3 = svc.HashStore(str(ROOT / "h3.db"), max_entries=100000, latency_budget_ms=1)
        for i in range(50):
            st3.put(f"{i:032x}", 10, f"/tmp/y{i}")
        st3._ema = 5.0
        before = st3.count()
        st3._guard()
        check("T5 延迟超预算触发淘汰", st3.count() < before, f"{before} -> {st3.count()}")

    # T7 reindex 登记存量重复（构造两个同内容文件，不删）
    out2 = ROOT / "out2"
    (out2 / "gX").mkdir(parents=True)
    (out2 / "gY").mkdir(parents=True)
    (out2 / "gX" / "a.jpg").write_bytes(IMG_B)
    (out2 / "gY" / "b.jpg").write_bytes(IMG_B)
    svc.SAVE_ROOT = out2
    st4 = svc.HashStore(str(ROOT / "h4.db"), max_entries=1000, latency_budget_ms=1000)
    app = svc.App(client=None, store=st4)
    added, dups = app.reindex()
    check("T7 reindex 索引数=1", added == 1, f"added={added}")
    check("T7 reindex 登记重复=1", dups == 1, f"dups={dups}")
    check("T7 重复文件未被删除", (out2 / "gX" / "a.jpg").exists()
          and (out2 / "gY" / "b.jpg").exists())
    kinds = [r[0] for r in st4.conn.execute("SELECT kind FROM duplicates").fetchall()]
    check("T7 台账 kind=reindex", kinds == ["reindex"], f"kinds={kinds}")

    srv.shutdown()

    # T8 v1(sha1) 索引迁移到 v2(xxh3_128)：旧表必须被 DROP 重建，不能报 no such column
    import sqlite3
    old = sqlite3.connect(str(ROOT / "h_old.db"))
    old.executescript("""
        CREATE TABLE hashes (sha1 TEXT PRIMARY KEY, size INTEGER, first_path TEXT,
                             first_seen REAL, last_seen REAL, hits INTEGER);
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
        INSERT INTO meta VALUES ('schema_version','1');
        INSERT INTO meta VALUES ('algo','sha1');
        INSERT INTO hashes VALUES ('deadbeef', 123, '/tmp/old.jpg', 0, 0, 0);
    """)
    old.commit()
    old.close()
    migrated = svc.HashStore(str(ROOT / "h_old.db"), 1000, 1000)
    cols = [r[1] for r in migrated.conn.execute("PRAGMA table_info(hashes)")]
    m = dict(migrated.conn.execute("SELECT key,value FROM meta").fetchall())
    check("T8 v1 表已重建（hash 列存在）", "hash" in cols and "sha1" not in cols, f"cols={cols}")
    check("T8 meta 升级为 v2/xxh3_128",
          m.get("schema_version") == "2" and m.get("algo") == "xxh3_128", f"meta={m}")
    check("T8 旧索引行已清除", migrated.count() == 0, f"count={migrated.count()}")
    migrated.close()

    # T9 中间态：meta 已写 v2/xxh3_128，但表仍是 v1(sha1 列) → 必须照样重建
    bad = sqlite3.connect(str(ROOT / "h_mid.db"))
    bad.executescript("""
        CREATE TABLE hashes (sha1 TEXT PRIMARY KEY, size INTEGER, first_path TEXT,
                             first_seen REAL, last_seen REAL, hits INTEGER);
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
        INSERT INTO meta VALUES ('schema_version','2');
        INSERT INTO meta VALUES ('algo','xxh3_128');
        INSERT INTO hashes VALUES ('cafebabe', 99, '/tmp/mid.jpg', 0, 0, 3);
    """)
    bad.commit()
    bad.close()
    mid = svc.HashStore(str(ROOT / "h_mid.db"), 1000, 1000)
    cols2 = [r[1] for r in mid.conn.execute("PRAGMA table_info(hashes)")]
    check("T9 中间态被识别并重建", cols2 == svc.HashStore.HASH_COLS, f"cols={cols2}")
    check("T9 中间态旧行已清除", mid.count() == 0, f"count={mid.count()}")
    mid.close()

    # T10 可复用性：默认配置不含绝对路径；--check-config CLI 可用
    import subprocess
    bad_defaults = [k for k, v in svc.DEFAULT_CONFIG.items()
                    if isinstance(v, str) and v.startswith("/")]
    check("T10 默认配置无绝对路径", not bad_defaults, f"绝对路径项={bad_defaults}")
    check("T10 默认不监听任何群", svc.DEFAULT_CONFIG["watch_groups"] == [],
          f"watch_groups={svc.DEFAULT_CONFIG['watch_groups']}")

    cfg2 = ROOT / "cli.json"
    cfg2.write_text('{"watch_groups": ["999"], "save_root": "%s/cli_out", '
                    '"hash_db_path": "%s/cli.db"}' % (ROOT, ROOT))
    proc = subprocess.run(
        [sys.executable, str(Path(__file__).resolve().parent / "group_img_saver_svc.py"),
         "--config", str(cfg2), "--check-config"],
        capture_output=True, text=True, timeout=60)
    import json as _json
    ok = proc.returncode == 0 and '"999"' in proc.stdout and str(ROOT / "cli_out") in proc.stdout
    check("T10 --config + --check-config 可用", ok,
          f"rc={proc.returncode} out={proc.stdout[:120]}")

    # T11 reindex 幂等：第二次扫描不能把正本文件本身记成重复
    st5 = svc.HashStore(str(ROOT / "h5.db"), max_entries=1000, latency_budget_ms=1000)
    app2 = svc.App(client=None, store=st5)
    svc.SAVE_ROOT = out2
    a1, d1 = app2.reindex()
    a2, d2 = app2.reindex()
    a3, d3 = app2.reindex()
    check("T11 首次扫描 1 正本 1 重复", (a1, d1) == (1, 1), f"{(a1, d1)}")
    check("T11 二次扫描不新增正本/不误记重复", (a2, d2) == (0, 0), f"{(a2, d2)}")
    check("T11 三次扫描仍幂等", (a3, d3) == (0, 0), f"{(a3, d3)}")
    n_dup = st5.conn.execute("SELECT COUNT(*) FROM duplicates").fetchone()[0]
    check("T11 台账总数仍为 1", n_dup == 1, f"duplicates={n_dup}")


asyncio.run(main())
fails = [r for r in results if not r[1]]
print(f"\n== {len(results)-len(fails)}/{len(results)} passed ==")
shutil.rmtree(ROOT, ignore_errors=True)
sys.exit(1 if fails else 0)
