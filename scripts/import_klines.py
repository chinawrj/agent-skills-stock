#!/usr/bin/env python3
"""
全市场日K线数据导入脚本

从腾讯财经 API 获取全A股1年日K线数据（前复权），保存为 CSV 后导入 DuckDB。

数据源: web.ifzq.gtimg.cn (腾讯财经)
覆盖: 最近1年全部A股日K线（前复权）
字段: code, trade_date, open, high, low, close, volume

用法:
    # 抽样测试 (5只股票)
    python scripts/import_klines.py --sample 5

    # 全量导入 (约5000只)
    python scripts/import_klines.py

    # 断点续传 (跳过已下载的股票)
    python scripts/import_klines.py --resume

    # 指定日期范围
    python scripts/import_klines.py --start 2025-03-16 --end 2026-03-16

    # 自定义延迟 (默认0.3-0.8s随机)
    python scripts/import_klines.py --delay 0.5

导入DuckDB:
    脚本运行后输出导入SQL, 通过 MCP mcp_duckdb_query 执行。
    重复执行安全: INSERT OR REPLACE 自动覆盖。
"""

import argparse
import csv
import json
import os
import random
import sys
import time
from pathlib import Path

import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://web.sqt.gtimg.cn/",
}

OUTPUT_CSV = "data/klines_daily.csv"
PROGRESS_FILE = "data/klines_progress.json"
BASE_DELAY = 0.3
MAX_DELAY = 0.8
PAUSE_EVERY_100 = 5       # seconds pause every 100 stocks
PAUSE_EVERY_500 = 30      # seconds pause every 500 stocks
MAX_RETRIES = 2
BATCH_DAYS = 300           # request up to 300 trading days


def get_prefix(code: str) -> str:
    """Get exchange prefix for stock code."""
    if code.startswith("6") or code.startswith("9"):
        return "sh"
    return "sz"


def fetch_klines_tencent(code: str, start: str, end: str) -> list[dict]:
    """
    Fetch daily K-line from Tencent API (前复权).

    Returns list of dicts: {date, open, high, low, close, volume}
    Tencent format: [date, open, close, high, low, volume_in_lots]
    """
    prefix = get_prefix(code)
    url = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
    params = {"param": f"{prefix}{code},day,{start},{end},{BATCH_DAYS},qfq"}

    resp = requests.get(url, params=params, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    stock_key = f"{prefix}{code}"
    klines = data.get("data", {}).get(stock_key, {}).get("qfqday", [])
    if not klines:
        klines = data.get("data", {}).get(stock_key, {}).get("day", [])

    results = []
    for k in klines:
        # Tencent format: [date, open, close, high, low, volume_lots]
        if len(k) < 6:
            continue
        results.append({
            "code": code,
            "trade_date": k[0],
            "open": k[1],
            "high": k[3],
            "low": k[4],
            "close": k[2],
            "volume": int(float(k[5]) * 100),  # lots -> shares
        })
    return results


def load_stock_codes() -> list[str]:
    """Load all stock codes from DuckDB export or CSV."""
    # First try to load from a pre-exported file
    codes_file = "data/stock_codes.csv"
    if os.path.exists(codes_file):
        codes = []
        with open(codes_file, "r") as f:
            reader = csv.reader(f)
            next(reader, None)  # skip header
            for row in reader:
                if row:
                    codes.append(row[0])
        return codes

    # Fallback: use DuckDB directly (may fail if locked)
    try:
        import duckdb
        db = duckdb.connect("data/a-share.db", read_only=True)
        df = db.execute(
            "SELECT code FROM stocks WHERE code NOT LIKE '9%' ORDER BY code"
        ).fetchdf()
        db.close()
        codes = df["code"].tolist()
        # Save for future use
        with open(codes_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["code"])
            for c in codes:
                writer.writerow([c])
        return codes
    except Exception as e:
        print(f"ERROR: Cannot load stock codes: {e}")
        print("Please export stock codes first:")
        print("  MCP: SELECT code FROM stocks WHERE code NOT LIKE '9%' ORDER BY code")
        sys.exit(1)


def load_progress() -> set:
    """Load set of already-downloaded stock codes."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            data = json.load(f)
            return set(data.get("completed", []))
    return set()


def save_progress(completed: set):
    """Save progress to file."""
    os.makedirs(os.path.dirname(PROGRESS_FILE), exist_ok=True)
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"completed": sorted(completed), "count": len(completed)}, f)


def run(args):
    start_date = args.start or "2025-03-16"
    end_date = args.end or "2026-03-16"
    delay = args.delay or BASE_DELAY

    print(f"=== 全市场日K线数据导入 ===")
    print(f"日期范围: {start_date} ~ {end_date}")
    print(f"输出文件: {OUTPUT_CSV}")
    print(f"延迟: {delay}~{delay + 0.5}s")

    # Load stock codes
    all_codes = load_stock_codes()
    print(f"全部股票: {len(all_codes)} 只")

    # Sample mode
    if args.sample:
        all_codes = all_codes[:args.sample]
        print(f"抽样模式: 仅处理前 {args.sample} 只")

    # Resume mode
    completed = set()
    if args.resume:
        completed = load_progress()
        print(f"断点续传: 已完成 {len(completed)} 只")

    # Filter out completed
    pending_codes = [c for c in all_codes if c not in completed]
    if args.limit:
        pending_codes = pending_codes[:args.limit]
        print(f"限制本次: {args.limit} 只")
    print(f"待处理: {len(pending_codes)} 只")
    print()

    # Open CSV (append if resuming, write if fresh)
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    csv_mode = "a" if args.resume and os.path.exists(OUTPUT_CSV) else "w"
    csv_fields = ["code", "trade_date", "open", "high", "low", "close", "volume"]

    csv_file = open(OUTPUT_CSV, csv_mode, newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_file, fieldnames=csv_fields)
    if csv_mode == "w":
        writer.writeheader()

    # Process stocks
    total_rows = 0
    success = 0
    fails = 0
    t_start = time.time()

    try:
        for i, code in enumerate(pending_codes, 1):
            # Rate limiting
            if i > 1:
                jitter = random.uniform(0, 0.5)
                time.sleep(delay + jitter)

            # Periodic pauses to avoid bans
            if i > 1 and i % 500 == 0:
                elapsed = time.time() - t_start
                rate = success / elapsed * 60 if elapsed > 0 else 0
                print(f"\n  === 暂停 {PAUSE_EVERY_500}s (防封) | "
                      f"进度 {i}/{len(pending_codes)} | "
                      f"速度 {rate:.0f}只/分 ===\n")
                time.sleep(PAUSE_EVERY_500)
            elif i > 1 and i % 100 == 0:
                elapsed = time.time() - t_start
                rate = success / elapsed * 60 if elapsed > 0 else 0
                eta_min = (len(pending_codes) - i) / rate if rate > 0 else 0
                print(f"  --- 暂停 {PAUSE_EVERY_100}s | "
                      f"进度 {i}/{len(pending_codes)} ({success}✓ {fails}✗) | "
                      f"速度 {rate:.0f}只/分 | ETA {eta_min:.0f}分 ---")
                time.sleep(PAUSE_EVERY_100)

            # Fetch with retry
            klines = None
            for retry in range(MAX_RETRIES + 1):
                try:
                    klines = fetch_klines_tencent(code, start_date, end_date)
                    break
                except Exception as e:
                    if retry < MAX_RETRIES:
                        wait = (retry + 1) * 3
                        print(f"  {code}: 重试 {retry+1}/{MAX_RETRIES} ({wait}s后)")
                        time.sleep(wait)
                    else:
                        print(f"  {code}: 失败 - {e}")
                        fails += 1

            if klines is None:
                continue

            if not klines:
                # Stock might be suspended or newly listed
                completed.add(code)
                continue

            # Write to CSV
            for row in klines:
                writer.writerow(row)
            csv_file.flush()

            total_rows += len(klines)
            success += 1
            completed.add(code)

            # Progress update every 50 stocks
            if i % 50 == 0 or i == len(pending_codes):
                save_progress(completed)
                elapsed = time.time() - t_start
                print(f"  [{i}/{len(pending_codes)}] "
                      f"{code} +{len(klines)}天 | "
                      f"累计 {total_rows:,} 行 | "
                      f"耗时 {elapsed:.0f}s")

    except KeyboardInterrupt:
        print(f"\n\n中断! 已保存进度: {len(completed)} 只完成")
        save_progress(completed)
    finally:
        csv_file.close()

    # Final report
    elapsed = time.time() - t_start
    file_size = os.path.getsize(OUTPUT_CSV) / (1024 * 1024) if os.path.exists(OUTPUT_CSV) else 0

    print(f"\n{'='*60}")
    print(f"=== 完成 ===")
    print(f"成功: {success} 只, 失败: {fails} 只")
    print(f"总行数: {total_rows:,}")
    print(f"耗时: {elapsed:.0f}s ({elapsed/60:.1f}分)")
    print(f"文件: {OUTPUT_CSV} ({file_size:.1f} MB)")
    print(f"{'='*60}")

    # Print import SQL
    if not args.no_sql and total_rows > 0:
        abs_path = os.path.abspath(OUTPUT_CSV)
        sql = f"""INSERT OR REPLACE INTO klines (code, trade_date, open, high, low, close, volume)
SELECT
    code,
    CAST(trade_date AS DATE),
    CAST(open AS DECIMAL(10,2)),
    CAST(high AS DECIMAL(10,2)),
    CAST(low AS DECIMAL(10,2)),
    CAST(close AS DECIMAL(10,2)),
    CAST(volume AS BIGINT)
FROM read_csv('{abs_path}', header=true, nullstr='');"""

        print(f"\n导入 SQL (通过 MCP mcp_duckdb_query 执行):")
        print(f"{'='*60}")
        print(sql)
        print(f"{'='*60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="导入全市场日K线数据")
    parser.add_argument("--start", help="起始日期 YYYY-MM-DD (默认: 1年前)")
    parser.add_argument("--end", help="结束日期 YYYY-MM-DD (默认: 今天)")
    parser.add_argument("--sample", type=int, help="抽样: 仅处理前N只")
    parser.add_argument("--resume", action="store_true", help="断点续传")
    parser.add_argument("--limit", type=int, help="限制本次处理数量 (配合 --resume 使用)")
    parser.add_argument("--delay", type=float, help="请求间隔秒数 (默认: 0.3)")
    parser.add_argument("--no-sql", action="store_true", help="不输出导入SQL")
    run(parser.parse_args())
