#!/usr/bin/env python3
"""
Backfill missing kline dates from Tencent Finance API.

Fetches recent klines for all stocks and filters to specific missing dates.
Uses concurrent requests with rate limiting for speed.

Usage:
    python db/backfill_klines.py --date 2026-03-17
    python db/backfill_klines.py --date 2026-03-17 --dry-run
"""

import argparse
import csv
import json
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

import requests

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
TENCENT_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Referer": "https://web.sqt.gtimg.cn/",
}


def get_prefix(code):
    if code.startswith("6") or code.startswith("9"):
        return "sh"
    return "sz"


def fetch_one(code, target_date, days=3):
    """Fetch recent klines for one stock, return target_date row if exists."""
    prefix = get_prefix(code)
    url = f"{TENCENT_URL}?param={prefix}{code},day,{target_date},,{days},qfq"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        data = resp.json()
        stock_data = data.get('data', {}).get(f'{prefix}{code}', {})
        klines = stock_data.get('qfqday', []) or stock_data.get('day', [])
        for k in klines:
            if k[0] == target_date:
                return {
                    'code': code,
                    'trade_date': k[0],
                    'open': k[1],
                    'close': k[2],
                    'high': k[3],
                    'low': k[4],
                    'volume': k[5],
                }
    except Exception:
        pass
    return None


def load_stock_codes():
    """Load stock codes from daily_market CSV."""
    codes = []
    csv_path = os.path.join(DATA_DIR, 'daily_market_2026-03-18.csv')
    if not os.path.exists(csv_path):
        # Fallback: get from any daily_market CSV
        for f in os.listdir(DATA_DIR):
            if f.startswith('daily_market_') and f.endswith('.csv'):
                csv_path = os.path.join(DATA_DIR, f)
                break
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            codes.append(row['code'])
    return codes


def main():
    parser = argparse.ArgumentParser(description='Backfill missing kline dates')
    parser.add_argument('--date', required=True, help='Target date to backfill (YYYY-MM-DD)')
    parser.add_argument('--dry-run', action='store_true', help='Fetch only, save CSV')
    parser.add_argument('--workers', type=int, default=8, help='Concurrent workers (default: 8)')
    args = parser.parse_args()

    target = args.date
    print(f"Backfilling klines for {target}")

    codes = load_stock_codes()
    print(f"  Stocks to check: {len(codes)}")

    results = []
    done = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {}
        for i, code in enumerate(codes):
            f = pool.submit(fetch_one, code, target)
            futures[f] = code
            # Stagger submissions to avoid burst
            if i % 50 == 49:
                time.sleep(0.2)

        for f in as_completed(futures):
            done += 1
            row = f.result()
            if row:
                results.append(row)
            if done % 500 == 0:
                print(f"\r  Progress: {done}/{len(codes)} ({len(results)} found)", end="", flush=True)

    print(f"\n  Done: {len(results)} stocks have data for {target}")

    if not results:
        print("  No data found. Was the market open?")
        return

    # Save CSV
    csv_path = os.path.join(DATA_DIR, f'klines_backfill_{target}.csv')
    cols = ['code', 'trade_date', 'open', 'high', 'low', 'close', 'volume']
    with open(csv_path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in results:
            w.writerow({c: r.get(c, '') for c in cols})
    print(f"  Saved: {csv_path}")

    if args.dry_run:
        print("  Dry run — not importing to DB")
        return

    # Import
    abs_path = os.path.abspath(csv_path)
    sql = f"""
INSERT INTO klines (code, trade_date, open, high, low, close, volume, amount)
SELECT
    code, TRY_CAST(trade_date AS DATE),
    TRY_CAST(open AS DECIMAL(10,2)),
    TRY_CAST(high AS DECIMAL(10,2)),
    TRY_CAST(low AS DECIMAL(10,2)),
    TRY_CAST(close AS DECIMAL(10,2)),
    TRY_CAST(volume AS BIGINT),
    NULL
FROM read_csv_auto('{abs_path}', nullstr='')
ON CONFLICT DO NOTHING;
"""
    try:
        import duckdb
        db_path = os.path.abspath(os.path.join(DATA_DIR, 'a-share.db'))
        conn = duckdb.connect(db_path)
        conn.execute(sql)
        count = conn.execute(f"SELECT COUNT(*) FROM klines WHERE trade_date = '{target}'").fetchone()[0]
        conn.close()
        print(f"  DB imported: {count} rows for {target}")
    except Exception as e:
        if 'lock' in str(e).lower():
            sql_path = os.path.join(DATA_DIR, f'klines_backfill_{target}.sql')
            with open(sql_path, 'w') as f:
                f.write(sql)
            print(f"  DB locked. SQL saved: {sql_path}")
        else:
            raise


if __name__ == "__main__":
    main()
