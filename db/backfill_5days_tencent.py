#!/usr/bin/env python3
"""
Backfill klines for 5 dates (2026-03-17 to 2026-03-23) using Tencent Finance API.
Uses HTTP concurrent requests for speed (~5 min for 5500 stocks).
Returns raw (不复权) prices.

Usage:
    .venv/bin/python3 db/backfill_5days_tencent.py
"""

import csv
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
TARGET_DATES = {'2026-03-17', '2026-03-18', '2026-03-19', '2026-03-20', '2026-03-23'}
TENCENT_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Referer": "https://web.sqt.gtimg.cn/",
}


def get_prefix(code):
    if code.startswith("6") or code.startswith("9"):
        return "sh"
    return "sz"


def fetch_one(code):
    """Fetch recent klines for one stock, return rows for target dates."""
    prefix = get_prefix(code)
    # Request 15 days from 2026-03-14 - raw (不复权) data (no qfq suffix)
    url = f"{TENCENT_URL}?param={prefix}{code},day,2026-03-14,,15,"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        data = resp.json()
        stock_data = data.get('data', {}).get(f'{prefix}{code}', {})
        # 'day' = 不复权 (raw), 'qfqday' = 前复权 (adjusted)
        klines = stock_data.get('day', []) or stock_data.get('qfqday', [])
        rows = []
        for k in klines:
            if k[0] in TARGET_DATES:
                # Tencent format: [date, open, close, high, low, volume_in_lots]
                rows.append({
                    'code': code,
                    'trade_date': k[0],
                    'open': k[1],
                    'high': k[3],
                    'low': k[4],
                    'close': k[2],
                    'volume': int(float(k[5]) * 100),  # lots -> shares
                })
        return rows
    except Exception:
        return None


def load_stock_codes():
    codes = []
    stocks_csv = os.path.join(DATA_DIR, 'all_stocks.csv')
    if os.path.exists(stocks_csv):
        with open(stocks_csv) as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = row.get('code', '')
                market = row.get('market', '')
                if market in ('SH', 'SZ'):
                    codes.append(code)
        return codes
    for fn in sorted(os.listdir(DATA_DIR), reverse=True):
        if fn.startswith('daily_market_') and fn.endswith('.csv'):
            csv_path = os.path.join(DATA_DIR, fn)
            with open(csv_path) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    codes.append(row['code'])
            return codes
    return codes


def main():
    print(f"Backfilling klines for {sorted(TARGET_DATES)} via Tencent Finance", flush=True)

    codes = load_stock_codes()
    print(f"Stocks to fetch: {len(codes)}", flush=True)
    if not codes:
        print("No stock codes found!")
        sys.exit(1)

    results = []
    done = 0
    errors = 0
    t_start = time.time()

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {}
        for i, code in enumerate(codes):
            f = pool.submit(fetch_one, code)
            futures[f] = code
            if i % 50 == 49:
                time.sleep(0.1)

        for f in as_completed(futures):
            done += 1
            code = futures[f]
            try:
                rows = f.result()
                if rows:
                    results.extend(rows)
                elif rows is None:
                    errors += 1
            except Exception:
                errors += 1

            if done % 500 == 0:
                elapsed = time.time() - t_start
                rate = done / elapsed
                eta = (len(codes) - done) / rate
                print(f"  Progress: {done}/{len(codes)} | rows={len(results)} err={errors} | {rate:.0f}/s | ETA {eta:.0f}s", flush=True)

    elapsed = time.time() - t_start
    print(f"\nCompleted in {elapsed:.1f}s ({elapsed/60:.1f}min)", flush=True)
    print(f"Total rows: {len(results)}", flush=True)
    print(f"Errors: {errors}", flush=True)

    # Count per date
    date_counts = {}
    for r in results:
        d = r['trade_date']
        date_counts[d] = date_counts.get(d, 0) + 1
    for d in sorted(date_counts):
        print(f"  {d}: {date_counts[d]} stocks", flush=True)

    if not results:
        print("No data fetched!")
        sys.exit(1)

    # Save CSV
    csv_path = os.path.join(DATA_DIR, 'klines_backfill_5days.csv')
    cols = ['code', 'trade_date', 'open', 'high', 'low', 'close', 'volume']
    with open(csv_path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in results:
            w.writerow(r)
    print(f"\nCSV saved: {csv_path} ({len(results)} rows)", flush=True)

    # Verify sample against Baostock data (000001)
    print("\n--- Verification sample (000001) ---", flush=True)
    baostock_truth = {
        '2026-03-17': {'open': '10.91', 'close': '11.03'},
        '2026-03-18': {'open': '11.04', 'close': '10.96'},
        '2026-03-19': {'open': '10.92', 'close': '10.88'},
        '2026-03-20': {'open': '10.87', 'close': '10.77'},
        '2026-03-23': {'open': '10.68', 'close': '10.45'},
    }
    for r in results:
        if r['code'] == '000001':
            truth = baostock_truth.get(r['trade_date'], {})
            match = "OK" if truth and str(r['open']) == truth['open'] and str(r['close']) == truth['close'] else "MISMATCH"
            print(f"  {r['trade_date']}: O={r['open']} C={r['close']} [{match}]", flush=True)


if __name__ == '__main__':
    main()
