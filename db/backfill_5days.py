#!/usr/bin/env python3
"""
Backfill klines for 2026-03-17 to 2026-03-23 (5 trading days) using Baostock.
Each stock fetches all 5 days in one API call for efficiency.

Usage:
    python db/backfill_5days.py
"""

import csv
import os
import sys
import time

import baostock as bs

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
TARGET_DATES = {'2026-03-17', '2026-03-18', '2026-03-19', '2026-03-20', '2026-03-23'}
START_DATE = '2026-03-17'
END_DATE = '2026-03-23'


def load_stock_codes():
    """Load stock codes from stocks table via CSV."""
    codes = []
    stocks_csv = os.path.join(DATA_DIR, 'all_stocks.csv')
    if os.path.exists(stocks_csv):
        with open(stocks_csv) as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = row.get('code', '')
                market = row.get('market', '')
                if market == 'SH':
                    codes.append((code, f'sh.{code}'))
                elif market == 'SZ':
                    codes.append((code, f'sz.{code}'))
        return codes

    # Fallback: daily_market CSV
    for fn in sorted(os.listdir(DATA_DIR), reverse=True):
        if fn.startswith('daily_market_') and fn.endswith('.csv'):
            csv_path = os.path.join(DATA_DIR, fn)
            with open(csv_path) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    c = row['code']
                    if c.startswith('6') or c.startswith('9'):
                        prefix = 'sh'
                    else:
                        prefix = 'sz'
                    codes.append((c, f'{prefix}.{c}'))
            return codes
    return codes


def main():
    print(f"Backfilling klines for {START_DATE} to {END_DATE} via Baostock")
    print(f"Target dates: {sorted(TARGET_DATES)}")

    codes = load_stock_codes()
    print(f"Stocks to fetch: {len(codes)}")
    if not codes:
        print("No stock codes found!")
        sys.exit(1)

    lg = bs.login()
    print(f"Baostock login: {lg.error_msg}")

    results = []
    skipped = 0
    errors = 0
    t_start = time.time()

    for i, (code, bs_code) in enumerate(codes):
        try:
            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,code,open,high,low,close,volume,amount",
                start_date=START_DATE,
                end_date=END_DATE,
                frequency="d",
                adjustflag="3",  # 不复权 - raw prices
            )
            found = 0
            while rs.error_code == '0' and rs.next():
                row = rs.get_row_data()
                if row[0] in TARGET_DATES and row[2]:  # date in targets and has open
                    results.append({
                        'code': code,
                        'trade_date': row[0],
                        'open': row[2],
                        'high': row[3],
                        'low': row[4],
                        'close': row[5],
                        'volume': row[6],
                        'amount': row[7],
                    })
                    found += 1
            if found == 0:
                skipped += 1
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  Error {bs_code}: {e}")

        if (i + 1) % 500 == 0:
            elapsed = time.time() - t_start
            rate = (i + 1) / elapsed
            eta = (len(codes) - i - 1) / rate / 60
            print(f"  Progress: {i+1}/{len(codes)} | rows={len(results)} skip={skipped} err={errors} | {rate:.1f}/s | ETA {eta:.1f}min")

    bs.logout()

    elapsed = time.time() - t_start
    print(f"\nCompleted in {elapsed:.1f}s ({elapsed/60:.1f}min)")
    print(f"Total rows: {len(results)}")
    print(f"Skipped: {skipped} (no data/suspended)")
    print(f"Errors: {errors}")

    # Count per date
    date_counts = {}
    for r in results:
        d = r['trade_date']
        date_counts[d] = date_counts.get(d, 0) + 1
    for d in sorted(date_counts):
        print(f"  {d}: {date_counts[d]} stocks")

    if not results:
        print("No data fetched. Exiting.")
        sys.exit(1)

    # Save CSV
    csv_path = os.path.join(DATA_DIR, 'klines_backfill_5days.csv')
    cols = ['code', 'trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount']
    with open(csv_path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in results:
            w.writerow(r)
    print(f"\nCSV saved: {csv_path}")

    # Generate SQL
    sql_path = os.path.join(DATA_DIR, 'klines_backfill_5days.sql')
    abs_csv = os.path.abspath(csv_path)

    with open(sql_path, 'w') as f:
        f.write(f"-- Backfill klines for {START_DATE} to {END_DATE}\n")
        f.write(f"-- {len(results)} total rows across {len(date_counts)} dates\n\n")
        for d in sorted(date_counts):
            f.write(f"-- {d}: {date_counts[d]} stocks\n")
        f.write(f"\nDELETE FROM klines WHERE trade_date IN ({', '.join(repr(d) for d in sorted(TARGET_DATES))});\n\n")
        f.write(f"""INSERT INTO klines (code, trade_date, open, high, low, close, volume, amount)
SELECT
    code,
    TRY_CAST(trade_date AS DATE),
    TRY_CAST(open AS DECIMAL(10,2)),
    TRY_CAST(high AS DECIMAL(10,2)),
    TRY_CAST(low AS DECIMAL(10,2)),
    TRY_CAST(close AS DECIMAL(10,2)),
    TRY_CAST(volume AS BIGINT),
    TRY_CAST(amount AS DECIMAL(18,2))
FROM read_csv_auto('{abs_csv}', nullstr='');\n""")
    print(f"SQL saved: {sql_path}")

    # Sample
    for d in sorted(TARGET_DATES):
        sample = [r for r in results if r['trade_date'] == d][:2]
        for s in sample:
            print(f"  {s['trade_date']} {s['code']}: O={s['open']} H={s['high']} L={s['low']} C={s['close']} V={s['volume']}")


if __name__ == '__main__':
    main()
