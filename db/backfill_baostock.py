#!/usr/bin/env python3
"""
Backfill klines with full OHLCV data using Baostock API.

Baostock is free, reliable, and returns full OHLCV for any historical date.
~330ms/stock, ~30min for 5500 stocks.

Usage:
    python db/backfill_baostock.py --date 2026-03-17
    python db/backfill_baostock.py --date 2026-03-17 --workers 4
"""

import argparse
import csv
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')


def get_bs_code(code, market):
    """Convert stock code + market to Baostock format."""
    if market == 'SH':
        return f'sh.{code}'
    elif market == 'SZ':
        return f'sz.{code}'
    else:
        return None  # Skip OTHER (北交所 not supported by Baostock)


def load_stock_codes():
    """Load stock codes from stocks CSV or daily_market CSV."""
    codes = []
    # Try all_stocks.csv first
    stocks_csv = os.path.join(DATA_DIR, 'all_stocks.csv')
    if os.path.exists(stocks_csv):
        with open(stocks_csv) as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = row.get('code', '')
                market = row.get('market', '')
                bs_code = get_bs_code(code, market)
                if bs_code:
                    codes.append((code, bs_code))
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


def fetch_one_baostock(bs_code, target_date):
    """Fetch OHLCV for a single stock on target_date using Baostock.
    
    Note: Each thread needs its own baostock connection since baostock
    uses a socket-based protocol that isn't thread-safe.
    """
    import baostock as bs
    try:
        lg = bs.login()
        if lg.error_code != '0':
            return None
        rs = bs.query_history_k_data_plus(
            bs_code,
            "date,code,open,high,low,close,volume,amount",
            start_date=target_date,
            end_date=target_date,
            frequency="d",
            adjustflag="2",  # 前复权, consistent with existing klines data
        )
        row = None
        while rs.error_code == '0' and rs.next():
            row = rs.get_row_data()
        bs.logout()
        return row
    except Exception:
        try:
            bs.logout()
        except Exception:
            pass
        return None


def fetch_one_serial(bs, bs_code, target_date):
    """Fetch OHLCV using shared baostock connection (serial mode)."""
    rs = bs.query_history_k_data_plus(
        bs_code,
        "date,code,open,high,low,close,volume,amount",
        start_date=target_date,
        end_date=target_date,
        frequency="d",
        adjustflag="2",
    )
    row = None
    while rs.error_code == '0' and rs.next():
        row = rs.get_row_data()
    return row


def main():
    parser = argparse.ArgumentParser(description='Backfill klines with full OHLCV via Baostock')
    parser.add_argument('--date', required=True, help='Target date (YYYY-MM-DD)')
    parser.add_argument('--workers', type=int, default=1,
                        help='Concurrent workers (default: 1, Baostock may not support concurrent well)')
    args = parser.parse_args()

    target = args.date
    print(f"Backfilling full OHLCV klines for {target} via Baostock")

    codes = load_stock_codes()
    print(f"  Stocks to fetch: {len(codes)}")
    if not codes:
        print("No stock codes found!")
        sys.exit(1)

    results = []
    skipped = 0
    errors = 0
    t_start = time.time()

    if args.workers <= 1:
        # Serial mode - single connection, most reliable
        import baostock as bs
        lg = bs.login()
        print(f"  Baostock login: {lg.error_msg}")

        for i, (code, bs_code) in enumerate(codes):
            try:
                row = fetch_one_serial(bs, bs_code, target)
                if row and row[2]:  # has open price
                    results.append({
                        'code': code,
                        'trade_date': target,
                        'open': row[2],
                        'high': row[3],
                        'low': row[4],
                        'close': row[5],
                        'volume': row[6],
                        'amount': row[7],
                    })
                else:
                    skipped += 1
            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"  Error {bs_code}: {e}")

            if (i + 1) % 200 == 0:
                elapsed = time.time() - t_start
                rate = (i + 1) / elapsed
                eta = (len(codes) - i - 1) / rate / 60
                print(f"  Progress: {i+1}/{len(codes)} | found={len(results)} skip={skipped} err={errors} | {rate:.1f} stocks/s | ETA {eta:.1f}min")

        bs.logout()
    else:
        # Concurrent mode - each worker gets its own connection
        print(f"  Using {args.workers} concurrent workers")
        done = 0
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {}
            for code, bs_code in codes:
                f = pool.submit(fetch_one_baostock, bs_code, target)
                futures[f] = code

            for f in as_completed(futures):
                done += 1
                code = futures[f]
                try:
                    row = f.result()
                    if row and row[2]:
                        results.append({
                            'code': code,
                            'trade_date': target,
                            'open': row[2],
                            'high': row[3],
                            'low': row[4],
                            'close': row[5],
                            'volume': row[6],
                            'amount': row[7],
                        })
                    else:
                        skipped += 1
                except Exception:
                    errors += 1

                if done % 200 == 0:
                    elapsed = time.time() - t_start
                    rate = done / elapsed
                    eta = (len(codes) - done) / rate / 60
                    print(f"  Progress: {done}/{len(codes)} | found={len(results)} | {rate:.1f} stocks/s | ETA {eta:.1f}min")

    elapsed = time.time() - t_start
    print(f"\n  Completed in {elapsed:.1f}s ({elapsed/60:.1f}min)")
    print(f"  Results: {len(results)} stocks with OHLCV data")
    print(f"  Skipped: {skipped} (no data/suspended)")
    print(f"  Errors: {errors}")

    if not results:
        print("No data fetched. Exiting.")
        sys.exit(1)

    # Save CSV
    csv_path = os.path.join(DATA_DIR, f'klines_ohlcv_{target}.csv')
    cols = ['code', 'trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount']
    with open(csv_path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in results:
            w.writerow(r)
    print(f"\n  CSV saved: {csv_path}")

    # Generate SQL: DELETE old rows + INSERT new ones
    sql_path = os.path.join(DATA_DIR, f'klines_ohlcv_{target}.sql')
    abs_csv = os.path.abspath(csv_path)

    with open(sql_path, 'w') as f:
        f.write(f"-- Full OHLCV backfill for {target} via Baostock\n")
        f.write(f"-- {len(results)} stocks\n\n")
        f.write(f"DELETE FROM klines WHERE trade_date = '{target}';\n\n")
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
    print(f"  SQL saved: {sql_path}")
    print(f"\n  To import: run each SQL statement via mcp_duckdb_query")

    # Show sample
    sample = results[:3]
    print(f"\n  Sample data:")
    for r in sample:
        print(f"    {r['code']}: O={r['open']} H={r['high']} L={r['low']} C={r['close']} V={r['volume']}")


if __name__ == '__main__':
    main()
