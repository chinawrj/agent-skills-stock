#!/usr/bin/env python3
"""
Daily market snapshot: fetch all A-share stocks' daily market data and store to DuckDB.

Data source: Sina Finance Market Center API (reliable, no aggressive rate limiting).
Fields: price, change, volume, market cap, PE, PB, turnover rate.

Usage:
    cd /Users/rjwang/fun/a-share && source .venv/bin/activate

    # Take today's snapshot
    python scripts/daily_market_snapshot.py

    # Dry run (fetch + save CSV, don't import to DB)
    python scripts/daily_market_snapshot.py --dry-run

    # Force specific date (e.g. for backfill)
    python scripts/daily_market_snapshot.py --date 2026-03-14
"""

import argparse
import csv
import json
import os
import subprocess
import sys
import time
from datetime import datetime, date

sys.path.insert(0, os.path.dirname(__file__))

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')

SINA_URL = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData"

# Sina API → DB field mapping
# Sina returns: symbol, code, name, trade, pricechange, changepercent, buy, sell,
# settlement, open, high, low, volume, amount, ticktime, per, pb, mktcap, nmc, turnoverratio
SINA_FIELD_MAP = {
    'code': 'code',              # 股票代码 (6-digit)
    'name': 'name',              # 名称
    'open': 'open',              # 开盘价
    'high': 'high',              # 最高价
    'low': 'low',                # 最低价
    'trade': 'close',            # 最新价/收盘价
    'settlement': 'prev_close',  # 昨收
    'pricechange': 'change_amount',  # 涨跌额
    'changepercent': 'change_pct',   # 涨跌幅%
    'volume': 'volume',          # 成交量(股)
    'amount': 'amount',          # 成交额
    'turnoverratio': 'turnover_rate',  # 换手率%
    'per': 'pe_dynamic',         # 动态市盈率
    'pb': 'pb',                  # 市净率
    'mktcap': 'total_mv',        # 总市值(万元)
    'nmc': 'circ_mv',            # 流通市值(万元)
}

# CSV column order
CSV_COLUMNS = [
    'code', 'name', 'trade_date', 'open', 'high', 'low', 'close', 'prev_close',
    'change_amount', 'change_pct', 'amplitude', 'volume', 'amount',
    'turnover_rate', 'pe_dynamic', 'pe_ttm', 'pb', 'total_mv', 'circ_mv'
]


def fetch_all_stocks():
    """Fetch all A-share stocks via Sina Finance Market Center API.
    
    Sina returns up to 100 records per page, ~55 pages for all A-shares (~5500 stocks).
    Reliable with no aggressive rate limiting.
    Note: mktcap/nmc are in 万元 (10k yuan), converted to yuan for consistency.
    """
    PAGE_SIZE = 100
    all_data = []
    page_num = 1

    while True:
        print(f"\r  Fetching page {page_num} ({len(all_data)} records)...", end="", flush=True)
        url = (f"{SINA_URL}?page={page_num}&num={PAGE_SIZE}"
               f"&sort=symbol&asc=1&node=hs_a&symbol=&_s_r_a=auto")

        result = subprocess.run(
            ["curl", "-s", "--max-time", "15", url,
             "-H", "Referer: https://finance.sina.com.cn/",
             "-H", "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"],
            capture_output=True, text=True
        )

        if result.returncode != 0 or not result.stdout.strip():
            print(f"\n  Fetch error on page {page_num}")
            break

        try:
            records = json.loads(result.stdout)
        except json.JSONDecodeError:
            print(f"\n  JSON parse error on page {page_num}")
            break

        if not records:
            break

        all_data.extend(records)
        if len(records) < PAGE_SIZE:
            break
        page_num += 1
        time.sleep(0.5)

    print(f"\n  Total fetched: {len(all_data)} stocks")
    return all_data


def transform_row(raw, trade_date):
    """Transform Sina API row to DB row.
    
    Sina-specific conversions:
    - symbol: strip market prefix (sh/sz/bj) to get 6-digit code
    - mktcap/nmc: in 万元, convert to 元 (multiply by 10000)
    - volume: already in 股 (shares), keep as-is
    """
    row = {'trade_date': trade_date}
    for api_field, db_field in SINA_FIELD_MAP.items():
        val = raw.get(api_field, '')
        if val == '' or val is None or val == 0:
            if api_field in ('volume', 'amount'):
                val = 0
            else:
                val = ''
        row[db_field] = val

    # Convert market cap from 万元 to 元
    for mv_field in ('total_mv', 'circ_mv'):
        if row[mv_field] not in ('', None, 0):
            try:
                row[mv_field] = round(float(row[mv_field]) * 10000, 2)
            except (ValueError, TypeError):
                row[mv_field] = ''

    # Sina doesn't provide amplitude or PE_TTM, set empty
    row['amplitude'] = ''
    row['pe_ttm'] = ''

    return row


def save_csv(data, trade_date):
    """Save snapshot to CSV."""
    csv_path = os.path.join(DATA_DIR, f'daily_market_{trade_date}.csv')
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for raw in data:
            row = transform_row(raw, trade_date)
            if row['code']:  # skip empty
                writer.writerow(row)
    count = len(data)
    print(f"Saved {count} records to {csv_path}")
    return csv_path


def import_to_db(csv_path):
    """Import CSV to DuckDB. Falls back to printing SQL if DB is locked."""
    import duckdb
    db_path = os.path.abspath(os.path.join(DATA_DIR, 'a-share.db'))
    sql = f"""
        INSERT OR REPLACE INTO daily_market
        (code, name, trade_date, open, high, low, close, prev_close,
         change_amount, change_pct, amplitude, volume, amount,
         turnover_rate, pe_dynamic, pe_ttm, pb, total_mv, circ_mv)
        SELECT
            code, name,
            TRY_CAST(trade_date AS DATE),
            TRY_CAST(open AS DECIMAL(10,2)),
            TRY_CAST(high AS DECIMAL(10,2)),
            TRY_CAST(low AS DECIMAL(10,2)),
            TRY_CAST(close AS DECIMAL(10,2)),
            TRY_CAST(prev_close AS DECIMAL(10,2)),
            TRY_CAST(change_amount AS DECIMAL(10,2)),
            TRY_CAST(change_pct AS DECIMAL(10,4)),
            TRY_CAST(amplitude AS DECIMAL(10,4)),
            TRY_CAST(volume AS BIGINT),
            TRY_CAST(amount AS DECIMAL(18,2)),
            TRY_CAST(turnover_rate AS DECIMAL(10,4)),
            TRY_CAST(pe_dynamic AS DECIMAL(12,2)),
            TRY_CAST(pe_ttm AS DECIMAL(12,2)),
            TRY_CAST(pb AS DECIMAL(10,4)),
            TRY_CAST(total_mv AS DECIMAL(18,2)),
            TRY_CAST(circ_mv AS DECIMAL(18,2))
        FROM read_csv_auto('{os.path.abspath(csv_path)}', nullstr='')
    """

    try:
        conn = duckdb.connect(db_path)
        before = conn.execute("SELECT COUNT(*) FROM daily_market").fetchone()[0]
        conn.execute(sql)
        after = conn.execute("SELECT COUNT(*) FROM daily_market").fetchone()[0]
        new = after - before
        conn.close()
        print(f"DB updated: {before} → {after} records (net new: {new})")
        return new
    except Exception as e:
        if 'lock' in str(e).lower():
            print(f"\n⚠️  DB locked. CSV saved at: {os.path.abspath(csv_path)}")
            print("Import SQL:")
            print(sql)
            sql_path = csv_path.replace('.csv', '.sql')
            with open(sql_path, 'w') as f:
                f.write(sql)
            print(f"SQL saved to: {sql_path}")
            return -1
        raise


def main():
    parser = argparse.ArgumentParser(description='Daily A-share market snapshot')
    parser.add_argument('--dry-run', action='store_true', help='Fetch and save CSV only')
    parser.add_argument('--date', type=str, help='Override trade date (YYYY-MM-DD)')
    args = parser.parse_args()

    trade_date = args.date or date.today().strftime('%Y-%m-%d')

    print("=" * 60)
    print(f"Daily Market Snapshot - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Trade date: {trade_date}")
    print("=" * 60)

    # Fetch
    data = fetch_all_stocks()

    if not data:
        print("ERROR: No data fetched!")
        sys.exit(1)

    # Save CSV
    csv_path = save_csv(data, trade_date)

    # Import
    if not args.dry_run:
        import_to_db(csv_path)

    print("\nDone!")


if __name__ == "__main__":
    main()
