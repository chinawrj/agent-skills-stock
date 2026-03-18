#!/usr/bin/env python3
"""
Build A-share stock database in DuckDB.

Fetches ALL A-share stocks and their shareholder data from EastMoney
via Playwright browser, then imports everything into DuckDB.

Usage:
    cd /Users/rjwang/fun/a-share && source .venv/bin/activate
    python scripts/build_stock_db.py          # Full build (stocks + shareholders)
    python scripts/build_stock_db.py --step 1 # Only stocks table
    python scripts/build_stock_db.py --step 2 # Only shareholders table
"""

import asyncio
import sys
import os
import argparse
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))

import duckdb

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'a-share.db')


def get_market(code):
    if code.startswith('6'):
        return 'SH'
    elif code.startswith(('0', '3')):
        return 'SZ'
    elif code.startswith('4') or code.startswith('8'):
        return 'BJ'
    return 'OTHER'


def get_db():
    return duckdb.connect(os.path.abspath(DB_PATH))


def ensure_tables(conn):
    """Ensure all required tables exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stocks (
            code VARCHAR PRIMARY KEY,
            name VARCHAR,
            market VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # shareholders table should already exist from init_db.sql
    conn.execute("""
        CREATE TABLE IF NOT EXISTS shareholders (
            code VARCHAR NOT NULL,
            name VARCHAR,
            shareholders INT,
            shareholders_prev INT,
            change INT,
            change_ratio DECIMAL(10,4),
            price DECIMAL(10,2),
            change_pct DECIMAL(10,2),
            stat_date DATE,
            announce_date DATE,
            avg_value DECIMAL(18,2),
            avg_shares DECIMAL(18,2),
            market_cap DECIMAL(18,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            range_change_pct DECIMAL(10,4),
            total_shares BIGINT,
            shares_change BIGINT,
            shares_change_reason VARCHAR,
            PRIMARY KEY (code, stat_date)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shareholders_announce ON shareholders(announce_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shareholders_change ON shareholders(change_ratio)")


async def fetch_all_shareholder_data(page_size=500):
    """Fetch ALL shareholder data from EastMoney via Playwright browser."""
    from browser_manager import get_browser_page

    print("Connecting to browser...")
    page = await get_browser_page()

    print("Fetching shareholder data from EastMoney API...")
    js_code = f"""
    async () => {{
        const url = "https://datacenter-web.eastmoney.com/api/data/v1/get";
        const pageSize = {page_size};
        let allData = [];
        let pageNumber = 1;
        let totalPages = 1;

        do {{
            const params = new URLSearchParams({{
                sortColumns: "HOLD_NOTICE_DATE,SECURITY_CODE",
                sortTypes: "-1,-1",
                pageSize: pageSize.toString(),
                pageNumber: pageNumber.toString(),
                reportName: "RPT_HOLDERNUMLATEST",
                columns: "ALL",
                source: "WEB",
                client: "WEB"
            }});

            const resp = await fetch(url + "?" + params.toString());
            const data = await resp.json();

            if (!data.success) {{
                throw new Error(data.message || "API error");
            }}

            totalPages = data.result.pages;
            allData = allData.concat(data.result.data);
            pageNumber++;
        }} while (pageNumber <= totalPages);

        return allData;
    }}
    """

    data = await page.evaluate(js_code)
    print(f"Fetched {len(data)} stocks (across {len(data)//page_size + 1} pages)")
    return data


def step1_import_stocks(raw_data):
    """Import stock list into 'stocks' table from EastMoney data."""
    print("\n" + "=" * 60)
    print("Step 1: Importing stock list into 'stocks' table")
    print("=" * 60)

    conn = get_db()
    ensure_tables(conn)

    # Extract unique stocks
    stocks = {}
    for row in raw_data:
        code = row.get('SECURITY_CODE', '')
        name = row.get('SECURITY_NAME_ABBR', '')
        if code and code not in stocks:
            stocks[code] = name

    print(f"Unique stocks found: {len(stocks)}")

    # Clear and insert
    conn.execute("DELETE FROM stocks")
    rows = [(code, name, get_market(code)) for code, name in stocks.items()]
    conn.executemany(
        "INSERT INTO stocks (code, name, market) VALUES (?, ?, ?)",
        rows
    )

    count = conn.execute("SELECT COUNT(*) FROM stocks").fetchone()[0]
    sh = conn.execute("SELECT COUNT(*) FROM stocks WHERE market='SH'").fetchone()[0]
    sz = conn.execute("SELECT COUNT(*) FROM stocks WHERE market='SZ'").fetchone()[0]
    bj = conn.execute("SELECT COUNT(*) FROM stocks WHERE market='BJ'").fetchone()[0]
    print(f"Inserted {count} stocks (SH={sh}, SZ={sz}, BJ={bj})")

    conn.execute("""
        INSERT INTO data_updates (table_name, update_type, records_count, notes)
        VALUES ('stocks', 'full', ?, ?)
    """, [count, f'Full import from EastMoney at {datetime.now().strftime("%Y-%m-%d %H:%M")}'])

    conn.close()
    return count


def step2_import_shareholders(raw_data):
    """Import shareholder data into 'shareholders' table."""
    print("\n" + "=" * 60)
    print("Step 2: Importing shareholder data into 'shareholders' table")
    print("=" * 60)

    conn = get_db()
    ensure_tables(conn)

    inserted = 0
    skipped = 0

    for row in raw_data:
        code = row.get('SECURITY_CODE', '')
        if not code:
            skipped += 1
            continue

        # Parse dates
        stat_date = None
        end_date_str = row.get('END_DATE', '')
        if end_date_str:
            stat_date = end_date_str[:10]  # "2026-01-31 00:00:00" -> "2026-01-31"

        announce_date = None
        ann_str = row.get('HOLD_NOTICE_DATE', '')
        if ann_str:
            announce_date = ann_str[:10]

        if not stat_date:
            skipped += 1
            continue

        name = row.get('SECURITY_NAME_ABBR', '')
        holder_num = row.get('HOLDER_NUM')
        pre_holder_num = row.get('PRE_HOLDER_NUM')
        holder_change = row.get('HOLDER_NUM_CHANGE')
        holder_ratio = row.get('HOLDER_NUM_RATIO')
        new_price = row.get('NEW_PRICE')
        change_rate = row.get('CHANGE_RATE')
        avg_market_cap = row.get('AVG_MARKET_CAP')
        avg_hold_num = row.get('AVG_HOLD_NUM')
        total_market_cap = row.get('TOTAL_MARKET_CAP')
        total_a_shares = row.get('TOTAL_A_SHARES')
        hold_ratio_change = row.get('HOLD_RATIO_CHANGE')

        try:
            conn.execute("""
                INSERT OR REPLACE INTO shareholders
                (code, name, shareholders, shareholders_prev, change, change_ratio,
                 price, change_pct, stat_date, announce_date, avg_value, avg_shares,
                 market_cap, total_shares, range_change_pct)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                code, name, holder_num, pre_holder_num, holder_change, holder_ratio,
                new_price, change_rate, stat_date, announce_date, avg_market_cap,
                avg_hold_num, total_market_cap, total_a_shares, hold_ratio_change
            ])
            inserted += 1
        except Exception as e:
            print(f"  Error inserting {code}: {e}")
            skipped += 1

    total = conn.execute("SELECT COUNT(*) FROM shareholders").fetchone()[0]
    unique = conn.execute("SELECT COUNT(DISTINCT code) FROM shareholders").fetchone()[0]

    print(f"Inserted/updated: {inserted}, Skipped: {skipped}")
    print(f"Total records in shareholders table: {total}")
    print(f"Unique stocks with shareholder data: {unique}")

    conn.execute("""
        INSERT INTO data_updates (table_name, update_type, records_count, notes)
        VALUES ('shareholders', 'full', ?, ?)
    """, [inserted, f'Bulk import from EastMoney at {datetime.now().strftime("%Y-%m-%d %H:%M")}'])

    conn.close()
    return inserted


def save_to_csv(raw_data):
    """Save fetched data to CSV files for DuckDB import."""
    import csv
    base_dir = os.path.join(os.path.dirname(__file__), '..', 'data')

    # Save stocks CSV
    stocks_csv = os.path.join(base_dir, 'all_stocks.csv')
    stocks = {}
    for row in raw_data:
        code = row.get('SECURITY_CODE', '')
        name = row.get('SECURITY_NAME_ABBR', '')
        if code and code not in stocks:
            stocks[code] = name

    with open(stocks_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['code', 'name', 'market'])
        for code, name in sorted(stocks.items()):
            writer.writerow([code, name, get_market(code)])
    print(f"Saved {len(stocks)} stocks to {stocks_csv}")

    # Save shareholders CSV
    sh_csv = os.path.join(base_dir, 'all_shareholders.csv')
    with open(sh_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'code', 'name', 'shareholders', 'shareholders_prev', 'change',
            'change_ratio', 'price', 'change_pct', 'stat_date', 'announce_date',
            'avg_value', 'avg_shares', 'market_cap', 'total_shares', 'range_change_pct'
        ])
        count = 0
        for row in raw_data:
            code = row.get('SECURITY_CODE', '')
            end_date_str = row.get('END_DATE', '')
            if not code or not end_date_str:
                continue
            stat_date = end_date_str[:10]
            ann_str = row.get('HOLD_NOTICE_DATE', '')
            announce_date = ann_str[:10] if ann_str else ''

            writer.writerow([
                code,
                row.get('SECURITY_NAME_ABBR', ''),
                row.get('HOLDER_NUM', ''),
                row.get('PRE_HOLDER_NUM', ''),
                row.get('HOLDER_NUM_CHANGE', ''),
                row.get('HOLDER_NUM_RATIO', ''),
                row.get('NEW_PRICE', ''),
                row.get('CHANGE_RATE', ''),
                stat_date,
                announce_date,
                row.get('AVG_MARKET_CAP', ''),
                row.get('AVG_HOLD_NUM', ''),
                row.get('TOTAL_MARKET_CAP', ''),
                row.get('TOTAL_A_SHARES', ''),
                row.get('HOLD_RATIO_CHANGE', '')
            ])
            count += 1
    print(f"Saved {count} shareholder records to {sh_csv}")
    return stocks_csv, sh_csv


async def main():
    parser = argparse.ArgumentParser(description='Build A-share stock database')
    parser.add_argument('--step', type=int, choices=[1, 2], help='Run only step 1 or 2')
    parser.add_argument('--csv-only', action='store_true', help='Only save to CSV (skip DB import)')
    args = parser.parse_args()

    print("=" * 60)
    print(f"A-Share Database Builder - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Database: {os.path.abspath(DB_PATH)}")
    print("=" * 60)

    # Fetch all data from EastMoney via Playwright
    raw_data = await fetch_all_shareholder_data()

    # Always save to CSV first
    stocks_csv, sh_csv = save_to_csv(raw_data)

    if args.csv_only:
        print("\nCSV files saved. Use DuckDB to import:")
        print(f"  COPY stocks FROM '{os.path.abspath(stocks_csv)}' (HEADER);")
        print(f"  COPY shareholders FROM '{os.path.abspath(sh_csv)}' (HEADER);")
        return

    # Try direct DB import
    try:
        if args.step is None or args.step == 1:
            step1_import_stocks(raw_data)
        if args.step is None or args.step == 2:
            step2_import_shareholders(raw_data)

        print("\n" + "=" * 60)
        print("Database build completed!")
        print("=" * 60)

        conn = get_db()
        stocks_count = conn.execute("SELECT COUNT(*) FROM stocks").fetchone()[0]
        sh_count = conn.execute("SELECT COUNT(*) FROM shareholders").fetchone()[0]
        sh_unique = conn.execute("SELECT COUNT(DISTINCT code) FROM shareholders").fetchone()[0]
        print(f"  stocks table: {stocks_count} records")
        print(f"  shareholders table: {sh_count} records ({sh_unique} unique stocks)")
        conn.close()
    except Exception as e:
        print(f"\nDirect DB import failed: {e}")
        print("CSV files are saved. Import manually with DuckDB MCP or CLI.")


if __name__ == "__main__":
    asyncio.run(main())
