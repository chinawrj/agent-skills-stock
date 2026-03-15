#!/usr/bin/env python3
"""
Incremental update script for shareholders database.

Logic:
  1. Query DB for MAX(announce_date) as last_update_date (A)
  2. Use A-1 as cutoff to fetch new records from EastMoney
  3. INSERT OR REPLACE into DuckDB (upsert by code+stat_date)

Usage:
    cd /Users/rjwang/fun/a-share && source .venv/bin/activate

    # Normal incremental update
    python scripts/update_shareholders.py

    # Dry run (show what would be updated, no DB write)
    python scripts/update_shareholders.py --dry-run

    # Force cutoff date (override auto-detection)
    python scripts/update_shareholders.py --since 2026-03-07

    # Test mode: simulate last update was N days ago
    python scripts/update_shareholders.py --test-days-ago 7
"""

import asyncio
import argparse
import csv
import os
import shutil
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(__file__))

import duckdb

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'a-share.db')
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')


def get_db():
    return duckdb.connect(os.path.abspath(DB_PATH))


def backup_db():
    """Backup the database before modification."""
    src = os.path.abspath(DB_PATH)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    dst = src.replace('.db', f'_backup_{ts}.db')
    shutil.copy2(src, dst)
    size_mb = os.path.getsize(dst) / (1024 * 1024)
    print(f"Backed up: {dst} ({size_mb:.1f} MB)")
    return dst


def get_last_announce_date(conn):
    """Get the most recent announce_date from the database."""
    result = conn.execute("SELECT MAX(announce_date) FROM shareholders").fetchone()
    return result[0] if result and result[0] else None


async def fetch_incremental(cutoff_date: str, page_size: int = 500):
    """
    Fetch shareholder records with HOLD_NOTICE_DATE >= cutoff_date.
    
    Uses the API filter parameter to limit server-side results.
    Falls back to client-side filtering if server filter fails.
    """
    from browser_manager import get_browser_page

    print(f"Connecting to browser...")
    page = await get_browser_page()

    # Try server-side filter first
    print(f"Fetching records with announce_date >= {cutoff_date}...")
    
    result = await page.evaluate(f"""
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
                reportName: "RPT_HOLDERNUM_DET",
                columns: "SECURITY_CODE,SECURITY_NAME_ABBR,END_DATE,HOLD_NOTICE_DATE,HOLDER_NUM,PRE_HOLDER_NUM,HOLDER_NUM_CHANGE,HOLDER_NUM_RATIO,INTERVAL_CHRATE,AVG_MARKET_CAP,AVG_HOLD_NUM,TOTAL_MARKET_CAP,TOTAL_A_SHARES,CHANGE_SHARES,CHANGE_REASON",
                filter: "(HOLD_NOTICE_DATE>='{cutoff_date}')",
                source: "WEB",
                client: "WEB"
            }});

            const resp = await fetch(url + "?" + params.toString());
            const data = await resp.json();

            if (!data.success || !data.result || !data.result.data) {{
                // Filter might not be supported, return empty
                return {{ data: allData, pages: 0, count: 0, filterWorked: false }};
            }}

            totalPages = data.result.pages;
            allData = allData.concat(data.result.data);
            pageNumber++;
        }} while (pageNumber <= totalPages);

        return {{ data: allData, pages: totalPages, count: allData.length, filterWorked: true }};
    }}
    """)

    if result['filterWorked'] and result['count'] > 0:
        print(f"Server-side filter worked: {result['count']} records ({result['pages']} pages)")
        return result['data']

    # Fallback: fetch all sorted by announce_date desc, stop when past cutoff
    print("Server filter returned no data, falling back to pagination with client-side filter...")
    
    all_data = await page.evaluate(f"""
    async () => {{
        const url = "https://datacenter-web.eastmoney.com/api/data/v1/get";
        const cutoff = "{cutoff_date}";
        const pageSize = {page_size};
        let allData = [];
        let pageNumber = 1;
        let totalPages = 1;
        let reachedEnd = false;

        do {{
            const params = new URLSearchParams({{
                sortColumns: "HOLD_NOTICE_DATE",
                sortTypes: "-1",
                pageSize: pageSize.toString(),
                pageNumber: pageNumber.toString(),
                reportName: "RPT_HOLDERNUM_DET",
                columns: "SECURITY_CODE,SECURITY_NAME_ABBR,END_DATE,HOLD_NOTICE_DATE,HOLDER_NUM,PRE_HOLDER_NUM,HOLDER_NUM_CHANGE,HOLDER_NUM_RATIO,INTERVAL_CHRATE,AVG_MARKET_CAP,AVG_HOLD_NUM,TOTAL_MARKET_CAP,TOTAL_A_SHARES,CHANGE_SHARES,CHANGE_REASON",
                source: "WEB",
                client: "WEB"
            }});

            const resp = await fetch(url + "?" + params.toString());
            const data = await resp.json();

            if (!data.success || !data.result || !data.result.data) break;

            totalPages = data.result.pages;
            
            for (const row of data.result.data) {{
                const annDate = (row.HOLD_NOTICE_DATE || "").substring(0, 10);
                if (annDate < cutoff) {{
                    reachedEnd = true;
                    break;
                }}
                allData.push(row);
            }}

            pageNumber++;
        }} while (pageNumber <= totalPages && !reachedEnd);

        return allData;
    }}
    """)

    print(f"Client-side filter: {len(all_data)} records")
    return all_data


def save_incremental_csv(data):
    """Save incremental data to CSV."""
    csv_path = os.path.join(DATA_DIR, 'shareholders_incremental.csv')
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'code', 'name', 'stat_date', 'announce_date', 'shareholders',
            'shareholders_prev', 'change', 'change_ratio', 'range_change_pct',
            'avg_value', 'avg_shares', 'market_cap', 'total_shares',
            'shares_change', 'shares_change_reason'
        ])
        for row in data:
            end_date = (row.get('END_DATE') or '')[:10]
            ann_date = (row.get('HOLD_NOTICE_DATE') or '')[:10]
            if not end_date:
                continue
            writer.writerow([
                row.get('SECURITY_CODE', ''),
                row.get('SECURITY_NAME_ABBR', ''),
                end_date, ann_date,
                row.get('HOLDER_NUM', ''),
                row.get('PRE_HOLDER_NUM', ''),
                row.get('HOLDER_NUM_CHANGE', ''),
                row.get('HOLDER_NUM_RATIO', ''),
                row.get('INTERVAL_CHRATE', ''),
                row.get('AVG_MARKET_CAP', ''),
                row.get('AVG_HOLD_NUM', ''),
                row.get('TOTAL_MARKET_CAP', ''),
                row.get('TOTAL_A_SHARES', ''),
                row.get('CHANGE_SHARES', ''),
                row.get('CHANGE_REASON', ''),
            ])
    print(f"Saved {len(data)} records to {csv_path}")
    return csv_path


def import_to_db(csv_path, dry_run=False):
    """
    Import incremental CSV into DuckDB.
    
    Tries direct connection first. If locked (MCP server running), 
    prints the SQL for MCP import and saves it to a .sql file.
    """
    try:
        conn = get_db()
    except Exception as e:
        if 'lock' in str(e).lower():
            return _print_mcp_import_sql(csv_path, dry_run)
        raise

    # Count before
    before = conn.execute("SELECT COUNT(*) FROM shareholders").fetchone()[0]
    before_stocks = conn.execute("SELECT COUNT(DISTINCT code) FROM shareholders").fetchone()[0]

    if dry_run:
        preview = conn.execute(f"""
            SELECT COUNT(*) as new_records,
                   COUNT(DISTINCT code) as stocks_affected
            FROM read_csv_auto('{csv_path}', nullstr='')
        """).fetchone()
        print(f"\n[DRY RUN] Would process {preview[0]} records for {preview[1]} stocks")
        print(f"Current DB: {before} records, {before_stocks} stocks")
        conn.close()
        return 0

    # Import with upsert
    conn.execute(_build_import_sql(csv_path))

    # Count after
    after = conn.execute("SELECT COUNT(*) FROM shareholders").fetchone()[0]
    after_stocks = conn.execute("SELECT COUNT(DISTINCT code) FROM shareholders").fetchone()[0]
    new_records = after - before
    new_stocks = after_stocks - before_stocks

    conn.execute("""
        INSERT INTO data_updates (table_name, update_type, records_count, notes)
        VALUES ('shareholders', 'incremental', ?, ?)
    """, [new_records, f'Incremental update at {datetime.now().strftime("%Y-%m-%d %H:%M")}'])

    conn.close()

    print(f"\nDB updated:")
    print(f"  Before: {before} records ({before_stocks} stocks)")
    print(f"  After:  {after} records ({after_stocks} stocks)")
    print(f"  Net new: {new_records} records, {new_stocks} new stocks")
    return new_records


def _build_import_sql(csv_path):
    """Build the INSERT OR REPLACE SQL."""
    abs_path = os.path.abspath(csv_path)
    return f"""
        INSERT OR REPLACE INTO shareholders
        (code, name, shareholders, shareholders_prev, change, change_ratio,
         stat_date, announce_date, avg_value, avg_shares, market_cap, total_shares,
         range_change_pct, shares_change, shares_change_reason)
        SELECT
            code, name,
            TRY_CAST(shareholders AS INT),
            TRY_CAST(shareholders_prev AS INT),
            TRY_CAST(change AS INT),
            TRY_CAST(change_ratio AS DECIMAL(10,4)),
            TRY_CAST(stat_date AS DATE),
            TRY_CAST(announce_date AS DATE),
            TRY_CAST(avg_value AS DECIMAL(18,2)),
            TRY_CAST(avg_shares AS DECIMAL(18,2)),
            TRY_CAST(market_cap AS DECIMAL(18,2)),
            TRY_CAST(total_shares AS BIGINT),
            TRY_CAST(range_change_pct AS DECIMAL(10,4)),
            TRY_CAST(shares_change AS BIGINT),
            shares_change_reason
        FROM read_csv_auto('{abs_path}', nullstr='')
    """


def _print_mcp_import_sql(csv_path, dry_run=False):
    """When DB is locked by MCP, output SQL for manual/MCP import."""
    abs_path = os.path.abspath(csv_path)
    sql = _build_import_sql(csv_path).strip()

    if dry_run:
        import csv as csv_mod
        with open(csv_path, 'r') as f:
            count = sum(1 for _ in csv_mod.reader(f)) - 1
        print(f"\n[DRY RUN] CSV has {count} records ready to import")
        print(f"CSV: {abs_path}")
        return 0

    print(f"\n⚠️  DB locked by MCP server. CSV saved at: {abs_path}")
    print(f"Run this SQL via MCP tool to import:\n")
    print(sql)

    # Also save to file
    sql_path = csv_path.replace('.csv', '.sql')
    with open(sql_path, 'w') as f:
        f.write(sql)
    print(f"\nSQL saved to: {sql_path}")
    return -1


async def main():
    parser = argparse.ArgumentParser(description='Incremental shareholders DB update')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be updated without writing')
    parser.add_argument('--since', type=str, help='Override cutoff date (YYYY-MM-DD)')
    parser.add_argument('--test-days-ago', type=int, help='Simulate last update was N days ago')
    parser.add_argument('--no-backup', action='store_true', help='Skip database backup')
    args = parser.parse_args()

    print("=" * 60)
    print(f"Shareholders Incremental Update - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Database: {os.path.abspath(DB_PATH)}")
    print("=" * 60)

    # Determine cutoff date
    if args.since:
        cutoff_date = args.since
        print(f"Using explicit cutoff: {cutoff_date}")
    elif args.test_days_ago:
        cutoff_date = (datetime.now() - timedelta(days=args.test_days_ago + 1)).strftime('%Y-%m-%d')
        print(f"TEST MODE: simulating last update was {args.test_days_ago} days ago")
        print(f"Cutoff date (A-1): {cutoff_date}")
    else:
        conn = get_db()
        last_date = get_last_announce_date(conn)
        conn.close()
        if last_date is None:
            print("ERROR: No existing data in DB. Run full import first.")
            sys.exit(1)
        # A-1: go back one day from last announce date
        cutoff_date = (last_date - timedelta(days=1)).strftime('%Y-%m-%d')
        print(f"Last announce_date in DB (A): {last_date}")
        print(f"Cutoff date (A-1): {cutoff_date}")

    # Backup
    if not args.no_backup and not args.dry_run:
        backup_db()

    # Fetch incremental data
    data = await fetch_incremental(cutoff_date)

    if not data:
        print("\nNo new records found. Database is up to date.")
        return

    print(f"\nFetched {len(data)} records")

    # Show sample
    sample = data[:3]
    for r in sample:
        code = r.get('SECURITY_CODE', '')
        name = r.get('SECURITY_NAME_ABBR', '')
        ann = (r.get('HOLD_NOTICE_DATE') or '')[:10]
        stat = (r.get('END_DATE') or '')[:10]
        print(f"  {code} {name} stat={stat} announce={ann}")
    if len(data) > 3:
        print(f"  ... and {len(data) - 3} more")

    # Save CSV
    csv_path = save_incremental_csv(data)

    # Import to DB
    import_to_db(csv_path, dry_run=args.dry_run)

    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
