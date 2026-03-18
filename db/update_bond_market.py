#!/usr/bin/env python3
"""
Update bond market data: real-time prices, convert_value, premium_rate, ytm, remaining_size.

Data source: Eastmoney datacenter API (RPT_BOND_CB_LIST).
This fills the critical gap for 卡书框架 MUST-HAVE #1: 到期正收益.

Usage:
    cd /Users/rjwang/fun/a-share && source .venv/bin/activate

    # Update all bond market data
    python db/update_bond_market.py

    # Dry run (fetch + save CSV, don't import to DB)
    python db/update_bond_market.py --dry-run
"""

import argparse
import csv
import os
import re
import sys
import time
from datetime import datetime

import requests

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
DB_PATH = os.path.abspath(os.path.join(DATA_DIR, 'a-share.db'))
CSV_PATH = os.path.join(DATA_DIR, 'bond_market.csv')

EASTMONEY_URL = 'https://datacenter-web.eastmoney.com/api/data/v1/get'


def fetch_bond_data():
    """Fetch all bond data from Eastmoney datacenter API.

    Uses quoteColumns with f2~10 for real-time bond price.
    Returns list of dicts with standardized field names.
    """
    params = {
        'sortColumns': 'PUBLIC_START_DATE',
        'sortTypes': '-1',
        'pageSize': '500',
        'pageNumber': '1',
        'reportName': 'RPT_BOND_CB_LIST',
        'columns': (
            'SECURITY_CODE,SECURITY_NAME_ABBR,CONVERT_STOCK_CODE,'
            'ACTUAL_ISSUE_SCALE,RATING,EXPIRE_DATE,'
            'TRANSFER_PRICE,INITIAL_TRANSFER_PRICE,'
            'REDEEM_CLAUSE,INTEREST_RATE_EXPLAIN,'
            'LISTING_DATE,DELIST_DATE,'
            'REDEEM_TRIG_PRICE,RESALE_TRIG_PRICE'
        ),
        'quoteColumns': (
            'f2~01~CONVERT_STOCK_CODE~CONVERT_STOCK_PRICE,'
            'f2~10~SECURITY_CODE~BOND_PRICE,'
            'f235~10~SECURITY_CODE~TRANSFER_PRICE'
        ),
        'quoteType': '0',
        'source': 'WEB',
        'client': 'WEB',
    }

    all_results = []
    page = 1

    while True:
        params['pageNumber'] = str(page)
        print(f"\r  Fetching page {page}...", end="", flush=True)

        resp = requests.get(EASTMONEY_URL, params=params, timeout=15)
        data = resp.json()

        if not data.get('success'):
            print(f"\n  API error: {data.get('message')}")
            break

        results = data['result']['data']
        all_results.extend(results)

        total_pages = data['result']['pages']
        if page >= total_pages:
            break
        page += 1
        time.sleep(0.3)

    print(f"\n  Total fetched: {len(all_results)} bonds")
    return all_results


def parse_maturity_value(redeem_clause):
    """Extract maturity redemption % from clause (e.g., '面值的115%' → 115.0)."""
    if not redeem_clause:
        return None
    m = re.search(r'到期后.*?面值的?(\d+(?:\.\d+)?)%', redeem_clause)
    if m:
        return float(m.group(1))
    m = re.search(r'面值的?(\d+(?:\.\d+)?)%', redeem_clause)
    if m:
        return float(m.group(1))
    return None


def parse_coupon_rates(text):
    """Parse INTEREST_RATE_EXPLAIN → list of floats.
    e.g. '第一年0.3%、第二年0.5%...第六年2.5%' → [0.3, 0.5, 1.0, 1.5, 2.0, 2.5]
    """
    if not text:
        return []
    return [float(x) for x in re.findall(r'(\d+(?:\.\d+)?)%', text)]


def calc_ytm(bond_price, maturity_val, coupon_rates, expire_date_str):
    """Calculate YTM considering coupon payments."""
    if not bond_price or bond_price <= 0 or not maturity_val or not expire_date_str:
        return None
    try:
        exp = datetime.strptime(expire_date_str, '%Y-%m-%d')
        remain_years = (exp - datetime.now()).days / 365.25
        if remain_years <= 0.05:
            return None
        total_years = len(coupon_rates) if coupon_rates else 6
        elapsed_years = total_years - remain_years
        remaining_coupons = 0.0
        for i, rate in enumerate(coupon_rates):
            year_num = i + 1
            if year_num > elapsed_years and year_num < total_years:
                remaining_coupons += rate
        total_return = maturity_val + remaining_coupons
        ytm = round((total_return / bond_price - 1) / remain_years * 100, 4)
        return ytm
    except (ValueError, TypeError):
        return None


def transform_row(raw):
    """Transform API row to DB-ready dict."""
    code = raw.get('SECURITY_CODE', '')
    if not code:
        return None

    bond_price = raw.get('BOND_PRICE')
    try:
        bond_price = float(bond_price) if bond_price else None
    except (ValueError, TypeError):
        bond_price = None

    stock_price = raw.get('CONVERT_STOCK_PRICE')
    try:
        stock_price = float(stock_price) if stock_price else None
    except (ValueError, TypeError):
        stock_price = None

    transfer_price = raw.get('TRANSFER_PRICE') or raw.get('INITIAL_TRANSFER_PRICE')
    try:
        transfer_price = float(transfer_price) if transfer_price else None
    except (ValueError, TypeError):
        transfer_price = None

    # Compute convert_value
    convert_value = None
    if stock_price and transfer_price and transfer_price > 0:
        convert_value = round(stock_price / transfer_price * 100, 3)

    # Compute premium_rate
    premium_rate = None
    if bond_price and convert_value and convert_value > 0:
        premium_rate = round((bond_price / convert_value - 1) * 100, 4)

    # Compute YTM with coupon rates
    maturity_val = parse_maturity_value(raw.get('REDEEM_CLAUSE', ''))
    coupon_rates = parse_coupon_rates(raw.get('INTEREST_RATE_EXPLAIN', ''))
    expire_date = str(raw.get('EXPIRE_DATE', ''))[:10]
    ytm = calc_ytm(bond_price, maturity_val, coupon_rates, expire_date)

    remaining_size = raw.get('ACTUAL_ISSUE_SCALE')
    try:
        remaining_size = float(remaining_size) if remaining_size else None
    except (ValueError, TypeError):
        remaining_size = None

    result = {
        'bond_code': code,
        'bond_price': bond_price,
        'convert_value': convert_value,
        'premium_rate': premium_rate,
        'ytm': ytm,
        'remaining_size': remaining_size,
        'convert_price': transfer_price,
        'maturity_redemption_price': maturity_val,
    }
    for i in range(10):
        result[f'coupon_rate_{i+1}'] = coupon_rates[i] if i < len(coupon_rates) else None
    return result


CSV_COLUMNS = ['bond_code', 'bond_price', 'convert_value', 'premium_rate', 'ytm',
               'remaining_size', 'convert_price', 'maturity_redemption_price',
               'coupon_rate_1', 'coupon_rate_2', 'coupon_rate_3',
               'coupon_rate_4', 'coupon_rate_5', 'coupon_rate_6',
               'coupon_rate_7', 'coupon_rate_8', 'coupon_rate_9',
               'coupon_rate_10']


def save_csv(rows):
    """Save bond market data to CSV."""
    with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print(f"  Saved {len(rows)} records to {CSV_PATH}")
    return CSV_PATH


def import_to_db(csv_path):
    """Import bond market CSV to DuckDB, updating existing rows."""
    import duckdb

    sql = f"""
        UPDATE bonds SET
            bond_price = src.bond_price,
            convert_value = src.convert_value,
            premium_rate = src.premium_rate,
            ytm = src.ytm,
            remaining_size = src.remaining_size,
            convert_price = COALESCE(src.convert_price, bonds.convert_price),
            maturity_redemption_price = COALESCE(src.maturity_redemption_price, bonds.maturity_redemption_price),
            coupon_rate_1 = COALESCE(src.coupon_rate_1, bonds.coupon_rate_1),
            coupon_rate_2 = COALESCE(src.coupon_rate_2, bonds.coupon_rate_2),
            coupon_rate_3 = COALESCE(src.coupon_rate_3, bonds.coupon_rate_3),
            coupon_rate_4 = COALESCE(src.coupon_rate_4, bonds.coupon_rate_4),
            coupon_rate_5 = COALESCE(src.coupon_rate_5, bonds.coupon_rate_5),
            coupon_rate_6 = COALESCE(src.coupon_rate_6, bonds.coupon_rate_6),
            coupon_rate_7 = COALESCE(src.coupon_rate_7, bonds.coupon_rate_7),
            coupon_rate_8 = COALESCE(src.coupon_rate_8, bonds.coupon_rate_8),
            coupon_rate_9 = COALESCE(src.coupon_rate_9, bonds.coupon_rate_9),
            coupon_rate_10 = COALESCE(src.coupon_rate_10, bonds.coupon_rate_10),
            updated_at = CURRENT_TIMESTAMP
        FROM (
            SELECT
                bond_code,
                TRY_CAST(bond_price AS DECIMAL(10,3)) AS bond_price,
                TRY_CAST(convert_value AS DECIMAL(10,3)) AS convert_value,
                TRY_CAST(premium_rate AS DECIMAL(10,4)) AS premium_rate,
                TRY_CAST(ytm AS DECIMAL(10,4)) AS ytm,
                TRY_CAST(remaining_size AS DECIMAL(12,4)) AS remaining_size,
                TRY_CAST(convert_price AS DECIMAL(10,3)) AS convert_price,
                TRY_CAST(maturity_redemption_price AS DECIMAL(10,3)) AS maturity_redemption_price,
                TRY_CAST(coupon_rate_1 AS DECIMAL(6,3)) AS coupon_rate_1,
                TRY_CAST(coupon_rate_2 AS DECIMAL(6,3)) AS coupon_rate_2,
                TRY_CAST(coupon_rate_3 AS DECIMAL(6,3)) AS coupon_rate_3,
                TRY_CAST(coupon_rate_4 AS DECIMAL(6,3)) AS coupon_rate_4,
                TRY_CAST(coupon_rate_5 AS DECIMAL(6,3)) AS coupon_rate_5,
                TRY_CAST(coupon_rate_6 AS DECIMAL(6,3)) AS coupon_rate_6,
                TRY_CAST(coupon_rate_7 AS DECIMAL(6,3)) AS coupon_rate_7,
                TRY_CAST(coupon_rate_8 AS DECIMAL(6,3)) AS coupon_rate_8,
                TRY_CAST(coupon_rate_9 AS DECIMAL(6,3)) AS coupon_rate_9,
                TRY_CAST(coupon_rate_10 AS DECIMAL(6,3)) AS coupon_rate_10
            FROM read_csv_auto('{os.path.abspath(csv_path)}', nullstr='')
        ) AS src
        WHERE bonds.bond_code = src.bond_code
    """

    try:
        conn = duckdb.connect(DB_PATH)
        # Check before
        before = conn.execute(
            "SELECT COUNT(*) FROM bonds WHERE bond_price IS NOT NULL AND bond_price != 100"
        ).fetchone()[0]

        conn.execute(sql)

        # Check after
        after = conn.execute(
            "SELECT COUNT(*) FROM bonds WHERE bond_price IS NOT NULL AND bond_price != 100"
        ).fetchone()[0]
        total = conn.execute("SELECT COUNT(*) FROM bonds").fetchone()[0]

        # Log update
        conn.execute("""
            INSERT INTO data_updates (table_name, update_type, records_count, notes)
            VALUES ('bonds', 'market_update', ?, 'bond_price/cv/premium/ytm/remaining updated')
        """, [after])

        conn.close()
        print(f"  DB updated: bonds with real prices {before} → {after} / {total} total")
        return after
    except Exception as e:
        if 'lock' in str(e).lower():
            print(f"\n⚠️  DB locked. CSV saved at: {os.path.abspath(csv_path)}")
            sql_path = csv_path.replace('.csv', '.sql')
            with open(sql_path, 'w') as f:
                f.write(sql)
            print(f"  SQL saved to: {sql_path}")
            return -1
        raise


def main():
    parser = argparse.ArgumentParser(description='Update bond market data')
    parser.add_argument('--dry-run', action='store_true', help='Fetch and save CSV only')
    args = parser.parse_args()

    print("=" * 60)
    print(f"Bond Market Update - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    # Fetch
    raw_data = fetch_bond_data()
    if not raw_data:
        print("ERROR: No data fetched!")
        sys.exit(1)

    # Transform
    rows = []
    for raw in raw_data:
        row = transform_row(raw)
        if row:
            rows.append(row)
    print(f"  Transformed: {len(rows)} bonds")

    # Stats
    with_price = sum(1 for r in rows if r['bond_price'])
    with_cv = sum(1 for r in rows if r['convert_value'])
    with_ytm = sum(1 for r in rows if r['ytm'])
    with_size = sum(1 for r in rows if r['remaining_size'])
    print(f"  With bond_price: {with_price}")
    print(f"  With convert_value: {with_cv}")
    print(f"  With ytm: {with_ytm}")
    print(f"  With remaining_size: {with_size}")

    # Save CSV
    csv_path = save_csv(rows)

    # Import
    if not args.dry_run:
        import_to_db(csv_path)

    print("\nDone!")


if __name__ == "__main__":
    main()
