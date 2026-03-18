#!/usr/bin/env python3
"""
Rebuild the entire A-share database from scratch using curl (no Playwright required).

Steps:
  1. Fetch all stocks from EastMoney RPT_HOLDERNUMLATEST → CSV
  2. Fetch all shareholder history from RPT_HOLDERNUM_DET → CSV
  3. Fetch daily market snapshot from Sina Finance → CSV
  4. Fetch listing dates from RPT_F10_BASIC_ORGINFO → CSV

All data is saved as CSV files. Import SQL is generated for each step.
Run SQL via MCP DuckDB server.

Usage:
    python scripts/rebuild_db.py --step 1   # Stocks only
    python scripts/rebuild_db.py --step 2   # Shareholders only
    python scripts/rebuild_db.py --step 3   # Daily market only
    python scripts/rebuild_db.py --step 4   # Listing dates only
    python scripts/rebuild_db.py             # All steps
"""

import argparse
import csv
import json
import os
import subprocess
import sys
import time
from datetime import date, datetime

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
EASTMONEY_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
SINA_URL = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData"


def curl_json(url, max_retries=3):
    """Fetch JSON via curl with retries."""
    for attempt in range(max_retries):
        result = subprocess.run(
            ["curl", "-s", "--max-time", "30", url,
             "-H", "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"],
            capture_output=True, text=True
        )
        if result.returncode == 0 and result.stdout.strip():
            try:
                return json.loads(result.stdout)
            except json.JSONDecodeError:
                pass
        if attempt < max_retries - 1:
            time.sleep(1)
    return None


# =============================================================================
# Step 1: Fetch all stocks from EastMoney
# =============================================================================
def step1_fetch_stocks():
    """Fetch stock list from EastMoney RPT_HOLDERNUMLATEST."""
    print("\n" + "=" * 60)
    print("Step 1: Fetching all A-share stocks from EastMoney")
    print("=" * 60)

    PAGE_SIZE = 500
    all_stocks = {}
    page = 1

    while True:
        print(f"\r  Fetching page {page} ({len(all_stocks)} stocks)...", end="", flush=True)
        params = (
            f"sortColumns=SECURITY_CODE&sortTypes=1"
            f"&pageSize={PAGE_SIZE}&pageNumber={page}"
            f"&reportName=RPT_HOLDERNUMLATEST"
            f"&columns=SECURITY_CODE,SECURITY_NAME_ABBR"
            f"&source=WEB&client=WEB"
        )
        url = f"{EASTMONEY_URL}?{params}"
        resp = curl_json(url)

        if not resp or not resp.get("success"):
            print(f"\n  API error on page {page}")
            break

        data = resp["result"]["data"]
        if not data:
            break

        for row in data:
            code = row.get("SECURITY_CODE", "")
            name = row.get("SECURITY_NAME_ABBR", "")
            if code and code not in all_stocks:
                all_stocks[code] = name

        total = resp["result"]["count"]
        if len(all_stocks) >= total or len(data) < PAGE_SIZE:
            break
        page += 1
        time.sleep(0.3)

    print(f"\n  Total unique stocks: {len(all_stocks)}")

    # Save CSV
    csv_path = os.path.join(DATA_DIR, 'rebuild_stocks.csv')
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['code', 'name', 'market'])
        for code, name in sorted(all_stocks.items()):
            if code.startswith('6'):
                market = 'SH'
            elif code.startswith(('0', '3')):
                market = 'SZ'
            elif code.startswith(('4', '8')):
                market = 'BJ'
            else:
                market = 'OTHER'
            writer.writerow([code, name, market])

    print(f"  Saved to {csv_path}")
    return csv_path, len(all_stocks)


# =============================================================================
# Step 2: Fetch all shareholder history
# =============================================================================
def step2_fetch_shareholders():
    """Fetch ALL historical shareholder records from EastMoney RPT_HOLDERNUM_DET."""
    print("\n" + "=" * 60)
    print("Step 2: Fetching all shareholder history from EastMoney")
    print("=" * 60)

    PAGE_SIZE = 500

    # First get total count
    params = (
        f"sortColumns=END_DATE&sortTypes=-1"
        f"&pageSize=1&pageNumber=1"
        f"&reportName=RPT_HOLDERNUM_DET"
        f"&columns=SECURITY_CODE"
        f"&source=WEB&client=WEB"
    )
    resp = curl_json(f"{EASTMONEY_URL}?{params}")
    if not resp or not resp.get("success"):
        print("  ERROR: Cannot get record count!")
        return None, 0

    total_count = resp["result"]["count"]
    total_pages = resp["result"]["pages"]
    print(f"  Total records: {total_count}, Pages: {total_pages}")

    # Fetch all pages
    csv_path = os.path.join(DATA_DIR, 'rebuild_shareholders.csv')
    columns = "SECURITY_CODE,SECURITY_NAME_ABBR,END_DATE,HOLD_NOTICE_DATE,HOLDER_NUM,PRE_HOLDER_NUM,HOLDER_NUM_CHANGE,HOLDER_NUM_RATIO,INTERVAL_CHRATE,AVG_MARKET_CAP,AVG_HOLD_NUM,TOTAL_MARKET_CAP,TOTAL_A_SHARES,CHANGE_SHARES,CHANGE_REASON"

    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'code', 'name', 'stat_date', 'announce_date', 'shareholders',
            'shareholders_prev', 'change', 'change_ratio', 'range_change_pct',
            'avg_value', 'avg_shares', 'market_cap', 'total_shares',
            'shares_change', 'shares_change_reason'
        ])

        total_written = 0
        failed_pages = []

        for pg in range(1, total_pages + 1):
            params = (
                f"sortColumns=END_DATE&sortTypes=-1"
                f"&pageSize={PAGE_SIZE}&pageNumber={pg}"
                f"&reportName=RPT_HOLDERNUM_DET"
                f"&columns={columns}"
                f"&source=WEB&client=WEB"
            )
            url = f"{EASTMONEY_URL}?{params}"
            resp = curl_json(url, max_retries=3)

            if not resp or not resp.get("success") or not resp["result"].get("data"):
                print(f"\n  FAILED page {pg}")
                failed_pages.append(pg)
                time.sleep(1)
                continue

            for row in resp["result"]["data"]:
                end_date = (row.get('END_DATE') or '')[:10]
                ann_date = (row.get('HOLD_NOTICE_DATE') or '')[:10]
                writer.writerow([
                    row.get('SECURITY_CODE', ''),
                    row.get('SECURITY_NAME_ABBR', ''),
                    end_date,
                    ann_date,
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
                total_written += 1

            if pg % 50 == 0 or pg == total_pages:
                print(f"  Page {pg}/{total_pages} — {total_written} records", flush=True)
                f.flush()

            # Light throttle to avoid rate limiting
            if pg % 10 == 0:
                time.sleep(0.5)
            else:
                time.sleep(0.15)

    # Retry failed pages
    if failed_pages:
        print(f"\n  Retrying {len(failed_pages)} failed pages...")
        with open(csv_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for pg in failed_pages:
                time.sleep(2)
                params = (
                    f"sortColumns=END_DATE&sortTypes=-1"
                    f"&pageSize={PAGE_SIZE}&pageNumber={pg}"
                    f"&reportName=RPT_HOLDERNUM_DET"
                    f"&columns={columns}"
                    f"&source=WEB&client=WEB"
                )
                resp = curl_json(f"{EASTMONEY_URL}?{params}", max_retries=3)
                if resp and resp.get("success") and resp["result"].get("data"):
                    for row in resp["result"]["data"]:
                        end_date = (row.get('END_DATE') or '')[:10]
                        ann_date = (row.get('HOLD_NOTICE_DATE') or '')[:10]
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
                        total_written += 1
                    print(f"    Page {pg} retry OK")
                else:
                    print(f"    Page {pg} still FAILED")

    print(f"\n  Total: {total_written} records saved to {csv_path}")
    return csv_path, total_written


# =============================================================================
# Step 3: Fetch daily market snapshot
# =============================================================================
def step3_fetch_daily_market():
    """Fetch daily market data from Sina Finance."""
    print("\n" + "=" * 60)
    print("Step 3: Fetching daily market snapshot from Sina Finance")
    print("=" * 60)

    trade_date = date.today().strftime('%Y-%m-%d')
    PAGE_SIZE = 100
    all_data = []
    page_num = 1

    while True:
        print(f"\r  Fetching page {page_num} ({len(all_data)} stocks)...", end="", flush=True)
        url = (f"{SINA_URL}?page={page_num}&num={PAGE_SIZE}"
               f"&sort=symbol&asc=1&node=hs_a&symbol=&_s_r_a=auto")

        result = subprocess.run(
            ["curl", "-s", "--max-time", "15", url,
             "-H", "Referer: https://finance.sina.com.cn/",
             "-H", "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"],
            capture_output=True, text=True
        )

        if result.returncode != 0 or not result.stdout.strip():
            break

        try:
            records = json.loads(result.stdout)
        except json.JSONDecodeError:
            break

        if not records:
            break

        all_data.extend(records)
        if len(records) < PAGE_SIZE:
            break
        page_num += 1
        time.sleep(0.5)

    print(f"\n  Total fetched: {len(all_data)} stocks")

    if not all_data:
        print("  WARNING: No data (market may be closed today)")
        return None, 0

    # Save CSV
    csv_fields = [
        'code', 'name', 'trade_date', 'open', 'high', 'low', 'close', 'prev_close',
        'change_amount', 'change_pct', 'amplitude', 'volume', 'amount',
        'turnover_rate', 'pe_dynamic', 'pe_ttm', 'pb', 'total_mv', 'circ_mv'
    ]
    field_map = {
        'code': 'code', 'name': 'name', 'open': 'open', 'high': 'high',
        'low': 'low', 'trade': 'close', 'settlement': 'prev_close',
        'pricechange': 'change_amount', 'changepercent': 'change_pct',
        'volume': 'volume', 'amount': 'amount',
        'turnoverratio': 'turnover_rate', 'per': 'pe_dynamic', 'pb': 'pb',
        'mktcap': 'total_mv', 'nmc': 'circ_mv',
    }

    csv_path = os.path.join(DATA_DIR, f'rebuild_daily_market_{trade_date}.csv')
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields)
        writer.writeheader()
        for raw in all_data:
            row = {'trade_date': trade_date, 'amplitude': '', 'pe_ttm': ''}
            for api_f, db_f in field_map.items():
                val = raw.get(api_f, '')
                if val == '' or val is None:
                    val = ''
                row[db_f] = val
            # Convert mktcap/nmc from 万元 to 元
            for mv_f in ('total_mv', 'circ_mv'):
                if row[mv_f] not in ('', None, 0):
                    try:
                        row[mv_f] = round(float(row[mv_f]) * 10000, 2)
                    except (ValueError, TypeError):
                        row[mv_f] = ''
            if row['code']:
                writer.writerow(row)

    print(f"  Saved to {csv_path}")
    return csv_path, len(all_data)


# =============================================================================
# Step 4: Fetch listing dates
# =============================================================================
def step4_fetch_listing_dates():
    """Fetch listing dates from EastMoney RPT_F10_BASIC_ORGINFO."""
    print("\n" + "=" * 60)
    print("Step 4: Fetching listing dates from EastMoney")
    print("=" * 60)

    PAGE_SIZE = 500
    all_data = []
    page = 1

    while True:
        print(f"\r  Fetching page {page} ({len(all_data)} records)...", end="", flush=True)
        params = (
            f"sortColumns=SECURITY_CODE&sortTypes=1"
            f"&pageSize={PAGE_SIZE}&pageNumber={page}"
            f"&reportName=RPT_F10_BASIC_ORGINFO"
            f"&columns=SECURITY_CODE,SECURITY_NAME_ABBR,LISTING_DATE"
            f"&filter=(SECURITY_TYPE_CODE%20in%20(%22058001001%22,%22058001002%22))"
        )
        url = f"{EASTMONEY_URL}?{params}"
        resp = curl_json(url)

        if not resp or not resp.get("success"):
            break

        data = resp["result"]["data"]
        if not data:
            break

        all_data.extend(data)
        total = resp["result"]["count"]
        if len(all_data) >= total:
            break
        page += 1
        time.sleep(0.3)

    print(f"\n  Total: {len(all_data)} records")

    csv_path = os.path.join(DATA_DIR, 'rebuild_listing_dates.csv')
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['code', 'name', 'listing_date'])
        for row in all_data:
            code = row['SECURITY_CODE']
            name = row['SECURITY_NAME_ABBR']
            listing_date = (row.get('LISTING_DATE') or '')[:10]
            writer.writerow([code, name, listing_date])

    print(f"  Saved to {csv_path}")
    return csv_path, len(all_data)


def main():
    parser = argparse.ArgumentParser(description='Rebuild A-share database')
    parser.add_argument('--step', type=int, choices=[1, 2, 3, 4],
                        help='Run specific step only')
    args = parser.parse_args()

    print("=" * 60)
    print(f"A-Share Database Rebuild - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    results = {}

    if not args.step or args.step == 1:
        csv_path, count = step1_fetch_stocks()
        results['stocks'] = (csv_path, count)

    if not args.step or args.step == 2:
        csv_path, count = step2_fetch_shareholders()
        results['shareholders'] = (csv_path, count)

    if not args.step or args.step == 3:
        csv_path, count = step3_fetch_daily_market()
        results['daily_market'] = (csv_path, count)

    if not args.step or args.step == 4:
        csv_path, count = step4_fetch_listing_dates()
        results['listing_dates'] = (csv_path, count)

    # Summary
    print("\n" + "=" * 60)
    print("REBUILD SUMMARY")
    print("=" * 60)
    for name, (path, count) in results.items():
        status = f"✅ {count} records" if count > 0 else "❌ FAILED"
        print(f"  {name}: {status}")
        if path:
            print(f"    CSV: {path}")

    print("\n📋 Next: Import CSVs to DuckDB via MCP SQL")


if __name__ == "__main__":
    main()
