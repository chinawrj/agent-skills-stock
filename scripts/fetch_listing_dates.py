#!/usr/bin/env python3
"""Fetch listing dates for all A-share stocks from EastMoney datacenter API and update stocks table."""
import csv
import json
import os
import subprocess
import sys
import time

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
BASE_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"

def fetch_all_listing_dates():
    """Fetch listing dates from EastMoney datacenter API."""
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
        url = f"{BASE_URL}?{params}"

        result = subprocess.run(
            ["curl", "-s", "--max-time", "30", url,
             "-H", "User-Agent: Mozilla/5.0"],
            capture_output=True, text=True
        )

        if result.returncode != 0 or not result.stdout.strip():
            print(f"\n  Error on page {page}")
            break

        resp = json.loads(result.stdout)
        if not resp.get("success"):
            print(f"\n  API error: {resp.get('message')}")
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
    return all_data


def save_csv(data):
    """Save listing dates to CSV."""
    csv_path = os.path.join(DATA_DIR, 'listing_dates.csv')
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['code', 'name', 'listing_date'])
        for row in data:
            code = row['SECURITY_CODE']
            name = row['SECURITY_NAME_ABBR']
            listing_date = (row.get('LISTING_DATE') or '')[:10]  # "1991-04-03 00:00:00" -> "1991-04-03"
            writer.writerow([code, name, listing_date])
    print(f"Saved to {csv_path}")
    return csv_path


def main():
    print("Fetching listing dates for all A-share stocks...")
    data = fetch_all_listing_dates()
    if not data:
        print("ERROR: No data!")
        sys.exit(1)
    csv_path = save_csv(data)

    # Generate SQL for updating stocks table
    sql_path = os.path.join(DATA_DIR, 'listing_dates.sql')
    abs_csv = os.path.abspath(csv_path)
    sql = f"""UPDATE stocks SET listing_date = ld.listing_date
FROM (
    SELECT code, TRY_CAST(listing_date AS DATE) as listing_date
    FROM read_csv_auto('{abs_csv}', nullstr='')
) ld
WHERE stocks.code = ld.code;"""

    with open(sql_path, 'w') as f:
        f.write(sql)
    print(f"SQL saved to {sql_path}")
    print(f"\nTo import: run the SQL in DuckDB or via MCP")


if __name__ == "__main__":
    main()
