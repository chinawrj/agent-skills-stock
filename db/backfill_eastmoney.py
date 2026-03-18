#!/usr/bin/env python3
"""Backfill klines for a specific date using Eastmoney datacenter bulk API.

Only ~2 API calls for the entire market (vs 5500+ individual requests).
Provides close price; open/high/low set to close as approximation.
"""

import requests
import csv
import sys

TARGET_DATE = sys.argv[1] if len(sys.argv) > 1 else '2026-03-17'

API = 'https://datacenter-web.eastmoney.com/api/data/v1/get'
BASE_PARAMS = {
    'reportName': 'RPT_DMSK_TS_STOCKNEW',
    'columns': 'SECURITY_CODE,SECURITY_NAME_ABBR,CLOSE_PRICE,CHANGE_RATE,TRADE_DATE,TURNOVERRATE',
    'filter': f"(TRADE_DATE='{TARGET_DATE}')",
    'pageSize': '5000',
    'source': 'WEB',
    'client': 'WEB',
}

all_data = []
for page in range(1, 20):
    params = {**BASE_PARAMS, 'pageNumber': str(page)}
    r = requests.get(API, params=params, timeout=15)
    d = r.json()
    if not d.get('success') or not d.get('result'):
        print(f"API error on page {page}: {d.get('message', 'unknown')}")
        break
    batch = d['result']['data']
    all_data.extend(batch)
    total_pages = d['result']['pages']
    print(f"  Page {page}/{total_pages}: {len(batch)} records (cumulative: {len(all_data)})")
    if page >= total_pages:
        break

print(f"\nTotal fetched: {len(all_data)} stocks")
if not all_data:
    print("No data returned. Exiting.")
    sys.exit(1)

print(f"Date check: {all_data[0]['TRADE_DATE'][:10]}")

# Generate SQL for klines insert
out_csv = f'data/klines_backfill_{TARGET_DATE}.csv'
out_sql = f'data/klines_backfill_{TARGET_DATE}.sql'

cols = ['code', 'trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount']
valid = 0
sql_values = []

with open(out_csv, 'w', newline='') as f:
    w = csv.DictWriter(f, fieldnames=cols)
    w.writeheader()
    for rec in all_data:
        cp = rec.get('CLOSE_PRICE')
        if cp is None:
            continue
        code = rec['SECURITY_CODE']
        w.writerow({
            'code': code,
            'trade_date': TARGET_DATE,
            'open': cp,
            'high': cp,
            'low': cp,
            'close': cp,
            'volume': '',
            'amount': '',
        })
        sql_values.append(f"('{code}', '{TARGET_DATE}', {cp}, {cp}, {cp}, {cp}, NULL, NULL)")
        valid += 1

# Write SQL file (chunked for DuckDB)
CHUNK = 1000
with open(out_sql, 'w') as f:
    for i in range(0, len(sql_values), CHUNK):
        chunk = sql_values[i:i+CHUNK]
        f.write("INSERT OR IGNORE INTO klines (code, trade_date, open, high, low, close, volume, amount) VALUES\n")
        f.write(",\n".join(chunk))
        f.write(";\n\n")

print(f"Saved {valid} records to {out_csv}")
print(f"SQL statements saved to {out_sql} ({len(sql_values)} rows, {(len(sql_values)-1)//CHUNK + 1} chunks)")
print(f"\nSample: {all_data[0]['SECURITY_CODE']} {all_data[0]['SECURITY_NAME_ABBR']} close={all_data[0]['CLOSE_PRICE']}")
