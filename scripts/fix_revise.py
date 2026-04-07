#!/usr/bin/env python3
"""Rebuild revise_history table from cache CSV."""
import duckdb
import csv

con = duckdb.connect('data/a-share.db')

con.execute('''
CREATE TABLE IF NOT EXISTS revise_history (
    bond_code VARCHAR NOT NULL,
    bond_name VARCHAR,
    meeting_date DATE NOT NULL,
    price_before DECIMAL(10,3),
    price_after DECIMAL(10,3),
    effective_date DATE,
    floor_price DECIMAL(10,3),
    PRIMARY KEY (bond_code, meeting_date)
)
''')

with open('.github/skills/db-manager/cache/revise_history.csv', 'r') as f:
    reader = csv.DictReader(f)
    rows = list(reader)

print(f'CSV has {len(rows)} rows')
import math

def safe_float(v):
    if not v or v == 'nan' or v == 'NaN':
        return None
    try:
        f = float(v)
        return None if math.isnan(f) else f
    except (ValueError, TypeError):
        return None

def safe_date(v):
    if not v or v == 'NaT' or v == 'nan':
        return None
    return v

count = 0
for row in rows:
    try:
        pb = safe_float(row.get('price_before'))
        pa = safe_float(row.get('price_after'))
        ed = safe_date(row.get('effective_date'))
        fp = safe_float(row.get('floor_price'))
        md = safe_date(row.get('meeting_date'))
        if not md:
            continue
        con.execute(
            'INSERT OR REPLACE INTO revise_history VALUES (?, ?, ?, ?, ?, ?, ?)',
            [row['bond_code'], row.get('bond_name', ''), md, pb, pa, ed, fp]
        )
        count += 1
    except Exception as e:
        print(f'Error: {row.get("bond_code")} {row.get("meeting_date")}: {e}')

print(f'Imported {count} rows')
r = con.execute('SELECT COUNT(*) FROM revise_history').fetchone()
print(f'Table now has {r[0]} rows')
con.close()
