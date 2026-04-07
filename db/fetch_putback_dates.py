#!/usr/bin/env python3
"""
获取可转债回售起始日 — 东方财富 RPT_BOND_CB_CLAUSE API

数据来源: datacenter-web.eastmoney.com RPT_BOND_CB_CLAUSE
核心字段: DAT_SDATEPUTS (回售起始日), DAT_EDATEPUTS (回售结束日)

用法:
    cd /Users/rjwang/fun/a-share && source .venv/bin/activate
    python db/fetch_putback_dates.py              # 获取并更新DB
    python db/fetch_putback_dates.py --dry-run    # 仅保存CSV
    python db/fetch_putback_dates.py --test       # 仅测试API
"""

import argparse
import csv
import json
import os
import sys
import time

import requests

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
DB_PATH = os.path.abspath(os.path.join(DATA_DIR, 'a-share.db'))
CSV_PATH = os.path.join(DATA_DIR, 'bond_putback_dates.csv')

EM_API = 'https://datacenter-web.eastmoney.com/api/data/v1/get'


def fetch_all_putback_dates():
    """Fetch putback dates from RPT_BOND_CB_CLAUSE API (all bonds, paginated)."""
    all_records = []
    page = 1
    while True:
        params = {
            'sortColumns': 'SECURITY_CODE',
            'sortTypes': '1',
            'pageSize': '500',
            'pageNumber': str(page),
            'reportName': 'RPT_BOND_CB_CLAUSE',
            'columns': (
                'SECURITY_CODE,BOND_NAME_ABBR,'
                'DAT_SDATEPUTS,DAT_EDATEPUTS,'
                'EXPIRE_DATE,VALUE_DATE,'
                'TRANSFER_START_DATE,TRANSFER_END_DATE,'
                'BONDPERIOD,HIST_ADJ_COUNT'
            ),
        }
        print(f"\r  [回售日期] 第{page}页 ({len(all_records)}条)...", end="", flush=True)
        try:
            r = requests.get(EM_API, params=params, timeout=30)
            data = r.json()
        except Exception as e:
            print(f"\n  ⚠️ 请求失败: {e}")
            break

        result = data.get('result')
        if not result or not result.get('data'):
            break

        rows = result['data']
        for row in rows:
            code = row.get('SECURITY_CODE', '')
            # Only keep exchange-traded bonds (1xxxxx codes)
            if not code or not code.startswith('1'):
                continue
            all_records.append({
                'bond_code': code,
                'bond_name': row.get('BOND_NAME_ABBR', ''),
                'putback_start': _parse_date(row.get('DAT_SDATEPUTS')),
                'putback_end': _parse_date(row.get('DAT_EDATEPUTS')),
                'convert_start': _parse_date(row.get('TRANSFER_START_DATE')),
                'convert_end': _parse_date(row.get('TRANSFER_END_DATE')),
                'value_date': _parse_date(row.get('VALUE_DATE')),
                'expire_date': _parse_date(row.get('EXPIRE_DATE')),
                'bond_period': row.get('BONDPERIOD'),
                'hist_adj_count': row.get('HIST_ADJ_COUNT'),
            })

        total_pages = result.get('pages', 1)
        if page >= total_pages:
            break
        page += 1
        time.sleep(0.3)

    print(f"\n  [回售日期] 共获取 {len(all_records)} 条转债记录")
    return all_records


def _parse_date(val):
    """Parse date string from API response."""
    if not val or val == 'None':
        return None
    # '2026-03-18 00:00:00' → '2026-03-18'
    return str(val)[:10]


def calculate_putback_start(records):
    """For bonds where DAT_SDATEPUTS is NULL, estimate from value_date + (period-2) years.
    
    Standard clause: "最后两个计息年度" → putback starts at year (N-2) of N-year bond.
    """
    filled = 0
    for rec in records:
        if rec['putback_start']:
            continue
        vd = rec.get('value_date')
        period = rec.get('bond_period')
        if vd and period and period > 2:
            # value_date + (period - 2) years
            try:
                from datetime import datetime, timedelta
                dt = datetime.strptime(vd, '%Y-%m-%d')
                # Add (period-2) years
                years = int(period) - 2
                putback_dt = dt.replace(year=dt.year + years)
                rec['putback_start'] = putback_dt.strftime('%Y-%m-%d')
                filled += 1
            except (ValueError, OverflowError):
                pass
    if filled:
        print(f"  [回售日期] 推算补充了 {filled} 条缺失的回售起始日")
    return records


def save_csv(records):
    """Save to CSV."""
    cols = ['bond_code', 'bond_name', 'putback_start', 'putback_end',
            'convert_start', 'convert_end', 'value_date', 'expire_date',
            'bond_period', 'hist_adj_count']
    with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for rec in records:
            w.writerow({k: rec.get(k) for k in cols})
    print(f"  保存至 {CSV_PATH} ({len(records)}条)")


def update_db(records):
    """Update putback_start and convert_start in bonds table."""
    try:
        import duckdb
        conn = duckdb.connect(DB_PATH)
    except Exception as e:
        if 'lock' in str(e).lower():
            sql_path = os.path.join(DATA_DIR, 'putback_dates.sql')
            _save_update_sql(records, sql_path)
            print(f"  ⚠️ DB锁定，SQL已保存至 {sql_path}")
            return False
        raise

    updated = 0
    for rec in records:
        if not rec.get('putback_start'):
            continue
        conn.execute("""
            UPDATE bonds SET putback_start = ?
            WHERE bond_code = ? AND (putback_start IS NULL OR putback_start != ?)
        """, [rec['putback_start'], rec['bond_code'], rec['putback_start']])
        updated += conn.fetchone()[0] if False else 0

    # Bulk update using temp table approach
    conn.execute("CREATE TEMP TABLE _putback AS SELECT * FROM read_csv_auto(?)", [CSV_PATH])
    result = conn.execute("""
        UPDATE bonds SET
            putback_start = CASE WHEN _p.putback_start IS NOT NULL 
                THEN TRY_CAST(_p.putback_start AS DATE) ELSE bonds.putback_start END,
            convert_start = CASE WHEN _p.convert_start IS NOT NULL 
                THEN TRY_CAST(_p.convert_start AS DATE) ELSE bonds.convert_start END
        FROM _putback _p
        WHERE bonds.bond_code = _p.bond_code
    """)
    count = result.fetchone()

    # Verify
    has_putback = conn.execute(
        "SELECT COUNT(*) FROM bonds WHERE putback_start IS NOT NULL AND delist_date IS NULL"
    ).fetchone()[0]
    total_active = conn.execute(
        "SELECT COUNT(*) FROM bonds WHERE delist_date IS NULL AND listing_date IS NOT NULL"
    ).fetchone()[0]

    conn.execute("DROP TABLE IF EXISTS _putback")
    conn.close()
    print(f"  ✅ 更新完成: {has_putback}/{total_active} 只活跃转债有回售起始日")
    return True


def _save_update_sql(records, sql_path):
    """Save update SQL for manual execution."""
    with open(sql_path, 'w', encoding='utf-8') as f:
        f.write("-- 更新回售起始日\n")
        for rec in records:
            if rec.get('putback_start'):
                f.write(f"UPDATE bonds SET putback_start = '{rec['putback_start']}' "
                        f"WHERE bond_code = '{rec['bond_code']}';\n")
    print(f"  SQL已保存至 {sql_path}")


def main():
    parser = argparse.ArgumentParser(description='获取可转债回售起始日')
    parser.add_argument('--dry-run', action='store_true', help='仅保存CSV')
    parser.add_argument('--test', action='store_true', help='仅测试API')
    args = parser.parse_args()

    print("=" * 50)
    print("  可转债回售起始日 — 东方财富")
    print("=" * 50)

    if args.test:
        records = fetch_all_putback_dates()
        has_start = sum(1 for r in records if r['putback_start'])
        print(f"  API有putback_start: {has_start}/{len(records)}")
        records = calculate_putback_start(records)
        has_start_after = sum(1 for r in records if r['putback_start'])
        print(f"  推算后有putback_start: {has_start_after}/{len(records)}")
        # Show some samples
        for r in records[:5]:
            print(f"  {r['bond_code']} {r['bond_name']}: putback={r['putback_start']} convert={r['convert_start']}")
        return

    records = fetch_all_putback_dates()
    records = calculate_putback_start(records)
    save_csv(records)

    if not args.dry_run:
        update_db(records)


if __name__ == '__main__':
    main()
