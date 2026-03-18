#!/usr/bin/env python3
"""
获取可转债转股价下修历史记录 — 集思录数据源

数据来源: akshare.bond_cb_adj_logs_jsl() → 集思录 /data/cbnew/adj_logs/
每条记录包含: 股东大会日、下修前转股价、下修后转股价、下修底价、生效日期

用法:
    cd /Users/rjwang/fun/a-share && source .venv/bin/activate

    python db/fetch_revise_history.py                  # 全量获取(所有有效债)
    python db/fetch_revise_history.py --all             # 含已退市债
    python db/fetch_revise_history.py --bond 113053     # 单只债
    python db/fetch_revise_history.py --dry-run         # 仅保存CSV
"""

import argparse
import csv
import os
import sys
import time
from datetime import datetime

import akshare as ak

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
DB_PATH = os.path.abspath(os.path.join(DATA_DIR, 'a-share.db'))
CSV_PATH = os.path.join(DATA_DIR, 'revise_history.csv')

CSV_COLS = [
    'bond_code', 'bond_name', 'meeting_date', 'price_before',
    'price_after', 'effective_date', 'floor_price',
]


def get_bond_codes(include_delisted=False):
    """Get bond codes from DB."""
    try:
        import duckdb
        conn = duckdb.connect(DB_PATH, read_only=True)
        where = "" if include_delisted else "WHERE delist_date IS NULL"
        rows = conn.execute(
            f"SELECT bond_code, bond_name FROM bonds {where} ORDER BY bond_code"
        ).fetchall()
        conn.close()
        return [(r[0], r[1]) for r in rows]
    except Exception as e:
        print(f"⚠️ DB读取失败({e})，尝试从CSV获取...")
        # Fallback: read from bond_market.csv
        csv_path = os.path.join(DATA_DIR, 'bond_market.csv')
        if not os.path.exists(csv_path):
            print("❌ 没有可用的债券列表")
            sys.exit(1)
        codes = []
        with open(csv_path, 'r', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                codes.append((row['bond_code'], ''))
        return codes


def fetch_one(bond_code):
    """Fetch revision history for one bond. Returns list of dicts."""
    try:
        df = ak.bond_cb_adj_logs_jsl(symbol=bond_code)
    except Exception:
        return []
    if df.empty:
        return []
    records = []
    for _, row in df.iterrows():
        records.append({
            'bond_code': bond_code,
            'bond_name': row.get('转债名称', ''),
            'meeting_date': str(row.get('股东大会日', '')) or None,
            'price_before': row.get('下修前转股价'),
            'price_after': row.get('下修后转股价'),
            'effective_date': str(row.get('新转股价生效日期', '')) or None,
            'floor_price': row.get('下修底价'),
        })
    return records


def fetch_all(bond_list, delay=0.3):
    """Fetch revision history for all bonds. Returns all records."""
    all_records = []
    total = len(bond_list)
    hit = 0
    for i, (code, name) in enumerate(bond_list, 1):
        print(f"\r  [{i}/{total}] {code} {name or ''}...  "
              f"({hit}只有下修记录, {len(all_records)}条)", end="", flush=True)
        recs = fetch_one(code)
        if recs:
            all_records.extend(recs)
            hit += 1
        if i < total:
            time.sleep(delay)
    print(f"\n  完成: {hit}/{total}只债有下修记录, 共{len(all_records)}条")
    return all_records


def save_csv(records):
    """Save records to CSV."""
    with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLS)
        w.writeheader()
        for r in records:
            w.writerow({k: r.get(k) for k in CSV_COLS})
    print(f"  保存至 {CSV_PATH} ({len(records)}条)")
    return CSV_PATH


def import_sql():
    """Generate SQL to create table and import CSV."""
    return f"""
-- 创建下修历史表
CREATE TABLE IF NOT EXISTS revise_history (
    bond_code    VARCHAR NOT NULL,
    bond_name    VARCHAR,
    meeting_date DATE,
    price_before DECIMAL(10,3),
    price_after  DECIMAL(10,3),
    effective_date DATE,
    floor_price  DECIMAL(10,3),
    PRIMARY KEY (bond_code, meeting_date)
);

-- 清空并重新导入
DELETE FROM revise_history;
INSERT INTO revise_history
SELECT
    bond_code,
    bond_name,
    TRY_CAST(meeting_date AS DATE),
    TRY_CAST(price_before AS DECIMAL(10,3)),
    TRY_CAST(price_after AS DECIMAL(10,3)),
    TRY_CAST(effective_date AS DATE),
    TRY_CAST(floor_price AS DECIMAL(10,3))
FROM read_csv('{CSV_PATH}', header=true, auto_detect=true);
"""


def import_to_db():
    """Import CSV to DuckDB."""
    sql = import_sql()
    try:
        import duckdb
        conn = duckdb.connect(DB_PATH)
        for stmt in sql.strip().split(';'):
            stmt = stmt.strip()
            if stmt and not stmt.startswith('--'):
                conn.execute(stmt)
        count = conn.execute("SELECT COUNT(*) FROM revise_history").fetchone()[0]
        bonds_with = conn.execute(
            "SELECT COUNT(DISTINCT bond_code) FROM revise_history"
        ).fetchone()[0]
        conn.close()
        print(f"  ✅ 导入完成: {count}条记录, {bonds_with}只债有下修历史")
        return True
    except Exception as e:
        if 'lock' in str(e).lower():
            sql_path = os.path.join(DATA_DIR, 'revise_history.sql')
            with open(sql_path, 'w', encoding='utf-8') as f:
                f.write(sql)
            print(f"  ⚠️ DB锁定，SQL已保存至 {sql_path}")
            print("  可通过MCP DuckDB工具执行导入")
            return False
        raise


def main():
    parser = argparse.ArgumentParser(description='获取可转债转股价下修历史')
    parser.add_argument('--all', action='store_true', help='包含已退市债')
    parser.add_argument('--bond', type=str, help='仅查询指定债券代码')
    parser.add_argument('--dry-run', action='store_true', help='仅保存CSV')
    parser.add_argument('--delay', type=float, default=0.3, help='请求间隔秒数')
    args = parser.parse_args()

    print("=" * 50)
    print("  可转债转股价下修历史 — 集思录")
    print("=" * 50)

    if args.bond:
        print(f"\n查询单只债券: {args.bond}")
        records = fetch_one(args.bond)
        if records:
            for r in records:
                print(f"  {r['meeting_date']} | "
                      f"{r['price_before']} → {r['price_after']} "
                      f"(底价{r['floor_price']}) "
                      f"| 生效{r['effective_date']}")
        else:
            print("  无下修记录")
        return

    # 全量获取
    bond_list = get_bond_codes(include_delisted=args.all)
    print(f"\n获取 {len(bond_list)} 只债券的下修历史...")

    records = fetch_all(bond_list, delay=args.delay)
    if not records:
        print("  无任何下修记录")
        return

    save_csv(records)

    if args.dry_run:
        print("\n  ✅ Dry run 完成 — CSV已保存")
        return

    import_to_db()


if __name__ == '__main__':
    main()
