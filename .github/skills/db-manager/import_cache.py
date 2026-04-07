#!/usr/bin/env python3
"""
从 cache/ CSV 文件重建完整 A股数据库

位置: .github/skills/db-manager/import_cache.py

用法:
    cd /Users/rjwang/fun/a-share && source .venv/bin/activate

    # 从 cache 完整重建（删除旧DB，从零导入）
    python .github/skills/db-manager/import_cache.py

    # 仅重建某张表
    python .github/skills/db-manager/import_cache.py --table bonds

    # 指定 cache 目录（默认 .github/skills/db-manager/cache）
    python .github/skills/db-manager/import_cache.py --cache-dir /path/to/cache

    # 仅生成SQL不执行（DB锁定时）
    python .github/skills/db-manager/import_cache.py --sql-only

核心理念: CSV 是持久数据层，DB 随时可从 cache 重建
"""

import argparse
import glob
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WORKSPACE = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..', '..'))
DEFAULT_CACHE = os.path.join(SCRIPT_DIR, 'cache')
DEFAULT_DB = os.path.join(WORKSPACE, 'data', 'a-share.db')

# ═══════════════════ Schema ═══════════════════

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS stocks (
    code VARCHAR NOT NULL PRIMARY KEY,
    name VARCHAR,
    market VARCHAR,
    listing_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bonds (
    bond_code VARCHAR NOT NULL PRIMARY KEY,
    bond_name VARCHAR,
    stock_code VARCHAR,
    stock_name VARCHAR,
    issue_date DATE,
    maturity_date DATE,
    issue_size DECIMAL(12,4),
    remaining_size DECIMAL(12,4),
    maturity_years SMALLINT,
    convert_start DATE,
    convert_price DECIMAL(10,3),
    original_price DECIMAL(10,3),
    redeem_pct DECIMAL(6,2),
    redeem_days SMALLINT,
    redeem_window SMALLINT,
    putback_start DATE,
    putback_pct DECIMAL(6,2),
    putback_days SMALLINT,
    putback_window SMALLINT,
    revise_pct DECIMAL(6,2),
    revise_days SMALLINT,
    revise_window SMALLINT,
    bond_price DECIMAL(10,3),
    convert_value DECIMAL(10,3),
    premium_rate DECIMAL(10,4),
    ytm DECIMAL(10,4),
    is_profitable BOOLEAN,
    consecutive_profit_years INTEGER,
    latest_roe DECIMAL(10,4),
    latest_net_profit DECIMAL(18,2),
    revise_trigger_count INTEGER,
    putback_trigger_count INTEGER,
    redeem_trigger_count INTEGER,
    stock_price_latest DECIMAL(10,4),
    rating VARCHAR,
    listing_date DATE,
    delist_date DATE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    maturity_redemption_price DECIMAL(10,3),
    coupon_rate_1 DECIMAL(6,3),
    coupon_rate_2 DECIMAL(6,3),
    coupon_rate_3 DECIMAL(6,3),
    coupon_rate_4 DECIMAL(6,3),
    coupon_rate_5 DECIMAL(6,3),
    coupon_rate_6 DECIMAL(6,3),
    coupon_rate_7 DECIMAL(6,3),
    coupon_rate_8 DECIMAL(6,3),
    coupon_rate_9 DECIMAL(6,3),
    coupon_rate_10 DECIMAL(6,3),
    initial_convert_price DECIMAL(10,3)
);

CREATE TABLE IF NOT EXISTS daily_market (
    code VARCHAR NOT NULL,
    name VARCHAR,
    trade_date DATE NOT NULL,
    open DECIMAL(10,2),
    high DECIMAL(10,2),
    low DECIMAL(10,2),
    close DECIMAL(10,2),
    prev_close DECIMAL(10,2),
    change_amount DECIMAL(10,2),
    change_pct DECIMAL(10,4),
    amplitude DECIMAL(10,4),
    volume BIGINT,
    amount DECIMAL(18,2),
    turnover_rate DECIMAL(10,4),
    pe_dynamic DECIMAL(12,2),
    pe_ttm DECIMAL(12,2),
    pb DECIMAL(10,4),
    total_mv DECIMAL(18,2),
    circ_mv DECIMAL(18,2),
    PRIMARY KEY (code, trade_date)
);

CREATE TABLE IF NOT EXISTS klines (
    code VARCHAR NOT NULL,
    trade_date DATE NOT NULL,
    open DECIMAL(10,2),
    high DECIMAL(10,2),
    low DECIMAL(10,2),
    close DECIMAL(10,2),
    volume BIGINT,
    amount DECIMAL(18,2),
    PRIMARY KEY (code, trade_date)
);

CREATE TABLE IF NOT EXISTS fundamentals (
    code VARCHAR NOT NULL,
    name VARCHAR,
    report_date DATE NOT NULL,
    report_type VARCHAR DEFAULT '年报',
    eps DECIMAL(10,4),
    roe DECIMAL(10,4),
    net_profit DECIMAL(18,2),
    revenue DECIMAL(18,2),
    profit_yoy DECIMAL(10,4),
    revenue_yoy DECIMAL(10,4),
    PRIMARY KEY (code, report_date)
);

CREATE TABLE IF NOT EXISTS shareholders (
    code VARCHAR NOT NULL,
    name VARCHAR,
    stat_date DATE NOT NULL,
    announce_date DATE,
    shareholders INTEGER,
    shareholders_prev INTEGER,
    change INTEGER,
    change_ratio DECIMAL(10,4),
    range_change_pct DECIMAL(10,4),
    avg_value DECIMAL(18,2),
    avg_shares DECIMAL(18,2),
    market_cap DECIMAL(18,2),
    total_shares BIGINT,
    shares_change BIGINT,
    shares_change_reason VARCHAR,
    PRIMARY KEY (code, stat_date)
);

CREATE TABLE IF NOT EXISTS revise_history (
    bond_code VARCHAR NOT NULL,
    bond_name VARCHAR,
    meeting_date DATE,
    price_before DECIMAL(10,3),
    price_after DECIMAL(10,3),
    effective_date DATE,
    floor_price DECIMAL(10,3),
    PRIMARY KEY (bond_code, meeting_date)
);

CREATE TABLE IF NOT EXISTS data_updates (
    id INTEGER PRIMARY KEY,
    table_name VARCHAR,
    update_type VARCHAR,
    records_count INTEGER,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes VARCHAR
);

CREATE TABLE IF NOT EXISTS fundamental_update_log (
    code VARCHAR NOT NULL,
    last_updated DATE,
    PRIMARY KEY (code)
);

CREATE INDEX IF NOT EXISTS idx_daily_market_date ON daily_market(trade_date);
CREATE INDEX IF NOT EXISTS idx_shareholders_announce ON shareholders(announce_date);
CREATE INDEX IF NOT EXISTS idx_shareholders_change ON shareholders(change_ratio);
CREATE INDEX IF NOT EXISTS idx_klines_code ON klines(code);
CREATE INDEX IF NOT EXISTS idx_klines_date ON klines(trade_date);
"""

# ═══════════════════ Import SQL Templates ═══════════════════

def import_stocks_sql(csv_path):
    return f"""
INSERT OR REPLACE INTO stocks (code, name, market)
SELECT code, name, market FROM read_csv_auto('{csv_path}', nullstr='');
"""

def import_listing_dates_sql(csv_path):
    return f"""
UPDATE stocks SET listing_date = TRY_CAST(src.listing_date AS DATE)
FROM (SELECT code, listing_date FROM read_csv('{csv_path}',
      columns={{'code': 'VARCHAR', 'name': 'VARCHAR', 'listing_date': 'VARCHAR'}},
      header=true, nullstr='')
      WHERE listing_date IS NOT NULL AND length(listing_date) >= 8) src
WHERE stocks.code = src.code;
"""

def import_bonds_full_sql(csv_path):
    return f"""
INSERT OR REPLACE INTO bonds (
    bond_code, bond_name, stock_code, issue_size, rating, maturity_date,
    convert_price, original_price, listing_date, delist_date,
    redeem_pct, redeem_days, redeem_window,
    putback_pct, putback_days, putback_window,
    revise_pct, revise_days, revise_window,
    bond_price, convert_value, premium_rate, ytm, remaining_size,
    maturity_redemption_price,
    coupon_rate_1, coupon_rate_2, coupon_rate_3,
    coupon_rate_4, coupon_rate_5, coupon_rate_6,
    coupon_rate_7, coupon_rate_8, coupon_rate_9,
    coupon_rate_10,
    updated_at
)
SELECT
    bond_code, bond_name, stock_code,
    TRY_CAST(issue_size AS DECIMAL(12,4)),
    rating,
    TRY_CAST(maturity_date AS DATE),
    TRY_CAST(convert_price AS DECIMAL(10,3)),
    TRY_CAST(original_price AS DECIMAL(10,3)),
    TRY_CAST(listing_date AS DATE),
    TRY_CAST(delist_date AS DATE),
    TRY_CAST(redeem_pct AS DECIMAL(6,2)),
    TRY_CAST(redeem_days AS SMALLINT),
    TRY_CAST(redeem_window AS SMALLINT),
    TRY_CAST(putback_pct AS DECIMAL(6,2)),
    TRY_CAST(putback_days AS SMALLINT),
    TRY_CAST(putback_window AS SMALLINT),
    TRY_CAST(revise_pct AS DECIMAL(6,2)),
    TRY_CAST(revise_days AS SMALLINT),
    TRY_CAST(revise_window AS SMALLINT),
    TRY_CAST(bond_price AS DECIMAL(10,3)),
    TRY_CAST(convert_value AS DECIMAL(10,3)),
    TRY_CAST(premium_rate AS DECIMAL(10,4)),
    TRY_CAST(ytm AS DECIMAL(10,4)),
    TRY_CAST(remaining_size AS DECIMAL(12,4)),
    TRY_CAST(maturity_redemption_price AS DECIMAL(10,3)),
    TRY_CAST(coupon_rate_1 AS DECIMAL(6,3)),
    TRY_CAST(coupon_rate_2 AS DECIMAL(6,3)),
    TRY_CAST(coupon_rate_3 AS DECIMAL(6,3)),
    TRY_CAST(coupon_rate_4 AS DECIMAL(6,3)),
    TRY_CAST(coupon_rate_5 AS DECIMAL(6,3)),
    TRY_CAST(coupon_rate_6 AS DECIMAL(6,3)),
    TRY_CAST(coupon_rate_7 AS DECIMAL(6,3)),
    TRY_CAST(coupon_rate_8 AS DECIMAL(6,3)),
    TRY_CAST(coupon_rate_9 AS DECIMAL(6,3)),
    TRY_CAST(coupon_rate_10 AS DECIMAL(6,3)),
    CURRENT_TIMESTAMP
FROM read_csv_auto('{csv_path}', nullstr='');
"""

def import_bonds_market_sql(csv_path):
    """Update existing bonds with latest market data (prices, ytm, etc.)."""
    return f"""
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
    FROM read_csv_auto('{csv_path}', nullstr='')
) AS src
WHERE bonds.bond_code = src.bond_code;
"""

def import_daily_market_sql(csv_path, trade_date=None):
    return f"""
INSERT OR REPLACE INTO daily_market
    (code, name, trade_date, open, high, low, close, prev_close,
     change_amount, change_pct, volume, amount,
     turnover_rate, pe_dynamic, pb, total_mv, circ_mv)
SELECT
    code, name, TRY_CAST(trade_date AS DATE),
    TRY_CAST(open AS DECIMAL(10,2)),
    TRY_CAST(high AS DECIMAL(10,2)),
    TRY_CAST(low AS DECIMAL(10,2)),
    TRY_CAST(close AS DECIMAL(10,2)),
    TRY_CAST(prev_close AS DECIMAL(10,2)),
    TRY_CAST(change_amount AS DECIMAL(10,2)),
    TRY_CAST(change_pct AS DECIMAL(10,4)),
    TRY_CAST(volume AS BIGINT),
    TRY_CAST(amount AS DECIMAL(18,2)),
    TRY_CAST(turnover_rate AS DECIMAL(10,4)),
    TRY_CAST(pe_dynamic AS DECIMAL(12,2)),
    TRY_CAST(pb AS DECIMAL(10,4)),
    TRY_CAST(total_mv AS DECIMAL(18,2)),
    TRY_CAST(circ_mv AS DECIMAL(18,2))
FROM read_csv_auto('{csv_path}', nullstr='');
"""

def import_klines_from_daily_sql(csv_path):
    """Import klines from a daily_market CSV (daily snapshots → klines table)."""
    return f"""
INSERT INTO klines (code, trade_date, open, high, low, close, volume, amount)
SELECT
    code, TRY_CAST(trade_date AS DATE),
    TRY_CAST(open AS DECIMAL(10,2)),
    TRY_CAST(high AS DECIMAL(10,2)),
    TRY_CAST(low AS DECIMAL(10,2)),
    TRY_CAST(close AS DECIMAL(10,2)),
    TRY_CAST(volume AS BIGINT),
    TRY_CAST(amount AS DECIMAL(18,2))
FROM read_csv_auto('{csv_path}', nullstr='')
WHERE TRY_CAST(close AS DECIMAL(10,2)) > 0
  AND TRY_CAST(volume AS BIGINT) > 0
ON CONFLICT DO NOTHING;
"""

def import_klines_sql(csv_path):
    return f"""
INSERT OR REPLACE INTO klines (code, trade_date, open, high, low, close, volume)
SELECT
    code, TRY_CAST(trade_date AS DATE),
    TRY_CAST(open AS DECIMAL(10,2)),
    TRY_CAST(high AS DECIMAL(10,2)),
    TRY_CAST(low AS DECIMAL(10,2)),
    TRY_CAST(close AS DECIMAL(10,2)),
    TRY_CAST(volume AS BIGINT)
FROM read_csv_auto('{csv_path}', nullstr='')
WHERE TRY_CAST(close AS DECIMAL(10,2)) IS NOT NULL;
"""

def import_fundamentals_sql(csv_path):
    return f"""
DELETE FROM fundamentals;
INSERT INTO fundamentals (code, name, report_date, report_type, eps, roe,
                          net_profit, revenue, profit_yoy, revenue_yoy)
SELECT
    code, name,
    TRY_CAST(report_date AS DATE),
    report_type,
    TRY_CAST(eps AS DECIMAL(10,4)),
    TRY_CAST(roe AS DECIMAL(10,4)),
    TRY_CAST(net_profit AS DECIMAL(18,2)),
    TRY_CAST(revenue AS DECIMAL(18,2)),
    TRY_CAST(profit_yoy AS DECIMAL(10,4)),
    TRY_CAST(revenue_yoy AS DECIMAL(10,4))
FROM read_csv_auto('{csv_path}', nullstr='');
"""

def import_shareholders_sql(csv_path):
    return f"""
INSERT OR REPLACE INTO shareholders
(code, name, stat_date, announce_date, shareholders, shareholders_prev,
 change, change_ratio, range_change_pct, avg_value, avg_shares,
 market_cap, total_shares, shares_change, shares_change_reason)
SELECT
    code, name,
    TRY_CAST(stat_date AS DATE),
    TRY_CAST(announce_date AS DATE),
    TRY_CAST(shareholders AS INTEGER),
    TRY_CAST(shareholders_prev AS INTEGER),
    TRY_CAST(change AS INTEGER),
    TRY_CAST(change_ratio AS DECIMAL(10,4)),
    TRY_CAST(range_change_pct AS DECIMAL(10,4)),
    TRY_CAST(avg_value AS DECIMAL(18,2)),
    TRY_CAST(avg_shares AS DECIMAL(18,2)),
    TRY_CAST(market_cap AS DECIMAL(18,2)),
    TRY_CAST(total_shares AS BIGINT),
    TRY_CAST(shares_change AS BIGINT),
    shares_change_reason
FROM read_csv_auto('{csv_path}', nullstr='');
"""

def import_revise_history_sql(csv_path):
    return f"""
DELETE FROM revise_history;
INSERT INTO revise_history
SELECT bond_code, bond_name, meeting_date, price_before, price_after, effective_date, floor_price
FROM (
    SELECT
        bond_code, bond_name,
        TRY_CAST(meeting_date AS DATE) AS meeting_date,
        TRY_CAST(price_before AS DECIMAL(10,3)) AS price_before,
        TRY_CAST(price_after AS DECIMAL(10,3)) AS price_after,
        TRY_CAST(effective_date AS DATE) AS effective_date,
        TRY_CAST(floor_price AS DECIMAL(10,3)) AS floor_price,
        ROW_NUMBER() OVER (PARTITION BY bond_code, TRY_CAST(meeting_date AS DATE) ORDER BY price_before DESC) AS rn
    FROM read_csv('{csv_path}', header=true, auto_detect=true)
    WHERE TRY_CAST(meeting_date AS DATE) IS NOT NULL
) WHERE rn = 1;
"""

def import_putback_dates_sql(csv_path):
    """Update bonds.putback_start (and convert_start) from bond_putback_dates.csv."""
    return f"""
UPDATE bonds SET
    putback_start = COALESCE(src.putback_start, bonds.putback_start),
    convert_start = COALESCE(src.convert_start, bonds.convert_start)
FROM (
    SELECT
        bond_code,
        TRY_CAST(putback_start AS DATE) AS putback_start,
        TRY_CAST(convert_start AS DATE) AS convert_start
    FROM read_csv_auto('{csv_path}', nullstr='')
    WHERE putback_start IS NOT NULL AND putback_start != ''
) AS src
WHERE bonds.bond_code = src.bond_code;
"""

# ═══════════════════ Analysis SQL ═══════════════════

TRIGGER_SQL = """
WITH recent_klines AS (
    SELECT k.code, k.trade_date, k.close,
           ROW_NUMBER() OVER (PARTITION BY k.code ORDER BY k.trade_date DESC) AS rn
    FROM klines k
    JOIN (SELECT DISTINCT stock_code FROM bonds WHERE maturity_date > CURRENT_DATE) b
      ON k.code = b.stock_code
),
latest_price AS (
    SELECT code, close AS latest_close FROM recent_klines WHERE rn = 1
),
trigger_counts AS (
    SELECT rk.code, b.bond_code,
           COUNT(CASE WHEN rk.close < b.convert_price * b.revise_pct / 100.0 THEN 1 END) AS revise_cnt,
           COUNT(CASE WHEN rk.close < b.convert_price * b.putback_pct / 100.0 THEN 1 END) AS putback_cnt,
           COUNT(CASE WHEN rk.close >= b.convert_price * b.redeem_pct / 100.0 THEN 1 END) AS redeem_cnt
    FROM recent_klines rk
    JOIN bonds b ON rk.code = b.stock_code AND b.maturity_date > CURRENT_DATE
    WHERE rk.rn <= COALESCE(b.revise_window, 30)
    GROUP BY rk.code, b.bond_code
)
UPDATE bonds
SET revise_trigger_count = tc.revise_cnt,
    putback_trigger_count = tc.putback_cnt,
    redeem_trigger_count = tc.redeem_cnt,
    stock_price_latest = lp.latest_close
FROM trigger_counts tc
JOIN latest_price lp ON tc.code = lp.code
WHERE bonds.bond_code = tc.bond_code;
"""

PROFITABILITY_SQL = """
WITH annual_data AS (
    SELECT code, EXTRACT(YEAR FROM report_date) AS yr, net_profit, roe
    FROM fundamentals WHERE report_type = '年报'
),
latest_year AS (
    SELECT code, MAX(yr) AS max_yr FROM annual_data GROUP BY code
),
latest_metrics AS (
    SELECT a.code, a.net_profit, a.roe
    FROM annual_data a JOIN latest_year l ON a.code = l.code AND a.yr = l.max_yr
),
profit_check AS (
    SELECT a.code, a.yr,
           CASE WHEN a.net_profit > 0 THEN 1 ELSE 0 END AS is_profit, l.max_yr
    FROM annual_data a JOIN latest_year l ON a.code = l.code
    WHERE a.yr <= l.max_yr AND a.yr >= l.max_yr - 5
),
consecutive AS (
    SELECT code,
        CASE
            WHEN SUM(CASE WHEN yr = max_yr AND is_profit = 0 THEN 1 ELSE 0 END) > 0 THEN 0
            WHEN SUM(CASE WHEN yr = max_yr - 1 AND is_profit = 0 THEN 1 ELSE 0 END) > 0 THEN 1
            WHEN SUM(CASE WHEN yr = max_yr - 2 AND is_profit = 0 THEN 1 ELSE 0 END) > 0 THEN 2
            WHEN SUM(CASE WHEN yr = max_yr - 3 AND is_profit = 0 THEN 1 ELSE 0 END) > 0 THEN 3
            WHEN SUM(CASE WHEN yr = max_yr - 4 AND is_profit = 0 THEN 1 ELSE 0 END) > 0 THEN 4
            ELSE 5
        END AS consec_years
    FROM profit_check GROUP BY code, max_yr
)
UPDATE bonds
SET is_profitable = (lm.net_profit > 0),
    latest_net_profit = lm.net_profit,
    latest_roe = lm.roe,
    consecutive_profit_years = c.consec_years
FROM latest_metrics lm
JOIN consecutive c ON lm.code = c.code
WHERE bonds.stock_code = lm.code;
"""

FILL_STOCK_NAME_SQL = """
UPDATE bonds SET stock_name = s.name
FROM stocks s WHERE bonds.stock_code = s.code;
"""

# ═══════════════════ Main ═══════════════════

IMPORT_STEPS = [
    # (name, cache_file, sql_func, description)
    ('stocks',        'stocks.csv',         import_stocks_sql,        '股票列表'),
    ('listing',       'listing_dates.csv',  import_listing_dates_sql, '上市日期'),
    ('bonds',         'bonds_full.csv',     import_bonds_full_sql,    '转债完整数据'),
    ('bonds_market',  'bonds_market.csv',   import_bonds_market_sql,  '转债最新行情'),
    ('putback',       'bond_putback_dates.csv', import_putback_dates_sql, '回售起始日'),
    ('fundamentals',  'fundamentals.csv',   import_fundamentals_sql,  '年度财务数据'),
    ('klines',        'klines.csv',         import_klines_sql,        'K线历史'),
    ('shareholders',  'shareholders.csv',   import_shareholders_sql,  '股东人数历史'),
    ('revise_history','revise_history.csv',  import_revise_history_sql,'下修事件历史'),
]

DAILY_MARKET_PATTERN = 'daily_market/*.csv'


def main():
    parser = argparse.ArgumentParser(
        description='从 cache CSV 重建 A股数据库',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--cache-dir', default=DEFAULT_CACHE,
                        help=f'Cache目录 (默认: {DEFAULT_CACHE})')
    parser.add_argument('--db', default=DEFAULT_DB,
                        help=f'数据库路径 (默认: {DEFAULT_DB})')
    parser.add_argument('--table', choices=[s[0] for s in IMPORT_STEPS] + ['analysis'],
                        help='仅重建指定表')
    parser.add_argument('--sql-only', action='store_true',
                        help='仅输出SQL不执行（DB锁定时用）')
    args = parser.parse_args()

    cache_dir = os.path.abspath(args.cache_dir)
    db_path = os.path.abspath(args.db)

    if not os.path.isdir(cache_dir):
        print(f"❌ Cache目录不存在: {cache_dir}")
        print(f"   请先运行: python .github/skills/db-manager/manage.py init")
        return 1

    print("=" * 60)
    print(f"  从 Cache 重建数据库")
    print(f"  Cache: {cache_dir}")
    print(f"  DB:    {db_path}")
    print("=" * 60)

    # Collect all SQL
    all_sql = []

    # 1. Schema
    if not args.table:
        all_sql.append(('创建表结构', SCHEMA_SQL))

    # 2. Import each table
    for name, cache_file, sql_func, desc in IMPORT_STEPS:
        if args.table and args.table != name:
            continue
        csv_path = os.path.join(cache_dir, cache_file)
        if not os.path.exists(csv_path):
            print(f"  ⚠️ 跳过 {desc}: {cache_file} 不存在")
            continue
        abs_path = os.path.abspath(csv_path)
        all_sql.append((desc, sql_func(abs_path)))

    # 3. Daily market snapshots → daily_market + klines
    if not args.table or args.table == 'stocks':
        dm_pattern = os.path.join(cache_dir, 'daily_market', '*.csv')
        dm_files = sorted(glob.glob(dm_pattern))
        if dm_files:
            # Use latest for daily_market
            latest = dm_files[-1]
            abs_latest = os.path.abspath(latest)
            all_sql.append(('最新日行情→daily_market',
                            import_daily_market_sql(abs_latest)))
            # All daily CSVs → klines (accumulate)
            for f in dm_files:
                abs_f = os.path.abspath(f)
                date_str = os.path.basename(f).replace('.csv', '')
                all_sql.append((f'日K线 {date_str}→klines',
                                import_klines_from_daily_sql(abs_f)))

    # 4. Fill stock_name in bonds
    if not args.table or args.table == 'bonds':
        all_sql.append(('填充 bonds.stock_name', FILL_STOCK_NAME_SQL))

    # 5. Analysis
    if not args.table or args.table == 'analysis':
        all_sql.append(('触发进度计算', TRIGGER_SQL))
        all_sql.append(('盈利状态计算', PROFITABILITY_SQL))

    if not all_sql:
        print("\n  无可导入的数据")
        return 1

    # SQL-only mode: just print
    if args.sql_only:
        sql_path = os.path.join(cache_dir, '..', 'rebuild.sql')
        with open(sql_path, 'w', encoding='utf-8') as f:
            for label, sql in all_sql:
                f.write(f"-- ═══ {label} ═══\n{sql}\n\n")
        print(f"\n  SQL 已保存: {sql_path}")
        print(f"  共 {len(all_sql)} 步，可通过 MCP mcp_duckdb_query 逐条执行")
        return 0

    # Execute
    try:
        import duckdb
    except ImportError:
        print("❌ 需要安装 duckdb: pip install duckdb")
        return 1

    try:
        conn = duckdb.connect(db_path)
    except Exception as e:
        if 'lock' in str(e).lower():
            print(f"\n  ⚠️ DB 锁定: {e}")
            print(f"  使用 --sql-only 生成SQL，通过MCP执行")
            return 1
        raise

    ok = 0
    failed = []
    for label, sql in all_sql:
        print(f"\n  ▸ {label}...")
        try:
            for stmt in sql.split(';'):
                # Strip comment lines, keep SQL
                lines = stmt.strip().splitlines()
                stmt = '\n'.join(l for l in lines if not l.strip().startswith('--')).strip()
                if stmt:
                    conn.execute(stmt)
            ok += 1
        except Exception as e:
            print(f"    ❌ {e}")
            failed.append(label)

    conn.close()

    # Summary
    print(f"\n{'=' * 60}")
    print(f"  重建完成: {ok}/{len(all_sql)} 步成功")
    if failed:
        print(f"  失败: {', '.join(failed)}")
    print(f"{'=' * 60}")

    # Quick verification
    try:
        conn = duckdb.connect(db_path, read_only=True)
        for tbl in ['stocks', 'bonds', 'klines', 'fundamentals',
                     'shareholders', 'revise_history', 'daily_market']:
            try:
                cnt = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
                print(f"  {tbl:20s} {cnt:>10,} 行")
            except Exception:
                pass
        conn.close()
    except Exception:
        pass

    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
