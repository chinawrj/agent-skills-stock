#!/usr/bin/env python3
"""
Rebuild the entire A-share database from CSV files.

All data was previously stored in the WAL which got corrupted.
This script recreates all tables and imports from existing CSV exports.
"""
import duckdb
import os
import sys

DB_PATH = '/Users/rjwang/fun/a-share/data/a-share.db'
DATA_DIR = '/Users/rjwang/fun/a-share/data'

# Remove old WAL if exists
wal_path = DB_PATH + '.wal'
if os.path.exists(wal_path):
    os.remove(wal_path)
    print(f"Removed stale WAL: {wal_path}")

con = duckdb.connect(DB_PATH)

# ============================================================
# Step 1: Create all missing tables
# ============================================================
print("=== Step 1: Creating tables ===")

con.execute("""
CREATE TABLE IF NOT EXISTS stocks (
    code VARCHAR NOT NULL PRIMARY KEY,
    name VARCHAR,
    market VARCHAR,
    listing_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

con.execute("""
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
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

con.execute("""
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
)
""")

con.execute("""
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
)
""")

# Create indexes
con.execute("CREATE INDEX IF NOT EXISTS idx_daily_market_date ON daily_market(trade_date)")
con.execute("CREATE INDEX IF NOT EXISTS idx_shareholders_announce ON shareholders(announce_date)")
con.execute("CREATE INDEX IF NOT EXISTS idx_shareholders_change ON shareholders(change_ratio)")

print("  Tables created OK")

# ============================================================
# Step 2: Import stocks from all_stocks.csv + listing_dates.csv
# ============================================================
print("\n=== Step 2: Importing stocks ===")
csv_path = os.path.join(DATA_DIR, 'all_stocks.csv')
if os.path.exists(csv_path):
    con.execute(f"""
        INSERT OR REPLACE INTO stocks (code, name, market)
        SELECT code, name, market FROM read_csv_auto('{csv_path}')
    """)
    cnt = con.execute("SELECT COUNT(*) FROM stocks").fetchone()[0]
    print(f"  stocks: {cnt} rows")

# Update listing dates
csv_path = os.path.join(DATA_DIR, 'listing_dates.csv')
if os.path.exists(csv_path):
    con.execute(f"""
        UPDATE stocks SET listing_date = TRY_CAST(src.listing_date AS DATE)
        FROM (SELECT code, listing_date FROM read_csv('{csv_path}', 
              columns={{'code': 'VARCHAR', 'name': 'VARCHAR', 'listing_date': 'VARCHAR'}}, 
              header=true, nullstr='')
              WHERE listing_date IS NOT NULL AND length(listing_date) >= 8) src
        WHERE stocks.code = src.code
    """)
    cnt = con.execute("SELECT COUNT(*) FROM stocks WHERE listing_date IS NOT NULL").fetchone()[0]
    print(f"  listing_date populated: {cnt}")

# ============================================================
# Step 3: Import shareholders from all_shareholders_history.csv
# ============================================================
print("\n=== Step 3: Importing shareholders ===")
csv_path = os.path.join(DATA_DIR, 'all_shareholders_history.csv')
if os.path.exists(csv_path):
    con.execute(f"""
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
        FROM read_csv_auto('{csv_path}', nullstr='')
    """)
    cnt = con.execute("SELECT COUNT(*) FROM shareholders").fetchone()[0]
    print(f"  shareholders: {cnt} rows")

# Import incremental shareholders
csv_path = os.path.join(DATA_DIR, 'shareholders_incremental.csv')
if os.path.exists(csv_path):
    con.execute(f"""
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
        FROM read_csv_auto('{csv_path}', nullstr='')
    """)
    cnt = con.execute("SELECT COUNT(*) FROM shareholders").fetchone()[0]
    print(f"  shareholders (after incremental): {cnt} rows")

# ============================================================
# Step 4: Import fundamentals from fundamentals_annual.csv
# ============================================================
print("\n=== Step 4: Importing fundamentals ===")
csv_path = os.path.join(DATA_DIR, 'fundamentals_annual.csv')
if os.path.exists(csv_path):
    # Clear old tiny dataset and replace with full annual data
    con.execute("DELETE FROM fundamentals")
    con.execute(f"""
        INSERT INTO fundamentals (code, name, report_date, report_type, eps, roe, net_profit, revenue, profit_yoy, revenue_yoy)
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
        FROM read_csv_auto('{csv_path}', nullstr='')
    """)
    cnt = con.execute("SELECT COUNT(*) FROM fundamentals").fetchone()[0]
    print(f"  fundamentals: {cnt} rows")

# ============================================================
# Step 5: Import klines from klines_daily.csv (64MB, ~1.2M rows)
# ============================================================
print("\n=== Step 5: Importing klines (historical) ===")
csv_path = os.path.join(DATA_DIR, 'klines_daily.csv')
if os.path.exists(csv_path):
    con.execute(f"""
        INSERT OR IGNORE INTO klines (code, trade_date, open, high, low, close, volume, amount)
        SELECT 
            code, TRY_CAST(trade_date AS DATE),
            TRY_CAST(open AS DECIMAL(10,2)),
            TRY_CAST(high AS DECIMAL(10,2)),
            TRY_CAST(low AS DECIMAL(10,2)),
            TRY_CAST(close AS DECIMAL(10,2)),
            TRY_CAST(volume AS BIGINT),
            NULL
        FROM read_csv_auto('{csv_path}', nullstr='')
        WHERE TRY_CAST(close AS DECIMAL(10,2)) IS NOT NULL
    """)
    cnt = con.execute("SELECT COUNT(*) FROM klines").fetchone()[0]
    print(f"  klines (from klines_daily.csv): {cnt} rows")

# Import 3/17 OHLCV (Baostock full data)
csv_path = os.path.join(DATA_DIR, 'klines_ohlcv_2026-03-17.csv')
if os.path.exists(csv_path):
    con.execute(f"""
        INSERT OR REPLACE INTO klines (code, trade_date, open, high, low, close, volume, amount)
        SELECT 
            code, TRY_CAST(trade_date AS DATE),
            TRY_CAST(open AS DECIMAL(10,2)),
            TRY_CAST(high AS DECIMAL(10,2)),
            TRY_CAST(low AS DECIMAL(10,2)),
            TRY_CAST(close AS DECIMAL(10,2)),
            TRY_CAST(volume AS BIGINT),
            TRY_CAST(amount AS DECIMAL(18,2))
        FROM read_csv('{csv_path}', 
             columns={{'code':'VARCHAR','trade_date':'VARCHAR','open':'VARCHAR','high':'VARCHAR',
                       'low':'VARCHAR','close':'VARCHAR','volume':'VARCHAR','amount':'VARCHAR'}},
             header=true, nullstr='')
        WHERE length(trim(volume)) > 0
    """)
    cnt = con.execute("SELECT COUNT(*) FROM klines WHERE trade_date = '2026-03-17'").fetchone()[0]
    print(f"  klines 2026-03-17: {cnt} rows")

# Import 3/18 from daily_market CSV → klines
csv_path = os.path.join(DATA_DIR, 'daily_market_2026-03-18.csv')
if os.path.exists(csv_path):
    con.execute(f"""
        INSERT OR IGNORE INTO klines (code, trade_date, open, high, low, close, volume, amount)
        SELECT 
            code, TRY_CAST(trade_date AS DATE),
            TRY_CAST(open AS DECIMAL(10,2)),
            TRY_CAST(high AS DECIMAL(10,2)),
            TRY_CAST(low AS DECIMAL(10,2)),
            TRY_CAST(close AS DECIMAL(10,2)),
            TRY_CAST(volume AS BIGINT),
            TRY_CAST(amount AS DECIMAL(18,2))
        FROM read_csv_auto('{csv_path}', nullstr='')
        WHERE TRY_CAST(close AS DECIMAL(10,2)) > 0 AND TRY_CAST(volume AS BIGINT) > 0
    """)
    cnt = con.execute("SELECT COUNT(*) FROM klines WHERE trade_date = '2026-03-18'").fetchone()[0]
    print(f"  klines 2026-03-18: {cnt} rows")

total_klines = con.execute("SELECT COUNT(*) FROM klines").fetchone()[0]
print(f"  klines total: {total_klines} rows")

# ============================================================
# Step 6: Import daily_market
# ============================================================
print("\n=== Step 6: Importing daily_market ===")
csv_path = os.path.join(DATA_DIR, 'daily_market_2026-03-18.csv')
if os.path.exists(csv_path):
    con.execute(f"""
        INSERT OR REPLACE INTO daily_market
        (code, name, trade_date, open, high, low, close, prev_close,
         change_amount, change_pct, volume, amount, turnover_rate, 
         pe_dynamic, pb, total_mv, circ_mv)
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
        FROM read_csv_auto('{csv_path}', nullstr='')
    """)
    cnt = con.execute("SELECT COUNT(*) FROM daily_market").fetchone()[0]
    print(f"  daily_market: {cnt} rows")

# ============================================================
# Step 7: Fetch and import bonds (fresh from API)
# ============================================================
print("\n=== Step 7: Importing bonds ===")

# We need to fetch bonds fresh since we don't have a full bonds CSV
# The bond_scores.csv has basic info, bond_market.csv has prices
# Let's use the daily_update.py fetch_bonds approach

sys.path.insert(0, '/Users/rjwang/fun/a-share/db')
from update_bond_market import fetch_bond_data

raw_bonds = fetch_bond_data()
if raw_bonds:
    import re
    from datetime import datetime
    
    def parse_trigger(clause, trigger_type):
        """Parse trigger conditions from clause text."""
        if not clause:
            return None, None, None
        # Common: "连续30个交易日中至少15个交易日的收盘价低于当期转股价格的85%"
        m = re.search(r'连续(\d+)个交易日中[^\d]*(\d+)个交易日.*?(\d+)%', clause)
        if m:
            return float(m.group(3)), int(m.group(2)), int(m.group(1))
        return None, None, None
    
    for raw in raw_bonds:
        code = raw.get('SECURITY_CODE', '')
        if not code:
            continue
        
        stock_code = raw.get('CONVERT_STOCK_CODE', '')
        
        # Parse redeem trigger info
        redeem_trig = raw.get('REDEEM_TRIG_PRICE')
        resale_trig = raw.get('RESALE_TRIG_PRICE')
        
        expire = str(raw.get('EXPIRE_DATE', ''))[:10] or None
        listing = str(raw.get('LISTING_DATE', ''))[:10] or None
        delist = str(raw.get('DELIST_DATE', ''))[:10] or None
        if listing == 'None': listing = None
        if delist == 'None': delist = None
        if expire == 'None': expire = None
        
        tp = raw.get('TRANSFER_PRICE')
        ip = raw.get('INITIAL_TRANSFER_PRICE')
        issue_size = raw.get('ACTUAL_ISSUE_SCALE')
        
        con.execute("""
            INSERT OR REPLACE INTO bonds 
            (bond_code, bond_name, stock_code, stock_name, issue_size, rating,
             maturity_date, convert_price, original_price,
             redeem_pct, redeem_days, redeem_window,
             putback_pct, putback_days, putback_window,
             revise_pct, revise_days, revise_window,
             listing_date, delist_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,
                    130, 15, 30, 70, 30, 30, 85, 15, 30, ?, ?)
        """, [
            code,
            raw.get('SECURITY_NAME_ABBR'),
            stock_code,
            None,  # stock_name (will be filled from stocks table)
            float(issue_size) if issue_size else None,
            raw.get('RATING'),
            expire,
            float(tp) if tp else (float(ip) if ip else None),
            float(ip) if ip else None,
            listing,
            delist,
        ])
    
    # Fill stock_name from stocks table
    con.execute("""
        UPDATE bonds SET stock_name = s.name
        FROM stocks s WHERE bonds.stock_code = s.code
    """)
    
    cnt = con.execute("SELECT COUNT(*) FROM bonds").fetchone()[0]
    active = con.execute("""
        SELECT COUNT(*) FROM bonds 
        WHERE listing_date IS NOT NULL AND (delist_date IS NULL OR delist_date > CURRENT_DATE)
    """).fetchone()[0]
    print(f"  bonds: {cnt} total, {active} active")

# Update bond market data from CSV
csv_path = os.path.join(DATA_DIR, 'bond_market.csv')
if os.path.exists(csv_path):
    con.execute(f"""
        UPDATE bonds SET
            bond_price = src.bond_price,
            convert_value = src.convert_value,
            premium_rate = src.premium_rate,
            ytm = src.ytm,
            remaining_size = src.remaining_size,
            convert_price = COALESCE(src.convert_price, bonds.convert_price),
            updated_at = CURRENT_TIMESTAMP
        FROM (
            SELECT
                bond_code,
                TRY_CAST(bond_price AS DECIMAL(10,3)) AS bond_price,
                TRY_CAST(convert_value AS DECIMAL(10,3)) AS convert_value,
                TRY_CAST(premium_rate AS DECIMAL(10,4)) AS premium_rate,
                TRY_CAST(ytm AS DECIMAL(10,4)) AS ytm,
                TRY_CAST(remaining_size AS DECIMAL(12,4)) AS remaining_size,
                TRY_CAST(convert_price AS DECIMAL(10,3)) AS convert_price
            FROM read_csv_auto('{csv_path}', nullstr='')
        ) AS src
        WHERE bonds.bond_code = src.bond_code
    """)
    priced = con.execute("SELECT COUNT(*) FROM bonds WHERE bond_price IS NOT NULL").fetchone()[0]
    print(f"  bonds with price: {priced}")

# ============================================================
# Step 8: Run trigger analysis + profitability
# ============================================================
print("\n=== Step 8: Running analysis ===")

# Trigger counts
con.execute("""
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
    WHERE bonds.bond_code = tc.bond_code
""")
trigger_cnt = con.execute("SELECT COUNT(*) FROM bonds WHERE revise_trigger_count IS NOT NULL").fetchone()[0]
print(f"  trigger analysis: {trigger_cnt} bonds")

# Profitability
con.execute("""
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
    WHERE bonds.stock_code = lm.code
""")
profit_cnt = con.execute("SELECT COUNT(*) FROM bonds WHERE is_profitable IS NOT NULL").fetchone()[0]
print(f"  profitability: {profit_cnt} bonds")

# ============================================================
# Final verification
# ============================================================
print("\n=== Final State ===")
tables = con.execute("SHOW TABLES").fetchall()
for t in tables:
    cnt = con.execute(f'SELECT COUNT(*) FROM "{t[0]}"').fetchone()[0]
    print(f"  {t[0]}: {cnt} rows")

# klines date range
r = con.execute("""
    SELECT MIN(trade_date), MAX(trade_date), COUNT(DISTINCT trade_date) 
    FROM klines
""").fetchone()
print(f"\n  klines: {r[0]} to {r[1]}, {r[2]} trading days")

# Recent klines
rows = con.execute("""
    SELECT trade_date, COUNT(*) cnt, COUNT(CASE WHEN volume IS NOT NULL THEN 1 END) as vol
    FROM klines WHERE trade_date >= '2026-03-14' 
    GROUP BY trade_date ORDER BY trade_date
""").fetchall()
for r in rows:
    print(f"    {r[0]}: {r[1]} rows ({r[2]} with volume)")

# Checkpoint
con.execute("CHECKPOINT")
con.close()

# Make a proper backup
import shutil
backup_path = DB_PATH + '.backup_full_20260318'
shutil.copy2(DB_PATH, backup_path)
print(f"\n  Backup saved: {backup_path}")
print(f"  Size: {os.path.getsize(backup_path)/1024/1024:.1f} MB")
print("\nDone!")
