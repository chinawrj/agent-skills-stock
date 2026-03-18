#!/usr/bin/env python3
"""Check DB state and recover: import 3/17 OHLCV + 3/18 daily data."""
import duckdb
import os
import sys
import csv

DB = 'data/a-share.db'

def check_state(con):
    """Print current DB state."""
    print("=== DB State ===")
    
    rows = con.execute("""
        SELECT trade_date, COUNT(*) cnt,
               COUNT(CASE WHEN volume IS NOT NULL THEN 1 END) as has_vol
        FROM klines WHERE trade_date >= '2026-03-14' 
        GROUP BY trade_date ORDER BY trade_date
    """).fetchall()
    print("klines recent dates:")
    for r in rows:
        print(f"  {r[0]}: {r[1]} rows ({r[2]} with volume)")
    
    r = con.execute("SELECT COUNT(*) FROM klines").fetchone()
    print(f"klines total: {r[0]}")
    
    r = con.execute("SELECT COUNT(*) FROM daily_market").fetchone()
    print(f"daily_market: {r[0]}")
    
    r = con.execute("SELECT COUNT(*) FROM bonds").fetchone()
    print(f"bonds: {r[0]}")
    
    r = con.execute("SELECT COUNT(*) FROM bonds WHERE bond_price IS NOT NULL").fetchone()
    print(f"bonds with price: {r[0]}")
    
    r = con.execute("SELECT COUNT(*) FROM bonds WHERE revise_trigger_count IS NOT NULL").fetchone()
    print(f"bonds with triggers: {r[0]}")

def import_ohlcv(con, csv_path):
    """Import OHLCV data from Baostock CSV."""
    print(f"\n=== Importing OHLCV from {csv_path} ===")
    
    # Count rows first
    with open(csv_path) as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        total = sum(1 for _ in reader)
    print(f"CSV has {total} rows")
    
    # Delete any existing 3/17 data
    r = con.execute("DELETE FROM klines WHERE trade_date = '2026-03-17'").fetchone()
    print(f"Deleted existing 3/17 rows: {r[0]}")
    
    # Import from CSV
    con.execute(f"""
        INSERT INTO klines (code, trade_date, open, high, low, close, volume, amount)
        SELECT code, trade_date::DATE, 
               open::DOUBLE, high::DOUBLE, low::DOUBLE, close::DOUBLE,
               CASE WHEN volume = '' OR volume IS NULL THEN NULL ELSE volume::BIGINT END,
               CASE WHEN amount = '' OR amount IS NULL THEN NULL ELSE amount::DECIMAL(18,2) END
        FROM read_csv_auto('{csv_path}')
    """)
    
    r = con.execute("SELECT COUNT(*) FROM klines WHERE trade_date = '2026-03-17'").fetchone()
    print(f"Imported: {r[0]} rows for 3/17")
    
    # Verify sample
    rows = con.execute("""
        SELECT code, open, high, low, close, volume 
        FROM klines WHERE trade_date = '2026-03-17' AND code IN ('600519', '000001', '601318')
        ORDER BY code
    """).fetchall()
    for r in rows:
        print(f"  {r[0]}: O={r[1]} H={r[2]} L={r[3]} C={r[4]} V={r[5]}")

def import_daily_sql(con, sql_path):
    """Import daily market/klines from SQL file."""
    if not os.path.exists(sql_path):
        print(f"\n{sql_path} not found, skipping")
        return
    print(f"\n=== Importing SQL from {sql_path} ===")
    sql = open(sql_path).read()
    statements = [s.strip() for s in sql.split(';') if s.strip()]
    for i, stmt in enumerate(statements):
        if not stmt:
            continue
        try:
            con.execute(stmt)
            tbl = "klines" if "klines" in stmt.lower() else "daily_market" if "daily_market" in stmt.lower() else "unknown"
            print(f"  Statement {i+1}: OK ({tbl})")
        except Exception as e:
            print(f"  Statement {i+1}: ERROR - {e}")

def run_trigger_analysis(con):
    """Re-compute trigger counts for bonds."""
    print("\n=== Running trigger analysis ===")
    r = con.execute("""
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
    """).fetchone()
    print(f"Updated {r[0]} bonds with trigger counts")

def main():
    action = sys.argv[1] if len(sys.argv) > 1 else 'all'
    
    con = duckdb.connect(DB)
    
    if action in ('check', 'all'):
        check_state(con)
    
    if action in ('import', 'all'):
        ohlcv_csv = 'data/klines_ohlcv_2026-03-17.csv'
        if os.path.exists(ohlcv_csv):
            import_ohlcv(con, ohlcv_csv)
        else:
            print(f"WARNING: {ohlcv_csv} not found!")
        
        # Also import 3/18 daily SQL if exists
        daily_sql = 'data/daily_update_2026-03-18.sql'
        import_daily_sql(con, daily_sql)
    
    if action in ('triggers', 'all'):
        run_trigger_analysis(con)
    
    if action in ('verify', 'all'):
        print("\n=== Final verification ===")
        check_state(con)
    
    con.execute("CHECKPOINT")
    con.close()
    print("\nDone. DB checkpointed.")

if __name__ == '__main__':
    main()
