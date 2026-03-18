#!/usr/bin/env python3
"""
Fix 3/18 data: the original daily_market_2026-03-18.csv was captured pre-market
and contained 3/17 data mislabeled as 3/18. Replace with correct post-market data.
"""
import duckdb
import os

DB_PATH = '/Users/rjwang/fun/a-share/data/a-share.db'
CSV_PATH = '/Users/rjwang/fun/a-share/data/daily_market_2026-03-18.csv'

con = duckdb.connect(DB_PATH)

# Step 1: Check current state
old_klines = con.execute("SELECT COUNT(*) FROM klines WHERE trade_date = '2026-03-18'").fetchone()[0]
old_dm = con.execute("SELECT COUNT(*) FROM daily_market WHERE trade_date = '2026-03-18'").fetchone()[0]
print(f"Before: klines 3/18={old_klines}, daily_market 3/18={old_dm}")

# Verify: show current (wrong) data for 花园生物
wrong = con.execute("SELECT open, high, low, close, volume FROM klines WHERE code='300401' AND trade_date='2026-03-18'").fetchone()
print(f"  花园生物 klines (WRONG): O={wrong[0]} H={wrong[1]} L={wrong[2]} C={wrong[3]} V={wrong[4]}")

# Step 2: Delete wrong 3/18 klines
con.execute("DELETE FROM klines WHERE trade_date = '2026-03-18'")
deleted = con.execute("SELECT COUNT(*) FROM klines WHERE trade_date = '2026-03-18'").fetchone()[0]
print(f"\nDeleted wrong klines, remaining 3/18: {deleted}")

# Step 3: Delete wrong daily_market
con.execute("DELETE FROM daily_market WHERE trade_date = '2026-03-18'")

# Step 4: Import correct daily_market from new CSV
con.execute(f"""
    INSERT INTO daily_market
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
    FROM read_csv_auto('{CSV_PATH}', nullstr='')
""")
new_dm = con.execute("SELECT COUNT(*) FROM daily_market").fetchone()[0]
print(f"Imported daily_market: {new_dm} rows")

# Step 5: Import correct klines from same CSV
con.execute(f"""
    INSERT INTO klines (code, trade_date, open, high, low, close, volume, amount)
    SELECT 
        code, TRY_CAST(trade_date AS DATE),
        TRY_CAST(open AS DECIMAL(10,2)),
        TRY_CAST(high AS DECIMAL(10,2)),
        TRY_CAST(low AS DECIMAL(10,2)),
        TRY_CAST(close AS DECIMAL(10,2)),
        TRY_CAST(volume AS BIGINT),
        TRY_CAST(amount AS DECIMAL(18,2))
    FROM read_csv_auto('{CSV_PATH}', nullstr='')
    WHERE TRY_CAST(close AS DECIMAL(10,2)) > 0 AND TRY_CAST(volume AS BIGINT) > 0
""")
new_klines = con.execute("SELECT COUNT(*) FROM klines WHERE trade_date = '2026-03-18'").fetchone()[0]
print(f"Imported klines 3/18: {new_klines} rows")

# Step 6: Verify 花园生物
correct = con.execute("SELECT open, high, low, close, volume FROM klines WHERE code='300401' AND trade_date='2026-03-18'").fetchone()
print(f"\n  花园生物 klines (CORRECT): O={correct[0]} H={correct[1]} L={correct[2]} C={correct[3]} V={correct[4]}")

dm_check = con.execute("SELECT open, close, prev_close FROM daily_market WHERE code='300401'").fetchone()
print(f"  花园生物 daily_market: O={dm_check[0]} C={dm_check[1]} prev={dm_check[2]}")

# Step 7: CHECKPOINT to persist
con.execute("CHECKPOINT")
con.close()
print("\nCHECKPOINT done. Fix complete.")
