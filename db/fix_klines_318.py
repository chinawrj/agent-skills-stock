#!/usr/bin/env python3
"""Fix 3/18 klines data - replace wrong data with correct post-market data."""
import duckdb

con = duckdb.connect('data/a-share.db')

# Check current state
cnt = con.execute("SELECT COUNT(*) FROM klines WHERE trade_date = '2026-03-18'").fetchone()[0]
print(f'Existing 3/18 klines: {cnt}')

if cnt > 0:
    con.execute("DELETE FROM klines WHERE trade_date = '2026-03-18'")
    print(f'  Deleted {cnt} wrong rows')

# Import correct 3/18 data
con.execute("""
INSERT OR REPLACE INTO klines (code, trade_date, open, high, low, close, volume, amount)
SELECT
    code, TRY_CAST(trade_date AS DATE),
    TRY_CAST(open AS DECIMAL(10,2)),
    TRY_CAST(high AS DECIMAL(10,2)),
    TRY_CAST(low AS DECIMAL(10,2)),
    TRY_CAST(close AS DECIMAL(10,2)),
    TRY_CAST(volume AS BIGINT),
    TRY_CAST(amount AS DECIMAL(18,2))
FROM read_csv_auto('data/daily_market_2026-03-18.csv', nullstr='')
WHERE TRY_CAST(close AS DECIMAL(10,2)) > 0
  AND TRY_CAST(volume AS BIGINT) > 0
""")
cnt = con.execute("SELECT COUNT(*) FROM klines WHERE trade_date = '2026-03-18'").fetchone()[0]
print(f'After import 3/18 klines: {cnt}')

# Verify 花园生物
row = con.execute("""
    SELECT code, trade_date, open, high, low, close, volume 
    FROM klines WHERE code = '300401' AND trade_date >= '2026-03-16'
    ORDER BY trade_date
""").fetchall()
for r in row:
    print(f'  {r[0]} {r[1]}: O={r[2]} H={r[3]} L={r[4]} C={r[5]} V={r[6]}')

con.execute('CHECKPOINT')
con.close()
print('Done')
