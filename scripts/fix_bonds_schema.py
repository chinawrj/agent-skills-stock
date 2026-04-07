#!/usr/bin/env python3
"""Fix bonds table - add missing columns lost from WAL corruption."""
import duckdb

con = duckdb.connect('data/a-share.db')

alter_stmts = [
    'ALTER TABLE bonds ADD COLUMN IF NOT EXISTS maturity_redemption_price DECIMAL(10,3)',
    'ALTER TABLE bonds ADD COLUMN IF NOT EXISTS coupon_rate_1 DECIMAL(6,3)',
    'ALTER TABLE bonds ADD COLUMN IF NOT EXISTS coupon_rate_2 DECIMAL(6,3)',
    'ALTER TABLE bonds ADD COLUMN IF NOT EXISTS coupon_rate_3 DECIMAL(6,3)',
    'ALTER TABLE bonds ADD COLUMN IF NOT EXISTS coupon_rate_4 DECIMAL(6,3)',
    'ALTER TABLE bonds ADD COLUMN IF NOT EXISTS coupon_rate_5 DECIMAL(6,3)',
    'ALTER TABLE bonds ADD COLUMN IF NOT EXISTS coupon_rate_6 DECIMAL(6,3)',
    'ALTER TABLE bonds ADD COLUMN IF NOT EXISTS coupon_rate_7 DECIMAL(6,3)',
    'ALTER TABLE bonds ADD COLUMN IF NOT EXISTS coupon_rate_8 DECIMAL(6,3)',
    'ALTER TABLE bonds ADD COLUMN IF NOT EXISTS coupon_rate_9 DECIMAL(6,3)',
    'ALTER TABLE bonds ADD COLUMN IF NOT EXISTS coupon_rate_10 DECIMAL(6,3)',
]

for stmt in alter_stmts:
    con.execute(stmt)
    col_name = stmt.split('IF NOT EXISTS ')[1].split(' ')[0]
    print(f'OK: {col_name}')

# Verify
cols = con.execute("""
    SELECT column_name FROM information_schema.columns
    WHERE table_name = 'bonds' ORDER BY ordinal_position
""").fetchall()
print(f'\nTotal columns: {len(cols)}')
con.close()
print('Done!')
