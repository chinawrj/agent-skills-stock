#!/usr/bin/env python3
"""
Update bond analysis data: profitability metrics + trigger tracking.

Computes from existing DB data (fundamentals + klines tables):
- Profitability: is_profitable, consecutive_profit_years, latest_roe, latest_net_profit
- Triggers: revise_trigger_count, putback_trigger_count, redeem_trigger_count, stock_price_latest

卡书框架: MUST-HAVE #2 质地安全 + NICE-TO-HAVE 下修/强赎预期

Usage:
    cd /Users/rjwang/fun/a-share && source .venv/bin/activate

    # Full update (profitability + triggers)
    python db/update_bond_analysis.py

    # Only profitability
    python db/update_bond_analysis.py --profitability-only

    # Only triggers
    python db/update_bond_analysis.py --triggers-only

    # Dry run (show what would be updated)
    python db/update_bond_analysis.py --dry-run
"""

import argparse
import os
import sys
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
DB_PATH = os.path.abspath(os.path.join(DATA_DIR, 'a-share.db'))

PROFITABILITY_SQL = """
WITH annual_data AS (
    SELECT code,
           EXTRACT(YEAR FROM report_date) AS yr,
           net_profit, roe
    FROM fundamentals
    WHERE report_type = '年报'
),
latest_year AS (
    SELECT code, MAX(yr) AS max_yr FROM annual_data GROUP BY code
),
latest_metrics AS (
    SELECT a.code, a.net_profit, a.roe
    FROM annual_data a
    JOIN latest_year l ON a.code = l.code AND a.yr = l.max_yr
),
profit_check AS (
    SELECT a.code, a.yr,
           CASE WHEN a.net_profit > 0 THEN 1 ELSE 0 END AS is_profit,
           l.max_yr
    FROM annual_data a
    JOIN latest_year l ON a.code = l.code
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
    FROM profit_check
    GROUP BY code, max_yr
)
UPDATE bonds
SET is_profitable = (lm.net_profit > 0),
    latest_net_profit = lm.net_profit,
    latest_roe = lm.roe,
    consecutive_profit_years = c.consec_years
FROM latest_metrics lm
JOIN consecutive c ON lm.code = c.code
WHERE bonds.stock_code = lm.code
"""

TRIGGERS_SQL = """
WITH recent_klines AS (
    SELECT k.code, k.trade_date, k.close,
           ROW_NUMBER() OVER (PARTITION BY k.code ORDER BY k.trade_date DESC) AS rn
    FROM klines k
    JOIN (SELECT DISTINCT stock_code FROM bonds WHERE maturity_date > CURRENT_DATE) b
      ON k.code = b.stock_code
),
latest_price AS (
    SELECT code, close AS latest_close
    FROM recent_klines
    WHERE rn = 1
),
trigger_counts AS (
    SELECT rk.code,
           b.bond_code,
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
"""


def run_sql(conn, sql, label):
    """Execute SQL and return affected row count."""
    result = conn.execute(sql)
    count = result.fetchone()[0] if result.description else 0
    print(f"  {label}: {count} rows updated")
    return count


def preview_profitability(conn):
    """Show what profitability update would produce."""
    sql = """
    WITH annual_data AS (
        SELECT code, EXTRACT(YEAR FROM report_date) AS yr, net_profit
        FROM fundamentals WHERE report_type = '年报'
    ),
    latest_year AS (
        SELECT code, MAX(yr) AS max_yr FROM annual_data GROUP BY code
    ),
    latest_metrics AS (
        SELECT a.code, a.net_profit
        FROM annual_data a
        JOIN latest_year l ON a.code = l.code AND a.yr = l.max_yr
    )
    SELECT
        COUNT(*) AS total,
        COUNT(CASE WHEN lm.net_profit > 0 THEN 1 END) AS profitable,
        COUNT(CASE WHEN lm.net_profit <= 0 THEN 1 END) AS loss_making
    FROM latest_metrics lm
    JOIN (SELECT DISTINCT stock_code FROM bonds WHERE maturity_date > CURRENT_DATE) bs
      ON lm.code = bs.stock_code
    """
    row = conn.execute(sql).fetchone()
    print(f"  Profitability preview: {row[0]} stocks ({row[1]} profitable, {row[2]} loss-making)")


def preview_triggers(conn):
    """Show what trigger update would produce."""
    sql = """
    SELECT COUNT(DISTINCT b.stock_code)
    FROM bonds b
    JOIN (SELECT DISTINCT code FROM klines) k ON b.stock_code = k.code
    WHERE b.maturity_date > CURRENT_DATE
    """
    row = conn.execute(sql).fetchone()
    print(f"  Trigger preview: {row[0]} bond stocks have kline data")


def main():
    parser = argparse.ArgumentParser(description='Update bond analysis (profitability + triggers)')
    parser.add_argument('--dry-run', action='store_true', help='Preview only')
    parser.add_argument('--profitability-only', action='store_true', help='Only update profitability')
    parser.add_argument('--triggers-only', action='store_true', help='Only update triggers')
    args = parser.parse_args()

    do_profit = not args.triggers_only
    do_triggers = not args.profitability_only

    print("=" * 60)
    print(f"Bond Analysis Update - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    try:
        import duckdb
    except ImportError:
        print("ERROR: duckdb not installed. Run: pip install duckdb")
        sys.exit(1)

    try:
        conn = duckdb.connect(DB_PATH)
    except Exception as e:
        if 'lock' in str(e).lower():
            print(f"\n⚠️  DB locked. Use MCP DuckDB tools or wait for lock release.")
            print(f"  Profitability SQL saved to: data/profitability_update.sql")
            print(f"  Trigger SQL saved to: data/trigger_update.sql")
            if do_profit:
                with open(os.path.join(DATA_DIR, 'profitability_update.sql'), 'w') as f:
                    f.write(PROFITABILITY_SQL)
            if do_triggers:
                with open(os.path.join(DATA_DIR, 'trigger_update.sql'), 'w') as f:
                    f.write(TRIGGERS_SQL)
            sys.exit(1)
        raise

    if args.dry_run:
        if do_profit:
            preview_profitability(conn)
        if do_triggers:
            preview_triggers(conn)
        conn.close()
        print("\nDry run complete.")
        return

    # Ensure columns exist
    for col_def in [
        ('is_profitable', 'BOOLEAN'),
        ('consecutive_profit_years', 'INTEGER'),
        ('latest_roe', 'DECIMAL(10,4)'),
        ('latest_net_profit', 'DECIMAL(18,2)'),
        ('revise_trigger_count', 'INTEGER'),
        ('putback_trigger_count', 'INTEGER'),
        ('redeem_trigger_count', 'INTEGER'),
        ('stock_price_latest', 'DECIMAL(10,4)'),
    ]:
        try:
            conn.execute(f"ALTER TABLE bonds ADD COLUMN {col_def[0]} {col_def[1]}")
            print(f"  Added column: {col_def[0]}")
        except Exception:
            pass  # column already exists

    if do_profit:
        print("\n[1/2] Updating profitability...")
        run_sql(conn, PROFITABILITY_SQL, "Profitability")

    if do_triggers:
        print("\n[2/2] Updating trigger tracking...")
        run_sql(conn, TRIGGERS_SQL, "Triggers")

    # Log update
    conn.execute("""
        INSERT INTO data_updates (table_name, update_type, records_count, notes)
        VALUES ('bonds', 'analysis_update', 0,
                'profitability + trigger tracking updated')
    """)

    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
