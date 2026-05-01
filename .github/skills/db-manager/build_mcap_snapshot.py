"""build_mcap_snapshot.py — 用 kline_daily 推算 market_cap_snapshot.

逻辑:
  total_mcap_cny ≈ close * outstanding_share，按 (code, MAX(date)) 取最新一根 K 线。
  outstanding_share 来自新浪 stock_zh_a_daily（单位：股）。
  float_mcap_cny 暂留 NULL（流通股≠总股本，需另接口；本工具仅近似总市值）。
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import duckdb

LOGGER = logging.getLogger("build_mcap_snapshot")
DEFAULT_DB = "data/a-share.db"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS market_cap_snapshot (
    code            VARCHAR NOT NULL,
    date            DATE NOT NULL,
    total_mcap_cny  DECIMAL(20,2),
    float_mcap_cny  DECIMAL(20,2),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (code, date)
);
CREATE INDEX IF NOT EXISTS idx_mcap_date ON market_cap_snapshot(date);
"""

BUILD_SQL = """
WITH latest AS (
    SELECT code, MAX(date) AS date
    FROM kline_daily
    WHERE outstanding_share IS NOT NULL
    GROUP BY code
)
SELECT
    k.code,
    k.date,
    CAST(k.close * k.outstanding_share AS DECIMAL(20,2)) AS total_mcap_cny
FROM kline_daily k
JOIN latest l ON l.code = k.code AND l.date = k.date
WHERE k.outstanding_share IS NOT NULL
  AND k.close IS NOT NULL
"""


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=DEFAULT_DB)
    p.add_argument("--log-level", default="INFO")
    args = p.parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )

    db = Path(args.db)
    if not db.exists():
        LOGGER.error("DB not found: %s", db)
        return 2

    con = duckdb.connect(str(db))
    try:
        con.execute(SCHEMA_SQL)
        rows = con.execute(BUILD_SQL).fetchdf()
        LOGGER.info("computed %d (code,date) snapshots", len(rows))
        if rows.empty:
            LOGGER.warning("no rows; market_cap_snapshot unchanged")
            return 0
        con.executemany(
            "DELETE FROM market_cap_snapshot WHERE code=? AND date=?",
            list(zip(rows["code"], rows["date"])),
        )
        con.executemany(
            "INSERT INTO market_cap_snapshot (code, date, total_mcap_cny) VALUES (?, ?, ?)",
            list(zip(rows["code"], rows["date"], rows["total_mcap_cny"])),
        )
        LOGGER.info("upserted %d rows", len(rows))
        ref = con.execute(
            "SELECT code, date, total_mcap_cny FROM market_cap_snapshot WHERE code='300401'"
        ).fetchone()
        if ref:
            yi = float(ref[2]) / 1e8
            LOGGER.info("300401 anchor: date=%s total_mcap=%.2f 亿元", ref[1], yi)
            if not (30 <= yi <= 200):
                LOGGER.warning("300401 mcap %.2f 亿 不在 [30,200] 区间", yi)
        else:
            LOGGER.warning("300401 missing in market_cap_snapshot")
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
