"""hkscc_quarterly.py — 日级港中结持股 → 季度末快照.

按 hkscc-screener SKILL.md「季度末快照」节：
  对每个 (code, quarter)，取该季度内**最后一个交易日**的 HKSCC 持仓作为代表。
  使用 pd.Period('Q') 做季度归档，避免月份/财年口径歧义。

依赖: duckdb, pandas
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import duckdb
import pandas as pd

DEFAULT_DB = "data/a-share.db"
SRC_TABLE = "hkscc_holdings"
DST_TABLE = "hkscc_quarterly"
LOGGER = logging.getLogger("hkscc_quarterly")


def _ensure_dst_schema(con: duckdb.DuckDBPyConnection) -> None:
    """目标表：每只股每个季度最多 1 行。"""
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {DST_TABLE} (
            code                   VARCHAR NOT NULL,
            quarter                VARCHAR NOT NULL,   -- 形如 '2024Q3'
            quarter_end            DATE NOT NULL,      -- 该季度内最后一个有持仓数据的交易日
            holding_shares         BIGINT,
            holding_ratio          DECIMAL(10,6),
            holding_market_cap_cny DECIMAL(20,2),
            created_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (code, quarter)
        )
        """
    )


def quarterize(df: pd.DataFrame) -> pd.DataFrame:
    """日级 → 季度末快照.

    输入字段: code, date, holding_shares, holding_ratio, holding_market_cap_cny
    输出字段: code, quarter (str '2024Q3'), quarter_end (date), 三个持仓字段
    """
    if df.empty:
        return df.assign(quarter=pd.Series(dtype=str), quarter_end=pd.Series(dtype="datetime64[ns]"))

    work = df.copy()
    work["date"] = pd.to_datetime(work["date"])
    work["_q"] = work["date"].dt.to_period("Q")

    # 在每个 (code, _q) 取 date 最大那一行
    idx = work.groupby(["code", "_q"])["date"].idxmax()
    snap = work.loc[idx].copy()
    snap["quarter"] = snap["_q"].astype(str)
    snap["quarter_end"] = snap["date"].dt.date
    out = snap[
        [
            "code",
            "quarter",
            "quarter_end",
            "holding_shares",
            "holding_ratio",
            "holding_market_cap_cny",
        ]
    ].reset_index(drop=True)
    return out


def _upsert(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    con.register("_stage", df)
    con.execute(
        f"""
        INSERT INTO {DST_TABLE}
            (code, quarter, quarter_end, holding_shares, holding_ratio, holding_market_cap_cny)
        SELECT code, quarter, quarter_end, holding_shares, holding_ratio, holding_market_cap_cny
        FROM _stage
        ON CONFLICT (code, quarter) DO UPDATE SET
            quarter_end = EXCLUDED.quarter_end,
            holding_shares = EXCLUDED.holding_shares,
            holding_ratio = EXCLUDED.holding_ratio,
            holding_market_cap_cny = EXCLUDED.holding_market_cap_cny
        """
    )
    con.unregister("_stage")
    return len(df)


def cmd_run(args: argparse.Namespace) -> int:
    """从 DuckDB 的 hkscc_holdings 读全量 → 季度化 → upsert 到 hkscc_quarterly."""
    db_path = Path(args.db)
    if not db_path.exists():
        LOGGER.error("DB 不存在: %s（先运行 fetch_hkscc.py）", db_path)
        return 2
    con = duckdb.connect(str(db_path))
    _ensure_dst_schema(con)
    raw = con.execute(
        f"SELECT code, date, holding_shares, holding_ratio, holding_market_cap_cny FROM {SRC_TABLE}"
    ).fetchdf()
    LOGGER.info("源表 %s 行数: %d", SRC_TABLE, len(raw))
    snap = quarterize(raw)
    LOGGER.info("季度化后行数: %d (covered codes=%d)", len(snap), snap["code"].nunique() if not snap.empty else 0)
    n = _upsert(con, snap)
    LOGGER.info("upsert 写入 %s: %d 行", DST_TABLE, n)
    return 0


def cmd_self_test(args: argparse.Namespace) -> int:
    """合成多季度数据，校验：
    1) (code, quarter) 唯一
    2) 季度末日期取该季度最大日
    3) 跨年季度也正确
    """
    rows = [
        # 300401 — 2024Q1 三天，2024Q2 两天，2024Q3 一天，2024Q4 两天
        ("300401", "2024-01-15", 100, 1.0, 1_000_000),
        ("300401", "2024-02-20", 110, 1.1, 1_100_000),
        ("300401", "2024-03-29", 120, 1.2, 1_200_000),  # 2024Q1 末
        ("300401", "2024-05-10", 130, 1.3, 1_300_000),
        ("300401", "2024-06-28", 140, 1.4, 1_400_000),  # 2024Q2 末
        ("300401", "2024-09-30", 150, 1.5, 1_500_000),  # 2024Q3 末
        ("300401", "2024-12-15", 160, 1.6, 1_600_000),
        ("300401", "2024-12-31", 170, 1.7, 1_700_000),  # 2024Q4 末
        # 000001 — 2025Q1 一天，跨年校验
        ("000001", "2025-01-02", 9, 0.1, 90_000),
        ("000001", "2025-03-31", 8, 0.08, 80_000),       # 2025Q1 末
    ]
    df = pd.DataFrame(
        rows,
        columns=["code", "date", "holding_shares", "holding_ratio", "holding_market_cap_cny"],
    )
    snap = quarterize(df)
    LOGGER.info("self-test snapshot:\n%s", snap.to_string(index=False))

    assert len(snap) == 5, f"应当 5 个 (code,quarter)，实际 {len(snap)}"
    assert snap.duplicated(["code", "quarter"]).sum() == 0, "(code,quarter) 不唯一"

    g_q1 = snap.query("code=='300401' and quarter=='2024Q1'").iloc[0]
    assert str(g_q1["quarter_end"]) == "2024-03-29", f"2024Q1 末应是 03-29 实际 {g_q1['quarter_end']}"
    assert g_q1["holding_shares"] == 120

    g_q4 = snap.query("code=='300401' and quarter=='2024Q4'").iloc[0]
    assert str(g_q4["quarter_end"]) == "2024-12-31"
    assert g_q4["holding_shares"] == 170

    pa = snap.query("code=='000001' and quarter=='2025Q1'").iloc[0]
    assert str(pa["quarter_end"]) == "2025-03-31"

    # 用内存 DuckDB 走一次 upsert 链路
    con = duckdb.connect(":memory:")
    _ensure_dst_schema(con)
    n = _upsert(con, snap)
    n2 = _upsert(con, snap)  # 幂等校验
    total = con.execute(f"SELECT COUNT(*) FROM {DST_TABLE}").fetchone()[0]
    assert total == 5, f"upsert 后应当 5 行，实际 {total}"
    LOGGER.info("upsert 1st=%d, 2nd=%d, total=%d (幂等 OK)", n, n2, total)

    print("SELF_TEST_PASS: hkscc_quarterly")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="日级港中结持股 → 季度末快照 (hkscc_quarterly)")
    p.add_argument("--db", default=DEFAULT_DB, help=f"DuckDB 路径 (默认 {DEFAULT_DB})")
    p.add_argument("--self-test", action="store_true", help="合成数据自检，不读 DB")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    if args.self_test:
        return cmd_self_test(args)
    return cmd_run(args)


if __name__ == "__main__":
    sys.exit(main())
