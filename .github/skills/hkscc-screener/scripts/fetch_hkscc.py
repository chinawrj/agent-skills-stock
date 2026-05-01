"""fetch_hkscc.py — 拉取港中结(HKSCC)A股个股日级持股数据 → DuckDB.

M1 阶段：本脚本为骨架，目前只实现：
  * argparse + logging 标准入口
  * --self-test：用合成数据走通"建表 → 写入 → 读出"链路
  * 真实 akshare 拉取标记 TODO，待 Day 2 接入（接口名 ak.stock_hsgt_hk_stock_statistics_em
    或 ak.stock_hk_ggt_components_em，akshare 版本敏感，到时 dir(ak) 现查）

依赖：duckdb / pandas（akshare 仅在非 self-test 路径下 import）
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd

DEFAULT_DB = "data/a-share.db"
TABLE = "hkscc_holdings"
LOGGER = logging.getLogger("fetch_hkscc")


def _ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    """幂等建表；与 db/init_db.sql 中的定义保持一致。"""
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS hkscc_holdings (
            code                   VARCHAR NOT NULL,
            date                   DATE NOT NULL,
            holding_shares         BIGINT,
            holding_ratio          DECIMAL(10,6),
            holding_market_cap_cny DECIMAL(20,2),
            source                 VARCHAR DEFAULT 'akshare',
            created_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (code, date)
        )
        """
    )


def _upsert(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    """按 (code, date) UPSERT。返回写入行数。"""
    if df.empty:
        return 0
    expected = {"code", "date", "holding_shares", "holding_ratio", "holding_market_cap_cny"}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"DataFrame 缺少字段: {missing}")
    df = df.copy()
    if "source" not in df.columns:
        df["source"] = "akshare"
    con.register("_stage", df)
    con.execute(
        f"""
        INSERT INTO {TABLE} (code, date, holding_shares, holding_ratio, holding_market_cap_cny, source)
        SELECT code, date, holding_shares, holding_ratio, holding_market_cap_cny, source
        FROM _stage
        ON CONFLICT (code, date) DO UPDATE SET
            holding_shares = EXCLUDED.holding_shares,
            holding_ratio = EXCLUDED.holding_ratio,
            holding_market_cap_cny = EXCLUDED.holding_market_cap_cny
        """
    )
    con.unregister("_stage")
    return len(df)


def _synthetic_frame(code: str = "300401", days: int = 5) -> pd.DataFrame:
    """生成花园生物的合成持仓样本，纯本地，禁用网络。"""
    today = date.today()
    rows = []
    for i in range(days):
        d = today - timedelta(days=i)
        rows.append(
            {
                "code": code,
                "date": d,
                "holding_shares": 1_000_000 + i * 1_000,
                "holding_ratio": 1.234567,
                "holding_market_cap_cny": 50_000_000.00 + i * 1_000,
            }
        )
    return pd.DataFrame(rows)


def fetch_real(start: str, end: str | None) -> pd.DataFrame:
    """TODO(M1, Day 2): 通过 akshare 拉取港股通持股全市场日级明细。"""
    raise NotImplementedError(
        "fetch_real 尚未实现。Day 2 接入 akshare（参考 hkscc-screener SKILL.md 注意事项）。"
    )


def cmd_self_test(args: argparse.Namespace) -> int:
    """用 in-memory DuckDB 走一遍 schema → upsert → query 流程。"""
    con = duckdb.connect(":memory:")
    _ensure_schema(con)
    df = _synthetic_frame()
    n = _upsert(con, df)
    LOGGER.info("self-test 写入 %d 行", n)

    # 重复写入做一次 upsert 校验
    n2 = _upsert(con, df)
    LOGGER.info("self-test 重复 upsert %d 行（应等于 0 净增）", n2)

    total = con.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
    if total != len(df):
        LOGGER.error("self-test FAIL: 期望 %d 行，实际 %d 行", len(df), total)
        return 1

    sample = con.execute(
        f"SELECT code, date, holding_shares FROM {TABLE} ORDER BY date DESC LIMIT 3"
    ).fetchdf()
    LOGGER.info("self-test 样本:\n%s", sample.to_string(index=False))
    print("SELF_TEST_PASS: fetch_hkscc")
    return 0


def cmd_fetch(args: argparse.Namespace) -> int:
    """真实拉取 → 写入 DuckDB。当前阶段抛 NotImplementedError。"""
    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    _ensure_schema(con)
    LOGGER.info("拉取窗口: %s → %s", args.start, args.end or "today")
    df = fetch_real(args.start, args.end)  # NotImplementedError 占位
    n = _upsert(con, df)
    LOGGER.info("写入 %d 行到 %s", n, db_path)
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="拉取港中结持股数据 → DuckDB")
    p.add_argument("--db", default=DEFAULT_DB, help=f"DuckDB 路径 (默认 {DEFAULT_DB})")
    p.add_argument("--start", default="2024-01-01", help="起始日期 YYYY-MM-DD")
    p.add_argument("--end", default=None, help="结束日期 YYYY-MM-DD（默认今天）")
    p.add_argument("--self-test", action="store_true", help="用合成数据自检，不联网")
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
    return cmd_fetch(args)


if __name__ == "__main__":
    sys.exit(main())
