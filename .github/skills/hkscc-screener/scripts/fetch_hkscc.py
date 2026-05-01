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


def fetch_real(symbols: list[str]) -> pd.DataFrame:
    """通过 akshare 拉取个股港股通持股历史。

    用 ``stock_hsgt_individual_em(symbol)`` 一次返回该股全历史（截至 EM 数据窗口）。
    若需要日期切片，调用方自行在 DataFrame 上过滤。

    返回字段：code / date / holding_shares / holding_ratio / holding_market_cap_cny
    """
    import akshare as ak  # 延迟导入；--self-test 路径不触发

    frames: list[pd.DataFrame] = []
    for sym in symbols:
        LOGGER.info("akshare stock_hsgt_individual_em(%s) ...", sym)
        try:
            raw = ak.stock_hsgt_individual_em(symbol=sym)
        except Exception as exc:  # noqa: BLE001 — 单股失败不应炸全量
            LOGGER.error("拉取 %s 失败: %r", sym, exc)
            continue
        if raw is None or raw.empty:
            LOGGER.warning("%s 返回空", sym)
            continue
        frame = pd.DataFrame(
            {
                "code": sym,
                "date": pd.to_datetime(raw["持股日期"]).dt.date,
                "holding_shares": pd.to_numeric(raw["持股数量"], errors="coerce").astype("Int64"),
                "holding_ratio": pd.to_numeric(raw["持股数量占A股百分比"], errors="coerce"),
                "holding_market_cap_cny": pd.to_numeric(raw["持股市值"], errors="coerce"),
            }
        ).dropna(subset=["date"])
        frames.append(frame)
        LOGGER.info("  %s: %d 行 (%s → %s)", sym, len(frame), frame["date"].min(), frame["date"].max())
    if not frames:
        return pd.DataFrame(
            columns=["code", "date", "holding_shares", "holding_ratio", "holding_market_cap_cny"]
        )
    return pd.concat(frames, ignore_index=True)


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
    """真实拉取 → 写入 DuckDB.

    --symbols 可逗号分隔多只股票。如未指定，默认拉 reference case 300401。
    """
    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    _ensure_schema(con)
    symbols = [s.strip() for s in (args.symbols or "300401").split(",") if s.strip()]
    LOGGER.info("拉取 symbols=%s 窗口=%s→%s", symbols, args.start, args.end or "today")
    df = fetch_real(symbols)
    if df.empty:
        LOGGER.warning("拉取结果为空，未写入")
        return 1
    # 按 start/end 过滤
    if args.start:
        df = df[df["date"] >= pd.to_datetime(args.start).date()]
    if args.end:
        df = df[df["date"] <= pd.to_datetime(args.end).date()]
    n = _upsert(con, df)
    LOGGER.info("写入 %d 行到 %s (table=%s)", n, db_path, TABLE)
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="拉取港中结持股数据 → DuckDB")
    p.add_argument("--db", default=DEFAULT_DB, help=f"DuckDB 路径 (默认 {DEFAULT_DB})")
    p.add_argument("--start", default="2024-01-01", help="起始日期 YYYY-MM-DD")
    p.add_argument("--end", default=None, help="结束日期 YYYY-MM-DD（默认今天）")
    p.add_argument(
        "--symbols",
        default=None,
        help="逗号分隔的股票代码列表 (默认 300401)；批量回填请显式传入",
    )
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
