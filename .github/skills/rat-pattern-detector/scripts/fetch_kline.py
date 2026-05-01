"""fetch_kline.py — 拉日线 K 线（前复权）入 DuckDB.kline_daily.

数据源策略（按可用性 fallback）：
  1. akshare `stock_zh_a_daily` (新浪) — adjust='qfq' 前复权，已验证沙盒可用
  2. akshare `stock_zh_a_hist` (东方财富 push2his) — Day 4 沙盒被拦
  3. baostock — Day 4 沙盒登录 hang
  当前默认仅尝试源 1，源 2/3 留 hook。

CLI:
  --symbols 300401,000001     # 指定股票（可多个）
  --from-parquet data/candidates_hkscc.parquet  # 从 parquet 读取 code 列
  --start 2022-01-01          # 默认回溯到 2022 以覆盖所有历史三元组
  --end   today
  --db    data/a-share.db
  --self-test                 # 合成数据 sanity
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import socket
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd

LOGGER = logging.getLogger("fetch_kline")

DEFAULT_DB = "data/a-share.db"
SOURCE_TAG_SINA = "akshare-sina"


def _market_prefix(code: str) -> str:
    code = str(code).zfill(6)
    if code.startswith(("60", "68", "9")):
        return f"sh{code}"
    return f"sz{code}"


def _ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS kline_daily (
            code VARCHAR NOT NULL,
            date DATE NOT NULL,
            open DECIMAL(10,3),
            high DECIMAL(10,3),
            low DECIMAL(10,3),
            close DECIMAL(10,3),
            volume BIGINT,
            amount DECIMAL(20,2),
            outstanding_share BIGINT,
            turnover DECIMAL(10,6),
            source VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (code, date)
        );
        """
    )


def _upsert(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    df = df.copy()
    df["source"] = df.get("source", SOURCE_TAG_SINA)
    cols = ["code", "date", "open", "high", "low", "close", "volume",
            "amount", "outstanding_share", "turnover", "source"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]
    con.register("_kline_stage", df)
    con.execute(
        """
        INSERT INTO kline_daily (code,date,open,high,low,close,volume,amount,outstanding_share,turnover,source)
        SELECT code,date,open,high,low,close,volume,amount,outstanding_share,turnover,source FROM _kline_stage
        ON CONFLICT (code,date) DO UPDATE SET
            open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close,
            volume=EXCLUDED.volume, amount=EXCLUDED.amount,
            outstanding_share=EXCLUDED.outstanding_share, turnover=EXCLUDED.turnover,
            source=EXCLUDED.source
        """
    )
    con.unregister("_kline_stage")
    return len(df)


def fetch_one_sina(code: str, start: str, end: str) -> pd.DataFrame:
    """单股新浪 K 线（前复权）。"""
    import akshare as ak
    socket.setdefaulttimeout(20)
    sym = _market_prefix(code)
    LOGGER.info("akshare stock_zh_a_daily(%s) %s→%s", sym, start, end)
    df = ak.stock_zh_a_daily(symbol=sym, start_date=start, end_date=end, adjust="qfq")
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["code"] = str(code).zfill(6)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    # 新浪 stock_zh_a_daily 返回 volume 单位为"股"（验证：amount/close ≈ volume，比值≈1）。
    df["volume"] = df["volume"].astype("int64")
    keep = ["code", "date", "open", "high", "low", "close", "volume", "amount",
            "outstanding_share", "turnover"]
    df = df[[c for c in keep if c in df.columns]]
    df["source"] = SOURCE_TAG_SINA
    return df


def fetch_real(symbols: list[str], start: str, end: str, *, sleep: float = 0.3) -> pd.DataFrame:
    frames = []
    for i, code in enumerate(symbols):
        try:
            df = fetch_one_sina(code, start.replace("-", ""), end.replace("-", ""))
            # 新浪要求 YYYY-MM-DD？实测 stock_zh_a_daily 接受两种，但稳妥用 dash 形式：
            if df.empty:
                df = fetch_one_sina(code, start, end)
            LOGGER.info("  %s: %d 行", code, len(df))
            frames.append(df)
        except Exception as e:  # noqa: BLE001
            LOGGER.warning("  %s 失败：%s", code, e)
        if sleep and i + 1 < len(symbols):
            time.sleep(sleep)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def cmd_run(args: argparse.Namespace) -> int:
    symbols: list[str] = []
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    if args.from_parquet:
        import pandas as pd
        try:
            df_cand = pd.read_parquet(args.from_parquet)
            symbols += list(df_cand["code"].astype(str).str.zfill(6).unique())
            LOGGER.info("从 %s 读取 %d 个 codes", args.from_parquet, len(symbols))
        except Exception as exc:
            LOGGER.error("读取 parquet 失败: %s", exc)
            return 2
    symbols = sorted(set(symbols))
    if not symbols:
        LOGGER.error("必须提供 --symbols 或 --from-parquet")
        return 2
    end = args.end or dt.date.today().isoformat()
    df = fetch_real(symbols, args.start, end)
    LOGGER.info("拉到 %d 行", len(df))
    if df.empty:
        return 1
    db = Path(args.db)
    db.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db))
    try:
        _ensure_schema(con)
        n = _upsert(con, df)
        LOGGER.info("写入 %d 行到 %s (table=kline_daily)", n, db)
    finally:
        con.close()
    return 0


def cmd_self_test(_args: argparse.Namespace) -> int:
    db = Path("/tmp/rt-test/kline.db")
    if db.exists():
        db.unlink()
    db.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db))
    try:
        _ensure_schema(con)
        df = pd.DataFrame(
            {
                "code": ["300401", "300401"],
                "date": [dt.date(2024, 1, 2), dt.date(2024, 1, 3)],
                "open": [11.6, 11.5],
                "high": [11.7, 11.6],
                "low": [11.4, 11.4],
                "close": [11.5, 11.45],
                "volume": [1000000, 800000],
                "amount": [11_500_000.0, 9_160_000.0],
                "outstanding_share": [542_225_085, 542_225_085],
                "turnover": [0.0146, 0.0113],
            }
        )
        n = _upsert(con, df)
        assert n == 2
        # idempotent
        n2 = _upsert(con, df.assign(close=df["close"] + 0.01))
        assert n2 == 2
        rows = con.execute("SELECT COUNT(*) FROM kline_daily").fetchone()[0]
        assert rows == 2, rows
        # market_prefix
        assert _market_prefix("300401") == "sz300401"
        assert _market_prefix("600519") == "sh600519"
        assert _market_prefix("688981") == "sh688981"
    finally:
        con.close()
    print("SELF_TEST_PASS: fetch_kline")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="日线 K 线拉取（前复权）")
    p.add_argument("--symbols", default=None, help="逗号分隔股票代码")
    p.add_argument("--from-parquet", default=None,
                   help="从 parquet 文件的 code 列读取股票列表（如 data/candidates_hkscc.parquet）")
    p.add_argument("--start", default="2022-01-01")
    p.add_argument("--end", default=None, help="默认 today")
    p.add_argument("--db", default=DEFAULT_DB)
    p.add_argument("--self-test", action="store_true")
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
