"""render_kline.py — M4 K 线 + 成交量复盘渲染（骨架版）。

Day 9: 仅写参数解析 + 数据加载 + 占位 main，matplotlib 渲染留待 Day 10 实装。
"""
from __future__ import annotations
import argparse
import json
import logging
from pathlib import Path

import duckdb
import pandas as pd

LOGGER = logging.getLogger("render_kline")

DEFAULT_PARQUET = "data/candidates_rat_pattern.parquet"
DEFAULT_DIAG = "data/_diag_rat_pattern.json"
DEFAULT_DB = "data/a-share.db"
DEFAULT_OUT_DIR = "reports/figures"
LOOKBACK_DAYS = 500
MA_WINDOW = 250


def load_inputs(parquet: Path, diag: Path) -> tuple[pd.DataFrame, list[dict]]:
    df = pd.read_parquet(parquet) if parquet.exists() else pd.DataFrame()
    diags = json.loads(diag.read_text(encoding="utf-8")) if diag.exists() else []
    return df, diags


def fetch_kline(con: duckdb.DuckDBPyConnection, code: str, lookback: int) -> pd.DataFrame:
    return con.execute(
        """
        SELECT date, close, volume
        FROM kline_daily
        WHERE code = ?
        ORDER BY date DESC
        LIMIT ?
        """,
        [code, lookback],
    ).fetchdf().sort_values("date").reset_index(drop=True)


def render_one(code: str, name: str, kline: pd.DataFrame, hit_triple: dict, out_dir: Path) -> Path | None:
    """Day 10 实装 matplotlib。当前仅 logging 占位。"""
    if kline.empty:
        LOGGER.warning("%s 无 K 线数据，跳过", code)
        return None
    LOGGER.info(
        "[占位] would render %s (%s): %d K bars; triple t1=%s t2=%s t3=%s",
        code, name, len(kline),
        hit_triple.get("t1"), hit_triple.get("t2"), hit_triple.get("t3"),
    )
    return None


def main() -> None:
    ap = argparse.ArgumentParser(description="K 线 + 成交量复盘图渲染")
    ap.add_argument("--parquet", default=DEFAULT_PARQUET)
    ap.add_argument("--diag", default=DEFAULT_DIAG)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    ap.add_argument("--lookback-days", type=int, default=LOOKBACK_DAYS)
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )

    if args.self_test:
        LOGGER.info("骨架版 self-test：仅校验 import + argparse OK")
        LOGGER.info("SELF_TEST_PASS: render_kline (skeleton)")
        return

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    df, diags = load_inputs(Path(args.parquet), Path(args.diag))
    if df.empty:
        LOGGER.warning("候选 parquet 为空：%s", args.parquet)
        return

    diag_by_code = {d["code"]: d for d in diags}
    db = Path(args.db)
    if not db.exists():
        LOGGER.error("DuckDB 不存在: %s", db)
        return
    con = duckdb.connect(str(db), read_only=True)
    try:
        for _, row in df.iterrows():
            code = str(row["code"]).zfill(6)
            name = row.get("name", "")
            d = diag_by_code.get(code, {})
            triples = d.get("triples", [])
            hit = next((t for t in triples if t.get("BCD")), {})
            kline = fetch_kline(con, code, args.lookback_days)
            render_one(code, name, kline, hit, out_dir)
    finally:
        con.close()


if __name__ == "__main__":
    main()
