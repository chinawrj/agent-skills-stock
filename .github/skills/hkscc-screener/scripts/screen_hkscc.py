"""screen_hkscc.py — 第二步过滤器：港中结持仓筛选 (rat-trader-screener M2 step 2).

输入：
  - DuckDB 表 hkscc_quarterly(code, quarter, quarter_end, holding_shares,
                              holding_ratio, holding_market_cap_cny)
  - DuckDB 表 market_cap_snapshot(code, date, total_mcap_cny, float_mcap_cny)
    可选；缺失时跳过总市值过滤并写 WARNING（FB-006）。
  - parquet `data/universe_non_soe.parquet`（来自 soe-filter）

筛选条件（来自 hkscc-screener SKILL.md 默认值）：
  - 港中结连续持仓 ≥ N 季度（默认 4）— 用 pd.Period('Q') 判定相邻
  - 最近一期持股市值 ≥ M（默认 3000 万）
  - 总市值 ∈ [low, high]（默认 30亿~200亿）— 仅当有 market_cap_snapshot 时启用
  - 与 universe_non_soe 求交集

输出：data/candidates_hkscc.parquet。锚定：300401 必须保留（若样本充足）。
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import duckdb
import pandas as pd

LOGGER = logging.getLogger("screen_hkscc")

DEFAULT_DB = "data/a-share.db"
DEFAULT_UNIVERSE = "data/universe_non_soe.parquet"
DEFAULT_OUT = "data/candidates_hkscc.parquet"


def has_continuous(quarters, n: int = 4) -> bool:
    qs = sorted(set(quarters))
    if len(qs) < n:
        return False
    for i in range(len(qs) - n + 1):
        if all(qs[i + k] == qs[i] + k for k in range(n)):
            return True
    return False


def screen(
    quarterly: pd.DataFrame,
    mcap: pd.DataFrame | None,
    universe: pd.DataFrame,
    *,
    min_quarters: int = 4,
    min_holding_mcap: float = 30_000_000,
    min_total_mcap: float = 3_000_000_000,
    max_total_mcap: float = 20_000_000_000,
) -> pd.DataFrame:
    if quarterly.empty:
        return quarterly.assign(quarters_held=0, latest_holding_mcap=0.0, total_mcap=0.0).iloc[0:0]

    df = quarterly.copy()
    df["code"] = df["code"].astype(str).str.zfill(6)
    df["_q"] = df["quarter"].apply(pd.Period)

    rows = []
    for code, g in df.groupby("code"):
        g = g.sort_values("_q")
        if not has_continuous(g["_q"].tolist(), n=min_quarters):
            continue
        latest_row = g.iloc[-1]
        latest_mcap = float(latest_row["holding_market_cap_cny"] or 0)
        if latest_mcap < min_holding_mcap:
            continue
        rows.append(
            {
                "code": code,
                "quarters_held": len(g),
                "latest_holding_mcap": latest_mcap,
                "latest_quarter": str(latest_row["_q"]),
            }
        )
    cand = pd.DataFrame(rows)
    LOGGER.info("HKSCC 持仓 + 持股市值过滤后: %d", len(cand))
    if cand.empty:
        return cand

    # 与 universe 求交集
    uni = universe.copy()
    uni["code"] = uni["code"].astype(str).str.zfill(6)
    keep_cols = [c for c in ("code", "name", "market") if c in uni.columns]
    cand = cand.merge(uni[keep_cols], on="code", how="inner")
    LOGGER.info("∩ universe_non_soe 后: %d", len(cand))

    if mcap is not None and not mcap.empty:
        m = mcap.copy()
        m["code"] = m["code"].astype(str).str.zfill(6)
        # 取每只股票最近一条
        m = m.sort_values(["code", "date"]).groupby("code").tail(1)
        cand = cand.merge(m[["code", "total_mcap_cny"]], on="code", how="left")
        cand = cand.rename(columns={"total_mcap_cny": "total_mcap"})
        before = len(cand)
        cand = cand[
            cand["total_mcap"].between(min_total_mcap, max_total_mcap, inclusive="both")
            | cand["total_mcap"].isna()  # 容忍 NaN，由后续步骤补
        ]
        LOGGER.info("总市值 ∈ [%.1e, %.1e] 后: %d (was %d)", min_total_mcap, max_total_mcap, len(cand), before)
    else:
        LOGGER.warning("market_cap_snapshot 不可用 → 跳过总市值过滤（FB-006）")
        cand["total_mcap"] = pd.NA

    if "name" not in cand.columns:
        cand["name"] = ""
    cand = cand[["code", "name", "quarters_held", "latest_holding_mcap", "total_mcap"]]
    cand = cand.sort_values(["quarters_held", "latest_holding_mcap"], ascending=[False, False]).reset_index(drop=True)
    return cand


def cmd_run(args: argparse.Namespace) -> int:
    db_path = Path(args.db)
    if not db_path.exists():
        LOGGER.error("DuckDB 不存在: %s", db_path)
        return 2
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        try:
            quarterly = con.execute("SELECT * FROM hkscc_quarterly").fetchdf()
        except duckdb.CatalogException:
            LOGGER.error("表 hkscc_quarterly 不存在，先跑 hkscc_quarterly.py")
            return 2
        try:
            mcap = con.execute("SELECT code, date, total_mcap_cny FROM market_cap_snapshot").fetchdf()
        except duckdb.CatalogException:
            mcap = None
    finally:
        con.close()

    uni_path = Path(args.universe)
    if not uni_path.exists():
        LOGGER.error("universe parquet 不存在: %s", uni_path)
        return 2
    universe = pd.read_parquet(uni_path)

    cand = screen(
        quarterly,
        mcap,
        universe,
        min_quarters=args.min_quarters,
        min_holding_mcap=args.min_holding_mcap,
        min_total_mcap=args.min_total_mcap,
        max_total_mcap=args.max_total_mcap,
    )
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    cand.to_parquet(out, index=False)
    LOGGER.info("写入: %s (%d 行)", out, len(cand))
    print(f"HKSCC 候选: {len(cand)}")

    if args.require_ref:
        if "300401" not in set(cand["code"].astype(str)):
            LOGGER.error("⚠ Reference 300401 不在候选中（require-ref=True）")
            return 1
        LOGGER.info("✓ 300401 在候选中")
    return 0


def cmd_self_test(_args: argparse.Namespace) -> int:
    # 合成 4 只票：A 满足全部条件、B 不连续、C 持股市值过低、D 季度不足
    rows = []
    for code, qs, mcap in [
        ("000001", ["2023Q4", "2024Q1", "2024Q2", "2024Q3"], 50_000_000),  # 连续 4 季度，足够 → keep
        ("000002", ["2023Q1", "2023Q3", "2024Q1", "2024Q3"], 80_000_000),  # 跳季 → drop
        ("000003", ["2023Q4", "2024Q1", "2024Q2", "2024Q3"], 5_000_000),   # 持股市值过低 → drop
        ("000004", ["2024Q2", "2024Q3"], 60_000_000),                       # 季度不足 → drop
    ]:
        for q in qs:
            rows.append(
                {
                    "code": code,
                    "quarter": q,
                    "quarter_end": pd.Period(q).end_time.date(),
                    "holding_shares": 1,
                    "holding_ratio": 0.01,
                    "holding_market_cap_cny": mcap,
                }
            )
    quarterly = pd.DataFrame(rows)
    universe = pd.DataFrame(
        [{"code": c, "name": f"N{c}", "market": "SZ"} for c in ["000001", "000002", "000003", "000004"]]
    )
    mcap_df = pd.DataFrame(
        [
            {"code": "000001", "date": "2024-09-30", "total_mcap_cny": 5_000_000_000},
            {"code": "000002", "date": "2024-09-30", "total_mcap_cny": 8_000_000_000},
            {"code": "000003", "date": "2024-09-30", "total_mcap_cny": 5_000_000_000},
            {"code": "000004", "date": "2024-09-30", "total_mcap_cny": 5_000_000_000},
        ]
    )
    cand = screen(quarterly, mcap_df, universe)
    LOGGER.info("self-test cand:\n%s", cand)
    assert list(cand["code"]) == ["000001"], f"unexpected: {list(cand['code'])}"
    # 无 mcap 表也走通
    cand2 = screen(quarterly, None, universe)
    assert "000001" in set(cand2["code"]), "name-only mode should still keep 000001"
    print("SELF_TEST_PASS: screen_hkscc")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="HKSCC 候选筛选")
    p.add_argument("--db", default=DEFAULT_DB)
    p.add_argument("--in", dest="universe", default=DEFAULT_UNIVERSE)
    p.add_argument("--out", dest="output", default=DEFAULT_OUT)
    p.add_argument("--min-quarters", type=int, default=4)
    p.add_argument("--min-holding-mcap", type=float, default=30_000_000)
    p.add_argument("--min-total-mcap", type=float, default=3_000_000_000)
    p.add_argument("--max-total-mcap", type=float, default=20_000_000_000)
    p.add_argument("--require-ref", action="store_true", help="要求 300401 在候选中（端到端验收）")
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
