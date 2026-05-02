"""detect_rat_pattern.py — M3 老鼠仓节奏识别 CLI (A∧B∧C∧D).

核心逻辑已移至 rat_pattern.py；本文件只保留 CLI 入口、常量定义和 I/O 编排。
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import duckdb
import pandas as pd

from bcd import Thresholds
from rat_pattern import (
    THR_UP, THR_DOWN, T3_MIN_RATIO,
    detect_pattern_for_code, assemble_hits, compute_bcd_score,
    # re-export for backward-compat (tests import from this module)
    find_triples, is_monotonic_up,
)

LOGGER = logging.getLogger("detect_rat_pattern")

PRICE_HIGH_PCT = 0.85
VOL_HIGH_RATIO = 1.3
SELL_FLY_LIMIT = 0.15
PLATEAU_RANGE = 0.25
LOW_POS_RATIO = 0.75
LOOKBACK = 250
POST_WINDOW = 60

DEFAULT_DB = "data/a-share.db"
DEFAULT_IN = "data/candidates_hkscc.parquet"
DEFAULT_OUT = "data/candidates_rat_pattern.parquet"
DEFAULT_DIAG = "data/_diag_rat_pattern.json"

_compute_bcd_score = compute_bcd_score


def cmd_run(args: argparse.Namespace) -> int:
    in_path = Path(args.input)
    if not in_path.exists():
        LOGGER.error("候选输入不存在: %s", in_path)
        return 2
    cand = pd.read_parquet(in_path)
    cand["code"] = cand["code"].astype(str).str.zfill(6)
    LOGGER.info("HKSCC 候选: %d", len(cand))

    db = Path(args.db)
    if not db.exists():
        LOGGER.error("DuckDB 不存在: %s", db)
        return 2
    con = duckdb.connect(str(db), read_only=True)
    try:
        q = con.execute(
            "SELECT code, quarter, quarter_end, holding_shares, holding_market_cap_cny FROM hkscc_quarterly"
        ).fetchdf()
        try:
            kdf = con.execute(
                "SELECT code, date, close, volume FROM kline_daily WHERE code IN ("
                + ",".join(["?"] * len(cand))
                + ")",
                cand["code"].tolist(),
            ).fetchdf()
        except duckdb.CatalogException:
            LOGGER.warning("kline_daily 表不存在 → B/C/D 计算跳过")
            kdf = None
    finally:
        con.close()
    q["code"] = q["code"].astype(str).str.zfill(6)
    q = q[q["code"].isin(set(cand["code"]))]
    LOGGER.info("hkscc_quarterly 行数（候选范围内）: %d", len(q))

    kline_by_code: dict = {}
    if kdf is not None and not kdf.empty:
        kdf["code"] = kdf["code"].astype(str).str.zfill(6)
        kdf["date"] = pd.to_datetime(kdf["date"])
        kdf["close"] = kdf["close"].astype(float)
        kdf["volume"] = kdf["volume"].astype("int64")
        kline_by_code = {c: g.sort_values("date").reset_index(drop=True)
                         for c, g in kdf.groupby("code")}
        LOGGER.info("kline_daily 覆盖股票数: %d", len(kline_by_code))

    thr = Thresholds(
        price_high_pct=args.price_high_pct,
        vol_high_ratio=args.vol_high_ratio,
        sell_fly_limit=args.sell_fly_limit,
        plateau_range=args.plateau_range,
        low_pos_ratio=args.low_pos_ratio,
        d_head_days=args.d_head_days,
    )
    LOGGER.info(
        "阈值: price>=%.2f vol>=%.2f sell_fly<%.2f plateau<%.2f low_pos<%.2f t3_min_ratio>=%.2f",
        thr.price_high_pct, thr.vol_high_ratio, thr.sell_fly_limit,
        thr.plateau_range, thr.low_pos_ratio, args.t3_min_ratio,
    )

    names = dict(zip(cand["code"], cand.get("name", pd.Series([""] * len(cand)))))
    qheld = dict(zip(cand["code"], cand["quarters_held"])) if "quarters_held" in cand.columns else {}
    lmcap = dict(zip(cand["code"], cand["latest_holding_mcap"])) if "latest_holding_mcap" in cand.columns else {}

    diags = [
        detect_pattern_for_code(g, code, kline=kline_by_code.get(code), thr=thr,
                                 t3_min_ratio=args.t3_min_ratio)
        for code, g in q.groupby("code")
    ]
    _log_funnel(diags, len(diags))

    hits = assemble_hits(diags, names, strict_bcd=args.strict,
                         quarters_held=qheld, latest_holding_mcap=lmcap)
    LOGGER.info("最终候选（strict_bcd=%s）: %d", args.strict, len(hits))

    hits_all = hits.copy()
    if args.min_bcd_score > 0 and not hits.empty:
        before = len(hits)
        hits = hits[hits["bcd_score"] >= args.min_bcd_score].reset_index(drop=True)
        LOGGER.info("--min-bcd-score=%.0f 过滤: %d → %d 只", args.min_bcd_score, before, len(hits))

    if getattr(args, "save_all", False) and args.min_bcd_score > 0:
        all_out = Path(args.output).with_name(
            Path(args.output).stem + "_all" + Path(args.output).suffix
        )
        all_out.parent.mkdir(parents=True, exist_ok=True)
        hits_all.to_parquet(all_out, index=False)
        LOGGER.info("--save-all: 未过滤候选写入 %s (%d 行)", all_out, len(hits_all))

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    hits.to_parquet(out, index=False)
    LOGGER.info("写入: %s (%d 行)", out, len(hits))

    diag_path = Path(args.diag)
    diag_path.parent.mkdir(parents=True, exist_ok=True)
    diag_path.write_text(
        json.dumps(diags, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )
    LOGGER.info("诊断 JSON: %s (%d 股)", diag_path, len(diags))

    return _check_ref(args, diags, hits)


def _log_funnel(diags: list[dict], total: int) -> None:
    a = sum(1 for d in diags if d["A"])
    b = sum(1 for d in diags if d.get("A") and any(t.get("B") for t in d.get("triples", [])))
    bc = sum(1 for d in diags if d.get("A") and any(
        t.get("B") and t.get("C") for t in d.get("triples", [])))
    bcd = sum(1 for d in diags if d.get("BCD"))
    LOGGER.info("漏斗: 总=%d → A=%d → A+B=%d → A+B+C=%d → A+B+C+D=%d", total, a, b, bc, bcd)


def _check_ref(args: argparse.Namespace, diags: list[dict], hits: pd.DataFrame) -> int:
    if not args.require_ref:
        return 0
    ref = "300401"
    ref_diag = next((d for d in diags if d["code"] == ref), None)
    if ref_diag is None:
        LOGGER.error("⚠ 300401 不在候选输入里")
        return 1
    if not ref_diag["A"]:
        LOGGER.error("⚠ 300401 未命中 A 段")
        return 1
    if args.strict and not ref_diag.get("BCD"):
        LOGGER.error("⚠ 300401 BCD 未命中（strict）；triple BCD 数值见 diag JSON")
        for t in ref_diag["triples"]:
            LOGGER.error(
                "  triple %s→%s→%s  B=%s(price=%s vol=%s) C=%s(post=%s) D=%s",
                t["t1"], t["t2"], t["t3"], t.get("B"),
                t.get("price_pct"), t.get("vol_ratio"),
                t.get("C"), t.get("post_ret_60d"), t.get("D"),
            )
        return 1
    ref_in_hits = not hits.empty and (hits["code"] == ref).any()
    if args.min_bcd_score > 0 and not ref_in_hits:
        LOGGER.warning(
            "⚠ 300401 BCD 命中但 bcd_score < --min-bcd-score(%.0f)，已被过滤", args.min_bcd_score
        )
    LOGGER.info(
        "✓ 300401 A=%s BCD=%s; %d triples%s",
        ref_diag["A"], ref_diag.get("BCD"), len(ref_diag["triples"]),
        " (在最终输出中 ✓)" if ref_in_hits else " (被 min-bcd-score 过滤)",
    )
    return 0


def cmd_self_test(_args: argparse.Namespace) -> int:
    df = pd.DataFrame({
        "quarter": ["2024Q1", "2024Q2", "2024Q3", "2024Q4", "2025Q1", "2025Q2"],
        "holding_shares": [1000, 1100, 1200, 1000, 950, 1300],
        "holding_market_cap_cny": [0] * 6,
    })
    diag = detect_pattern_for_code(df.assign(code="SELF"), "SELF")
    assert diag["A"] is True, f"expected A hit; got {diag}"
    assert len(diag["triples"]) >= 1

    df_fr = pd.DataFrame({
        "quarter": ["2024Q1", "2024Q2", "2024Q3", "2024Q4"],
        "holding_shares": [2000, 1500, 1000, 1100],
        "holding_market_cap_cny": [0] * 4,
    })
    assert detect_pattern_for_code(df_fr, "FAKE")["A"] is False, "FB-011 fake rebound must reject"

    df_mono = pd.DataFrame({
        "quarter": ["2024Q1", "2024Q2", "2024Q3", "2024Q4"],
        "holding_shares": [1000, 1100, 1300, 1500],
        "holding_market_cap_cny": [0] * 4,
    })
    assert detect_pattern_for_code(df_mono, "MONO")["A"] is False, "monotonic up must reject"

    print("SELF_TEST_PASS: detect_rat_pattern (A段)")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="老鼠仓节奏识别 (A∧B∧C∧D)")
    p.add_argument("--db", default=DEFAULT_DB)
    p.add_argument("--in", dest="input", default=DEFAULT_IN)
    p.add_argument("--out", dest="output", default=DEFAULT_OUT)
    p.add_argument("--diag", default=DEFAULT_DIAG)
    p.add_argument("--strict", action="store_true")
    p.add_argument("--require-ref", action="store_true")
    p.add_argument("--price-high-pct", type=float, default=0.85)
    p.add_argument("--vol-high-ratio", type=float, default=1.30)
    p.add_argument("--sell-fly-limit", type=float, default=0.15)
    p.add_argument("--plateau-range", type=float, default=0.25)
    p.add_argument("--low-pos-ratio", type=float, default=0.75)
    p.add_argument("--d-head-days", type=int, default=20)
    p.add_argument("--t3-min-ratio", type=float, default=T3_MIN_RATIO)
    p.add_argument("--min-bcd-score", type=float, default=0)
    p.add_argument("--save-all", action="store_true")
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
