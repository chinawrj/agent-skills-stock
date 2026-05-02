"""detect_rat_pattern.py — M3 老鼠仓节奏识别 (A∧B∧C∧D).

Day 4 范围：A 段（持仓节奏三元组 t1<t2<t3）+ B/C/D 占位。

A 段算法（来自 SKILL.md）：
  Δh_t = (H_t - H_{t-1}) / H_{t-1}
  存在 (t1<t2<t3) 三元组：
    Δh_{t1} > +THR_UP   （加仓）
    Δh_{t2} < -THR_DOWN （减仓）
    Δh_{t3} > +THR_UP   （再加仓）
  单边加仓（全程 Δh ≥ 0 且 max 在最后一期）→ 拒绝。

B/C/D 需要 K 线（kline_daily），Day 5 实装。当前先输出 placeholder（None / 留空）
但保留字段结构，让回归测试可对齐 schema。

输出：
  - data/candidates_rat_pattern.parquet（A∧B∧C∧D 命中；当前 B/C/D 强制 True 由 --strict 控制）
  - data/_diag_rat_pattern.json （所有候选股，含未命中，逐三元组诊断）
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import duckdb
import pandas as pd

from bcd import Thresholds, compute_bcd

LOGGER = logging.getLogger("detect_rat_pattern")

# ---- 模块级阈值常量（SKILL.md 默认值） ----
THR_UP = 0.05
THR_DOWN = 0.05
T3_MIN_RATIO = 1.0  # FB-011: holding_shares[t3] >= holding_shares[t2] * T3_MIN_RATIO
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


def find_triples(
    df: pd.DataFrame,
    *,
    thr_up: float = THR_UP,
    thr_down: float = THR_DOWN,
    t3_min_ratio: float = T3_MIN_RATIO,
) -> list[tuple[int, int, int]]:
    """A 段：返回 (i_t1, i_t2, i_t3) 索引三元组列表。

    df 需含按时间升序的 'holding_shares' 列；返回所有满足
    Δh_{t1}>thr_up & Δh_{t2}<-thr_down & Δh_{t3}>thr_up 且 t1<t2<t3 的最早组合。

    FB-011: 额外要求 holding_shares[t3] >= holding_shares[t2] * t3_min_ratio
    （拒绝单边减仓途中的小反弹假阳）
    """
    if "delta_pct" not in df.columns:
        df = df.copy()
        df["delta_pct"] = df["holding_shares"].pct_change()
    triples: list[tuple[int, int, int]] = []
    deltas = df["delta_pct"].tolist()
    holdings = df["holding_shares"].tolist()
    n = len(deltas)
    for i2 in range(n):
        d2 = deltas[i2]
        if pd.isna(d2) or d2 >= -thr_down:
            continue
        # t1 候选：i2 之前最近的加仓
        t1 = None
        for j in range(i2 - 1, -1, -1):
            dj = deltas[j]
            if pd.notna(dj) and dj > thr_up:
                t1 = j
                break
        if t1 is None:
            continue
        # t3 候选：i2 之后最近的加仓 + 持仓量约束
        t3 = None
        for j in range(i2 + 1, n):
            dj = deltas[j]
            if pd.notna(dj) and dj > thr_up:
                if holdings[j] >= holdings[i2] * t3_min_ratio:
                    t3 = j
                    break
        if t3 is None:
            continue
        triples.append((t1, i2, t3))
    return triples


def is_monotonic_up(df: pd.DataFrame) -> bool:
    """单边加仓拒绝判据：全程 Δh ≥ 0 且 max 在最后一期。"""
    deltas = df["delta_pct"].dropna()
    if deltas.empty:
        return False
    if not (deltas >= 0).all():
        return False
    holdings = df["holding_shares"].dropna()
    if holdings.empty:
        return False
    return holdings.iloc[-1] == holdings.max()


def detect_pattern_for_code(
    g: pd.DataFrame,
    code: str,
    kline: pd.DataFrame | None = None,
    thr: Thresholds | None = None,
    t3_min_ratio: float = T3_MIN_RATIO,
) -> dict:
    """对单一 code 的季度持仓序列做 A 段判定 + 各 triple 的 B/C/D 数值。

    kline 为 None 时只算 A 段，B/C/D 留 None（当前 fallback 行为）。
    """
    g = g.sort_values("quarter").reset_index(drop=True)
    g["delta_pct"] = g["holding_shares"].pct_change()
    triples = find_triples(g, t3_min_ratio=t3_min_ratio)
    monotonic = is_monotonic_up(g)
    thr = thr or Thresholds()

    triple_diags = []
    any_bcd = False
    hit_triple_idx = None
    for idx, (i1, i2, i3) in enumerate(triples):
        td = {
            "t1": str(g.loc[i1, "quarter"]),
            "t2": str(g.loc[i2, "quarter"]),
            "t3": str(g.loc[i3, "quarter"]),
            "delta_t1": float(g.loc[i1, "delta_pct"]),
            "delta_t2": float(g.loc[i2, "delta_pct"]),
            "delta_t3": float(g.loc[i3, "delta_pct"]),
        }
        if kline is not None and not kline.empty:
            bcd = compute_bcd(kline, td["t1"], td["t2"], td["t3"], thr)
            td.update(bcd)
            if bcd.get("BCD") and hit_triple_idx is None:
                any_bcd = True
                hit_triple_idx = idx
        triple_diags.append(td)

    diag = {
        "code": code,
        "n_quarters": len(g),
        "monotonic_up": bool(monotonic),
        "deltas": [
            {"quarter": str(g.loc[i, "quarter"]),
             "delta_pct": (None if pd.isna(g.loc[i, "delta_pct"]) else float(g.loc[i, "delta_pct"]))}
            for i in range(len(g))
        ],
        "triples": triple_diags,
        "A": (len(triples) > 0) and (not monotonic),
        # 任一 triple 的 B/C/D 全 True 即视为命中
        "B": any(t.get("B") is True for t in triple_diags) if triple_diags else None,
        "C": any(t.get("C") is True for t in triple_diags) if triple_diags else None,
        "D": any(t.get("D") is True for t in triple_diags) if triple_diags else None,
        "BCD": any_bcd,
        "hit_triple_idx": hit_triple_idx,
    }
    return diag


def _compute_bcd_score(chosen: dict) -> float:
    """Simple signal-strength score (0–100) for ranking BCD candidates.

    Components:
      - price_pct score: how far above 0.85 threshold (max 30 pts)
      - vol_ratio score: how far above 1.30 threshold (max 30 pts)
      - sell-fly margin: how far below 0.15 limit (max 20 pts)
      - bonus: 20 pts for having all 3 signals (high vol AND high price AND sell-fly pass)
    """
    pp = float(chosen.get("price_pct") or 0)
    vr = float(chosen.get("vol_ratio") or 0)
    pr = chosen.get("post_ret_60d")

    # Price score: normalise relative to [0.85, 1.5] range → [0, 30]
    price_score = min(max((pp - 0.85) / (1.5 - 0.85), 0), 1.0) * 30

    # Volume score: normalise relative to [1.30, 8.0] range → [0, 30]
    vol_score = min(max((vr - 1.30) / (8.0 - 1.30), 0), 1.0) * 30

    # C score: lower post_ret = better; [0, 0.15] → [20, 0], negative clamped to 20
    if pr is None or (isinstance(pr, float) and pd.isna(pr)):
        c_score = 10  # unknown → neutral
    else:
        c_score = min(max((0.15 - float(pr)) / 0.15, 0), 1.0) * 20

    # Bonus: both price_pct ≥ 0.85 AND vol_ratio ≥ 1.30
    bonus = 20 if pp >= 0.85 and vr >= 1.30 else 0

    return round(price_score + vol_score + c_score + bonus, 1)


def assemble_hits(
    diags: list[dict],
    names: dict[str, str],
    *,
    strict_bcd: bool,
    quarters_held: dict[str, int] | None = None,
    latest_holding_mcap: dict[str, float] | None = None,
) -> pd.DataFrame:
    rows = []
    for d in diags:
        if not d["A"]:
            continue
        triples = d.get("triples") or []
        # 选择"BCD 命中的 triple"；若无且非 strict，退回 first triple（仅 A 段命中）
        chosen = None
        if d.get("BCD") and d.get("hit_triple_idx") is not None:
            chosen = triples[d["hit_triple_idx"]]
        elif not strict_bcd and triples:
            chosen = triples[0]
        if chosen is None:
            continue
        rows.append(
            {
                "code": d["code"],
                "name": names.get(d["code"], ""),
                "quarters_held": (quarters_held or {}).get(d["code"]),
                "latest_mcap_cny": (latest_holding_mcap or {}).get(d["code"]),
                "t1": chosen["t1"],
                "t2": chosen["t2"],
                "t3": chosen["t3"],
                "B": chosen.get("B"),
                "C": chosen.get("C"),
                "D": chosen.get("D"),
                "price_pct": chosen.get("price_pct"),
                "vol_ratio": chosen.get("vol_ratio"),
                "post_ret_60d": chosen.get("post_ret_60d"),
                "bcd_score": _compute_bcd_score(chosen),
            }
        )
    df = pd.DataFrame(
        rows,
        columns=["code", "name", "quarters_held", "latest_mcap_cny", "t1", "t2", "t3",
                 "B", "C", "D", "price_pct", "vol_ratio", "post_ret_60d", "bcd_score"],
    )
    if not df.empty:
        df = df.sort_values("bcd_score", ascending=False).reset_index(drop=True)
    return df


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

    if kdf is not None and not kdf.empty:
        kdf["code"] = kdf["code"].astype(str).str.zfill(6)
        kdf["date"] = pd.to_datetime(kdf["date"])
        kdf["close"] = kdf["close"].astype(float)
        kdf["volume"] = kdf["volume"].astype("int64")
        kline_by_code = {c: g.sort_values("date").reset_index(drop=True) for c, g in kdf.groupby("code")}
        LOGGER.info("kline_daily 覆盖股票数: %d", len(kline_by_code))
    else:
        kline_by_code = {}

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
    qheld = {}
    if "quarters_held" in cand.columns:
        qheld = dict(zip(cand["code"], cand["quarters_held"]))
    lmcap = {}
    if "latest_holding_mcap" in cand.columns:
        lmcap = dict(zip(cand["code"], cand["latest_holding_mcap"]))
    diags = []
    for code, g in q.groupby("code"):
        kl = kline_by_code.get(code)
        diags.append(detect_pattern_for_code(g, code, kline=kl, thr=thr, t3_min_ratio=args.t3_min_ratio))
    a_hits = sum(1 for d in diags if d["A"])
    b_hits = sum(
        1 for d in diags
        if d.get("A") and any(t.get("B") for t in d.get("triples", []))
    )
    bc_hits = sum(
        1 for d in diags
        if d.get("A") and any(t.get("B") and t.get("C") for t in d.get("triples", []))
    )
    bcd_hits = sum(1 for d in diags if d.get("BCD"))
    LOGGER.info(
        "漏斗: 总=%d → A(4Q+持值)=%d → A+B(加仓)=%d → A+B+C(减仓)=%d → A+B+C+D(再加仓)=%d",
        len(diags), a_hits, b_hits, bc_hits, bcd_hits,
    )

    hits = assemble_hits(
        diags, names, strict_bcd=args.strict,
        quarters_held=qheld, latest_holding_mcap=lmcap,
    )
    LOGGER.info("最终候选（strict_bcd=%s）: %d", args.strict, len(hits))

    # Apply min-bcd-score filter; optionally save pre-filter "all" snapshot
    hits_all = hits.copy()
    if args.min_bcd_score > 0 and not hits.empty:
        before = len(hits)
        hits = hits[hits["bcd_score"] >= args.min_bcd_score].reset_index(drop=True)
        LOGGER.info(
            "--min-bcd-score=%.0f 过滤: %d → %d 只",
            args.min_bcd_score, before, len(hits),
        )

    # --save-all: 保存 score 过滤前的完整候选集（供存档/分析）
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
    diag_path.write_text(json.dumps(diags, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    LOGGER.info("诊断 JSON: %s (%d 股)", diag_path, len(diags))

    if args.require_ref:
        ref = "300401"
        ref_diag = next((d for d in diags if d["code"] == ref), None)
        if ref_diag is None:
            LOGGER.error("⚠ 300401 不在候选输入里")
            return 1
        if not ref_diag["A"]:
            LOGGER.error("⚠ 300401 未命中 A 段")
            return 1
        if args.strict and not ref_diag.get("BCD"):
            LOGGER.error(
                "⚠ 300401 BCD 未命中（strict）；triple BCD 数值见 diag JSON"
            )
            for t in ref_diag["triples"]:
                LOGGER.error(
                    "  triple %s→%s→%s  B=%s(price=%s vol=%s) C=%s(post=%s) D=%s",
                    t["t1"], t["t2"], t["t3"], t.get("B"),
                    t.get("price_pct"), t.get("vol_ratio"),
                    t.get("C"), t.get("post_ret_60d"), t.get("D"),
                )
            return 1
        # Also check 300401 survives min-bcd-score filter
        ref_in_hits = not hits.empty and (hits["code"] == ref).any()
        if args.min_bcd_score > 0 and not ref_in_hits:
            ref_score = hits[hits["code"] == ref]["bcd_score"].max() if ref_in_hits else \
                next((r["bcd_score"] for r in [
                    {"bcd_score": _compute_bcd_score(t)} for d in diags
                    if d["code"] == ref for t in (d.get("triples") or []) if t.get("BCD")
                ] if r), 0)
            LOGGER.warning(
                "⚠ 300401 BCD 命中但 bcd_score(%.1f) < --min-bcd-score(%.0f)，"
                "已从输出中过滤（可降低 --min-bcd-score 以包含）",
                ref_score, args.min_bcd_score,
            )
        LOGGER.info(
            "✓ 300401 A=%s BCD=%s; %d triples%s",
            ref_diag["A"], ref_diag.get("BCD"), len(ref_diag["triples"]),
            " (在最终输出中 ✓)" if ref_in_hits else " (被 min-bcd-score 过滤)",
        )
    return 0


def cmd_self_test(_args: argparse.Namespace) -> int:
    # SKILL self-test 序列：1000→1100→1200→1000→950→1300（FB-011 后 t3 持仓 1300 ≥ t2 持仓 1200）
    # 期望：t1=Q3(+9.1%) t2=Q4(-16.7%) t3=Q6(+36.8%)；non-monotonic
    df = pd.DataFrame(
        {
            "quarter": ["2024Q1", "2024Q2", "2024Q3", "2024Q4", "2025Q1", "2025Q2"],
            "holding_shares": [1000, 1100, 1200, 1000, 950, 1300],
            "holding_market_cap_cny": [0] * 6,
        }
    )
    diag = detect_pattern_for_code(df.assign(code="SELF"), "SELF")
    assert diag["A"] is True, f"expected A hit; got {diag}"
    assert len(diag["triples"]) >= 1
    LOGGER.info("triple example: %s", diag["triples"][0])

    # FB-011: 单边减仓途中末尾小反弹（t3 持仓 < t2 持仓）应被拒
    df_fakerebound = pd.DataFrame(
        {
            "quarter": ["2024Q1", "2024Q2", "2024Q3", "2024Q4"],
            "holding_shares": [2000, 1500, 1000, 1100],  # 1100 < 1500 (t2)
            "holding_market_cap_cny": [0] * 4,
        }
    )
    diag_fr = detect_pattern_for_code(df_fakerebound, "FAKE")
    assert diag_fr["A"] is False, f"FB-011: fake rebound must be rejected; got {diag_fr}"
    LOGGER.info("FB-011 fake rebound rejected ✓")

    # 单边加仓拒绝
    df_mono = pd.DataFrame(
        {
            "quarter": ["2024Q1", "2024Q2", "2024Q3", "2024Q4"],
            "holding_shares": [1000, 1100, 1300, 1500],
            "holding_market_cap_cny": [0] * 4,
        }
    )
    diag2 = detect_pattern_for_code(df_mono, "MONO")
    assert diag2["monotonic_up"] is True
    assert diag2["A"] is False, "monotonic up must reject"

    # 仅有减仓但无后续加仓
    df_no3 = pd.DataFrame(
        {
            "quarter": ["2024Q1", "2024Q2", "2024Q3"],
            "holding_shares": [1000, 1200, 1000],
            "holding_market_cap_cny": [0] * 3,
        }
    )
    diag3 = detect_pattern_for_code(df_no3, "NO3")
    assert diag3["A"] is False

    print("SELF_TEST_PASS: detect_rat_pattern (A段)")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="老鼠仓节奏识别 (A∧B∧C∧D)")
    p.add_argument("--db", default=DEFAULT_DB)
    p.add_argument("--in", dest="input", default=DEFAULT_IN)
    p.add_argument("--out", dest="output", default=DEFAULT_OUT)
    p.add_argument("--diag", default=DEFAULT_DIAG)
    p.add_argument("--strict", action="store_true",
                   help="要求至少一个 triple B∧C∧D 都为 True 才入命中")
    p.add_argument("--require-ref", action="store_true",
                   help="要求 300401 命中 A 段（与 --strict 联用时还需 BCD）")
    # 阈值（默认与 SKILL.md 一致）
    p.add_argument("--price-high-pct", type=float, default=0.85)
    p.add_argument("--vol-high-ratio", type=float, default=1.30)
    p.add_argument("--sell-fly-limit", type=float, default=0.15)
    p.add_argument("--plateau-range", type=float, default=0.25)
    p.add_argument("--low-pos-ratio", type=float, default=0.75)
    p.add_argument("--d-head-days", type=int, default=20)
    p.add_argument("--t3-min-ratio", type=float, default=T3_MIN_RATIO,
                   help="FB-011: holding_shares[t3] >= holding_shares[t2] * 该值；默认 1.0")
    p.add_argument("--min-bcd-score", type=float, default=0,
                   help="BCD 候选最低 bcd_score（0=不过滤，建议 50 用于生产）")
    p.add_argument("--save-all", action="store_true",
                   help="同时将 min-bcd-score 过滤前的完整候选集写入 <output>_all.parquet")
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
