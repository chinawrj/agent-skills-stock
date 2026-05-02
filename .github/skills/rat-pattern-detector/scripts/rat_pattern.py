"""rat_pattern.py — 核心模式识别逻辑（纯计算，无 IO，无 CLI）.

从 detect_rat_pattern.py 抽出，让主文件保持 ≤ 300 行。
"""
from __future__ import annotations

import pandas as pd

from bcd import Thresholds, compute_bcd, compute_score_components

# ---- 模块级阈值常量（SKILL.md 默认值，CLI 可覆盖） ----
THR_UP = 0.05
THR_DOWN = 0.05
T3_MIN_RATIO = 1.0  # FB-011: holding_shares[t3] >= holding_shares[t2] * T3_MIN_RATIO


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
    kline: "pd.DataFrame | None" = None,
    thr: "Thresholds | None" = None,
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

    return {
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
        "B": any(t.get("B") is True for t in triple_diags) if triple_diags else None,
        "C": any(t.get("C") is True for t in triple_diags) if triple_diags else None,
        "D": any(t.get("D") is True for t in triple_diags) if triple_diags else None,
        "BCD": any_bcd,
        "hit_triple_idx": hit_triple_idx,
    }


def compute_bcd_score(chosen: dict) -> float:
    """Signal-strength score (0–100). Delegates to bcd.compute_score_components."""
    return compute_score_components(
        chosen.get("price_pct"),
        chosen.get("vol_ratio"),
        chosen.get("post_ret_60d"),
    )["total"]


def assemble_hits(
    diags: list[dict],
    names: dict[str, str],
    *,
    strict_bcd: bool,
    quarters_held: "dict[str, int] | None" = None,
    latest_holding_mcap: "dict[str, float] | None" = None,
) -> pd.DataFrame:
    """Filter diags to final BCD hits DataFrame, sorted by bcd_score descending."""
    rows = []
    for d in diags:
        if not d["A"]:
            continue
        triples = d.get("triples") or []
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
                "bcd_score": compute_bcd_score(chosen),
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
