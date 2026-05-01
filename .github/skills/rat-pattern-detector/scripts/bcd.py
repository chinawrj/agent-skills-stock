"""bcd.py — 老鼠仓节奏 B/C/D 段判定（与 detect_rat_pattern.py 解耦）.

输入约定：
  kline 是单只股的 K 线 DataFrame，按 date 升序，列至少含 date/close/volume；
  date 列必须是 pandas Timestamp（或可被 pd.Timestamp 识别）。

阈值通过 thresholds dict 传入；调用方负责从 detect_rat_pattern.py 顶部模块常量取值。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import pandas as pd

LOGGER = logging.getLogger("rat_pattern.bcd")


@dataclass(frozen=True)
class Thresholds:
    price_high_pct: float = 0.85
    vol_high_ratio: float = 1.30
    sell_fly_limit: float = 0.15
    plateau_range: float = 0.25
    low_pos_ratio: float = 0.75
    lookback: int = 250
    post_window: int = 60
    d_head_days: int = 20  # 加仓季度建仓初期窗口（D 段）


def _quarter_slice(kline: pd.DataFrame, quarter: str) -> pd.DataFrame:
    """取属于 pd.Period(quarter) 的 K 线行。"""
    p = pd.Period(quarter, freq="Q")
    start = p.start_time.normalize()
    end = p.end_time.normalize()
    mask = (kline["date"] >= start) & (kline["date"] <= end)
    return kline.loc[mask].sort_values("date").reset_index(drop=True)


def _lookback_slice(kline: pd.DataFrame, before: pd.Timestamp, n: int) -> pd.DataFrame:
    sub = kline[kline["date"] < before].sort_values("date").tail(n)
    return sub.reset_index(drop=True)


def _post_slice(kline: pd.DataFrame, after: pd.Timestamp, n: int) -> pd.DataFrame:
    sub = kline[kline["date"] > after].sort_values("date").head(n)
    return sub.reset_index(drop=True)


def _b_section(kline: pd.DataFrame, t2_q: str, thr: Thresholds) -> dict:
    t2 = _quarter_slice(kline, t2_q)
    if t2.empty:
        return {"price_pct": None, "vol_ratio": None, "B": False, "_reason_b": "t2 quarter empty"}
    t2_first = t2["date"].min()
    lb = _lookback_slice(kline, t2_first, thr.lookback)
    if lb.empty:
        return {"price_pct": None, "vol_ratio": None, "B": False, "_reason_b": "lookback empty"}
    price_pct = float(t2["close"].max()) / float(lb["close"].max())
    vol_ratio = float(t2["volume"].mean()) / float(lb["volume"].mean()) if lb["volume"].mean() else None
    high = price_pct >= thr.price_high_pct
    big_vol = (vol_ratio is not None) and (vol_ratio >= thr.vol_high_ratio)
    return {
        "price_pct": round(price_pct, 4),
        "vol_ratio": round(vol_ratio, 4) if vol_ratio is not None else None,
        "B": bool(high or big_vol),
    }


def _c_section(kline: pd.DataFrame, t2_q: str, t3_q: str, thr: Thresholds) -> dict:
    t2 = _quarter_slice(kline, t2_q)
    if t2.empty:
        return {"post_ret_60d": None, "C": False, "_reason_c": "t2 quarter empty"}
    t2_end = t2["date"].max()
    t2_close = float(t2.iloc[-1]["close"])
    t3_start = pd.Period(t3_q, freq="Q").start_time.normalize()
    post = _post_slice(kline, t2_end, thr.post_window)
    # 窗口截止到 t3 开始日（exclusive） — 排除机构自推推涨期
    post = post[post["date"] < t3_start].reset_index(drop=True)
    if len(post) < 10:
        # 几乎全部落入 t3：机构自推推涨，C 默认通过
        return {
            "post_ret_60d": None,
            "C": True,
            "_reason_c": f"post window <10d (n={len(post)}) — t3 self-push, pass",
        }
    post_max = float(post["close"].max())
    post_ret = post_max / t2_close - 1.0
    return {
        "post_ret_60d": round(post_ret, 4),
        "C": bool(post_ret < thr.sell_fly_limit),
        "post_n": int(len(post)),
    }


def _d_part(kline: pd.DataFrame, q: str, thr: Thresholds) -> Optional[dict]:
    """对加仓季度 q，取季度内前 d_head_days 个交易日作为"建仓初期"窗口判定低位/平台。"""
    g = _quarter_slice(kline, q)
    if g.empty:
        return None
    head = g.head(thr.d_head_days)
    if head.empty:
        return None
    cmin = float(head["close"].min())
    cmax = float(head["close"].max())
    plateau = (cmax - cmin) / cmin if cmin > 0 else None
    lb = _lookback_slice(kline, head["date"].min(), thr.lookback)
    low_pos = (float(head["close"].mean()) / float(lb["close"].max())) if (not lb.empty and lb["close"].max()) else None
    is_plateau = (plateau is not None) and (plateau < thr.plateau_range)
    is_low = (low_pos is not None) and (low_pos < thr.low_pos_ratio)
    return {
        "n_head": int(len(head)),
        "plateau_range": round(plateau, 4) if plateau is not None else None,
        "low_pos": round(low_pos, 4) if low_pos is not None else None,
        "ok": bool(is_plateau or is_low),
    }


def _d_section(kline: pd.DataFrame, t1_q: str, t3_q: str, thr: Thresholds) -> dict:
    d1 = _d_part(kline, t1_q, thr)
    d3 = _d_part(kline, t3_q, thr)
    return {
        "D_t1": d1,
        "D_t3": d3,
        "D": bool(d1 and d3 and d1["ok"] and d3["ok"]),
    }


def compute_bcd(kline: pd.DataFrame, t1_q: str, t2_q: str, t3_q: str, thr: Thresholds) -> dict:
    """对一个 (t1,t2,t3) 三元组计算 B/C/D 数值与命中。"""
    if kline.empty:
        return {"B": False, "C": False, "D": False, "_reason": "kline empty"}
    out = {}
    out.update(_b_section(kline, t2_q, thr))
    out.update(_c_section(kline, t2_q, t3_q, thr))
    out.update(_d_section(kline, t1_q, t3_q, thr))
    out["BCD"] = bool(out.get("B") and out.get("C") and out.get("D"))
    return out
