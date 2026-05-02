"""report_helpers.py — 报告渲染辅助函数（无 CLI，纯计算/格式化）.

从 render_rat_report.py 抽出，让主文件保持 ≤ 300 行。
"""
from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd


def load_hkscc_quarterly(db_path: Path) -> Optional[pd.DataFrame]:
    """Load hkscc_quarterly from DB for holding history display."""
    if not db_path.exists():
        return None
    try:
        con = duckdb.connect(str(db_path), read_only=True)
        try:
            df = con.execute(
                "SELECT code, quarter, holding_shares, holding_market_cap_cny "
                "FROM hkscc_quarterly ORDER BY code, quarter"
            ).fetchdf()
            return df
        finally:
            con.close()
    except Exception as exc:
        logging.getLogger("render_rat_report").warning("无法加载 hkscc_quarterly: %s", exc)
        return None


def format_holding_history(
    quarterly: pd.DataFrame,
    code: str,
    last_n: int = 12,
    t1: Optional[str] = None,
    t2: Optional[str] = None,
    t3: Optional[str] = None,
) -> str:
    """Format the recent N quarters of HKSCC holding history as a markdown table."""
    sub = quarterly[quarterly["code"].astype(str).str.zfill(6) == code].copy()
    if sub.empty:
        return "_（无港中结持仓历史数据）_"
    sub = sub.sort_values("quarter").tail(last_n).reset_index(drop=True)
    sub["持股市值(亿)"] = (sub["holding_market_cap_cny"] / 1e8).round(2)
    prev_mcap = sub["holding_market_cap_cny"].shift(1)
    sub["环比"] = ""
    sub.loc[sub["holding_market_cap_cny"] > prev_mcap * 1.05, "环比"] = "↑"
    sub.loc[sub["holding_market_cap_cny"] < prev_mcap * 0.95, "环比"] = "↓"
    signal_map = {t1: "▲ t1 加仓", t2: "▼ t2 减仓", t3: "▲ t3 再加仓"}
    sub["信号"] = sub["quarter"].map(lambda q: signal_map.get(q, ""))
    out = sub.rename(columns={"quarter": "季度"})[["季度", "持股市值(亿)", "环比", "信号"]]
    return out.to_markdown(index=False)


def format_detection_reason(
    diag: dict,
    best_triple: Optional[dict] = None,
    compute_score_fn=None,
) -> str:
    """Generate a human-readable detection reason string with score breakdown."""
    parts = []
    if best_triple:
        t1, t2, t3 = best_triple.get("t1"), best_triple.get("t2"), best_triple.get("t3")
        pp = best_triple.get("price_pct", 0)
        vr = best_triple.get("vol_ratio", 0)
        parts.append(f"最佳 triple: **{t1} → {t2} → {t3}**")
        parts.append(f"B 触发: price_pct={pp:.2f} vol_ratio={vr:.2f}")
        if best_triple.get("post_ret_60d") is not None and str(best_triple.get("post_ret_60d")) != "nan":
            parts.append(f"t2 后 60 日收益: {float(best_triple['post_ret_60d']):.1%}")
        if compute_score_fn is not None:
            bd = compute_score_fn(pp, vr, best_triple.get("post_ret_60d"))
            parts.append(
                f"评分分解: price={bd['price_pts']} + vol={bd['vol_pts']} "
                f"+ c={bd['c_pts']} + bonus={bd['bonus_pts']:.0f} = **{bd['total']}**"
            )
    n_t = len(diag.get("triples", []))
    parts.append(f"候选 triple 数: {n_t}")
    return " ｜ ".join(parts) if parts else "_（无额外信息）_"


def fetch_industry_map() -> dict[str, str]:
    """Return {6-digit-code: industry_name} from baostock (证监会行业分类). Graceful-degrades."""
    try:
        import baostock as bs  # type: ignore[import]

        lg = bs.login()
        if lg.error_code != "0":
            return {}
        rs = bs.query_stock_industry()
        rows = []
        while rs.error_code == "0" and rs.next():
            rows.append(rs.get_row_data())
        bs.logout()
        ind_map: dict[str, str] = {}
        for row in rows:
            if len(row) < 4:
                continue
            bs_code, industry_raw = row[1], row[3]
            m = re.match(r"(?:sh|sz)\.(\d{6})", bs_code)
            if not m:
                continue
            ind_map[m.group(1)] = re.sub(r"^[A-Z]\d*", "", industry_raw).strip()
        return ind_map
    except Exception:
        return {}
