"""render_rat_report.py — M4 报告渲染入口.

辅助函数已移至 report_helpers.py；本文件只保留渲染主逻辑和 CLI。
"""
from __future__ import annotations
import argparse
import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd
import sys

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PARQUET = ROOT / "data" / "candidates_rat_pattern.parquet"
DEFAULT_DIAG = ROOT / "data" / "_diag_rat_pattern.json"
DEFAULT_OUT_DIR = ROOT / "reports"
DEFAULT_DB = ROOT / "data" / "a-share.db"

_BCD_SCRIPT_DIR = ROOT / ".github" / "skills" / "rat-pattern-detector" / "scripts"
if str(_BCD_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_BCD_SCRIPT_DIR))

try:
    from bcd import compute_score_components as _compute_score_components
except ImportError:
    _compute_score_components = None  # type: ignore[assignment]

from report_helpers import (
    load_hkscc_quarterly,
    format_holding_history,
    format_detection_reason,
    fetch_industry_map,
)

# Keep old private names as aliases so external code doesn't break
_load_hkscc_quarterly = load_hkscc_quarterly
_format_holding_history = format_holding_history
_fetch_industry_map = fetch_industry_map


def render(parquet: Path, diag: Path, out_dir: Path, db_path: Optional[Path] = None) -> Path:
    log = logging.getLogger("render_rat_report")
    df = pd.read_parquet(parquet) if parquet.exists() else pd.DataFrame()
    diags = json.loads(diag.read_text()) if diag.exists() else []

    industry_map: dict[str, str] = fetch_industry_map()
    if industry_map:
        log.info("加载行业标签: %d 只", len(industry_map))
    if not df.empty and industry_map:
        df["行业"] = df["code"].astype(str).str.zfill(6).map(industry_map).fillna("")

    quarterly: Optional[pd.DataFrame] = None
    hkscc_latest_date: Optional[str] = None
    kline_code_count: int = 0
    if db_path:
        quarterly = load_hkscc_quarterly(db_path)
        if quarterly is not None:
            quarterly["code"] = quarterly["code"].astype(str).str.zfill(6)
            log.info("加载 hkscc_quarterly: %d 行", len(quarterly))
        try:
            con = duckdb.connect(str(db_path), read_only=True)
            try:
                row = con.execute("SELECT MAX(quarter_end) FROM hkscc_quarterly").fetchone()
                if row and row[0]:
                    hkscc_latest_date = str(row[0])
                kline_code_count = con.execute(
                    "SELECT COUNT(DISTINCT code) FROM kline_daily"
                ).fetchone()[0]
            finally:
                con.close()
        except Exception:
            pass

    diag_by_code: dict[str, dict] = {d.get("code", ""): d for d in diags}
    today = date.today().strftime("%Y%m%d")
    out = out_dir / f"review-{today}.md"
    out_dir.mkdir(parents=True, exist_ok=True)

    lines: list[str] = []
    lines.append(f"# Rat-Trader 候选报告 — {today}")
    lines.append("")
    lines.append(f"- 候选数 (strict_bcd=True): **{len(df)}**")
    lines.append(f"- 诊断股票数 (A 段进入 BCD 的): **{len(diags)}**")
    if hkscc_latest_date:
        lines.append(f"- 港中结数据截止: **{hkscc_latest_date}**")
        try:
            latest_dt = datetime.strptime(hkscc_latest_date[:10], "%Y-%m-%d").date()
            stale_days = (date.today() - latest_dt).days
            if stale_days > 90:
                lines.append(
                    f"  ⚠️ **数据已过期 {stale_days} 天** — akshare 港中结数据截止约 2024-08-16，"
                    f"建议关注官方渠道更新（FB-015）"
                )
        except Exception:
            pass
    if kline_code_count:
        n_hkscc = len(pd.read_parquet(str(parquet.parent / "candidates_hkscc.parquet"))) \
            if (parquet.parent / "candidates_hkscc.parquet").exists() else "?"
        lines.append(
            f"- 筛选漏斗: HKSCC 候选 **{n_hkscc}** → "
            f"K 线覆盖 **{kline_code_count}** → "
            f"A 段命中 **{len(diags)}** → BCD 命中 **{len(df)}**"
        )
    lines.append("")

    lines.append("## 最终候选 (B∧C∧D)")
    if df.empty:
        lines.append("> 无命中。建议人工复查阈值或扩大 universe。")
    else:
        _render_candidates_table(lines, df)
        _render_industry_summary(lines, df, industry_map)
        _render_score_distribution(lines, df, parquet)
        _render_per_stock_sections(lines, df, today, out_dir, diag_by_code, quarterly, industry_map)

    lines.append("")
    lines.append("## 全部诊断（A 段命中股的 BCD 数值）")
    lines.append("")
    for d in diags:
        code = d.get("code", "?")
        name = d.get("name", "")
        lines.append(f"### {code} {name}")
        lines.append(f"- A={d.get('A')} BCD={d.get('BCD')} triples={len(d.get('triples', []))} "
                     f"monotonic_up={d.get('monotonic_up')}")
        triples = d.get("triples", [])
        if triples:
            tdf = pd.DataFrame(triples)
            keep = [c for c in ["t1", "t2", "t3", "B", "C", "D",
                                 "price_pct", "vol_ratio", "post_ret_60d",
                                 "plateau_range", "low_pos_ratio"] if c in tdf.columns]
            if keep:
                lines.append("")
                lines.append(tdf[keep].to_markdown(index=False))
        lines.append("")

    out.write_text("\n".join(lines))
    log.info("写入: %s (%d 候选 / %d 诊断)", out, len(df), len(diags))
    return out


def _render_candidates_table(lines: list[str], df: pd.DataFrame) -> None:
    cols = [c for c in [
        "code", "name", "行业", "quarters_held", "latest_mcap_cny", "t1", "t2", "t3",
        "B", "C", "D", "price_pct", "vol_ratio", "post_ret_60d", "bcd_score",
    ] if c in df.columns]
    display_df = df[cols].copy()
    if "latest_mcap_cny" in display_df.columns:
        display_df["latest_mcap_亿"] = (display_df["latest_mcap_cny"] / 1e8).round(2)
        display_df = display_df.drop(columns=["latest_mcap_cny"])
        col_order = ["code", "name", "行业", "quarters_held", "latest_mcap_亿"] + [
            c for c in display_df.columns if c not in
            ["code", "name", "行业", "quarters_held", "latest_mcap_亿"]
        ]
        col_order = [c for c in col_order if c in display_df.columns]
        display_df = display_df[col_order]
    lines.append(display_df.to_markdown(index=False))
    lines.append("")


def _render_industry_summary(lines: list[str], df: pd.DataFrame, industry_map: dict) -> None:
    if "行业" not in df.columns or not df["行业"].ne("").any():
        return
    lines.append("### 行业分布（相同行业聚集 = 嫌疑信号更强）")
    lines.append("")
    ind_counts = (
        df["行业"].replace("", "未分类").value_counts().reset_index()
    )
    if "行业" not in ind_counts.columns:
        ind_counts.columns = ["行业", "只数"]
    else:
        ind_counts = ind_counts.rename(columns={ind_counts.columns[1]: "只数"})
    lines.append(ind_counts.to_markdown(index=False))
    lines.append("")


def _render_score_distribution(lines: list[str], df: pd.DataFrame, parquet: Path) -> None:
    if "bcd_score" not in df.columns:
        return
    all_parquet = parquet.parent / "candidates_rat_pattern_all.parquet"
    all_n = len(pd.read_parquet(all_parquet)) if all_parquet.exists() else "?"
    rows = [{"分段": f"{lo}–{hi}", "BCD命中只数": int(((df["bcd_score"] >= lo) & (df["bcd_score"] < hi)).sum())}
            for lo, hi in [(0, 20), (20, 50), (50, 100)]]
    score_df = pd.DataFrame(rows)
    score_df.loc[len(score_df)] = {"分段": "全量(all)", "BCD命中只数": str(all_n)}
    lines.append("### Score 分布")
    lines.append("")
    lines.append(score_df.to_markdown(index=False))
    lines.append("")


def _render_per_stock_sections(
    lines: list[str], df: pd.DataFrame, today: str, out_dir: Path,
    diag_by_code: dict, quarterly: Optional[pd.DataFrame], industry_map: dict,
) -> None:
    figures_dir = out_dir / "figures"
    bcd_codes = df["code"].astype(str).str.zfill(6).tolist()
    names_map = dict(zip(
        df["code"].astype(str).str.zfill(6),
        df["name"] if "name" in df.columns else [""] * len(df),
    ))
    for code in bcd_codes:
        name = names_map.get(code, "")
        ind_label = f" ｜ {industry_map[code]}" if code in industry_map else ""
        png = figures_dir / f"{code}_{today}.png"
        if png.exists():
            rel = png.relative_to(out_dir)
            lines.append(f"### {code} {name}{ind_label} K 线 + 成交量")
            lines.append(f"![{code}]({rel.as_posix()})")
            lines.append("")
        else:
            lines.append(f"### {code} {name}{ind_label}")
            lines.append("")

        d = diag_by_code.get(code, {})
        best_triple: Optional[dict] = None
        if d.get("triples"):
            bcd_triples = [t for t in d["triples"] if t.get("B") and t.get("C") and t.get("D")]
            best_triple = bcd_triples[0] if bcd_triples else d["triples"][-1]
        reason = format_detection_reason(d, best_triple, _compute_score_components)
        lines.append(f"#### 🔍 检测依据 — {code} {name}")
        lines.append(f"> {reason}")
        lines.append("")

        if quarterly is not None:
            row_t1 = best_triple.get("t1") if best_triple else None
            row_t2 = best_triple.get("t2") if best_triple else None
            row_t3 = best_triple.get("t3") if best_triple else None
            lines.append(f"#### 📊 港中结持仓节奏 — {code} {name}")
            lines.append("")
            lines.append(format_holding_history(quarterly, code, t1=row_t1, t2=row_t2, t3=row_t3))
            lines.append("")

        lines.append(f"#### ✍️ 人工复盘 — {code} {name}")
        lines.append("")
        lines.append("| 检查项 | 结果 |")
        lines.append("|--------|------|")
        lines.append("| t2 季度价格在近 250 日高位区间（视觉） | ⬜ |")
        lines.append("| t2 季度出现明显放量 bar（红色高量） | ⬜ |")
        lines.append("| t3 季度买回后股价未大幅上涨（非卖飞） | ⬜ |")
        lines.append("| t1→t2→t3 三段节奏符合「建仓→出货→再建仓」 | ⬜ |")
        lines.append("")
        lines.append("**决策**: ⬜ PASS（纳入关注名单）　⬜ REJECT（假信号）")
        lines.append("")
        lines.append("**备注**: _（人工填写）_")
        lines.append("")


def main() -> None:
    ap = argparse.ArgumentParser(description="渲染 rat-trader markdown 报告")
    ap.add_argument("--parquet", default=str(DEFAULT_PARQUET))
    ap.add_argument("--diag", default=str(DEFAULT_DIAG))
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    ap.add_argument("--db", default=str(DEFAULT_DB))
    ap.add_argument("--no-db", action="store_true")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()
    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(name)s | %(message)s")
    db_path = None if args.no_db else Path(args.db)
    render(Path(args.parquet), Path(args.diag), Path(args.out_dir), db_path=db_path)


if __name__ == "__main__":
    main()
