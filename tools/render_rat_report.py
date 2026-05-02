"""
M4 最小报告渲染器 — 把 candidates_rat_pattern.parquet + diag JSON 渲染成
reports/rat_candidates_YYYYMMDD.md，供人工复盘。

不画 K 线（那是 M4 完整版 kline-volume-review 的事），先把表格 + 命中
诊断写出来，让人工能据此决定是否进一步看图。
"""
from __future__ import annotations
import argparse
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PARQUET = ROOT / "data" / "candidates_rat_pattern.parquet"
DEFAULT_DIAG = ROOT / "data" / "_diag_rat_pattern.json"
DEFAULT_OUT_DIR = ROOT / "reports"
DEFAULT_DB = ROOT / "data" / "a-share.db"

# Score breakdown (mirrors bcd.compute_score_components without requiring sys.path change)
_BCD_SCRIPT_DIR = ROOT / ".github" / "skills" / "rat-pattern-detector" / "scripts"
if str(_BCD_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_BCD_SCRIPT_DIR))

try:
    from bcd import compute_score_components as _compute_score_components
except ImportError:
    _compute_score_components = None  # type: ignore[assignment]


def _load_hkscc_quarterly(db_path: Path) -> Optional[pd.DataFrame]:
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


def _format_holding_history(
    quarterly: pd.DataFrame,
    code: str,
    last_n: int = 12,
    t1: Optional[str] = None,
    t2: Optional[str] = None,
    t3: Optional[str] = None,
) -> str:
    """Format the recent N quarters of HKSCC holding history as a markdown table.

    Marks t1/t2/t3 quarters with signal labels and shows QoQ change direction.
    """
    sub = quarterly[quarterly["code"].astype(str).str.zfill(6) == code].copy()
    if sub.empty:
        return "_（无港中结持仓历史数据）_"
    sub = sub.sort_values("quarter").tail(last_n).reset_index(drop=True)
    sub["持股市值(亿)"] = (sub["holding_market_cap_cny"] / 1e8).round(2)
    # QoQ change direction arrow
    prev_mcap = sub["holding_market_cap_cny"].shift(1)
    sub["环比"] = ""
    sub.loc[sub["holding_market_cap_cny"] > prev_mcap * 1.05, "环比"] = "↑"
    sub.loc[sub["holding_market_cap_cny"] < prev_mcap * 0.95, "环比"] = "↓"
    # t1/t2/t3 signal markers
    signal_map = {t1: "▲ t1 加仓", t2: "▼ t2 减仓", t3: "▲ t3 再加仓"}
    sub["信号"] = sub["quarter"].map(lambda q: signal_map.get(q, ""))
    out = sub.rename(columns={"quarter": "季度"})[["季度", "持股市值(亿)", "环比", "信号"]]
    return out.to_markdown(index=False)


def _format_detection_reason(diag: dict, best_triple: Optional[dict] = None) -> str:
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
        if _compute_score_components is not None:
            bd = _compute_score_components(pp, vr, best_triple.get("post_ret_60d"))
            parts.append(
                f"评分分解: price={bd['price_pts']} + vol={bd['vol_pts']} "
                f"+ c={bd['c_pts']} + bonus={bd['bonus_pts']:.0f} = **{bd['total']}**"
            )
    n_t = len(diag.get("triples", []))
    parts.append(f"候选 triple 数: {n_t}")
    return " ｜ ".join(parts) if parts else "_（无额外信息）_"


def render(parquet: Path, diag: Path, out_dir: Path, db_path: Optional[Path] = None) -> Path:
    log = logging.getLogger("render_rat_report")
    df = pd.read_parquet(parquet) if parquet.exists() else pd.DataFrame()
    diags = json.loads(diag.read_text()) if diag.exists() else []

    # Load HKSCC quarterly history for holding trajectory display
    quarterly: Optional[pd.DataFrame] = None
    hkscc_latest_date: Optional[str] = None
    kline_code_count: int = 0
    if db_path:
        quarterly = _load_hkscc_quarterly(db_path)
        if quarterly is not None:
            quarterly["code"] = quarterly["code"].astype(str).str.zfill(6)
            log.info("加载 hkscc_quarterly: %d 行", len(quarterly))
        # Get HKSCC data freshness and kline coverage
        try:
            con = duckdb.connect(str(db_path), read_only=True)
            try:
                row = con.execute(
                    "SELECT MAX(quarter_end) FROM hkscc_quarterly"
                ).fetchone()
                if row and row[0]:
                    hkscc_latest_date = str(row[0])
                kline_code_count = con.execute(
                    "SELECT COUNT(DISTINCT code) FROM kline_daily"
                ).fetchone()[0]
            finally:
                con.close()
        except Exception:
            pass

    # Build diag lookup by code
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
        # Warn if HKSCC data is stale (akshare cap: 2024-08-16)
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
            f"A 段命中 **{len(diags)}** → "
            f"BCD 命中 **{len(df)}**"
        )
    lines.append("")

    lines.append("## 最终候选 (B∧C∧D)")
    if df.empty:
        lines.append("> 无命中。建议人工复查阈值或扩大 universe。")
    else:
        cols = [c for c in [
            "code", "name", "quarters_held", "latest_mcap_cny", "t1", "t2", "t3",
            "B", "C", "D",
            "price_pct", "vol_ratio", "post_ret_60d", "bcd_score",
        ] if c in df.columns]
        # Format latest_mcap_cny as millions (亿)
        display_df = df[cols].copy()
        if "latest_mcap_cny" in display_df.columns:
            display_df["latest_mcap_亿"] = (display_df["latest_mcap_cny"] / 1e8).round(2)
            display_df = display_df.drop(columns=["latest_mcap_cny"])
            # reorder to put 亿 after quarters_held
            col_order = ["code", "name", "quarters_held", "latest_mcap_亿"] + [
                c for c in display_df.columns if c not in
                ["code", "name", "quarters_held", "latest_mcap_亿"]
            ]
            display_df = display_df[col_order]
        lines.append(display_df.to_markdown(index=False))
        lines.append("")
        # 嵌入 K 线图（kline-volume-review 产物）
        figures_dir = out_dir / "figures"
        bcd_codes = df["code"].astype(str).str.zfill(6).tolist()
        names_map = dict(zip(
            df["code"].astype(str).str.zfill(6),
            df["name"] if "name" in df.columns else [""] * len(df),
        ))
        for code in bcd_codes:
            name = names_map.get(code, "")
            png = figures_dir / f"{code}_{today}.png"
            if png.exists():
                rel = png.relative_to(out_dir)
                lines.append(f"### {code} {name} K 线 + 成交量")
                lines.append(f"![{code}]({rel.as_posix()})")
                lines.append("")

            # Detection reason
            d = diag_by_code.get(code, {})
            best_triple: Optional[dict] = None
            if d.get("triples"):
                # Pick the triple where B∧C∧D all True if possible, else first
                bcd_triples = [t for t in d["triples"] if t.get("B") and t.get("C") and t.get("D")]
                best_triple = bcd_triples[0] if bcd_triples else d["triples"][-1]
            reason = _format_detection_reason(d, best_triple)
            lines.append(f"#### 🔍 检测依据 — {code} {name}")
            lines.append(f"> {reason}")
            lines.append("")

            # HKSCC holding history (with t1/t2/t3 markers from best triple)
            if quarterly is not None:
                row_t1 = best_triple.get("t1") if best_triple else None
                row_t2 = best_triple.get("t2") if best_triple else None
                row_t3 = best_triple.get("t3") if best_triple else None
                lines.append(f"#### 📊 港中结持仓节奏 — {code} {name}")
                lines.append("")
                lines.append(_format_holding_history(quarterly, code, t1=row_t1, t2=row_t2, t3=row_t3))
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
    lines.append("")

    lines.append("## 全部诊断（A 段命中股的 BCD 数值）")
    lines.append("")
    for d in diags:
        code = d.get("code", "?")
        name = d.get("name", "")
        a = d.get("A")
        bcd = d.get("BCD")
        n_t = len(d.get("triples", []))
        lines.append(f"### {code} {name}")
        lines.append(f"- A={a} BCD={bcd} triples={n_t} monotonic_up={d.get('monotonic_up')}")
        triples = d.get("triples", [])
        if triples:
            tdf = pd.DataFrame(triples)
            keep = [c for c in [
                "t1", "t2", "t3", "B", "C", "D",
                "price_pct", "vol_ratio", "post_ret_60d",
                "plateau_range", "low_pos_ratio",
            ] if c in tdf.columns]
            if keep:
                lines.append("")
                lines.append(tdf[keep].to_markdown(index=False))
        lines.append("")

    out.write_text("\n".join(lines))
    log.info("写入: %s (%d 候选 / %d 诊断)", out, len(df), len(diags))
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="渲染 rat-trader markdown 报告")
    ap.add_argument("--parquet", default=str(DEFAULT_PARQUET))
    ap.add_argument("--diag", default=str(DEFAULT_DIAG))
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    ap.add_argument("--db", default=str(DEFAULT_DB), help="DuckDB 路径（读取 hkscc_quarterly）")
    ap.add_argument("--no-db", action="store_true", help="跳过 hkscc 历史加载")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    db_path = None if args.no_db else Path(args.db)
    render(Path(args.parquet), Path(args.diag), Path(args.out_dir), db_path=db_path)


if __name__ == "__main__":
    main()
