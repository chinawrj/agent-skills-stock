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
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PARQUET = ROOT / "data" / "candidates_rat_pattern.parquet"
DEFAULT_DIAG = ROOT / "data" / "_diag_rat_pattern.json"
DEFAULT_OUT_DIR = ROOT / "reports"


def render(parquet: Path, diag: Path, out_dir: Path) -> Path:
    log = logging.getLogger("render_rat_report")
    df = pd.read_parquet(parquet) if parquet.exists() else pd.DataFrame()
    diags = json.loads(diag.read_text()) if diag.exists() else []

    today = date.today().strftime("%Y%m%d")
    out = out_dir / f"rat_candidates_{today}.md"
    out_dir.mkdir(parents=True, exist_ok=True)

    lines: list[str] = []
    lines.append(f"# Rat-Trader 候选报告 — {today}")
    lines.append("")
    lines.append(f"- 候选数 (strict_bcd=True): **{len(df)}**")
    lines.append(f"- 诊断股票数 (A 段进入 BCD 的): **{len(diags)}**")
    lines.append("")

    lines.append("## 最终候选 (B∧C∧D)")
    if df.empty:
        lines.append("> 无命中。建议人工复查阈值或扩大 universe。")
    else:
        cols = [c for c in [
            "code", "name", "t1", "t2", "t3",
            "B", "C", "D",
            "price_pct", "vol_ratio", "post_ret_60d",
        ] if c in df.columns]
        lines.append(df[cols].to_markdown(index=False))
        lines.append("")
        # 嵌入 K 线图（kline-volume-review 产物）
        figures_dir = out_dir / "figures"
        for code in df["code"].astype(str).str.zfill(6):
            png = figures_dir / f"{code}_{today}.png"
            if png.exists():
                rel = png.relative_to(out_dir)
                lines.append(f"### {code} K 线 + 成交量")
                lines.append(f"![{code}]({rel.as_posix()})")
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
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    render(Path(args.parquet), Path(args.diag), Path(args.out_dir))


if __name__ == "__main__":
    main()
