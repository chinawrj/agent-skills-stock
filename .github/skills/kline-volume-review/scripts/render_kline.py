"""render_kline.py — M4 K 线 + 成交量复盘渲染。

为每只 BCD 命中候选股渲染一张 PNG：
  - 上图：close + MA250 + t1/t2/t3 季度末竖线（绿/红/蓝）
  - 下图：成交量柱（高位红、低位灰）

t1/t2/t3 来自 _diag_rat_pattern.json 中 hit_triple 的季度标识（如 "2024Q2"），
解析为该季度最后一个交易日，作为标注锚点。
"""
from __future__ import annotations
import argparse
import json
import logging
import re
from pathlib import Path

import duckdb
import matplotlib
matplotlib.use("Agg")  # headless
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

# CJK 字体 fallback：依次尝试 macOS / Linux 常见中文字体，不可用时降级
for _f in ("PingFang SC", "Heiti SC", "Hiragino Sans GB", "Arial Unicode MS",
           "Noto Sans CJK SC", "WenQuanYi Zen Hei", "SimHei", "Microsoft YaHei"):
    if any(_f.lower() == ff.name.lower()
           for ff in matplotlib.font_manager.fontManager.ttflist):
        plt.rcParams["font.sans-serif"] = [_f, "DejaVu Sans"]
        plt.rcParams["axes.unicode_minus"] = False
        break

LOGGER = logging.getLogger("render_kline")

DEFAULT_PARQUET = "data/candidates_rat_pattern.parquet"
DEFAULT_DIAG = "data/_diag_rat_pattern.json"
DEFAULT_DB = "data/a-share.db"
DEFAULT_OUT_DIR = "reports/figures"
LOOKBACK_DAYS = 500
MA_WINDOW = 250

QUARTER_RE = re.compile(r"^(\d{4})Q([1-4])$")
QUARTER_END_MONTH_DAY = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}
TRIPLE_COLORS = {"t1": "#2ca02c", "t2": "#d62728", "t3": "#1f77b4"}  # 绿减红蓝


def load_inputs(parquet: Path, diag: Path) -> tuple[pd.DataFrame, list[dict]]:
    df = pd.read_parquet(parquet) if parquet.exists() else pd.DataFrame()
    diags = json.loads(diag.read_text(encoding="utf-8")) if diag.exists() else []
    return df, diags


def fetch_kline(con: duckdb.DuckDBPyConnection, code: str, lookback: int) -> pd.DataFrame:
    df = con.execute(
        """
        SELECT date, close, volume
        FROM kline_daily
        WHERE code = ?
        ORDER BY date DESC
        LIMIT ?
        """,
        [code, lookback],
    ).fetchdf().sort_values("date").reset_index(drop=True)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df


def quarter_to_date(q: str) -> pd.Timestamp | None:
    m = QUARTER_RE.match(q or "")
    if not m:
        return None
    y, qi = int(m.group(1)), int(m.group(2))
    mo, d = QUARTER_END_MONTH_DAY[qi]
    return pd.Timestamp(year=y, month=mo, day=d)


def nearest_trading_date(kline: pd.DataFrame, target: pd.Timestamp) -> pd.Timestamp | None:
    if kline.empty or target is None:
        return None
    le = kline[kline["date"] <= target]
    if le.empty:
        return kline["date"].iloc[0]
    return le["date"].iloc[-1]


def render_one(code: str, name: str, kline: pd.DataFrame, hit_triple: dict,
               out_dir: Path, run_date: str) -> Path | None:
    if kline.empty:
        LOGGER.warning("%s 无 K 线数据，跳过", code)
        return None

    kline = kline.copy()
    kline["ma"] = kline["close"].rolling(MA_WINDOW, min_periods=1).mean()

    fig, (ax_p, ax_v) = plt.subplots(
        2, 1, figsize=(12, 6), sharex=True,
        gridspec_kw={"height_ratios": [2, 1]},
    )
    ax_p.plot(kline["date"], kline["close"], color="#222", linewidth=1.0, label="close")
    ax_p.plot(kline["date"], kline["ma"], color="#888", linewidth=0.8,
              linestyle="--", label=f"MA{MA_WINDOW}")

    # volume bars; 高量(>=1.3*median) 标红
    vol_med = kline["volume"].median() if len(kline) else 0
    colors = ["#c0392b" if v >= 1.3 * vol_med else "#bbbbbb" for v in kline["volume"]]
    ax_v.bar(kline["date"], kline["volume"], color=colors, width=1.0)

    # 标 t1/t2/t3 竖线
    for key in ("t1", "t2", "t3"):
        q = hit_triple.get(key)
        d = nearest_trading_date(kline, quarter_to_date(q))
        if d is None:
            continue
        color = TRIPLE_COLORS[key]
        for ax in (ax_p, ax_v):
            ax.axvline(d, color=color, linewidth=1.2, alpha=0.85)
        ax_p.text(d, ax_p.get_ylim()[1], f" {key}={q}",
                  color=color, fontsize=9, va="top", ha="left")

    ax_p.set_title(
        f"{code} {name} — t1={hit_triple.get('t1')} t2={hit_triple.get('t2')} "
        f"t3={hit_triple.get('t3')} | price_pct={hit_triple.get('price_pct')} "
        f"vol_ratio={hit_triple.get('vol_ratio')}"
    )
    ax_p.set_ylabel("close")
    ax_p.legend(loc="upper left", fontsize=8)
    ax_p.grid(True, alpha=0.25)
    ax_v.set_ylabel("volume")
    ax_v.grid(True, alpha=0.25)
    ax_v.xaxis.set_major_locator(mdates.YearLocator())
    ax_v.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax_v.xaxis.set_minor_locator(mdates.MonthLocator(bymonth=[1, 4, 7, 10]))
    fig.autofmt_xdate()
    fig.tight_layout()

    out_path = out_dir / f"{code}_{run_date}.png"
    fig.savefig(out_path, dpi=110)
    plt.close(fig)
    LOGGER.info("写入 %s (%d K bars)", out_path, len(kline))
    return out_path


def main() -> None:
    ap = argparse.ArgumentParser(description="K 线 + 成交量复盘图渲染")
    ap.add_argument("--parquet", default=DEFAULT_PARQUET)
    ap.add_argument("--diag", default=DEFAULT_DIAG)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    ap.add_argument("--lookback-days", type=int, default=LOOKBACK_DAYS)
    ap.add_argument("--run-date", default=pd.Timestamp.today().strftime("%Y%m%d"))
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )

    if args.self_test:
        # 合成数据 self-test：100 个交易日，确保 PNG 实际产出且无异常
        import numpy as np
        rng = pd.date_range("2023-01-03", periods=200, freq="B")
        rs = np.random.default_rng(42)
        close = 10 + np.cumsum(rs.normal(0, 0.2, len(rng)))
        kline = pd.DataFrame({"date": rng, "close": close,
                              "volume": rs.integers(1_000_000, 5_000_000, len(rng))})
        hit = {"t1": "2023Q2", "t2": "2023Q4", "t3": "2024Q2",
               "price_pct": 0.92, "vol_ratio": 1.5}
        out_dir = Path(args.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        out = render_one("SELFTEST", "synthetic", kline, hit, out_dir, args.run_date)
        assert out is not None and out.exists(), "self-test PNG 未生成"
        LOGGER.info("SELF_TEST_PASS: render_kline → %s (%d bytes)",
                    out, out.stat().st_size)
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
    rendered = 0
    try:
        for _, row in df.iterrows():
            code = str(row["code"]).zfill(6)
            name = row.get("name", "")
            d = diag_by_code.get(code, {})
            triples = d.get("triples", [])
            hit = next((t for t in triples if t.get("BCD")), {})
            kline = fetch_kline(con, code, args.lookback_days)
            if render_one(code, name, kline, hit, out_dir, args.run_date):
                rendered += 1
    finally:
        con.close()
    LOGGER.info("完成: 渲染 %d / %d 候选", rendered, len(df))


if __name__ == "__main__":
    main()
