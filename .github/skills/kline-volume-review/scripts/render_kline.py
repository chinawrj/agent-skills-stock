"""render_kline.py — M4 K 线 + 成交量复盘渲染。

为每只 BCD 命中候选股渲染一张 PNG：
  - 上图：mplfinance OHLC 蜡烛图 + MA250 + t1/t2/t3 季度末竖线（绿/红/蓝）
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
import matplotlib.pyplot as plt
import mplfinance as mpf
import pandas as pd

# CJK 字体 fallback：依次尝试 macOS / Linux 常见中文字体，不可用时降级
_CJK_FONT: str | None = None
for _f in ("PingFang HK", "PingFang SC", "PingFang TC",
           "Hiragino Sans", "Heiti SC", "Hiragino Sans GB", "Arial Unicode MS",
           "Noto Sans CJK SC", "WenQuanYi Zen Hei", "SimHei", "Microsoft YaHei"):
    if any(_f.lower() == ff.name.lower()
           for ff in matplotlib.font_manager.fontManager.ttflist):
        _CJK_FONT = _f
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
TRIPLE_COLORS = {"t1": "#2ca02c", "t2": "#d62728", "t3": "#1f77b4"}  # 绿/红/蓝
VOL_HIGH_COLOR = "#c0392b"
VOL_LOW_COLOR = "#bbbbbb"
VOL_HIGH_RATIO = 1.3


def load_inputs(parquet: Path, diag: Path) -> tuple[pd.DataFrame, list[dict]]:
    df = pd.read_parquet(parquet) if parquet.exists() else pd.DataFrame()
    diags = json.loads(diag.read_text(encoding="utf-8")) if diag.exists() else []
    return df, diags


def fetch_kline(con: duckdb.DuckDBPyConnection, code: str, lookback: int) -> pd.DataFrame:
    df = con.execute(
        """
        SELECT date, open, high, low, close, volume
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

    # mplfinance 需要 DatetimeIndex + OHLCV 列名大写
    ohlcv = kline.copy()
    ohlcv["date"] = pd.to_datetime(ohlcv["date"])
    ohlcv = ohlcv.set_index("date")
    has_ohlc = all(c in ohlcv.columns for c in ("open", "high", "low"))
    ohlcv = ohlcv.rename(columns={
        "open": "Open", "high": "High", "low": "Low",
        "close": "Close", "volume": "Volume",
    })

    # MA250 overlay
    ma = ohlcv["Close"].rolling(MA_WINDOW, min_periods=1).mean()
    ap_list = [mpf.make_addplot(ma, color="#888888", linestyle="--", width=0.8)]

    # t1/t2/t3 vlines（mplfinance 接受 Timestamp）
    vline_dates: list[pd.Timestamp] = []
    vline_colors: list[str] = []
    vline_labels: dict[pd.Timestamp, str] = {}
    for key in ("t1", "t2", "t3"):
        q = hit_triple.get(key)
        d = nearest_trading_date(kline, quarter_to_date(q))
        if d is not None:
            vline_dates.append(d)
            vline_colors.append(TRIPLE_COLORS[key])
            vline_labels[d] = f"{key}={q}"

    title = (
        f"{code} {name}  t1={hit_triple.get('t1')} t2={hit_triple.get('t2')} "
        f"t3={hit_triple.get('t3')} | price_pct={hit_triple.get('price_pct')} "
        f"vol_ratio={hit_triple.get('vol_ratio')}"
    )

    # mplfinance style（透传 CJK 字体）
    rc_font: dict = {}
    if _CJK_FONT:
        rc_font = {"font.sans-serif": [_CJK_FONT, "DejaVu Sans"],
                   "axes.unicode_minus": False}
    mpf_style = mpf.make_mpf_style(base_mpf_style="yahoo", rc=rc_font)

    mpf_kwargs: dict = dict(
        type="candle" if has_ohlc else "line",
        style=mpf_style,
        volume=True,
        addplot=ap_list,
        returnfig=True,
        figsize=(12, 6),
        panel_ratios=(3, 1),
        title=title,
    )
    if vline_dates:
        mpf_kwargs["vlines"] = dict(
            vlines=vline_dates,
            linewidths=1.2,
            colors=vline_colors,
            alpha=0.85,
        )

    fig, axes = mpf.plot(ohlcv, **mpf_kwargs)
    ax_price = axes[0]
    ax_vol = axes[2]  # volume panel index with volume=True

    # 高量柱标红
    vol_med = ohlcv["Volume"].median()
    for patch, vol in zip(ax_vol.patches, ohlcv["Volume"]):
        patch.set_color(VOL_HIGH_COLOR if vol >= VOL_HIGH_RATIO * vol_med else VOL_LOW_COLOR)

    # t1/t2/t3 文字标注（x 轴用整数位置）
    for ts, lbl in vline_labels.items():
        locs = ohlcv.index.get_indexer([ts], method="nearest")
        if locs[0] < 0:
            continue
        pos = locs[0]
        color = vline_colors[vline_dates.index(ts)]
        ylim = ax_price.get_ylim()
        ax_price.text(pos, ylim[1], f" {lbl}", color=color, fontsize=9, va="top", ha="left")

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
    ap.add_argument("--top-n", type=int, default=0,
                   help="仅渲染 bcd_score 前 N 只（0=全部）")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )

    if args.self_test:
        # 合成数据 self-test：200 个交易日，确保蜡烛图 PNG 实际产出且无异常
        import numpy as np
        rng = pd.date_range("2023-01-03", periods=200, freq="B")
        rs = np.random.default_rng(42)
        close = 10 + np.cumsum(rs.normal(0, 0.2, len(rng)))
        high = close + rs.uniform(0.1, 0.5, len(rng))
        low = close - rs.uniform(0.1, 0.5, len(rng))
        open_ = close + rs.normal(0, 0.15, len(rng))
        kline = pd.DataFrame({
            "date": rng,
            "open": open_, "high": high, "low": low, "close": close,
            "volume": rs.integers(1_000_000, 5_000_000, len(rng)),
        })
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

    # Apply top-N filter by bcd_score
    if args.top_n > 0 and "bcd_score" in df.columns:
        total = len(df)
        df = df.sort_values("bcd_score", ascending=False).head(args.top_n).reset_index(drop=True)
        LOGGER.info("--top-n=%d 过滤: %d → %d 只（按 bcd_score 取前 %d）",
                    args.top_n, total, len(df), args.top_n)

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
