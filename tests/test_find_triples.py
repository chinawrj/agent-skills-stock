"""find_triples 单元测试 — 不依赖 DB。

覆盖 FB-011 (持仓量绝对约束) + 常规真三段 / 单边加仓 / 末尾减仓等边界。
"""
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / ".github" / "skills" / "rat-pattern-detector" / "scripts"))

from detect_rat_pattern import find_triples, detect_pattern_for_code  # noqa: E402


def _df(holdings: list[int]) -> pd.DataFrame:
    return pd.DataFrame({
        "quarter": [f"2024Q{i+1}" if i < 4 else f"2025Q{i-3}" for i in range(len(holdings))],
        "holding_shares": holdings,
        "holding_market_cap_cny": [0] * len(holdings),
    })


def test_real_three_stage_hits():
    df = _df([1000, 1100, 1200, 1000, 950, 1300])  # t3=1300 ≥ t2=1200
    triples = find_triples(df.assign(delta_pct=df["holding_shares"].pct_change()))
    assert len(triples) >= 1, "真三段必须命中"


def test_fb011_threshold_filters_triples():
    # [3000, 8000, 4000, 7000]: 加(+167%)→减(-50%)→加(+75%)
    # holdings[t3]/holdings[t2] = 7000/4000 = 1.75
    df = _df([3000, 8000, 4000, 7000])
    deltas_df = df.assign(delta_pct=df["holding_shares"].pct_change())
    # ratio=1.0 (默认严格): 7000 ≥ 4000 → 通过
    triples_loose = find_triples(deltas_df, t3_min_ratio=1.0)
    assert len(triples_loose) == 1
    # ratio=2.0 (人为更严): 7000 ≥ 8000 False → 拒绝
    triples_strict = find_triples(deltas_df, t3_min_ratio=2.0)
    assert triples_strict == []


def test_fb011_fake_rebound_rejected():
    # 单边减仓，最末小反弹但缺前置加仓 → 无 t1 → 0 triples
    df = _df([2000, 1500, 1000, 1100])
    triples = find_triples(df.assign(delta_pct=df["holding_shares"].pct_change()))
    assert triples == [], f"FB-011 假反弹必须被拒, got {triples}"


def test_monotonic_up_no_t2_no_triple():
    df = _df([1000, 1100, 1300, 1500])
    triples = find_triples(df.assign(delta_pct=df["holding_shares"].pct_change()))
    assert triples == []


def test_garden_biotech_synthetic():
    # 模拟 300401 hit triple: t1 加仓 → t2 持续减仓 → t3 大幅再加仓
    # 数值序列接近真实（百万股级别）
    df = _df([3664087, 7504422, 6000000, 5000000, 3358441, 2424731, 4636076])
    diag = detect_pattern_for_code(df.assign(code="X"), "X")
    assert diag["A"] is True, f"synthetic 300401 must hit A; diag={diag}"
