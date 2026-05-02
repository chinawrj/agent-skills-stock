"""Tests for tools/report_helpers.py.

Covers format_holding_history and format_detection_reason;
no network calls required.
"""
from __future__ import annotations
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT / "tools") not in sys.path:
    sys.path.insert(0, str(ROOT / "tools"))

from report_helpers import format_holding_history, format_detection_reason


# ---- format_holding_history ----

def _make_quarterly(code: str, n: int = 8) -> pd.DataFrame:
    """Minimal synthetic hkscc_quarterly data."""
    import pandas as pd  # noqa: F811
    quarters = [f"202{3 + i // 4}Q{(i % 4) + 1}" for i in range(n)]
    return pd.DataFrame({
        "code": [code] * n,
        "quarter": quarters,
        "holding_shares": [1_000_000 + i * 50_000 for i in range(n)],
        "holding_market_cap_cny": [3e7 + i * 5e6 for i in range(n)],
    })


def test_holding_history_returns_markdown_table():
    df = _make_quarterly("300401")
    result = format_holding_history(df, "300401")
    assert "季度" in result
    assert "持股市值(亿)" in result
    assert "2023Q1" in result


def test_holding_history_empty_code_returns_placeholder():
    df = _make_quarterly("300401")
    result = format_holding_history(df, "999999")
    assert "无港中结" in result


def test_holding_history_last_n_rows():
    df = _make_quarterly("300401", n=16)
    result = format_holding_history(df, "300401", last_n=4)
    lines = [l for l in result.split("\n") if l.strip().startswith("|") and "Q" in l]
    assert len(lines) == 4


def test_holding_history_signal_markers():
    df = _make_quarterly("300401", n=8)
    quarters = df["quarter"].tolist()
    t1, t2, t3 = quarters[1], quarters[3], quarters[5]
    result = format_holding_history(df, "300401", t1=t1, t2=t2, t3=t3)
    assert "▲ t1 加仓" in result
    assert "▼ t2 减仓" in result
    assert "▲ t3 再加仓" in result


def test_holding_history_qoq_arrows():
    df = pd.DataFrame({
        "code": ["300401"] * 3,
        "quarter": ["2024Q1", "2024Q2", "2024Q3"],
        "holding_shares": [1_000_000, 1_200_000, 900_000],
        "holding_market_cap_cny": [3e7, 5e7, 2e7],  # up then down
    })
    result = format_holding_history(df, "300401")
    assert "↑" in result
    assert "↓" in result


# ---- format_detection_reason ----

def test_detection_reason_no_triple():
    diag = {"triples": [], "code": "300401"}
    result = format_detection_reason(diag, None)
    assert "候选 triple 数: 0" in result


def test_detection_reason_with_triple():
    diag = {"triples": [{"t1": "2023Q1", "t2": "2023Q3", "t3": "2024Q1",
                          "B": True, "C": True, "D": True}], "code": "300401"}
    triple = diag["triples"][0]
    triple["price_pct"] = 0.92
    triple["vol_ratio"] = 1.45
    triple["post_ret_60d"] = 0.05
    result = format_detection_reason(diag, triple)
    assert "2023Q1" in result
    assert "price_pct=0.92" in result


def test_detection_reason_with_score_fn():
    diag = {"triples": [{}]}
    triple = {"t1": "A", "t2": "B", "t3": "C", "price_pct": 0.9, "vol_ratio": 1.5, "post_ret_60d": 0.0}
    called_with = []

    def mock_score(pp, vr, pr):
        called_with.append((pp, vr, pr))
        return {"price_pts": 30, "vol_pts": 30, "c_pts": 20, "bonus_pts": 20, "total": 100}

    result = format_detection_reason(diag, triple, compute_score_fn=mock_score)
    assert called_with
    assert "100" in result
