"""test_score_components.py — 验证 bcd.compute_score_components 评分公式.

测试 price/vol/c/bonus 各分量计算，并回归验证 300401 总分。
"""
from __future__ import annotations
import math
import sys
from pathlib import Path

import pytest

BCD_DIR = Path(__file__).resolve().parent.parent / ".github" / "skills" / "rat-pattern-detector" / "scripts"
sys.path.insert(0, str(BCD_DIR))

from bcd import compute_score_components


# ── helper ──────────────────────────────────────────────────────────────────

def _score(pp, vr, pr=None):
    return compute_score_components(pp, vr, pr)


# ── unit tests ───────────────────────────────────────────────────────────────

def test_zero_signal():
    """Both conditions below threshold → all zero except neutral c_pts."""
    bd = _score(0.5, 0.5, None)
    assert bd["price_pts"] == 0.0
    assert bd["vol_pts"] == 0.0
    assert bd["bonus_pts"] == 0.0
    assert bd["c_pts"] == 10.0   # NaN → neutral 10 pts
    assert bd["total"] == 10.0


def test_price_only_above_threshold():
    """price_pct at threshold (0.85) → price_pts = 0, no bonus."""
    bd = _score(0.85, 0.5, None)
    assert bd["price_pts"] == 0.0
    assert bd["bonus_pts"] == 0.0


def test_both_conditions_triggers_bonus():
    """price_pct ≥ 0.85 AND vol_ratio ≥ 1.30 → bonus_pts = 20."""
    bd = _score(0.9, 1.5, None)
    assert bd["bonus_pts"] == 20.0


def test_c_pts_post_ret_zero():
    """post_ret_60d = 0 (stock flat after sell) → max c_pts = 20."""
    bd = _score(0.0, 0.0, 0.0)
    assert bd["c_pts"] == 20.0


def test_c_pts_sell_fly():
    """post_ret_60d ≥ 0.15 (stock surged after sell) → c_pts = 0."""
    bd = _score(0.0, 0.0, 0.15)
    assert bd["c_pts"] == 0.0


def test_c_pts_negative_ret():
    """Negative post_ret → stock fell → clamped to 20 pts (even better than flat)."""
    bd = _score(0.0, 0.0, -0.05)
    assert bd["c_pts"] == 20.0


def test_price_pts_max():
    """price_pct ≥ 1.5 → capped at 30 pts."""
    bd = _score(2.0, 0.0, None)
    assert bd["price_pts"] == 30.0


def test_vol_pts_max():
    """vol_ratio ≥ 8.0 → capped at 30 pts."""
    bd = _score(0.0, 10.0, None)
    assert bd["vol_pts"] == 30.0


def test_total_sum():
    """total = price_pts + vol_pts + c_pts + bonus_pts."""
    bd = _score(1.2, 3.0, 0.05)
    expected = bd["price_pts"] + bd["vol_pts"] + bd["c_pts"] + bd["bonus_pts"]
    assert abs(bd["total"] - round(expected, 1)) < 0.05


def test_300401_regression():
    """花园生物 300401: price_pct=1.0393, vol_ratio=5.0809, post_ret_60d=NaN → score=55.7."""
    bd = _score(1.0393, 5.0809, None)
    assert bd["total"] == 55.7, f"300401 score changed: {bd}"
    assert bd["bonus_pts"] == 20.0
    assert bd["c_pts"] == 10.0  # NaN → neutral
