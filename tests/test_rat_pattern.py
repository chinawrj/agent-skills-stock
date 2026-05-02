"""Tests for rat_pattern.py core logic (A-segment + assemble_hits).

All tests are DB-independent and CI-safe.
"""
from __future__ import annotations
import sys
from pathlib import Path

import pandas as pd
import pytest

SCRIPTS_DIR = Path(__file__).resolve().parent.parent / ".github" / "skills" / "rat-pattern-detector" / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from rat_pattern import find_triples, is_monotonic_up, detect_pattern_for_code, assemble_hits, compute_bcd_score


# ---- helper ----

def _make_hkscc(shares: list[int], code: str = "300401") -> pd.DataFrame:
    n = len(shares)
    quarters = [f"202{2 + i // 4}Q{(i % 4) + 1}" for i in range(n)]
    return pd.DataFrame({"code": [code] * n, "quarter": quarters, "holding_shares": shares})


# ---- find_triples ----

def test_find_triples_basic():
    # up, down, up pattern
    shares = [100, 120, 90, 110]
    df = _make_hkscc(shares)
    triples = find_triples(df)
    assert len(triples) == 1
    t1, t2, t3 = triples[0]
    assert t1 == 1 and t2 == 2 and t3 == 3


def test_find_triples_empty_no_pattern():
    shares = [100, 105, 110, 115]  # monotone up, no down
    df = _make_hkscc(shares)
    assert find_triples(df) == []


def test_find_triples_rejects_t3_below_t2():
    # t3 shares < t2 shares (t3_min_ratio=1.0 rejects this)
    shares = [100, 120, 90, 85]  # t3=85 < t2=90
    df = _make_hkscc(shares)
    assert find_triples(df) == []


def test_find_triples_t3_min_ratio_zero_still_needs_positive_delta():
    # index-3 delta is -5.5% (85/90-1), negative — no t3 regardless of t3_min_ratio
    shares = [100, 120, 90, 85]
    df = _make_hkscc(shares)
    triples = find_triples(df, t3_min_ratio=0.0)
    assert triples == []


def test_find_triples_small_rebound_allowed_when_delta_positive():
    # t3 share = 91 → delta = 91/90-1 ≈ +1.1% > 0 but < thr_up=5%; t3_min_ratio ignored
    # We need delta > thr_up; use 96 to ensure 96/90-1 ≈ 6.7% > 5%
    shares = [100, 120, 90, 96]
    df = _make_hkscc(shares)
    # With default thr_up=0.05: 96/90-1=6.7% > 5%, t3_min_ratio=1.0: 96>=90*1 ✓
    assert len(find_triples(df)) == 1


def test_find_triples_multiple():
    # two independent triples
    shares = [100, 130, 90, 120, 80, 110]
    df = _make_hkscc(shares)
    triples = find_triples(df)
    assert len(triples) >= 2


# ---- is_monotonic_up ----

def test_is_monotonic_up_true():
    shares = [100, 110, 120, 130]
    df = _make_hkscc(shares)
    df["delta_pct"] = df["holding_shares"].pct_change()
    assert is_monotonic_up(df)  # may be np.True_, use truthiness not 'is True'


def test_is_monotonic_up_false_with_drop():
    shares = [100, 120, 90, 110]
    df = _make_hkscc(shares)
    df["delta_pct"] = df["holding_shares"].pct_change()
    assert is_monotonic_up(df) is False


def test_is_monotonic_up_false_max_not_last():
    shares = [100, 130, 120, 120]  # max at index 1, not last
    df = _make_hkscc(shares)
    df["delta_pct"] = df["holding_shares"].pct_change()
    assert is_monotonic_up(df) is False


# ---- detect_pattern_for_code ----

def test_detect_pattern_a_true_no_kline():
    df = _make_hkscc([100, 120, 90, 110])
    result = detect_pattern_for_code(df, "300401", kline=None)
    assert result["A"] is True
    assert result["code"] == "300401"
    assert len(result["triples"]) == 1


def test_detect_pattern_a_false_monotone():
    df = _make_hkscc([100, 110, 120, 130])
    result = detect_pattern_for_code(df, "000001", kline=None)
    assert result["A"] is False
    assert result["monotonic_up"] is True


def test_detect_pattern_a_false_no_triples():
    df = _make_hkscc([100, 90, 85, 80])  # monotone down
    result = detect_pattern_for_code(df, "000001", kline=None)
    assert result["A"] is False
    assert result["triples"] == []


def test_detect_pattern_bcd_none_without_kline():
    df = _make_hkscc([100, 120, 90, 110])
    result = detect_pattern_for_code(df, "300401", kline=None)
    # Without kline, B/C/D default to False (no BCD computation), BCD is False
    assert result["B"] is False
    assert result["BCD"] is False


# ---- compute_bcd_score ----

def test_compute_bcd_score_zero_signal():
    # c_pts=10 is always awarded (post_ret_60d not negative), total=10 at minimum
    chosen = {"price_pct": 0.5, "vol_ratio": 0.8, "post_ret_60d": None}
    score = compute_bcd_score(chosen)
    assert score == 10.0  # minimum: c_pts baseline only


def test_compute_bcd_score_non_zero():
    # high price_pct should produce score above 0
    chosen = {"price_pct": 0.95, "vol_ratio": 2.5, "post_ret_60d": 0.10}
    score = compute_bcd_score(chosen)
    assert score > 0


# ---- assemble_hits ----

def test_assemble_hits_basic():
    diags = [
        {"code": "300401", "A": True, "BCD": False, "hit_triple_idx": None,
         "triples": [{"t1": "2023Q1", "t2": "2023Q3", "t3": "2024Q1",
                      "B": None, "C": None, "D": None,
                      "price_pct": 0.0, "vol_ratio": 0.0, "post_ret_60d": None}]},
    ]
    df = assemble_hits(diags, {"300401": "花园生物"}, strict_bcd=False)
    assert len(df) == 1
    assert df.iloc[0]["code"] == "300401"


def test_assemble_hits_strict_bcd_excludes_non_bcd():
    diags = [
        {"code": "300401", "A": True, "BCD": False, "hit_triple_idx": None,
         "triples": [{"t1": "A", "t2": "B", "t3": "C",
                      "B": None, "C": None, "D": None,
                      "price_pct": None, "vol_ratio": None, "post_ret_60d": None}]},
    ]
    df = assemble_hits(diags, {}, strict_bcd=True)
    assert df.empty


def test_assemble_hits_sorted_by_score():
    def _diag(code: str, price_pct: float):
        return {
            "code": code, "A": True, "BCD": False, "hit_triple_idx": None,
            "triples": [{"t1": "A", "t2": "B", "t3": "C",
                         "B": None, "C": None, "D": None,
                         "price_pct": price_pct, "vol_ratio": 0.0, "post_ret_60d": None}],
        }
    diags = [_diag("X", 0.85), _diag("Y", 0.95)]
    df = assemble_hits(diags, {}, strict_bcd=False)
    assert df.iloc[0]["code"] == "Y"  # higher price_pct → higher score


def test_assemble_hits_a_false_excluded():
    diags = [{"code": "000001", "A": False, "BCD": False, "hit_triple_idx": None, "triples": []}]
    df = assemble_hits(diags, {}, strict_bcd=False)
    assert df.empty
