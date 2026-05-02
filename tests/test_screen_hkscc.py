"""Tests for hkscc-screener/screen_hkscc.py (screen() function, DB-independent).

All tests are CI-safe — no real data or network calls required.
"""
from __future__ import annotations
import sys
from pathlib import Path

import pandas as pd
import pytest

SCRIPTS_DIR = Path(__file__).resolve().parent.parent / ".github" / "skills" / "hkscc-screener" / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from screen_hkscc import screen, has_continuous


# ---- helpers ----

def _make_quarterly(rows: list[tuple[str, str, int, float]]) -> pd.DataFrame:
    """rows: [(code, quarter, holding_shares, holding_market_cap_cny), ...]"""
    return pd.DataFrame(rows, columns=["code", "quarter", "holding_shares", "holding_market_cap_cny"])


def _make_universe(*entries: tuple[str, str]) -> pd.DataFrame:
    """entries: [(code, name)]"""
    return pd.DataFrame(entries, columns=["code", "name"])


# ---- has_continuous ----

def test_has_continuous_true_consecutive():
    from pandas import Period
    quarters = [Period("2023Q1"), Period("2023Q2"), Period("2023Q3"), Period("2023Q4")]
    assert has_continuous(quarters, n=4)


def test_has_continuous_false_gap():
    from pandas import Period
    quarters = [Period("2023Q1"), Period("2023Q3"), Period("2023Q4"), Period("2024Q1")]
    assert not has_continuous(quarters, n=4)


def test_has_continuous_false_too_few():
    from pandas import Period
    quarters = [Period("2023Q1"), Period("2023Q2"), Period("2023Q3")]
    assert not has_continuous(quarters, n=4)


# ---- screen() delist filter (FB-017) ----

def _base_quarterly_4q(code: str) -> list:
    return [
        (code, "2023Q1", 1_000_000, 50_000_000),
        (code, "2023Q2", 1_100_000, 55_000_000),
        (code, "2023Q3", 1_050_000, 52_000_000),
        (code, "2023Q4", 1_200_000, 60_000_000),
    ]


def test_screen_filters_delist_name():
    # 300379 is named "东通退" — should be filtered out
    q = _make_quarterly(
        _base_quarterly_4q("300401") +  # 花园生物, keep
        _base_quarterly_4q("300379")    # 东通退, drop
    )
    uni = _make_universe(("300401", "花园生物"), ("300379", "东通退"))
    result = screen(q, None, uni, min_quarters=4, min_holding_mcap=1_000)
    codes = result["code"].tolist()
    assert "300401" in codes
    assert "300379" not in codes


def test_screen_keeps_non_delist_name():
    q = _make_quarterly(_base_quarterly_4q("300401"))
    uni = _make_universe(("300401", "花园生物"))
    result = screen(q, None, uni, min_quarters=4, min_holding_mcap=1_000)
    assert "300401" in result["code"].tolist()


def test_screen_filters_st_not_filtered_by_delist_logic():
    # ST stocks are filtered by SOE/universe, not this delist filter
    # This test ensures non-退 names are not incorrectly removed
    q = _make_quarterly(_base_quarterly_4q("000001"))
    uni = _make_universe(("000001", "平安银行"))
    result = screen(q, None, uni, min_quarters=4, min_holding_mcap=1_000)
    assert "000001" in result["code"].tolist()


# ---- screen() basic filters ----

def test_screen_drops_insufficient_quarters():
    q = _make_quarterly([
        ("300401", "2023Q1", 1_000_000, 50_000_000),
        ("300401", "2023Q2", 1_100_000, 55_000_000),
        ("300401", "2023Q3", 1_050_000, 52_000_000),
        # only 3 quarters — should be dropped for min_quarters=4
    ])
    uni = _make_universe(("300401", "花园生物"))
    result = screen(q, None, uni, min_quarters=4, min_holding_mcap=1_000)
    assert result.empty


def test_screen_drops_low_holding_mcap():
    q = _make_quarterly(_base_quarterly_4q("300401"))
    # make the last quarter holding_market_cap_cny very low
    q.loc[q["quarter"] == "2023Q4", "holding_market_cap_cny"] = 100  # tiny
    uni = _make_universe(("300401", "花园生物"))
    result = screen(q, None, uni, min_quarters=4, min_holding_mcap=50_000_000)
    assert result.empty


def test_screen_output_columns():
    q = _make_quarterly(_base_quarterly_4q("300401"))
    uni = _make_universe(("300401", "花园生物"))
    result = screen(q, None, uni, min_quarters=4, min_holding_mcap=1_000)
    for col in ("code", "name", "quarters_held", "latest_holding_mcap", "total_mcap"):
        assert col in result.columns


def test_screen_sorted_by_quarters_then_mcap():
    q = _make_quarterly(
        _base_quarterly_4q("300401") +
        [("000001", "2023Q1", 2_000_000, 80_000_000),
         ("000001", "2023Q2", 2_100_000, 85_000_000),
         ("000001", "2023Q3", 2_050_000, 82_000_000),
         ("000001", "2023Q4", 2_200_000, 90_000_000),
         ("000001", "2024Q1", 2_300_000, 95_000_000)]  # 5 quarters
    )
    uni = _make_universe(("300401", "花园生物"), ("000001", "平安银行"))
    result = screen(q, None, uni, min_quarters=4, min_holding_mcap=1_000)
    assert result.iloc[0]["code"] == "000001"  # 5q > 4q
