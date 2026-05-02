"""Pipeline quality tests — verifies DB/parquet state after full pipeline run.

Tests in this file require that the pipeline has been run at least once.
They are skipped (not failed) if the required artifacts don't exist.
"""
import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "a-share.db"
CAND_RAT_PATH = ROOT / "data" / "candidates_rat_pattern.parquet"
CAND_HKSCC_PATH = ROOT / "data" / "candidates_hkscc.parquet"

REF_CODE = "300401"


# ── helper ────────────────────────────────────────────────────────────────────

def _db_available() -> bool:
    return DB_PATH.exists()


def _kline_table_exists() -> bool:
    if not _db_available():
        return False
    try:
        import duckdb
        con = duckdb.connect(str(DB_PATH), read_only=True)
        tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        con.close()
        return "kline_daily" in tables
    except Exception:
        return False


# ── tests ─────────────────────────────────────────────────────────────────────

@pytest.mark.skipif(
    not CAND_RAT_PATH.exists(),
    reason="candidates_rat_pattern.parquet not found — run full pipeline first",
)
def test_bcd_candidate_count_positive():
    """After a full pipeline run there must be at least 1 BCD candidate."""
    import pandas as pd
    df = pd.read_parquet(str(CAND_RAT_PATH))
    count = len(df)
    assert count > 0, (
        f"BCD 候选数为 0 — 请检查 detect_rat_pattern.py 的阈值或 kline 数据覆盖"
    )


@pytest.mark.skipif(
    not _kline_table_exists(),
    reason="kline_daily table not found — run fetch_kline first",
)
def test_ref_code_has_kline_data():
    """300401 (花园生物) must have kline rows in kline_daily."""
    import duckdb
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        n = con.execute(
            "SELECT COUNT(*) FROM kline_daily WHERE code = ?", [REF_CODE]
        ).fetchone()[0]
    finally:
        con.close()
    assert n > 0, (
        f"300401 在 kline_daily 中没有数据（{n} 行）— "
        f"请运行 fetch_kline.py --symbols {REF_CODE}"
    )


@pytest.mark.skipif(
    not CAND_HKSCC_PATH.exists(),
    reason="candidates_hkscc.parquet not found — run screen_hkscc first",
)
def test_hkscc_candidates_contain_ref_code():
    """300401 must be in the HKSCC filtered candidate set."""
    import pandas as pd
    df = pd.read_parquet(str(CAND_HKSCC_PATH))
    codes = set(df["code"].astype(str).str.zfill(6))
    assert REF_CODE in codes, (
        f"300401 不在 candidates_hkscc.parquet 中（共 {len(df)} 只）— "
        f"检查 screen_hkscc.py 阈值"
    )
