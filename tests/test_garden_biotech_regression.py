"""花园生物 (300401) 回归测试 — Rat-Trader Screener。

任何阈值修改、skill 改动都必须保证花园生物在覆盖期内仍命中。
该测试在 candidates_rat_pattern.parquet 不存在时 SKIP（流水线尚未运行）。
"""
import os
import pytest

CAND_PATH = "data/candidates_rat_pattern.parquet"
REF_CODE = "300401"  # 花园生物


@pytest.mark.skipif(
    not os.path.exists(CAND_PATH),
    reason=(
        "流水线尚未生成 candidates_rat_pattern.parquet — "
        "请先运行：\n"
        "  python .github/skills/soe-filter/scripts/filter_soe.py\n"
        "  python .github/skills/hkscc-screener/scripts/screen_hkscc.py\n"
        "  python .github/skills/rat-pattern-detector/scripts/detect_rat_pattern.py"
    ),
)
def test_garden_biotech_must_be_a_candidate():
    import pandas as pd
    df = pd.read_parquet(CAND_PATH)
    codes = set(df["code"].astype(str).tolist())
    assert REF_CODE in codes, (
        f"花园生物 ({REF_CODE}) 未在 candidates_rat_pattern.parquet 中。\n"
        f"任何阈值变更都必须保证此 reference case 命中。\n"
        f"请检查 rat-pattern-detector 的诊断 JSON："
        f" data/_diag_rat_pattern.json，看 B/C/D 哪一项失败，"
        f"并在 .copilot/docs/skill-feedback.md 记录原因。"
    )
