"""花园生物 (300401) 回归测试 — Rat-Trader Screener。

任何阈值修改、skill 改动都必须保证花园生物在覆盖期内仍命中 A 段，
且其选定 triple 的 t1/t2/t3 字段格式合法（季度形如 2023Q3）。

注意 BCD 全 True 当前在 SKILL D 段定义下并不能保证（见 FB-009：300401 t3
季度股价已脱离低位/平台，D@t3 不通过）。本测试断言 A 段 + 字段格式，但
不强制 BCD 全 True；如果未来调整算法或阈值使其命中 BCD，将更新此断言。
"""
import os
import re

import pytest

CAND_PATH = "data/candidates_rat_pattern.parquet"
REF_CODE = "300401"  # 花园生物
QUARTER_RE = re.compile(r"^20\d{2}Q[1-4]$")


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
    df["code"] = df["code"].astype(str).str.zfill(6)
    codes = set(df["code"].tolist())
    assert REF_CODE in codes, (
        f"花园生物 ({REF_CODE}) 未在 candidates_rat_pattern.parquet 中。\n"
        f"任何阈值变更都必须保证此 reference case 命中。\n"
        f"请检查 _diag_rat_pattern.json，看 A/B/C/D 哪一项失败，"
        f"并在 .copilot/docs/skill-feedback.md 记录原因。"
    )

    row = df[df["code"] == REF_CODE].iloc[0]
    for col in ("t1", "t2", "t3"):
        val = row[col]
        assert isinstance(val, str) and QUARTER_RE.match(val), (
            f"300401 字段 {col}={val!r} 不是合法季度格式 (e.g. 2023Q3)"
        )
    # BCD 全 True：自 Day 5 算法升级后，300401 在默认阈值下应全段命中
    for col in ("B", "C", "D"):
        assert row[col] is True or row[col] == True, (  # noqa: E712
            f"300401 {col}={row[col]!r}，期望 True；阈值或算法变更可能破坏命中。\n"
            f"请检查 _diag_rat_pattern.json 并参考 .copilot/docs/skill-feedback.md。"
        )
    # bcd_score: 验证字段存在且 300401 有合理分数（>= 20, 因为至少有 B 触发）
    if "bcd_score" in df.columns:
        score = float(row["bcd_score"])
        assert score >= 20, (
            f"300401 bcd_score={score}，期望 >= 20（至少有基础 B 信号强度）"
        )

