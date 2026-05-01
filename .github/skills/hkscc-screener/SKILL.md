---
name: hkscc-screener
description: 港股通(港中结/HKSCC)持股筛选工具。当用户询问港股通持股、北向资金持仓、港中结、外资持续买入、多季度持仓筛选、机构长期持有时使用此技能。负责把日级港股通持股降采样到季度末，并按"多季度连续 + 持股市值 + 总市值区间"做候选股初筛。
---

# 港中结(HKSCC) 持仓筛选

从已有 DuckDB / akshare 拉取的港股通持股明细中，挑选符合"被外资席位长期、节奏性持有"特征的中盘股。本 skill 是 `rat-trader-screener` agent 的第二步过滤器（第一步是 `soe-filter`）。

## 触发条件

当用户：
- 想筛选港股通持股市值 > 某阈值的票
- 关心机构连续多个季度持有的标的
- 在做"老鼠仓 / 外资底仓"类筛选
- 已经有 universe（剔除国企后）需要继续缩减

## 输入数据

- DuckDB 表 `hkscc_holdings(code, date, holding_shares, holding_ratio, holding_market_cap_cny)`  
  → 若不存在，请用 `scripts/fetch_hkscc.py` 通过 akshare 回填
- 表 `market_cap_snapshot(code, date, total_mcap_cny, float_mcap_cny)`
- 上游：`universe_non_soe.parquet`（来自 `soe-filter`）

## 命令用法

```bash
cd /Users/rjwang/fun/agent-skills-stock && source .venv/bin/activate

# 1. (可选) 回填港股通持股历史 (≥ 6 季度)
python .github/skills/hkscc-screener/scripts/fetch_hkscc.py --start 2024-01-01

# 2. 季度化降采样
python .github/skills/hkscc-screener/scripts/hkscc_quarterly.py

# 3. 候选股筛选
python .github/skills/hkscc-screener/scripts/screen_hkscc.py \
    --min-quarters 4 \
    --min-holding-mcap 30000000 \
    --min-total-mcap 3000000000 \
    --max-total-mcap 20000000000 \
    --in  data/universe_non_soe.parquet \
    --out data/candidates_hkscc.parquet
```

## 筛选硬指标（默认）

| 参数 | 默认值 | 含义 |
|------|--------|------|
| `--min-quarters` | 4 | 港中结连续持仓的最少季度数 |
| `--min-holding-mcap` | 3000 万 CNY | 最近一期港中结持股市值下限 |
| `--min-total-mcap` | 30 亿 CNY | 总市值下限（用户硬指标） |
| `--max-total-mcap` | 200 亿 CNY | 总市值上限（用户硬指标） |

## 算法要点

### 1. 多季度连续性

```python
def has_continuous(quarters, n=4):
    qs = sorted(set(quarters))
    if len(qs) < n: return False
    for i in range(len(qs)-n+1):
        if all(qs[i+k] == qs[i] + k for k in range(n)):
            return True
    return False
```

必须用 `pd.Period('Q')` 做相邻判断，避免日期回填造成的乱序假阴性。

### 2. 季度末快照

按 `(code, quarter)` 取**该季度内最后一个交易日**的 HKSCC 持仓作为代表，避免日内噪声。

### 3. 总市值区间

总市值 30–200 亿，覆盖中盘——大盘难控盘、小盘流动性差、都不利于"老鼠仓"形态。

## 输出

`data/candidates_hkscc.parquet`，字段：

| 字段 | 含义 |
|------|------|
| `code` | 股票代码 |
| `name` | 股票名称 |
| `quarters_held` | 港中结持仓季度数 |
| `latest_holding_mcap` | 最近一期持股市值（CNY） |
| `total_mcap` | 总市值（CNY） |

打印计数：`HKSCC 候选: N`

## 输出验证

```bash
python -c "
import pandas as pd
df = pd.read_parquet('data/candidates_hkscc.parquet')
print(df[['code','name','quarters_held','latest_holding_mcap','total_mcap']].head(20))
print(f'total: {len(df)}')
assert (df['quarters_held'] >= 4).all()
assert (df['latest_holding_mcap'] >= 30_000_000).all()
"
```

候选数量经验值：A 股全市场约 5000 只 → 经过 SOE 剔除 + HKSCC 筛选后通常 < 200 只。

## 注意事项

- akshare 港股通接口名称随版本变动，遇 `AttributeError` 用 `dir(ak)` 查 `hsgt`/`hkscc` 关键字
- 港股通调出股票期间应视为持仓**中断**，不可填零（影响连续性判定）
- 总市值用前复权数据计算
- 该 skill **不**判定持仓节奏（加仓/减仓），那是下一步 `rat-pattern-detector` 的职责

## Self-Test（自检）

```bash
python3 - <<'PY' && echo "SELF_TEST_PASS: continuous" || echo "SELF_TEST_FAIL: continuous"
import pandas as pd
def has_continuous(qs, n=4):
    qs = sorted(set(qs))
    if len(qs) < n: return False
    for i in range(len(qs)-n+1):
        if all(qs[i+k]==qs[i]+k for k in range(n)): return True
    return False
P = pd.Period
assert has_continuous([P('2024Q1'),P('2024Q2'),P('2024Q3'),P('2024Q4')]) is True
assert has_continuous([P('2024Q1'),P('2024Q3'),P('2024Q4'),P('2025Q1')]) is False
assert has_continuous([P('2024Q1'),P('2024Q2'),P('2024Q3')]) is False
print("ok")
PY
```

## Blind Test（盲测）

**Prompt:**
```
读完此 Skill 后，写一个 screen_hkscc.py：
1) 从 DuckDB 表 hkscc_holdings 读季度末快照；
2) 用 has_continuous() 判定 ≥4 季度连续；
3) latest_holding_mcap ≥ 3000 万；
4) 总市值 ∈ [30 亿, 200 亿]；
5) 与 universe_non_soe.parquet 求交集；
6) 写出 candidates_hkscc.parquet 并打印计数。
```

**验收标准:**
- [ ] 阈值全部用 argparse 暴露，且与 SKILL 默认一致
- [ ] 使用 `pd.Period('Q')` 做季度连续性
- [ ] 输出字段齐全（含 quarters_held / latest_holding_mcap / total_mcap）
- [ ] 花园生物 (300401) 在历史窗口内能命中

## 成功标准

- [ ] 每日可重跑、结果可比对
- [ ] 候选池规模 < 200 只
- [ ] Reference case 花园生物在覆盖期内必中
