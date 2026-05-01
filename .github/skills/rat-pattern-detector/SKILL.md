---
name: rat-pattern-detector
description: 老鼠仓持仓节奏识别工具。当用户询问加仓减仓节奏、港中结建仓-派发-再建仓、卖飞校验、高位减仓低位加仓、机构行为指纹时使用此技能。在 hkscc-screener 输出之上，识别"加仓→减仓→再加仓"三段节奏，并联合 K 线/成交量校验：减仓在高位放量、减仓后股价不大涨（非卖飞）、加仓在低位/平台。
---

# 老鼠仓节奏识别 (Rat-Pattern Detector)

老鼠仓的核心**行为指纹**不是"持续买买买"，而是节奏性的"**加仓 → 减仓 → 再加仓**"，且：

- 减仓发生在**股价高位 / 放量**窗口
- 减仓后股价**未明显走高**（非"卖飞"）
- 再加仓发生在**回踩低位 / 平台期**

本 skill 把这一指纹工程化为可计算的判定器。

## 触发条件

当用户：
- 已经有港中结候选池，想进一步识别老鼠仓节奏
- 需要把"单边加仓"票从结果中剔除
- 想给每只候选股出"加仓/减仓位置"诊断
- 正在调"卖飞"门槛阈值

## 输入

- `data/hkscc_quarterly.parquet`（来自 `hkscc-screener`）
- `data/candidates_hkscc.parquet`
- DuckDB `kline_daily(code, date, open, high, low, close, volume, amount)` — **必须前复权**

## 命令用法

```bash
cd /Users/rjwang/fun/agent-skills-stock && source .venv/bin/activate

python .github/skills/rat-pattern-detector/scripts/detect_rat_pattern.py \
    --in  data/candidates_hkscc.parquet \
    --out data/candidates_rat_pattern.parquet \
    --diag data/_diag_rat_pattern.json
```

## 算法

### A. 持仓节奏（必须满足）

设季度持仓相对变动 `Δh_t = (H_t − H_{t-1}) / H_{t-1}`。

存在三元组 `(t1 < t2 < t3)`：
- `Δh_{t1} > +THR_UP`（加仓）
- `Δh_{t2} < −THR_DOWN`（减仓）
- `Δh_{t3} > +THR_UP`（再加仓）

**单边加仓**（全程 `Δh ≥ 0` 且 max 在最后一期）→ **拒绝**。

默认 `THR_UP = THR_DOWN = 5%`。

### B. 减仓位置：高位 / 放量（OR）

对 `t2` 季度：

- `price_pct = close_max(t2) / close_max(过去 250 日)` ≥ **0.85**（高位）
- `vol_ratio = volume_mean(t2) / volume_mean(过去 250 日)` ≥ **1.3**（放量）

满足任一即视为"高位/放量减仓"。

### C. 非"卖飞"

减仓季度 `t2` 结束后的 post 窗口检验股价是否大涨：

- post 窗口 = 从 `t2` 末日次日起 60 个交易日，**截止到 `t3` 季度开始日（exclusive）**
- `max(close, post 窗口) / close(t2_end) − 1 < 15%`（短期涨幅有限）

> 若 post 窗口完全或几乎落入 `t3`（即 `t2` 与 `t3` 紧邻或紧贴一个季度），
> 视为"机构在 `t3` 自推推涨"而非"低位卖飞"，**C 默认通过**（窗口 < 10 个交易日时）。
> 这避免了"减仓-紧接再加仓"场景被卖飞条件误杀。

### D. 加仓位置：低位 / 平台（OR）

机构在加仓季度通常**在季度初低位/平台位置建仓**，季度后期股价可能已被推升。
因此 D 段判定窗口为每个加仓季度的**前 `D_HEAD_DAYS=20` 个交易日**（建仓初期），
而非整个季度，避免被"加仓即拉升"场景误杀。

对 `t1` 与 `t3` 加仓季度的前 20 个交易日窗口 `H` 任一：

- 平台：`(close_max(H) − close_min(H)) / close_min(H) < 25%`（横盘）
- 低位：`close_mean(H) / max(close, 过去 250 日) < 0.75`

`t1` 与 `t3` 都必须满足。

### 综合判定

`hit = A ∧ B ∧ C ∧ D`

## 默认阈值

```python
THR_UP = 0.05
THR_DOWN = 0.05
PRICE_HIGH_PCT = 0.85
VOL_HIGH_RATIO = 1.3
SELL_FLY_LIMIT = 0.15      # 减仓后 60 日 max 涨幅
PLATEAU_RANGE = 0.25
LOW_POS_RATIO = 0.75
LOOKBACK = 250             # 交易日
POST_WINDOW = 60           # 交易日
D_HEAD_DAYS = 20           # 加仓季度建仓初期窗口（D 段）
```

所有阈值在 `scripts/detect_rat_pattern.py` 顶部以模块级常量声明，禁止 magic number。

## 输出

### `candidates_rat_pattern.parquet`

| 字段 | 含义 |
|------|------|
| `code` | 股票代码 |
| `name` | 名称 |
| `t1` `t2` `t3` | 三段节奏季度 (Period) |
| `B` `C` `D` | 三项布尔诊断 |
| `price_pct` | t2 高位指标 |
| `vol_ratio` | t2 放量指标 |
| `post_ret_60d` | 减仓后 60 日 max 涨幅 |

### `_diag_rat_pattern.json`

包含**所有**候选股（含未命中）的逐三元组诊断，便于反向调阈值。

## 调参建议

| 参数 | 想更严 | 想更松 |
|------|--------|--------|
| `THR_UP/DOWN` | 8% | 3% |
| `PRICE_HIGH_PCT` | 0.9 | 0.8 |
| `VOL_HIGH_RATIO` | 1.5 | 1.1 |
| `SELL_FLY_LIMIT` | 10% | 20% |
| `PLATEAU_RANGE` | 20% | 30% |

> **重要**：调任何阈值后必须运行 `tests/test_garden_biotech_regression.py`，花园生物 (300401) 必须仍命中。

## 边界情况

- **送配除权** → K 线必须前复权 (`baostock adjust_flag=2`)；参考 `baostock-guide`
- **港股通调出/调入** → 中断段不计 Δh
- **大盘普涨期** → C 步建议同步对比沪深 300 超额收益（FB 反馈中可能升级）
- **持股 < 100 万股** → 噪声大，先剔除（在 `hkscc-screener` 阶段已通过 3000 万市值过滤大部分）

## Self-Test（自检）

```bash
python3 - <<'PY' && echo "SELF_TEST_PASS: triple_detect" || echo "SELF_TEST_FAIL: triple_detect"
import pandas as pd
THR_UP=0.05; THR_DOWN=0.05
data = pd.DataFrame({
  'quarter': pd.PeriodIndex(['2024Q1','2024Q2','2024Q3','2024Q4','2025Q1','2025Q2'], freq='Q'),
  'holding_shares':[1000, 1100, 1200, 1000, 950, 1100],
})
data['delta_pct'] = data['holding_shares'].pct_change()
trips=[]
for i in range(len(data)):
    if pd.isna(data.loc[i,'delta_pct']): continue
    if data.loc[i,'delta_pct'] < -THR_DOWN:
        before = data[(data.index<i)&(data['delta_pct']>THR_UP)]
        after  = data[(data.index>i)&(data['delta_pct']>THR_UP)]
        if not before.empty and not after.empty:
            trips.append((before.index[-1],i,after.index[0]))
assert len(trips) >= 1
print("ok")
PY
```

## Blind Test（盲测）

**Prompt:**
```
读完此 Skill，对 candidates_hkscc.parquet 中的每只股，输出：
1) 命中表 candidates_rat_pattern.parquet（A∧B∧C∧D 全成立）；
2) 诊断 JSON 含每只股的所有三元组与 B/C/D 数值；
3) 阈值用模块级常量、可命令行覆盖；
4) K 线必须前复权；
5) 在花园生物 (300401) 上能命中。
```

**验收标准:**
- [ ] 命中条件 = A ∧ B ∧ C ∧ D（不允许放宽到 OR）
- [ ] B 是 (price_pct≥0.85) OR (vol_ratio≥1.3)
- [ ] C 是 post_ret_60d < 15%
- [ ] D 是 t1 与 t3 都在低位/平台
- [ ] 诊断 JSON 含未命中股票
- [ ] 单边加仓票被显式拒绝
- [ ] 花园生物在覆盖期内命中

## 成功标准

- [ ] 候选池经过本 skill 缩减到 < 30 只
- [ ] 阈值修改可被回归测试稳定回传
- [ ] 诊断 JSON 可被 `kline-volume-review`（如有）或人工审计
