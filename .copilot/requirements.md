# agent-skills-stock — Rat-Trader Screener 项目需求文档

> 本需求文档针对 **rat-trader-screener** agent。该 agent 与项目已有的
> `zhuanggu-screener`（股东人数视角）、`securities-screener`（通用筛选）**正交并互补**——
> 本 agent 切入"**港股通(港中结)席位的持仓节奏**"维度，独有"卖飞校验"逻辑。

## 项目概述

- **Agent 名**: rat-trader-screener
- **核心目标**: 从全市场 A 股发现疑似"老鼠仓"股票（被外资席位长期、节奏性持有的非国资中盘股）
- **Reference Case**: 花园生物 (300401) —— 任何阈值变更后回归测试必须命中
- **节奏**: 每日迭代

## 核心选股硬指标

| # | 指标 | 阈值 | 落地 Skill |
|---|------|------|-----------|
| 1 | 排除国企/央企 | 实控人 ∉ 国资委/央企集团 | `soe-filter` |
| 2 | 港中结多季度持有 | ≥ 4 个连续季度 | `hkscc-screener` |
| 3 | 港中结持股市值 | > 3,000 万 CNY | `hkscc-screener` |
| 4 | 总市值范围 | 30–200 亿 CNY | `hkscc-screener` |
| 5 | 持仓节奏 | 加仓→减仓→再加仓 | `rat-pattern-detector` |
| 6 | 减仓位置 | 高位/放量窗口 | `rat-pattern-detector` |
| 7 | 非"卖飞" | 减仓后 60 日 max 涨幅 < 15% | `rat-pattern-detector` |
| 8 | 加仓位置 | 低位/平台期 | `rat-pattern-detector` |
| 9 | 人工复盘 | K 线 + 成交量确认 | (人工 / `kline-volume-review` 后续可选) |

## 功能需求

- 利用已有 `db-manager` 维护的 DuckDB 作为数据底座（不重复造轮）
- **新增** 港股通持仓数据采集（`hkscc-screener/scripts/fetch_hkscc.py`，akshare）
- 国企/央企剔除（实控人 + 名称前缀双兜底）
- 港中结持仓季度化（日级 → 季度末快照）
- 多季度连续性 + 持股市值 + 总市值阈值过滤
- 加仓-减仓-再加仓三段节奏识别
- 联合判定：高位/放量减仓 + 减仓后非卖飞 + 低位/平台加仓
- 花园生物 (300401) 回归测试
- 每日 wrap-up：daily-plan 累加 + skill-feedback 累加 + git commit

## 验收标准

- [ ] 流水线 1–3 步**无人值守**端到端跑通（≤ 30 分钟）
- [ ] 候选池规模合理：~5000 → ~3200 → < 200 → < 30 → 1–10 进入复盘
- [ ] 花园生物在历史回测窗口必中
- [ ] 阈值全部使用模块级常量
- [ ] `pytest tests/` 全部通过
- [ ] 每日 daily-plan 有 Day N 条目，含阈值快照
- [ ] skill-feedback.md 持续累积反馈

## 里程碑

### M1 — 数据基座（依赖已有 db-manager）
- [ ] 确认 DuckDB schema 含 `company_basic`、`kline_daily`（已有）
- [ ] 新增 `hkscc_holdings` 表 + 回填 ≥ 6 季度
- [ ] `requirements.txt` 补 `akshare` 等依赖

### M2 — 过滤管道
- [ ] `soe-filter` skill + 脚本
- [ ] `hkscc-screener` 季度化与筛选
- [ ] 第一批 pytest（边界条件）

### M3 — 节奏识别
- [ ] `rat-pattern-detector` A/B/C/D 全实现
- [ ] 在花园生物上调阈值至命中
- [ ] `tests/test_garden_biotech_regression.py`
- [ ] 诊断 JSON 含未命中股

### M4 — 复盘 + 报告
- [ ] K 线/成交量自动渲染（`kline-volume-review` skill 后续添加）
- [ ] 复盘报告模板
- [ ] PASS/REJECT 决策回写

### M5 — 调度 + 反馈环
- [ ] tmux 一键启动整条流水线
- [ ] 每日 wrap-up 自动写 daily-plan + skill-feedback
- [ ] 阈值反馈 → skill 更新 → 推回 agent-builder 上游

## 技术栈

- Python 3.10+ / `.venv`
- DuckDB（已有 db-manager）
- akshare / baostock（K 线 + 港股通）
- pandas / pyarrow / matplotlib / mplfinance / pytest
- tmux / git

## 复用 Skills

- `db-manager`、`baostock-guide`、`stock-fundamental`、`fundamental-manager`
- `shareholder-manager`、`shareholders-latest`（交叉验证用）
- `skill-authoring-guide`、`code-refactoring`

## 新建 Skills

- `soe-filter`、`hkscc-screener`、`rat-pattern-detector`
- `daily-iteration`（来自 agent-builder 库）

## 非功能需求

- 全市场流水线 1–3 步**单机** ≤ 30 分钟
- 数据落盘可重现：相同输入 → 相同候选
- 阈值修改后回归测试 ≤ 1 分钟
- 所有诊断信息可被人工审计（JSON / Markdown）

## 约束

- 默认免费数据源（akshare / baostock）
- 不直接修改已有 `zhuanggu-screener` / `securities-screener` 逻辑
- 不在源码存放 API token
- 数据时间窗：≥ 最近 8 个季度

## 不是这个 agent 做的

- ❌ 短线择时 / 量化交易
- ❌ 财务因子选股
- ❌ T+0 高频
- ❌ 港股 / 美股
- ❌ 任何不依赖"机构持仓行为指纹"的策略

> 这些维度交给 `securities-screener` / 其他 agent。
