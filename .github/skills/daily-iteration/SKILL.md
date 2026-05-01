---
name: daily-iteration
description: 每日迭代开发工作流。当用户询问每日开发节奏、晨会计划、晚间回顾、迭代日志、里程碑跟踪、daily-plan 模板时使用此技能。定义 AI agent 驱动的"晨会 → 执行 → 回顾"循环，配合 .copilot/daily-plan.md 输出每日工作记录。
---

# Skill: 每日迭代工作流

## 用途

定义 AI agent 驱动的每日开发迭代流程，包括计划制定、任务执行、进度验证和日报输出。在 agent-skills-stock 项目中，**每日迭代** 是逐步逼近"发现老鼠仓股票"目标的核心节奏。

## 触发条件

当用户：
- 让 agent 进入"每日开发模式"
- 询问今日 / 昨日 / 明日工作安排
- 需要回顾本周或本里程碑进度
- 需要把阈值调参 / skill 改进 沉淀为 daily-plan 条目

## 每日迭代模型

```
┌─────────────────────────────────────────────────┐
│  ┌──────────┐   ┌──────────┐   ┌──────────┐    │
│  │ Morning  │──▶│ Execute  │──▶│ Evening  │    │
│  │ Planning │   │ & Test   │   │ Review   │    │
│  └──────────┘   └──────────┘   └──────────┘    │
│       │                              │           │
│       └──────── 下一天 ◀─────────────┘           │
└─────────────────────────────────────────────────┘
```

## 1. Morning Planning（晨会计划）

每日开始时执行，写入 `.copilot/daily-plan.md` 的 Day N 段：

```markdown
## Day N — YYYY-MM-DD

### 昨日回顾
- 完成: ...
- 未完成: ...
- 阻塞: ...
- 候选池变化: 5000 → 3215 → 187 → 24 → 3 PASS

### 今日目标
1. [ ] 调 SELL_FLY_LIMIT 阈值，对比沪深300超额收益
2. [ ] 修复 hkscc-screener 在港股通调出股票上的连续性误判
3. [ ] 跑全流水线，确认花园生物仍命中

### 风险与依赖
- akshare 港股通接口可能在版本升级后变名

### 验收检查点
- [ ] tests/test_garden_biotech_regression.py PASS
- [ ] 候选池规模在合理区间
- [ ] skill-feedback.md 记录今日反馈
```

## 2. Execute & Test（执行与测试）

每个任务遵循以下流程：

```
代码 → 单元测试 → 全流水线回归 → 花园生物回归 → commit
   ↑                                              │
   └────── 修复 ◀──────────────────────────────────┘
```

关键规则：
- 每改一个 skill / 阈值 → 立即运行 `tests/test_garden_biotech_regression.py`
- 测试失败必须在继续下一任务前修复
- 完成一个目标 → git commit

## 3. Evening Review（晚间回顾）

每日结束时追加到 `.copilot/daily-plan.md` 的 Day N 段：

```markdown
### Day N 回顾

#### 流水线指标
| 阶段 | 数量 |
|------|------|
| universe 全量 | 5025 |
| 剔除国企后 | 3198 |
| HKSCC 候选 | 187 |
| 模式命中 | 24 |
| 人工 PASS | 3 |

#### 阈值快照
- MIN_QUARTERS=4, MIN_HOLDING_MCAP=3000万, 总市值=30-200亿
- THR_UP/DOWN=5%, PRICE_HIGH=0.85, VOL_RATIO=1.3
- SELL_FLY=15%, PLATEAU=25%

#### 今日反馈
- FB-007: SELL_FLY_LIMIT 在 2024Q1 普涨期偏松 → 已加 alpha 校验

#### 明日优先
1. 加 vs 沪深300 alpha 校验
2. 试跑 MIN_QUARTERS=6 看是否更精

#### Git
- feat(rat-pattern): add HS300 alpha sell-fly check
- test: parameterize garden biotech regression
```

## 4. 里程碑评审

每完成一个里程碑（M1–M5）做一次评审，写到 `.copilot/daily-plan.md` 末尾：

```markdown
## Milestone M3 评审 — YYYY-MM-DD

### 目标达成度
- [x] detect_rat_pattern 实现 A/B/C/D
- [x] 花园生物回归测试 PASS
- [ ] 诊断 JSON 含未命中股 — 进度 70%

### 整体进度: 78%
### 是否需要调整里程碑: 否
```

## 5. 重构窗口

详见 `code-refactoring` skill。触发条件：
- 单文件 ≥ 250 行
- 单函数 ≥ 40 行
- TODO/FIXME ≥ 5
- 连续功能开发 ≥ 4 天
- 重复代码 ≥ 2 处
- pyflakes/ruff 警告 ≥ 3

## 工作日志格式

所有日志保存在 `.copilot/daily-plan.md`，分日累加，禁止删历史。

```
.copilot/
├── daily-plan.md             # 主日志，按 Day N 累加
├── requirements.md           # 项目硬指标
└── docs/skill-feedback.md    # 反馈循环（与 daily-plan 解耦）
```

## Self-Test（自检）

```bash
mkdir -p /tmp/__si_daily__
cat > /tmp/__si_daily__/daily-plan.md << 'PLAN'
## Day 1 — 2026-05-01
### 今日目标
1. [x] M1 数据基座
2. [ ] HKSCC 历史回填
### 验收检查点
- [x] tests/test_garden_biotech_regression.py PASS
PLAN
grep -c '\[x\]' /tmp/__si_daily__/daily-plan.md | xargs -I{} bash -c '[ {} -ge 2 ] && echo "SELF_TEST_PASS: format" || echo "SELF_TEST_FAIL: format"'
command -v git >/dev/null && echo "SELF_TEST_PASS: git_available" || echo "SELF_TEST_FAIL: git_available"
rm -rf /tmp/__si_daily__
```

## Blind Test（盲测）

**Prompt:**
```
你是 agent-skills-stock 的开发 agent。请阅读此 Skill，
为今天 (Day 5) 写一个完整的晨会+执行+回顾段落，
今日目标：跑通 hkscc-screener 第一版。
```

**验收标准:**
- [ ] 包含三段：晨会计划 / 执行记录 / 晚间回顾
- [ ] 用 `[ ] / [x]` checkbox
- [ ] 包含验收检查点（含花园生物回归）
- [ ] 把候选池规模/阈值快照写进回顾
- [ ] 提到 skill-feedback.md

## 成功标准

- [ ] daily-plan.md 每日有 Day N 条目
- [ ] 任务执行有对应的测试验证
- [ ] Git 提交与任务完成同步
- [ ] 里程碑节点有评审段
- [ ] 阈值变化都进入回顾记录
