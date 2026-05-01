# agent-skills-stock — Rat-Trader Screener 每日工作计划

> 每日由 `rat-trader-screener` agent 维护。在末尾累加新 Day N 段，**不要**删除历史。

## 项目目标

每日迭代式 A 股选股 agent。从全市场逐步筛出"被港股通(港中结)长期、节奏性持有"的疑似老鼠仓股票。
Reference: 花园生物 (300401)。

---

## Day N 模板（复制此模板新建条目）

### Day N — YYYY-MM-DD

#### 晨会计划

##### 昨日回顾
- 完成:
- 未完成:
- 阻塞:
- 候选池变化: universe → non-soe → hkscc → pattern → picks

##### 今日目标
1. [ ] 目标 1（具体到 skill / 文件 / 阈值） — 预计耗时
2. [ ] 目标 2 — 预计耗时
3. [ ] 目标 3 — 预计耗时

##### 风险与依赖
-

##### 验收检查点
- [ ] tests/test_garden_biotech_regression.py PASS
- [ ] 全流水线 (1→3) 跑通
- [ ] skill-feedback 记录今日反馈（如有）

#### 执行记录

##### 任务 1
- 开始 / 完成:
- 测试: ✅/❌
- 候选数 before → after:
- 备注:

##### 任务 2
- ...

#### 晚间回顾

##### 流水线指标
| 阶段 | 数量 |
|------|------|
| universe 全量 | |
| 剔除国企后 | |
| HKSCC 候选 | |
| 模式命中 | |
| 人工 PASS | |

##### 阈值快照
| 参数 | 当前 | 上次 | 备注 |
|------|------|------|------|
| MIN_QUARTERS | 4 | | |
| MIN_HOLDING_MCAP | 3000 万 | | |
| 总市值上下限 | 30–200 亿 | | |
| THR_UP/DOWN | 5% | | |
| PRICE_HIGH_PCT | 0.85 | | |
| VOL_HIGH_RATIO | 1.3 | | |
| SELL_FLY_LIMIT | 15% | | |
| PLATEAU_RANGE | 25% | | |
| LOW_POS_RATIO | 0.75 | | |

##### 反馈与笔记
-

##### 明日优先
1.
2.

##### Git
-

---

## Day 0 — 项目脚手架（YYYY-MM-DD）

### 完成
- ✅ rat-trader-screener.agent.md 创建
- ✅ 新增 skills: soe-filter / hkscc-screener / rat-pattern-detector / daily-iteration
- ✅ requirements.md / daily-plan.md / skill-feedback.md 模板
- ✅ tests/test_garden_biotech_regression.py 占位

### 未完成 (M1 起)
- HKSCC 数据回填（≥ 6 季度）
- 三步过滤脚本实现
- 花园生物阈值调参

### 明日 (Day 1) 优先
1. db-manager status 检查现有 schema
2. 设计 hkscc_holdings 表 schema
3. 实现 fetch_hkscc.py（akshare）

---

<!-- 在此行下方新增 Day N 段落 -->

---

## 里程碑跟踪

### M1: 数据基座
- [ ] hkscc_holdings 表 schema
- [ ] 历史回填 ≥ 6 季度
- [ ] requirements.txt 补依赖

### M2: 过滤管道
- [ ] soe-filter
- [ ] hkscc-screener
- [ ] 第一批 pytest

### M3: 节奏识别
- [ ] rat-pattern-detector A/B/C/D
- [ ] 花园生物回归测试
- [ ] 诊断 JSON

### M4: 复盘 + 报告
- [ ] K 线/成交量渲染
- [ ] 报告模板
- [ ] 决策回写

### M5: 调度 + 反馈环
- [ ] tmux 一键启动
- [ ] 自动 wrap-up
- [ ] skill-feedback 推回上游

## 验收标准进度

- [ ] 流水线 1–3 步无人值守跑通
- [ ] 候选池规模符合预期
- [ ] 花园生物 (300401) 必中
- [ ] 每只候选附 t1/t2/t3 + B/C/D 指标 + K 线图
- [ ] reports/review-YYYYMMDD.md 自动生成
- [ ] 阈值全部为模块级常量
- [ ] pytest tests/ 全部通过
