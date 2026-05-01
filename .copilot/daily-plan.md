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

### Day 2 — 2026-05-01

#### 晨会计划

##### 昨日回顾
- 完成: venv / requirements / hkscc_holdings + market_cap_snapshot schema / fetch_hkscc.py 骨架
- 未完成: 真接 akshare、季度化、FB
- 阻塞: 无
- 候选池变化: 仍为 0（M1 数据层）

##### 今日目标
1. [x] `pip install akshare` + `fetch_hkscc.fetch_real()` 真实接入（先 300401 验证） — 90min
2. [x] `hkscc_quarterly.py`：日级 → 季度末快照 + self-test — 90min
3. [x] `skill-feedback.md` 追加 FB-001/002/003 — 30min

##### 风险与依赖
- akshare 在 Python 3.14 wheel 可装吗？→ 装得上（akshare 1.18.59）
- HKSCC API 名实测可能不准（hkscc-screener SKILL 的提示） → 实测 `stock_hsgt_individual_detail_em` 不可用，`stock_hsgt_individual_em` 可用

##### 验收检查点
- [x] tests/test_garden_biotech_regression.py PASS（仍 1 skip 0 fail）
- [x] 300401 真数据可拉取并季度化
- [x] skill-feedback FB-001/002/003 落地

#### 执行记录

##### 任务 1 — akshare + fetch_real
- 完成: 20:35
- 接口选型: ❌ `stock_hsgt_individual_detail_em`（NoneType 错） → ✅ `stock_hsgt_individual_em`（一次返回个股全历史）
- 实测 300401: 1150 行 (2019-06-17 → 2024-08-16)
- `--start 2023-01-01` 过滤后 326 行入 sandbox DB
- CLI：新增 `--symbols` 参数，逗号分隔，默认 300401

##### 任务 2 — hkscc_quarterly.py
- 完成: 20:35
- 算法: `pd.Period('Q')` + `groupby([code,_q])['date'].idxmax()` → 季度末快照
- self-test: 跨年/不连续季度 5 行断言 + DuckDB UPSERT 幂等校验 → ✅
- 真数据 300401: 7 个季度（2023Q1–2024Q3），全部满足 ≥3000 万 CNY 阈值

##### 任务 3 — skill-feedback
- 完成: 20:36
- FB-001 db-manager 路径硬编码（medium）
- FB-002 hkscc-screener akshare 接口名提示需更新（medium）
- FB-003 Python 3.14 兼容性提示（low）

#### 晚间回顾

##### 流水线指标
| 阶段 | 数量 |
|------|------|
| HKSCC 候选（仅 300401 单股测试） | 7 quarters |
| 余下 stages | M1 阶段未启动 |

##### 阈值快照（未变）
| 参数 | 当前 | 上次 | 备注 |
|------|------|------|------|
| MIN_QUARTERS | 4 | 4 | 默认 |
| MIN_HOLDING_MCAP | 3000 万 | — | 300401 7 季度全部满足 |
| 总市值上下限 | 30–200 亿 | — | M1 未读 |

##### 300401 早期信号（人眼瞄一眼）
- 持仓节奏（持股市值 万元）：23Q1 8680 → 23Q2 4162 → 23Q3 8578 → 23Q4 4863 → 24Q1 3261 → 24Q2 3477 → 24Q3 6625
- 加→减→加节奏多次出现；直观符合"老鼠仓"行为指纹的预期方向
- ⚠️ 数据窗口仅到 2024-08-16，M3 调阈值需对齐此 reference window

##### 反馈与笔记
- akshare individual_em 是 per-symbol 接口；全市场回填要并发节流（Day 3 任务）
- 数据截止 2024-08-16 是 EM 端限制，akshare 没新窗口

##### 明日优先 (Day 3)
1. 全市场股票池获取（stocks 表回填 / 或写 `fetch_universe.py`）
2. 编排 HKSCC 批量回填（300401 单股 4s，全市场 5000 股 ≈ 5h，需 tmux + 节流）
3. 总市值快照 → market_cap_snapshot 表

##### Git
- `feat(M1): real akshare hkscc fetch + quarterly snapshot (300401 verified)`

---

### Day 1 — 2026-05-01

#### 晨会计划

##### 昨日回顾
- 完成: Day 0 脚手架（agent / skills / requirements / daily-plan / 回归测试占位）
- 未完成: 全部 M1 任务
- 阻塞: 无（项目根缺 .venv + requirements.txt，需先解决）
- 候选池变化: universe → non-soe → hkscc → pattern → picks（流水线尚未通电）

##### 今日目标
1. [x] 项目根 .venv + requirements.txt（核心 6 包） — 30min
2. [x] db/init_db.sql 增补 hkscc_holdings + market_cap_snapshot 表 — 30min
3. [x] fetch_hkscc.py 骨架 + 合成数据 self-test（不联网） — 90min

##### 风险与依赖
- Python 3.14 较新，`akshare` / `mplfinance` 未在今日 install（避免 wheel 风险）；推迟到 Day 2 真接入时再装
- db-manager skill 内硬编码路径仍指向 `/Users/rjwang/fun/a-share`，与本仓库 CWD 不一致；今日不动它，等 M2 串联流水线时再处理（→ FB 待补）

##### 验收检查点
- [x] tests/test_garden_biotech_regression.py PASS（实际 1 skip 0 fail，符合预期）
- [ ] 全流水线 (1→3) 跑通（Day 1 不要求）
- [ ] skill-feedback 记录今日反馈（路径不一致问题，Day 2 一并提）

#### 执行记录

##### 任务 1 — venv & deps
- 完成: 20:25
- 安装包: duckdb 1.5.2, pandas 3.0.2, pyarrow 24.0.0, numpy, pytest 9.0.3, requests
- 备注: akshare/mplfinance/baostock/matplotlib 仅写入 requirements.txt，未在今日 pip install

##### 任务 2 — schema 扩展
- 完成: 20:27
- 新增表: `hkscc_holdings(code,date PK)`、`market_cap_snapshot(code,date PK)`
- 索引: `idx_hkscc_date` / `idx_hkscc_code` / `idx_mcap_date`
- Sanity: 用 `/tmp/rt-test/sanity.db` 跑过 `init_db.sql` → 4 张表全部建出，DESCRIBE 字段无误

##### 任务 3 — fetch_hkscc.py 骨架
- 完成: 20:29
- 文件: `.github/skills/hkscc-screener/scripts/fetch_hkscc.py`（约 165 行，含 docstring）
- 接口: `--db / --start / --end / --self-test / --log-level`
- self-test: 内存 DuckDB → 写入 5 行合成数据（300401）→ 重复 UPSERT 校验主键 → 输出 `SELF_TEST_PASS`
- TODO: `fetch_real()` 留 NotImplementedError，Day 2 接 akshare
- 测试: `--self-test` ✅；`pytest tests/` 1 skip 0 fail

#### 晚间回顾

##### 流水线指标
| 阶段 | 数量 |
|------|------|
| universe 全量 | — (M1 未到) |
| 剔除国企后 | — |
| HKSCC 候选 | — |
| 模式命中 | — |
| 人工 PASS | — |

##### 阈值快照
| 参数 | 当前 | 上次 | 备注 |
|------|------|------|------|
| MIN_QUARTERS | 4 | — | 默认未改 |
| MIN_HOLDING_MCAP | 3000 万 | — | |
| 总市值上下限 | 30–200 亿 | — | |
| THR_UP/DOWN | 5% | — | |
| PRICE_HIGH_PCT | 0.85 | — | |
| VOL_HIGH_RATIO | 1.3 | — | |
| SELL_FLY_LIMIT | 15% | — | |
| PLATEAU_RANGE | 25% | — | |
| LOW_POS_RATIO | 0.75 | — | |

##### 反馈与笔记
- db-manager 路径硬编码 `/Users/rjwang/fun/a-share` 与当前 repo 不一致，会成为 Day 2 串流水线的隐患（明天写到 skill-feedback FB-001）
- DuckDB UPSERT 在 `INSERT … SELECT col …` 时若 SELECT 列名与 stage 表字段不存在会 BinderError；改成"先在 DataFrame 加列再 register"更稳

##### 明日优先 (Day 2 → 仍在 M1)
1. 安装 akshare 并实现 `fetch_hkscc.fetch_real()`，单只票（300401）小窗口拉一次落库
2. 写 `hkscc_quarterly.py`：日级 → 季度末快照
3. 在 skill-feedback.md 追加 FB-001（db-manager 路径硬编码）

##### Git
- `feat(M1): bootstrap venv + hkscc_holdings/market_cap_snapshot schema + fetch_hkscc 骨架`

---

---

## 里程碑跟踪

### M1: 数据基座
- [x] hkscc_holdings 表 schema (Day 1)
- [x] market_cap_snapshot 表 schema (Day 1)
- [x] fetch_hkscc.fetch_real() 接入 akshare (Day 2，300401 验证)
- [x] hkscc_quarterly.py 季度化 (Day 2)
- [ ] 历史回填 ≥ 6 季度（全市场，Day 3）
- [x] requirements.txt 补依赖 (Day 1，akshare Day 2 实装)

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
