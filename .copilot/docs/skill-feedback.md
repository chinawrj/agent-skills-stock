# agent-skills-stock — Rat-Trader Screener Skill & Workflow Feedback

> 本文件由 rat-trader-screener agent 在每日开发中维护。
> 每条记录针对 skill / workflow / tools 的具体问题或改进建议。
> 这些反馈会持续回流到上游 [agent-builder](../../../agent-builder) 改进 skill 库。
>
> **格式规则**：在末尾追加新条目，**不得**删除既有条目。

---

## Feedback Items

<!-- 示例:
### FB-001 (YYYY-MM-DD)
- **Skill**: rat-pattern-detector
- **Category**: improvement
- **Summary**: SELL_FLY_LIMIT=15% 在大盘普涨期偏宽松
- **Detail**: 2024Q1 沪深 300 涨 12%，导致 600519/000001 等单纯 beta 票被误判为"非卖飞"。
              建议改为相对沪深 300 的超额收益判定。
- **Workaround**: 复盘时人工剔除
- **Priority**: high
-->

<!-- AI: 在此行下方追加新反馈，编号从 FB-001 起递增 -->

### FB-001 (2026-05-01)
- **Skill**: db-manager
- **Category**: bug
- **Summary**: SKILL.md 内 cwd 硬编码 `/Users/rjwang/fun/a-share`，与本仓库 `/Users/rjwang/fun/agent-skills-stock` 不一致
- **Detail**: `.github/skills/db-manager/SKILL.md` Part 1 起多处写 `cd /Users/rjwang/fun/a-share && source .venv/bin/activate`。
  本仓库 `data/README.md` 也仍以 a-share 仓库视角描述初始化路径。后果：
  M2 起串联 db-manager 的 `daily` 流水时，SKILL 给出的命令在 agent-skills-stock 中直接 cd 失败。
  当前在 fetch_hkscc.py / hkscc_quarterly.py 里改用相对路径 `data/a-share.db`。
- **Workaround**: rat-trader-dev agent 内统一用项目相对路径 `data/a-share.db`，不 cd 进 a-share；db-manager 提供的 manage.py 命令未在本项目实际执行
- **Priority**: medium

### FB-002 (2026-05-01)
- **Skill**: hkscc-screener
- **Category**: documentation
- **Summary**: SKILL.md 推荐的 akshare 接口名（"hsgt"/"hkscc" 关键字）未指明具体可用函数；实测 `stock_hsgt_individual_detail_em` 报 `'NoneType' object is not subscriptable`，可用替代是 `stock_hsgt_individual_em`
- **Detail**: akshare 1.18.59 下：
  - ❌ `stock_hsgt_individual_detail_em(symbol,start_date,end_date)` — 触发 EM API 解析失败
  - ✅ `stock_hsgt_individual_em(symbol)` — 返回该股全历史 9 列 DataFrame（含 持股日期/持股数量/持股市值/持股数量占A股百分比 等），300401 拉到 1150 行（2019-06-17 → 2024-08-16）
  数据窗口截止 2024-08-16，是 EM 端限制；M3 阈值调参时 reference window 需对齐到此截止日。
- **Workaround**: fetch_hkscc.py 已改用 individual_em；调用方在 DataFrame 上做日期切片
- **Priority**: medium

### FB-003 (2026-05-01)
- **Skill**: 项目环境
- **Category**: improvement
- **Summary**: 项目根使用 Python 3.14.4，部分库 wheel 滞后；建议 SKILL/agent 文档在"开发规范"节明示版本兼容性
- **Detail**: Python 3.14 下 akshare 1.18.59 + curl_cffi 0.15 可直接装；但若未来加 mplfinance / matplotlib 旧版可能需要源码编译。建议在 agent.md 或 daily-iteration SKILL 中加一行"Python 3.10–3.12 经过验证；3.13+ 可用但需关注 wheel 可用性"。
- **Workaround**: requirements.txt 中目前只锁了下界；遇到具体 wheel 缺失再 fallback
- **Priority**: low

### FB-004 (2026-05-01)
- **Skill**: hkscc-screener / db-manager
- **Category**: bug
- **Summary**: `datacenter-web.eastmoney.com` DNS 解析失败，影响 akshare 中走该域名的全市场接口
- **Detail**: Day 3 网络复测：`stock_hsgt_individual_em(symbol)` ✅（走 `data.eastmoney.com`），但
  `stock_hsgt_stock_statistics_em / stock_hsgt_individual_detail_em` 等批量接口走
  `datacenter-web.eastmoney.com` 全部 DNS NXDOMAIN。这导致全市场 universe / market_cap 批量回填
  无法在沙盒里直接跑。
- **Workaround**: M2 阶段 pivot 到本地过滤层；M1 全市场回填推到 Day 4 用单股串行 `individual_em`
  循环，预估 5h；或用 baostock 备援。
- **Priority**: high

### FB-005 (2026-05-01)
- **Skill**: soe-filter
- **Category**: improvement
- **Summary**: SKILL.md 假设输入含 controller / controller_type 列，但 db-manager cache 只有 code/name/market
- **Detail**: 实测 `cache/stocks.csv` 5480 行只有 code/name/market 三列，没有 controller 字段。
  按 SKILL 的 4 条规则，95% 股票（既无 controller 又非中字头）会落入 review 桶 → 输出
  `universe_non_soe.parquet` 0 行，把整池打空（300401 也丢）。
- **Workaround**: `filter_soe.py` 内增加列存在性探测：当数据不含 controller* 列时降级到
  name-only 模式（仅 R3 名称前缀生效），review 桶置空。已在 Day 3 实现并通过 300401 锚定。
- **Priority**: medium

### FB-006 (2026-05-01)
- **Skill**: hkscc-screener
- **Category**: improvement
- **Summary**: `screen_hkscc.py` 在缺失 `market_cap_snapshot` 时需要明确降级路径
- **Detail**: SKILL.md 把"总市值 30-200 亿"作为硬指标，但当前 DB 还没有 `market_cap_snapshot` 数据
  （依赖 datacenter-web，见 FB-004）。Day 3 实现里把它降级为 WARNING + 跳过总市值过滤，候选集
  暂留全部通过持仓+连续性的票。
- **Workaround**: 已在脚本内打 WARNING；M3 之前必须把 market_cap_snapshot 跑起来，否则 30-200 亿
  这条硬指标形同虚设。
- **Priority**: medium

### FB-007 (2026-05-01)
- **Skill**: rat-pattern-detector
- **Category**: improvement
- **Summary**: 命中判定应在所有三元组上搜索 B∧C∧D，而非只测 first triple
- **Detail**: SKILL.md "存在三元组" 是存在量化语义。Day 4 在 300401 真盘上发现：3 个候选 triple
  里 first triple (2023Q3→2023Q4→2024Q3) 的 t3=2024Q3 已脱离低位（涨 39%），D 段不命中；
  但 (2023Q3→2024Q1→2024Q3) / (2023Q3→2024Q2→2024Q3) 等组合的 t2 数值更深，B 段判定也不同。
  当前实现 `assemble_hits` 仅取 first triple → 强制让 300401 命中只能调松 PRICE_HIGH_PCT
  到 0.7 以下，会污染全市场。
- **Workaround**: Day 5 实装 B/C/D 时，对每只股遍历所有 triples，任一满足 B∧C∧D 即视为命中；
  diag JSON 同时给出每个 triple 的 B/C/D 数值，便于反向调阈值。
- **Priority**: high

### FB-008 (2026-05-01)
- **Skill**: baostock-guide / rat-pattern-detector
- **Category**: bug
- **Summary**: baostock TCP 登录在沙盒环境 hang；akshare 东方财富 push2his K 线接口被网络拦截
- **Detail**: Day 4 网络复测：
  - `bs.login()` 阻塞 ≥ 90s 无响应
  - `ak.stock_zh_a_hist` (push2his.eastmoney.com) `RemoteDisconnected`
  - `ak.stock_zh_a_daily` (新浪 finance.sina.com.cn) ✅ 22 行/秒级返回
- **Workaround**: `fetch_kline.py` 默认走新浪 (`stock_zh_a_daily`, adjust='qfq')，
  300401 单股 1046 行 < 1s。新浪返回 volume 单位为"股"（已 amount/close≈volume 验证），
  额外提供 outstanding_share / turnover 字段，便于估算总市值（解锁 FB-006）。
- **Priority**: medium

### FB-009 (2026-05-01)
- **Skill**: rat-pattern-detector
- **Category**: algorithm
- **Summary**: D 段对 t3 用整季度均值/区间偏严；C 段未排除 t3 自推涨
- **Detail**: Day 5 实装 B/C/D 在 300401 全部 3 个 triple 上 BCD=False。诊断:
  - D@t3=2024Q3 整季度 plateau_range=0.39 (>0.25)、low_pos=0.97 (>0.75)
    都因机构在 t3 季度初低位加仓后股价被推升而失败
  - C@triple#2 (t2=2024Q2, t3=2024Q3) post_ret_60d=0.17，因 post 60 日全部
    落入 t3 加仓季，被机构自推推涨误判为"卖飞"
- **Resolution**: 用户授权调整 SKILL 算法（"目标不变，参数可调"）：
  - **D 段**：仅看每个加仓季度的**前 D_HEAD_DAYS=20 个交易日**（建仓初期窗口），
    避免"机构推涨即被拒"。300401 t3 head-20: plateau=0.217 ✅
  - **C 段**：post 窗口截止到 t3 季度开始日（exclusive）；若有效 < 10 日，
    判定为机构自推 → C 默认通过
  - SKILL.md "B/C/D 段" + "默认阈值" 章节已更新；新增常量 `D_HEAD_DAYS=20`
- **Result**: 默认阈值下 300401 BCD 全 True ✅，回归测试加强 B/C/D == True
- **Priority**: resolved

### FB-010 (2026-05-02)
- **Skill**: hkscc-screener / fetch_hkscc.py
- **Category**: improvement
- **Summary**: akshare `stock_hsgt_individual_em` 数据窗口截止 2024-08-16，~9 个月滞后
- **Detail**: Day 6 批量拉 31 只 sample 港股通 universe：
  - 21 只成功（北交所 920xxx + 部分新发科创 688xxx + 沪深小盘约 10 只无港股通持仓 → ak 抛 NoneType）
  - 全部成功股票最新数据日期都 = 2024-08-16，akshare 维护方未更新
  - 同时 fetch_real 的 try/except 已正确隔离单股失败，不需要修代码
- **Workaround**:
  - 数据窗口够 6+ 季度（覆盖 2023Q1-2024Q3），M3 算法验证不受影响
  - 后续若需更新到 2025/2026，需切换数据源（可考虑直接对接港股通官方数据 / wind / choice）
- **Priority**: low（M3 验证 OK，数据滞后非阻塞）

### FB-011 (2026-05-02)
- **Skill**: rat-pattern-detector / detect_rat_pattern.py find_triples
- **Category**: bug
- **Summary**: A 段 triple 不要求 t3 持仓量 >= t2，导致单边减仓股被识别成"加-减-加"
- **Detail**: Day 7 002434 万里扬被 strict-BCD 命中：
  - 持仓 t1=2023Q2 28.86M → t2=2023Q4 17.74M → t3=2024Q3 13.17M
  - t3 vs 2024Q2: 12.44M→13.17M = +5.87% (刚过 THR_UP=0.05)
  - 但 t3 持仓 13.17M < t2 17.74M < t1 28.86M, 整体单边减仓
  - find_triples 只看 delta_pct 环比, 不看绝对量, 假阳通过
- **Workaround**: Day 8 修, find_triples 加约束 `holding_shares[t3] >= holding_shares[t2] * alpha` (alpha 待定, 0.9 ~ 1.0); 同时验证 300401 仍命中
- **Priority**: high (直接污染最终候选)

### FB-011 update (2026-05-02 resolved)
- 已修：find_triples 增 t3_min_ratio (default 1.0) 约束 H_t3 ≥ H_t2
- 验证：002434 假阳被剔除，300401 5/6 triples 仍命中
- SKILL.md A 段 + 默认阈值章节已同步

### FB-012 (2026-05-02)
- **Skill**: tools/run_rat_screener.py (M5 一键化 runner)
- **Category**: bug
- **Summary**: pipeline 缺 hkscc_quarterly 步骤 → universe 扩展后季度化层不刷新
- **Detail**: Day 10 universe 扩到 212 codes (hkscc_holdings 36537 行)，但 hkscc_quarterly 仍停留在 20 codes / 179 行。原因：run_rat_screener 只调 fetch_hkscc → build_mcap → screen_hkscc → detect，**漏了 hkscc_quarterly.py**（日级→季度末快照）。screen_hkscc 直接读 hkscc_quarterly，没新数据进去。
- **Workaround**: 已修。STEPS 加 "hkscc_quarterly"，插在 build_mcap 之后、screen_hkscc 之前。Day 10 实测：750 行 / 212 codes，screen 6 候选 (19→17→6 经 4q/mv30M/total_mcap 30-200亿)，BCD 仍仅 300401。
- **Priority**: high (universe 扩展无效，等于 pipeline 阻塞)
