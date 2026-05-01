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
