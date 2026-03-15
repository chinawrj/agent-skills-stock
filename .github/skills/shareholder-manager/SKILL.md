---
name: shareholder-manager
description: 股东人数数据的缓存管理工具。当用户询问股东人数历史、股东户数变化趋势、更新股东数据、增量更新数据库、同步最新股东数据时使用此技能。支持单股查询、批量更新和全库增量更新。
---

# 股东人数管理 Skill

股东人数数据的查询和管理工具，支持从 DuckDB 缓存读取、单股在线获取和全库增量更新。

## 触发条件

当用户询问以下内容时使用此 skill：
- 查询某只股票的股东人数历史
- 获取股东人数变化趋势
- 更新/同步全库股东人数数据（增量更新）
- 初始化或重建股东人数数据库

## ⚠️ 重要：优先使用缓存

**除非用户明确要求"更新"、"同步"、"获取最新"等，否则始终从缓存读取数据。**

- 用户说"查看/查询股东人数" → 使用默认命令（从缓存）
- 用户说"更新/同步股东人数" → 使用 `update` 命令（从远端）

## 命令用法

### 默认：从缓存查询（推荐）

```bash
# 查询单只股票最近10条记录
python3 .github/skills/shareholder-manager/shareholder_manager.py 300401

# 查询更多记录
python3 .github/skills/shareholder-manager/shareholder_manager.py 300401 -n 20

# 查询所有历史记录
python3 .github/skills/shareholder-manager/shareholder_manager.py 300401 -a

# 无参数显示缓存汇总
python3 .github/skills/shareholder-manager/shareholder_manager.py
```

### 从远端更新数据（仅在用户明确要求时使用）

```bash
# 更新单只或多只股票
python3 .github/skills/shareholder-manager/shareholder_manager.py update 300401 600519

# 获取并显示最新数据
python3 .github/skills/shareholder-manager/shareholder_manager.py get 300401
```

## Python API

```python
from shareholder_manager import (
    get_cached_latest,      # 从缓存获取最新 N 条
    get_cached_all,         # 从缓存获取所有历史
    get_cached_count,       # 获取缓存记录数
    fetch_online_history,   # 从东方财富在线获取
    get_merged_latest,      # 融合缓存+在线，自动缓存
    update_stock_history,   # 静默更新单只股票
    batch_update,           # 批量更新
    get_cache_summary,      # 缓存汇总统计
)

# 示例：获取融合数据
df = get_merged_latest('300401', auto_cache=True, silent=False)

# 示例：批量更新
results = batch_update(['300401', '600519', '000858'], silent=False)
```

## 数据字段说明

| 字段 | 说明 | 类型 |
|------|------|------|
| code | 股票代码 | VARCHAR |
| name | 股票名称 | VARCHAR |
| stat_date | 统计截止日 | DATE |
| announce_date | 公告日期 | DATE |
| shareholders | 股东户数（本次） | INT |
| shareholders_prev | 股东户数（上次） | INT |
| change | 增减户数 | INT |
| change_ratio | 增减比例(%) | DECIMAL |
| range_change_pct | 区间涨跌幅(%) | DECIMAL |
| avg_value | 户均持股市值(元) | DECIMAL |
| avg_shares | 户均持股数量(股) | DECIMAL |
| market_cap | 总市值(元) | DECIMAL |
| total_shares | 总股本(股) | BIGINT |
| shares_change | 股本变动(股) | BIGINT |
| shares_change_reason | 股本变动原因 | VARCHAR |

## 数据来源

- 东方财富网股东人数数据
- 单股查询 API: `akshare.stock_zh_a_gdhs_detail_em(symbol)`
- 全量/增量 API: `https://datacenter-web.eastmoney.com/api/data/v1/get`
  - 报告名: `RPT_HOLDERNUM_DET`（全历史）、`RPT_HOLDERNUMLATEST`（最新一期）
  - 支持 `filter=(HOLD_NOTICE_DATE>='YYYY-MM-DD')` 服务端日期过滤

## 全库增量更新

**当用户说"更新数据库"、"增量更新"、"同步最新股东数据"时使用此功能。**

### 增量更新脚本

脚本位置：`scripts/update_shareholders.py`

**原理**：
1. 查询 DB 中 `MAX(announce_date)` 得到最后更新日期 A
2. 用 A-1 作为截止日，调用东方财富 API 获取 `announce_date >= A-1` 的所有新记录
3. 通过 `INSERT OR REPLACE`（按 code + stat_date 主键）upsert 到 DuckDB
4. API 支持服务端日期过滤 `filter=(HOLD_NOTICE_DATE>='cutoff')`，无需全量拉取

### 命令用法

```bash
cd /Users/rjwang/fun/a-share && source .venv/bin/activate

# 正常增量更新（自动检测截止日、自动备份）
python scripts/update_shareholders.py

# 预览模式，不写入数据库
python scripts/update_shareholders.py --dry-run

# 指定截止日期（覆盖自动检测）
python scripts/update_shareholders.py --since 2026-03-07

# 模拟测试：假设最后更新日期是 N 天前
python scripts/update_shareholders.py --test-days-ago 7

# 跳过备份（不推荐）
python scripts/update_shareholders.py --no-backup
```

### ⚠️ DuckDB 锁冲突处理

当 MCP DuckDB 服务器正在运行时，脚本无法直接写入数据库。此时脚本会：
1. 将增量数据保存到 `data/shareholders_incremental.csv`
2. 生成导入 SQL 保存到 `data/shareholders_incremental.sql`
3. 输出 SQL 语句，可通过 MCP `mcp_duckdb_query` 工具执行导入

**MCP 导入方式**：使用 `mcp_duckdb_query` 工具执行 `data/shareholders_incremental.sql` 中的 SQL。

### 全量重建

如需从零重建全部历史数据：

```bash
# 1. 获取全量历史数据（约 41 万条，需要 10-15 分钟）
python scripts/fetch_all_shareholders.py

# 2. 通过 MCP 导入 CSV
# 使用 mcp_duckdb_query 执行：
# INSERT OR REPLACE INTO shareholders ... FROM read_csv_auto('data/all_shareholders_history.csv', nullstr='')
```

初始股票列表和最新股东数据构建：
```bash
python scripts/build_stock_db.py --csv-only
# 然后通过 MCP 导入生成的 CSV
```

## 数据库结构

- **数据库路径**: `data/a-share.db`
- **shareholders 表主键**: `(code, stat_date)`
- **索引**: `idx_shareholders_announce(announce_date)`, `idx_shareholders_change(change_ratio)`
- **规模**: 约 41 万条记录，覆盖 5,480 只 A 股

## 依赖

- duckdb
- pandas
- akshare（单股查询）
- playwright（全库增量更新，通过 `scripts/browser_manager.py` 管理）
