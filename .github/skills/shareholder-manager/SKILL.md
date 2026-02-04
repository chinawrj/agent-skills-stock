---
name: shareholder-manager
description: 股东人数数据的缓存管理工具。当用户询问股东人数历史、股东户数变化趋势、更新股东数据时使用此技能。支持从DuckDB缓存读取和从东方财富更新数据。
---

# 股东人数管理 Skill

股东人数数据的查询和管理工具，支持从 DuckDB 缓存读取和从东方财富更新数据。

## 触发条件

当用户询问以下内容时使用此 skill：
- 查询某只股票的股东人数历史
- 获取股东人数变化趋势

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
- API: `akshare.stock_zh_a_gdhs_detail_em(symbol)`

## 依赖

- duckdb
- pandas
- akshare
