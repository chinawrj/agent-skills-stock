---
name: fundamental-manager
description: 财务数据(业绩/估值)的缓存管理工具。当用户询问ROE、净利润、每股收益、财务数据、盈利筛选时使用此技能。支持智能缓存策略，自动判断是否需要从东方财富更新数据。
---

# 财务数据管理 Skill

财务数据（业绩、估值）的查询和管理工具，支持从 DuckDB 缓存读取和从东方财富更新数据。

## 智能缓存策略

**对调用者完全透明** - 自动判断是否需要更新，无需手动管理。

### 更新触发条件（同时满足才会更新）

1. **在财报披露窗口期内**：1-4月、7-8月、10月
2. **缓存可能过期**：缓存中最新报告日期早于预期应有的报告
3. **距上次检查超过24小时**：避免频繁请求

### 财报披露时间表

| 报告 | 数据截止日 | 强制披露日 |
|------|-----------|-----------|
| 年报 | 12-31 | 次年4月30日前 |
| 一季报 | 3-31 | 4月30日前 |
| 半年报 | 6-30 | 8月31日前 |
| 三季报 | 9-30 | 10月31日前 |

## 触发条件

当用户询问以下内容时使用此 skill：
- 查询某只股票的历史财务数据
- 批量获取股票的ROE、净利润等
- 筛选盈利/亏损股票

## 命令用法

### 默认：智能获取（推荐）

```bash
# 查询单只股票（自动判断是否需要更新）
python3 .github/skills/fundamental-manager/fundamental_manager.py 300401

# 显示更新检查信息
python3 .github/skills/fundamental-manager/fundamental_manager.py 300401 -v

# 禁用自动更新（纯缓存模式）
python3 .github/skills/fundamental-manager/fundamental_manager.py 300401 --no-update

# 无参数显示缓存汇总
python3 .github/skills/fundamental-manager/fundamental_manager.py
```

### 强制更新（仅在需要时使用）

```bash
# 更新单只或多只股票
python3 .github/skills/fundamental-manager/fundamental_manager.py update 300401 600519

# 从CSV文件批量更新
python3 .github/skills/fundamental-manager/fundamental_manager.py update -f screened_shareholders_v2.csv
```

### 筛选盈利股票

```bash
# 从缓存筛选最近2年盈利的股票
python3 .github/skills/fundamental-manager/fundamental_manager.py profitable --years 2

# 结合CSV筛选
python3 .github/skills/fundamental-manager/fundamental_manager.py profitable -f screened_shareholders_v2.csv --years 2
```

## Python API

```python
from fundamental_manager import (
    # 智能获取（推荐）
    get_smart,              # async 智能获取单只
    get_smart_sync,         # 同步版本
    batch_get_smart,        # async 批量智能获取
    
    # 缓存操作
    get_cached_latest,      # 从缓存获取最新 N 条
    get_cached_all,         # 从缓存获取所有历史
    
    # 强制更新
    fetch_online_data,      # 从东方财富在线获取
    batch_update,           # 批量更新
    
    # 筛选
    is_profitable,          # 判断是否盈利
    filter_profitable,      # 筛选盈利股票
    
    # 缓存策略
    should_check_update,    # 判断是否需要检查更新
)

# 示例：智能获取（自动判断更新）
df = await get_smart('300401', limit=10)

# 示例：批量智能获取
results = await batch_get_smart(['300401', '600519'])
```

## 数据字段说明

| 字段 | 说明 | 类型 |
|------|------|------|
| code | 股票代码 | VARCHAR |
| name | 股票名称 | VARCHAR |
| report_date | 报告日期 | DATE |
| report_type | 报告类型(年报/季报) | VARCHAR |
| eps | 每股收益(元) | DECIMAL |
| roe | ROE(%) | DECIMAL |
| net_profit | 净利润(元) | DECIMAL |
| revenue | 营业收入(元) | DECIMAL |
| profit_yoy | 净利润同比(%) | DECIMAL |
| revenue_yoy | 营收同比(%) | DECIMAL |

## 数据来源

- 东方财富网业绩数据
- 页面: `https://data.eastmoney.com/stockdata/{code}.html`

## 依赖

- duckdb
- pandas
- playwright (用于更新)
