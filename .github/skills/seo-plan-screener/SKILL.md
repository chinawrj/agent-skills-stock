---
name: seo-plan-screener
description: 筛选A股定向增发(定增)预案公司。当用户询问定增预案、定向增发、非公开发行、向特定对象发行股票时使用此技能。支持筛选同时有定增预案和可转债的公司，用于分析"定增压价+转债下修"策略。
---

# 定增预案筛选技能

## 技能概述

本技能用于筛选A股市场中处于定增预案阶段的公司，帮助投资者发现：
- 最近公告定增预案的公司
- 同时有定增预案和可转债的公司（可能存在"定增压价+转债下修"策略）
- 定增募资规模、用途等信息

## 核心投资逻辑

**定增+转债联动策略假设**：
```
定增公告 → 机构打压股价 → 机构以均价8折认购
     ↓
股价跌破下修触发价 → 公司下修转股价
     ↓
机构+公司配合拉升股价 → 触发强赎
     ↓
机构获利（低价筹码+拉升收益）
公司获利（转债转股，免还现金）
```

### 筛选维度

1. **定增预案阶段**：预案、草案、方案、议案
2. **有可转债**：存续期内的可转债
3. **可能触发下修**：正股价格接近下修触发价

## 使用方法

### 方式1：批量筛选

```bash
cd /Users/rjwang/fun/a-share && source .venv/bin/activate
python .github/skills/seo-plan-screener/screen_seo_plans.py
```

**输出文件**：
- `screened_seo_plans.csv` - 定增预案公司列表
- `screened_seo_with_bonds.csv` - 同时有定增和可转债的公司

### 方式2：查询单只股票

```bash
cd /Users/rjwang/fun/a-share && source .venv/bin/activate
python .github/skills/seo-plan-screener/query_seo.py 新希望     # 按名称查询
python .github/skills/seo-plan-screener/query_seo.py 000876    # 按代码查询
```

### 依赖安装

```bash
pip install akshare pandas
```

## 输出格式

筛选结果包含以下字段：
- 股票代码、股票名称
- 公告标题、公告日期
- 是否有可转债
- 转债名称、转债价格、转股价、下修触发价

## 脚本文件

| 脚本 | 用途 |
|------|------|
| [screen_seo_plans.py](screen_seo_plans.py) | 批量筛选定增预案公司 |
| [query_seo.py](query_seo.py) | 查询单只股票定增信息 |

## 数据源

- **AKShare 库**（需安装：`pip install akshare`）
- `stock_notice_report(symbol="融资公告", date)`: 融资公告
- `bond_cov_comparison()`: 存续转债行情

## 注意事项

1. **数据时效性**：公告数据来自东方财富，通常T+1更新
2. **预案不等于实施**：定增预案需经股东大会、证监会审批
3. **策略风险**：下修是公司权利而非义务，存在不下修风险
