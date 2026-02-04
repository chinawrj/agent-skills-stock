---
name: stock-fundamental
description: 从东方财富获取A股股票基本面数据。当用户询问股票业绩、EPS、净利润、营收、ROE、盈利筛选时使用此技能。支持批量查询和盈利年数筛选。
---

# 股票基本面数据获取

从东方财富获取A股股票基本面数据，包括业绩、估值、行业等信息。

## 触发条件

当用户询问以下内容时使用此 skill：
- 获取股票业绩数据（EPS、净利润、营收、ROE等）
- 批量查询多只股票的财务数据
- 筛选最近N年盈利的股票

## 命令用法

```bash
cd /Users/rjwang/fun/a-share && source .venv/bin/activate

# 查询单只股票
python .github/skills/stock-fundamental/scripts/fetch_fundamental.py 301216

# 查询多只股票
python .github/skills/stock-fundamental/scripts/fetch_fundamental.py 301216 002801 300530

# 从CSV文件读取代码列表
python .github/skills/stock-fundamental/scripts/fetch_fundamental.py -f screened_shareholders_v2.csv

# 只筛选最近2年盈利的
python .github/skills/stock-fundamental/scripts/fetch_fundamental.py -f screened_shareholders_v2.csv --profit-years 2 --only-profitable

# JSON输出
python .github/skills/stock-fundamental/scripts/fetch_fundamental.py 301216 --json
```

## 参数说明

| 参数 | 说明 |
|------|------|
| `codes` | 股票代码列表（位置参数） |
| `-f, --file` | 从CSV文件读取股票代码 |
| `--profit-years N` | 筛选最近N年年报盈利的股票 |
| `--only-profitable` | 只输出盈利股票 |
| `--limit N` | 限制查询数量 |
| `-o, --output` | 输出CSV文件名 |
| `--json` | JSON格式输出 |
| `--close` | 执行后关闭浏览器 |

## 输出数据字段

| 字段 | 说明 |
|------|------|
| `code` | 股票代码 |
| `name` | 股票名称 |
| `reports[].report_date` | 报告日期 |
| `reports[].eps` | 每股收益 |
| `reports[].net_profit` | 净利润（元） |
| `reports[].revenue` | 营业收入（元） |
| `reports[].roe` | ROE(%) |
| `reports[].profit_yoy` | 净利润同比(%) |

## 数据来源

- 东方财富网: `https://data.eastmoney.com/stockdata/{code}.html`

## 注意事项

1. 数据自动缓存到 DuckDB，后续查询可使用 `fundamental-manager` skill
2. 批量查询建议配合 `--limit` 参数控制数量
3. 东方财富数据有时效性，业绩数据更新可能有延迟
