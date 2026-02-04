# stock-fundamental

从东方财富获取A股股票基本面数据，包括业绩、估值、行业等信息。

## 功能

- 获取股票业绩数据（EPS、净利润、营收、ROE等）
- 获取行业信息
- 支持批量查询
- 支持盈利筛选（最近N年年报盈利）

## 数据来源

通过 Playwright 在浏览器中调用东方财富 datacenter API：
- API: `https://datacenter-web.eastmoney.com/api/data/v1/get`
- reportName: `RPT_LICO_FN_CPD`

## 用法

```bash
cd /Users/rjwang/fun/a-share && source .venv/bin/activate

# 查询单只股票
python .github/skills/stock-fundamental/scripts/fetch_fundamental.py 301216

# 查询多只股票
python .github/skills/stock-fundamental/scripts/fetch_fundamental.py 301216 002801 300530

# 从CSV文件读取代码列表（配合股东人数筛选使用）
python .github/skills/stock-fundamental/scripts/fetch_fundamental.py -f screened_shareholders_v2.csv

# 只筛选最近2年盈利的
python .github/skills/stock-fundamental/scripts/fetch_fundamental.py -f screened_shareholders_v2.csv --profit-years 2 --only-profitable

# 限制查询数量（前20只）
python .github/skills/stock-fundamental/scripts/fetch_fundamental.py -f screened_shareholders_v2.csv --limit 20

# 保存结果到CSV
python .github/skills/stock-fundamental/scripts/fetch_fundamental.py -f screened_shareholders_v2.csv -o fundamental_results.csv

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
| `--all` | 显示全部结果（默认30条） |

## 输出数据字段

| 字段 | 说明 |
|------|------|
| `code` | 股票代码 |
| `name` | 股票名称 |
| `industry` | 所属行业 |
| `reports` | 报告数据列表 |
| `reports[].report_date` | 报告日期 |
| `reports[].data_type` | 报告类型（年报/季报） |
| `reports[].eps` | 每股收益 |
| `reports[].net_profit` | 净利润（元） |
| `reports[].revenue` | 营业收入（元） |
| `reports[].roe` | ROE(%) |
| `reports[].profit_yoy` | 净利润同比(%) |
| `reports[].revenue_yoy` | 营收同比(%) |

## 配合庄股筛选使用

```bash
# Step 1: 筛选股东减少的股票
python .github/skills/baostock-guide/scripts/screen_shareholders.py -m 5 --min-cap 30 --max-cap 150

# Step 2: 获取基本面并过滤亏损股
python .github/skills/stock-fundamental/scripts/fetch_fundamental.py \
    -f screened_shareholders_v2.csv \
    --profit-years 2 \
    --only-profitable \
    -o screened_profitable.csv
```

## 盈利判断逻辑

检查最近 N 个年报的净利润是否全部大于 0：
- `--profit-years 2`：检查最近2个年报（如2024年报、2023年报）
- 只有年报计入盈利判断，季报/半年报不计入

## 依赖

- Python 3.8+
- playwright

## 注意事项

1. 首次运行需要安装 Playwright 浏览器：`playwright install chromium`
2. 批量查询每只股票约需0.1秒（API调用，非DOM解析）
3. 建议配合 `--limit` 参数控制查询数量
4. 东方财富数据有时效性，业绩数据更新可能有延迟
