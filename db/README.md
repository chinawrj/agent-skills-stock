# A股 DuckDB 数据库总结报告

**最后更新**: 2025-03-16  
**数据库文件**: `data/a-share.db` (44 MB)

---

## 一、数据库概览

| 表名 | 行数 | 说明 | 状态 |
|:---:|---:|------|:---:|
| stocks | 5,480 | A股全部股票基础信息 | ✅ 完整 |
| shareholders | 410,187 | 股东人数历史记录 | ✅ 完整 |
| fundamentals | 57,343 | 年度财务数据 (2020-2024) | ✅ 完整 |
| bonds | 1,008 | 可转债信息 | ✅ 完整 |
| klines | 2,809 | 日K线数据 (仅7只测试股) | ⏸️ 18% |

### K线数据导入进度

- **已完成**: 950 / 5,181 只股票 (18.3%)
- **CSV已保存**: `data/klines_daily.csv` (12 MB, 225,165 行)
- **CSV未导入DuckDB**: 仅前5只测试数据(1,209行)在DB中
- **进度文件**: `data/klines_progress.json` (包含950个已完成的股票代码)
- **预计总耗时**: ~85分钟 (按61只/分速度)
- **失败数**: 0

---

## 二、数据源 & API

### 1. 年度财务数据 — 东方财富 RPT_LICO_FN_CPD

| 项目 | 值 |
|------|---|
| URL | `https://datacenter-web.eastmoney.com/api/data/v1/get` |
| 报表名 | RPT_LICO_FN_CPD |
| 参数 | PageSize=500, 年报, 2020-2024 |
| 总量 | 115页, 57,207条 |
| 延迟 | 0.3s/页 |
| 脚本 | `import_fundamentals.py` |

**字段映射**:
```
SECURITY_CODE     → code
SECURITY_NAME_ABBR → name
DATEMMDD          → report_type (年报/半年报)
DATAYEAR+12-31    → report_date
BASIC_EPS         → eps
BPS               → bps
WEIGHTAVG_ROE     → roe
PARENT_NETPROFIT  → net_profit
TOTAL_OPERATE_INCOME → revenue
YSTZ              → profit_yoy
SJLTZ             → revenue_yoy
```

### 2. 日K线数据 — 腾讯 ifzq API

| 项目 | 值 |
|------|---|
| URL | `https://web.ifzq.gtimg.cn/appstock/app/fqkline/get` |
| 类型 | 前复权 (qfq) |
| 格式 | `{prefix}{code},day,{start},{end},{count},qfq` |
| 返回 | `[date, open, close, high, low, volume_lots]` |
| Volume | 手 (×100 = 股) |
| 速度 | ~61只/分 |
| 脚本 | `import_klines.py` |

**频率控制策略**:
- 每次请求间隔: 0.3-0.8s (随机抖动)
- 每100只暂停: 5s
- 每500只暂停: 30s

### ⚠️ 已知被封API
- ❌ `push2his.eastmoney.com` (东方财富K线) — RemoteDisconnected
- ❌ `akshare stock_zh_a_hist` — RemoteDisconnected
- ⚠️ `新浪K线API` — 可用但返回不复权数据，作为备份

---

## 三、脚本清单

### 🔧 生产脚本 (scripts/ 中的主力脚本)

| 脚本 | 用途 | 关键参数 |
|------|------|----------|
| `import_fundamentals.py` | 导入年度财务数据到DuckDB | `--sample N`, `--years 2020,2024` |
| `import_klines.py` | 导入日K线到DuckDB | `--sample N`, `--resume`, `--days 365` |
| `rebuild_db.py` | 重建整个DuckDB (stocks+shareholders) | `--db-path`, `--csv-dir` |
| `build_stock_db.py` | 初始化stocks表 (5480只) | 自动从东方财富获取 |
| `fetch_all_shareholders.py` | 全量获取股东人数 | `--start`, `--limit` |
| `update_shareholders.py` | 增量更新股东人数 | 自动检测需要更新的股票 |
| `fetch_listing_dates.py` | 获取上市日期 | 补充stocks表字段 |
| `daily_market_snapshot.py` | 每日行情快照 | 暂未整合到DB |

### 🧪 工具脚本 (庄股筛选 & API测试)

| 脚本 | 用途 |
|------|------|
| `check_zhuang_profit.py` | 批量验证股票连续2年盈利 |
| `score_zhuang.py` | 庄股5维度综合评分 |
| `bench_kline.py` | K线API速度基准测试 |
| `test_kline_sources.py` | 多源K线API对比 (东方财富/腾讯/新浪) |
| `test_kline_deep.py` | 腾讯/新浪K线深度测试 (数据量+速度) |

### 📄 SQL
| 文件 | 用途 |
|------|------|
| `init_db.sql` | 建表DDL (stocks, shareholders, klines, bonds, fundamentals) |

---

## 四、继续更新指南

### 1. 继续K线导入 (从950/5181处恢复)

```bash
cd /Users/rjwang/fun/a-share
source .venv/bin/activate

# 方法一：后台运行 (推荐)
PYTHONUNBUFFERED=1 nohup python3 scripts/import_klines.py --resume --no-sql > /tmp/klines_import.log 2>&1 &

# 查看进度
tail -f /tmp/klines_import.log
```

导入完成后，通过 DuckDB MCP 执行以下SQL导入：

```sql
INSERT OR REPLACE INTO klines (code, trade_date, open, close, high, low, volume, amount, change_pct)
SELECT 
  code, trade_date, open, close, high, low, volume, amount,
  CASE WHEN change_pct = '' THEN NULL ELSE CAST(change_pct AS DECIMAL(10,4)) END
FROM read_csv('data/klines_daily.csv', header=true, nullstr='');
```

### 2. 更新年度财务数据

```bash
# 全量刷新 (每年年报季结束后运行一次)
python3 scripts/import_fundamentals.py --years 2020,2024

# 输出SQL后通过MCP导入
```

### 3. 更新股东人数

```bash
python3 scripts/update_shareholders.py
```

### 4. 重建整个数据库 (如DB损坏)

```bash
python3 scripts/rebuild_db.py
```

---

## 五、DuckDB查询示例

### 筛选庄股候选 (股东人数 + 盈利)

```sql
-- 找到股东人数连续减少且连续2年盈利的股票
WITH sh_decrease AS (
  SELECT code, count(*) as decrease_periods,
    min(range_change_pct) as max_decrease
  FROM shareholders
  WHERE range_change_pct < -10
  GROUP BY code
  HAVING count(*) >= 2
),
profitable AS (
  SELECT code FROM fundamentals
  WHERE report_type = '年报' AND report_date >= '2023-12-31'
    AND net_profit > 0
  GROUP BY code
  HAVING count(*) >= 2
)
SELECT s.code, s.name, d.decrease_periods, d.max_decrease
FROM sh_decrease d
JOIN stocks s ON s.code = d.code
JOIN profitable p ON p.code = d.code
ORDER BY d.max_decrease ASC;
```

### 查询单股财务趋势

```sql
SELECT report_date, eps, roe, net_profit/1e8 as "净利润(亿)", revenue/1e8 as "营收(亿)"
FROM fundamentals
WHERE code = '000001' AND report_type = '年报'
ORDER BY report_date;
```

---

## 六、已知问题 & 注意事项

1. **revenue_yoy 溢出处理**: 部分股票营收同比增长率极端值(如1207993.82%)超过 DECIMAL(10,4) 范围，导入时已用 `CASE WHEN ABS(...) > 99999 THEN NULL` 处理
2. **DuckDB写锁**: MCP Server 持有写锁时，Python脚本无法直接写入DB。解决方案：脚本输出CSV+SQL，通过MCP执行SQL导入
3. **K线数据为前复权**: 腾讯API返回前复权数据，适合技术分析但不适合计算历史真实价格
4. **fundamentals表已含5年数据**: 2020-2024年报，每年约11K只股票
5. **K线CSV可追加**: `import_klines.py --resume` 会在已有CSV后继续追加，不会重复

---

## 七、文件结构

```
data/
├── a-share.db                  # 主数据库 (44MB)
├── fundamentals_annual.csv     # 年度财务CSV (57,207行, 6MB)
├── klines_daily.csv            # K线CSV (225,165行, 12MB, 部分)
├── klines_progress.json        # K线导入进度 (950/5181)
├── stock_codes.csv             # 全部股票代码 (5,181个)
└── README.md                   # 数据说明

db/                             # 本目录 - 脚本汇总
├── README.md                   # 本文件
├── init_db.sql                 # 建表DDL
├── import_fundamentals.py      # 财务数据导入
├── import_klines.py            # K线数据导入
├── rebuild_db.py               # 数据库重建
├── build_stock_db.py           # stocks表初始化
├── fetch_all_shareholders.py   # 股东人数全量获取
├── update_shareholders.py      # 股东人数增量更新
├── fetch_listing_dates.py      # 上市日期获取
├── daily_market_snapshot.py    # 每日行情快照
├── check_zhuang_profit.py      # 庄股盈利验证
├── score_zhuang.py             # 庄股综合评分
├── bench_kline.py              # K线API基准测试
├── test_kline_sources.py       # K线多源对比
└── test_kline_deep.py          # K线深度测试
```
