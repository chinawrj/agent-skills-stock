---
name: duckdb-schema
description: A股DuckDB数据库的完整Schema参考。当需要查询数据库、编写SQL、了解表结构和字段含义时使用此技能。包含9张表的字段定义、数据单位、主键索引、数据覆盖范围和已知限制。
---

# A股 DuckDB 数据库 Schema 参考

## 数据库信息

- **文件路径**: `data/a-share.db`
- **引擎**: DuckDB v1.4.4
- **MCP 访问**: 通过 `mcp_duckdb_query`、`mcp_duckdb_list_tables`、`mcp_duckdb_describe` 工具
- **连接命令**: `duckdb data/a-share.db`
- **Python 连接**: `duckdb.connect('data/a-share.db')`
- **⚠️ 并发限制**: 同一时间只能有一个写连接，多进程写入会报 "Could not set lock on file" 错误

## 表概览

| 表名 | 行数 | 说明 | 主键 |
|------|------|------|------|
| `stocks` | 5,480 | 全A股股票基本信息 | `code` |
| `shareholders` | 410,013 | 股东人数历史数据 | `(code, stat_date)` |
| `bonds` | 1,008 | 可转债全生命周期+行情+分析数据 | `bond_code` |
| `daily_market` | ~5,488 | 每日行情快照（最新一天） | `(code, trade_date)` |
| `fundamentals` | 57,343 | 财务报表数据（全A股年报2020-2024） | `(code, report_date)` |
| `klines` | 1,240,581+ | 历史K线（全市场，每日追加） | `(code, trade_date)` |
| `revise_history` | 494 | 可转债转股价下修历史（集思录） | `(bond_code, meeting_date)` |
| `data_updates` | N | 数据更新元记录 | 无 |
| `fundamental_update_log` | N | 基本面更新追踪 | `code` |

---

## 表详细定义

### 1. stocks — 股票基本信息

全A股（含北交所）股票代码和名称映射。

```sql
CREATE TABLE stocks (
    code         VARCHAR NOT NULL PRIMARY KEY,  -- 6位股票代码，如 '000001', '600519', '920027'
    name         VARCHAR,                        -- 股票名称，如 '平安银行', '*ST国华'
    market       VARCHAR,                        -- 交易所: 'SZ'(深交所2891只), 'SH'(上交所2290只), 'OTHER'(北交所299只)
    listing_date DATE,                           -- 上市日期
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**market 字段说明**:
- `SZ` = 深圳证券交易所（000xxx/001xxx/002xxx/003xxx/300xxx）
- `SH` = 上海证券交易所（600xxx/601xxx/603xxx/605xxx/688xxx）
- `OTHER` = 北京证券交易所（920xxx）

---

### 2. shareholders — 股东人数历史

东方财富来源的股东人数时序数据，用于庄控盘分析。约75条/股。

```sql
CREATE TABLE shareholders (
    code                 VARCHAR NOT NULL,       -- 股票代码
    name                 VARCHAR,                -- 股票名称
    shareholders         INTEGER,                -- 本期股东户数
    shareholders_prev    INTEGER,                -- 上期股东户数
    change               INTEGER,                -- 增减户数（本期-上期）
    change_ratio         DECIMAL(10,4),          -- 增减比例，百分比值（如 -16.67 表示减少16.67%）
    price                DECIMAL(10,2),          -- 统计时最新价（元）
    change_pct           DECIMAL(10,2),          -- 统计时涨跌幅（%）
    stat_date            DATE NOT NULL,          -- 统计截止日（如季报日 2025-09-30）
    announce_date        DATE,                   -- 公告日期
    avg_value            DECIMAL(18,2),          -- 户均持股市值（元）
    avg_shares           DECIMAL(18,2),          -- 户均持股数量（股）
    market_cap           DECIMAL(18,2),          -- 总市值（元）⚠️ 需 /1e8 转亿元
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    range_change_pct     DECIMAL(10,4),          -- 区间涨跌幅（%）
    total_shares         BIGINT,                 -- 总股本（股）
    shares_change        BIGINT,                 -- 股本变动（股）
    shares_change_reason VARCHAR,                -- 股本变动原因
    PRIMARY KEY (code, stat_date)
);
-- 索引
CREATE INDEX idx_shareholders_announce ON shareholders(announce_date);
CREATE INDEX idx_shareholders_change ON shareholders(change_ratio);
```

**数据范围**: 5,443只股票，2013-01-14 至 2026-03-10

**常用查询**:
```sql
-- 查询单股股东人数历史（最近10期）
SELECT stat_date, shareholders, change, change_ratio
FROM shareholders WHERE code = '300401'
ORDER BY stat_date DESC LIMIT 10;

-- 筛选最近一期股东减少超10%的股票
SELECT s.code, s.name, s.shareholders, s.change_ratio, s.market_cap / 1e8 AS market_cap_yi
FROM shareholders s
INNER JOIN (
    SELECT code, MAX(stat_date) AS latest FROM shareholders GROUP BY code
) t ON s.code = t.code AND s.stat_date = t.latest
WHERE s.change_ratio < -10
ORDER BY s.change_ratio;

-- 筛选连续N期股东减少的股票
-- 建议使用 baostock-guide skill 的批量筛选脚本
```

---

### 3. bonds — 可转债

全A股可转债（含已退市），包含发行信息、转股价和三大触发条件。

```sql
CREATE TABLE bonds (
    bond_code      VARCHAR NOT NULL PRIMARY KEY, -- 转债代码，如 '113700', '127112'
    bond_name      VARCHAR,                      -- 转债名称，如 '海天转债'
    stock_code     VARCHAR,                      -- 正股代码（6位）
    stock_name     VARCHAR,                      -- 正股名称
    issue_date     DATE,                         -- 发行日期
    maturity_date  DATE,                         -- 到期日期
    issue_size     DECIMAL(12,4),                -- 发行规模（亿元）⚠️ 已经是亿元单位
    remaining_size DECIMAL(12,4),                -- 剩余规模（亿元）⚠️ 全部为NULL，未填充
    maturity_years SMALLINT,                     -- 存续期（年）
    convert_start  DATE,                         -- 转股起始日
    convert_price  DECIMAL(10,3),                -- 最新转股价（元）⚠️ 已含下修/送股/分红等所有调整（来自东财f235实时推送）
    original_price DECIMAL(10,3),                -- 初始转股价（元）= 发行时原始价格（不变）
    -- 强赎条件（正股连续N天达到转股价的X%触发）
    redeem_pct     DECIMAL(6,2),                 -- 强赎触发百分比，如 130.00 表示130%
    redeem_days    SMALLINT,                     -- 连续天数，如 15
    redeem_window  SMALLINT,                     -- 观察窗口天数，如 30
    -- 回售条件
    putback_start  DATE,                         -- 回售起始日
    putback_pct    DECIMAL(6,2),                 -- 回售触发百分比，如 70.00 表示70%
    putback_days   SMALLINT,                     -- 连续天数，如 30
    putback_window SMALLINT,                     -- 观察窗口天数，如 30
    -- 下修条件
    revise_pct     DECIMAL(6,2),                 -- 下修触发百分比，如 85.00 表示85%
    revise_days    SMALLINT,                     -- 连续天数，如 15
    revise_window  SMALLINT,                     -- 观察窗口天数，如 30
    -- 行情数据（每日更新 via daily_update.py Step 2）
    bond_price     DECIMAL(10,3),                -- 转债价格（361/371有值）
    convert_value  DECIMAL(10,3),                -- 转股价值
    premium_rate   DECIMAL(10,4),                -- 溢价率
    ytm            DECIMAL(10,4),                -- 到期收益率（343/361有值，含票息精确计算）
    -- 到期赎回价和票面利率（每日更新 via daily_update.py Step 2）
    maturity_redemption_price DECIMAL(10,3),      -- 到期赎回价（面值百分比，如 115.000 = 面值的115%）
    coupon_rate_1  DECIMAL(6,3),                 -- 第1年票面利率（%），如 0.300
    coupon_rate_2  DECIMAL(6,3),                 -- 第2年票面利率
    coupon_rate_3  DECIMAL(6,3),                 -- 第3年票面利率
    coupon_rate_4  DECIMAL(6,3),                 -- 第4年票面利率
    coupon_rate_5  DECIMAL(6,3),                 -- 第5年票面利率
    coupon_rate_6  DECIMAL(6,3),                 -- 第6年票面利率
    coupon_rate_7  DECIMAL(6,3),                 -- 第7年（如有）
    coupon_rate_8  DECIMAL(6,3),                 -- 第8年（如有）
    coupon_rate_9  DECIMAL(6,3),                 -- 第9年（如有）
    coupon_rate_10 DECIMAL(6,3),                 -- 第10年（如有，绝大多数为NULL）
    -- 分析指标（每日更新 via daily_update.py Step 3）
    is_profitable        BOOLEAN,                -- 正股是否盈利（最新年报）
    consecutive_profit_years INTEGER,             -- 连续盈利年数（0-5）
    latest_roe           DECIMAL(10,4),           -- 最新ROE（%）
    latest_net_profit    DECIMAL(18,2),           -- 最新净利润（元）
    revise_trigger_count INTEGER,                 -- 下修触发天数（近30日窗口内）
    putback_trigger_count INTEGER,                -- 回售触发天数
    redeem_trigger_count INTEGER,                 -- 强赎触发天数
    stock_price_latest   DECIMAL(10,4),           -- 正股最新价
    -- 其他
    rating         VARCHAR,                      -- 信用评级，如 'AA', 'AA+', 'A+'
    listing_date   DATE,                         -- 上市日期
    delist_date    DATE,                         -- 退市日期（NULL表示仍在交易）
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**数据分布**:
- 已退市: 633，上市交易中: ~371，未上市: 少量
- 强赎条件最常见: 130/15/30（893只），其次 130/20/30（45只）
- 回售条件最常见: 70/30/30（906只）
- ✅ 下修条件已从RPT_BOND_CB_CLAUSE API获取实际值：85/15/30(267只)、80/15/30(54只)、90/15/30(28只)等
- ✅ bond_price: 361/371活跃转债有值（每日更新）
- ✅ remaining_size: 全部已填充（每日更新）
- ✅ ytm: 343/361有值（含票息精确YTM，18只NULL=即将退市或缺到期赎回价）
- ✅ maturity_redemption_price: 356/361有值
- ✅ coupon_rate_1~6: 361/361全覆盖（6年期最常见，7-10年极少）
- ✅ 分析指标: 368/371有盈利状态，368有触发进度
- ✅ putback_start: 349/351活跃转债有回售起始日（来自东财RPT_BOND_CB_CLAUSE API，含计算补全）

**常用查询**:
```sql
-- 查询当前在交易的转债
SELECT bond_code, bond_name, stock_code, stock_name, issue_size, convert_price, rating, listing_date
FROM bonds
WHERE listing_date IS NOT NULL AND listing_date <= CURRENT_DATE
  AND (delist_date IS NULL OR delist_date > CURRENT_DATE)
ORDER BY listing_date DESC;

-- 最近N个月上市的转债
SELECT * FROM bonds
WHERE listing_date >= CURRENT_DATE - INTERVAL '3 months'
ORDER BY listing_date DESC;

-- 查询转债与正股关联
SELECT b.bond_code, b.bond_name, b.stock_code, s.name AS stock_name, b.convert_price
FROM bonds b
JOIN stocks s ON b.stock_code = s.code
WHERE b.delist_date IS NULL;
```

---

### 4. daily_market — 每日行情

全A股最新一日行情数据（非历史序列，仅存储最近一天的快照）。

```sql
CREATE TABLE daily_market (
    code          VARCHAR NOT NULL,       -- 股票代码
    name          VARCHAR,                -- 股票名称
    trade_date    DATE NOT NULL,          -- 交易日期（目前仅 2026-03-16 一天）
    open          DECIMAL(10,2),          -- 开盘价（元）
    high          DECIMAL(10,2),          -- 最高价（元）
    low           DECIMAL(10,2),          -- 最低价（元）
    close         DECIMAL(10,2),          -- 收盘价（元）
    prev_close    DECIMAL(10,2),          -- 昨收价（元）
    change_amount DECIMAL(10,2),          -- 涨跌额（元）
    change_pct    DECIMAL(10,4),          -- 涨跌幅（%）
    amplitude     DECIMAL(10,4),          -- 振幅（%）
    volume        BIGINT,                 -- 成交量（股）
    amount        DECIMAL(18,2),          -- 成交额（元）
    turnover_rate DECIMAL(10,4),          -- 换手率（%）
    pe_dynamic    DECIMAL(12,2),          -- 动态市盈率（可能为NULL）
    pe_ttm        DECIMAL(12,2),          -- 滚动市盈率（可能为NULL）
    pb            DECIMAL(10,4),          -- 市净率
    total_mv      DECIMAL(18,2),          -- 总市值（元）⚠️ 需 /1e8 转亿元
    circ_mv       DECIMAL(18,2),          -- 流通市值（元）⚠️ 需 /1e8 转亿元
    PRIMARY KEY (code, trade_date)
);
CREATE INDEX idx_daily_market_date ON daily_market(trade_date);
```

**注意**: 此表只有最新一天的数据（5,488只股票 × 1天）。历史K线数据请使用 `klines` 表。

**常用查询**:
```sql
-- 按市值排序的大盘股
SELECT code, name, close, total_mv / 1e8 AS total_mv_yi, pe_ttm, pb
FROM daily_market ORDER BY total_mv DESC LIMIT 20;

-- 涨幅前10
SELECT code, name, close, change_pct FROM daily_market
ORDER BY change_pct DESC LIMIT 10;

-- 低PE高市值筛选
SELECT code, name, close, pe_ttm, total_mv / 1e8 AS mv_yi
FROM daily_market
WHERE pe_ttm > 0 AND pe_ttm < 15 AND total_mv > 10e9
ORDER BY pe_ttm;
```

---

### 5. fundamentals — 财务报表

部分股票的财务数据（目前仅34只股票，用于特定筛选场景）。

```sql
CREATE TABLE fundamentals (
    code        VARCHAR NOT NULL,         -- 股票代码
    name        VARCHAR,                  -- 股票名称
    report_date DATE NOT NULL,            -- 报告期（如 2025-09-30）
    report_type VARCHAR,                  -- 报告类型: '一季报', '半年报', '三季报', '年报'
    eps         DECIMAL(10,4),            -- 每股收益（元）
    bps         DECIMAL(10,4),            -- 每股净资产（元）（大部分为NULL）
    roe         DECIMAL(10,4),            -- 净资产收益率（%），如 1.45 表示 1.45%
    net_profit  DECIMAL(18,2),            -- 净利润（元）⚠️ 需 /1e8 转亿元
    revenue     DECIMAL(18,2),            -- 营业收入（元）⚠️ 需 /1e8 转亿元
    profit_yoy  DECIMAL(10,4),            -- 净利润同比增长率（%）
    revenue_yoy DECIMAL(10,4),            -- 营业收入同比增长率（%）
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (code, report_date)
);
```

**数据范围**: 全A股年报数据 2020-2024（57,343条），覆盖绝大部分转债正股

**更新方式**: `db/import_fundamentals.py` 全量拉取东方财富年报数据。季报数据仅部分股票有。

---

### 6. klines — 历史K线（前复权）

日K线数据，来自腾讯财经API。

```sql
CREATE TABLE klines (
    code       VARCHAR NOT NULL,          -- 股票代码
    trade_date DATE NOT NULL,             -- 交易日期
    open       DECIMAL(10,2),             -- 开盘价（元，前复权）
    high       DECIMAL(10,2),             -- 最高价（元，前复权）
    low        DECIMAL(10,2),             -- 最低价（元，前复权）
    close      DECIMAL(10,2),             -- 收盘价（元，前复权）
    volume     BIGINT,                    -- 成交量（股）
    amount     DECIMAL(18,2),             -- 成交额（元）
    PRIMARY KEY (code, trade_date)
);
```

**数据范围**: 全市场5500只股票，2022-11-23 至今（每日追加），1,240,581+行

**更新方式**: `python db/daily_update.py` 每日自动从 daily_market 追加当日 OHLCV 到 klines（ON CONFLICT DO NOTHING）。

**历史数据源**: 腾讯财经API（前复权，已弃用批量拉取）
```
https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={market}{code},day,{start},,{count},qfq
```

**常用查询**:
```sql
-- 周K线聚合
SELECT code,
    DATE_TRUNC('week', trade_date) AS week_start,
    FIRST(open ORDER BY trade_date) AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    LAST(close ORDER BY trade_date) AS close,
    SUM(volume) AS volume
FROM klines
WHERE code = '300401'
GROUP BY code, week_start
ORDER BY week_start DESC LIMIT 10;
```

---

### 7. revise_history — 可转债转股价下修历史

集思录数据源，记录每次股东大会决议的转股价下修事件。仅包含主动下修（deliberate downward revision），不含因送股/分红/配股导致的被动调整。

```sql
CREATE TABLE revise_history (
    bond_code      VARCHAR NOT NULL,     -- 转债代码
    bond_name      VARCHAR,              -- 转债名称
    meeting_date   DATE,                 -- 股东大会日期（可能NULL，如闻泰转债历史数据缺失）
    price_before   DECIMAL(10,3),        -- 下修前转股价
    price_after    DECIMAL(10,3),        -- 下修后转股价
    effective_date DATE,                 -- 新转股价生效日期
    floor_price    DECIMAL(10,3)         -- 下修底价（=净资产/前20日均价较高者，可能NULL）
);
```

**数据分布**:
- 覆盖2017-2026年，共494条下修记录，涉及329只转债
- 2024年为下修大年（~200次），2023年~60次，2025年~80次
- 下修到底率：2019-2020年约90-100%，2023-2025年降至45-56%
- 下修最多的债：蓝帆转债(8次)、维尔转债(6次)、汇车退债(6次)
- ⚠️ 已退市债在集思录API中可能返回空数据，约26条记录含 nan/空 字段（已做NULL处理）

**更新方式**: `python db/fetch_revise_history.py --all`（集思录数据源，全量刷新，含已退市债。约30分钟）

**常用查询**:
```sql
-- 某只债的下修历史
SELECT * FROM revise_history WHERE bond_code = '128108' ORDER BY meeting_date;

-- 最近30天的下修事件
SELECT * FROM revise_history WHERE meeting_date >= CURRENT_DATE - 30 ORDER BY meeting_date DESC;

-- 下修到底率统计
SELECT 
  EXTRACT(YEAR FROM meeting_date) as year,
  COUNT(*) as total,
  SUM(CASE WHEN ABS(price_after - floor_price)/floor_price < 0.005 THEN 1 ELSE 0 END) as to_floor
FROM revise_history WHERE floor_price IS NOT NULL AND meeting_date IS NOT NULL
GROUP BY year ORDER BY year;

-- 当前活跃债的下修次数排名
SELECT rh.bond_code, rh.bond_name, COUNT(*) as revise_count,
  b.convert_price, b.original_price
FROM revise_history rh JOIN bonds b ON rh.bond_code = b.bond_code
WHERE b.delist_date IS NULL
GROUP BY rh.bond_code, rh.bond_name, b.convert_price, b.original_price
ORDER BY revise_count DESC;
```

---

### 8. data_updates — 更新记录

```sql
CREATE TABLE data_updates (
    table_name    VARCHAR NOT NULL,
    update_type   VARCHAR,          -- 'init', 'full', 'incremental'
    records_count INTEGER,
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes         VARCHAR
);
```

### 9. fundamental_update_log — 基本面更新追踪

```sql
CREATE TABLE fundamental_update_log (
    code             VARCHAR NOT NULL PRIMARY KEY,
    last_check_time  TIMESTAMP NOT NULL,
    last_report_date DATE
);
```

---

## 单位约定速查

| 字段位置 | 字段 | 单位 | 转亿元 |
|----------|------|------|--------|
| shareholders | market_cap | 元 | ÷ 1e8 |
| daily_market | total_mv, circ_mv | 元 | ÷ 1e8 |
| daily_market | amount | 元 | ÷ 1e8 |
| fundamentals | net_profit, revenue | 元 | ÷ 1e8 |
| bonds | issue_size, remaining_size | **亿元** | 无需转换 |
| bonds | convert_price, original_price | 元 | - |
| revise_history | price_before, price_after, floor_price | 元 | - |
| bonds | redeem_pct, putback_pct, revise_pct | % | 如 130.00 = 130% |

## 已知限制

1. **bonds.revise_pct/days/window 全部为默认值 85/15/30**：API无法获取实际下修条件
2. **daily_market 仅保留最新一天**：历史价格数据存于 klines 表
3. **baostock 库不可用**：调用会挂起，禁止使用
4. **akshare 部分接口不稳定**：`stock_zh_a_hist` 常 RemoteDisconnected，`bond_cb_jsl()` 仅返回30条
5. **DuckDB 并发限制**：同一时间只能有一个写连接，MCP server 可能持锁

## 数据源与更新方式

**⭐ 推荐统一入口**: `python db/daily_update.py` （见 `daily-db-update` skill）

| 表 | 数据源 | 更新方式 |
|----|--------|----------|
| stocks | 东方财富 RPT_HOLDERNUMLATEST | `scripts/rebuild_db.py --step 1`（月度） |
| shareholders | 东方财富 RPT_HOLDERNUM_DET | `shareholder-manager` skill 增量更新 |
| bonds | 东方财富 RPT_BOND_CB_LIST | `db/daily_update.py --step bonds`（每日） |
| daily_market | Sina Finance Market Center | `db/daily_update.py --step stocks`（每日） |
| fundamentals | 东方财富 RPT_LICO_FN_CPD | `db/import_fundamentals.py`（季度） |
| klines | 从 daily_market 每日追加 | `db/daily_update.py --step stocks`（每日自动） |
| revise_history | 集思录 adj_logs (via akshare) | `db/fetch_revise_history.py`（需时全量刷新） |

## 跨表关联

```sql
-- stocks ↔ shareholders: 通过 code 关联
SELECT s.code, s.name, sh.shareholders, sh.change_ratio, sh.stat_date
FROM stocks s JOIN shareholders sh ON s.code = sh.code;

-- bonds ↔ stocks: 通过 bonds.stock_code = stocks.code
SELECT b.bond_code, b.bond_name, s.code, s.name
FROM bonds b JOIN stocks s ON b.stock_code = s.code;

-- stocks ↔ daily_market: 通过 code 关联
SELECT s.code, s.name, d.close, d.pe_ttm, d.total_mv / 1e8 AS mv_yi
FROM stocks s JOIN daily_market d ON s.code = d.code;

-- stocks ↔ klines: 通过 code 关联
SELECT s.name, k.* FROM klines k JOIN stocks s ON k.code = s.code;

-- bonds ↔ daily_market (正股行情): 通过 bonds.stock_code = daily_market.code
SELECT b.bond_name, b.convert_price, d.close AS stock_price,
    d.close / b.convert_price * 100 AS convert_value_pct
FROM bonds b JOIN daily_market d ON b.stock_code = d.code
WHERE b.delist_date IS NULL;
```
