---
name: db-manager
description: A股数据库建立与日常维护统一管理。当用户需要建库、更新数据库、刷新行情、更新转债/股东/下修数据、检查数据状态时使用此技能。涵盖从零建库到每日/每周维护的完整流程。
applyTo: "**"
---

# A股数据库管理 — 建库 + 日常维护

## 统一入口

```bash
cd /Users/rjwang/fun/a-share && source .venv/bin/activate
python .github/skills/db-manager/manage.py <命令>
```

| 命令 | 用途 | 频率 | 耗时 |
|------|------|------|------|
| `init` | 从零建库 | 一次性 | ~2小时 |
| `daily` | 每日行情更新 | 每个交易日盘后 | ~2分钟 |
| `weekly` | 股东人数+下修历史 | 每周一次 | ~15分钟 |
| `status` | 数据库健康检查 | 随时 | 即时 |
| `rebuild` | 从Cache CSV重建DB | 灾难恢复 | ~1分钟 |
| `cache-status` | 查看缓存状态 | 随时 | 即时 |

---

## Part 1: 从零建库 (`init`)

### 完整流程

```bash
python .github/skills/db-manager/manage.py init
```

按顺序执行10个步骤：

| 步骤 | 名称 | 说明 | 底层脚本 | 数据源 | 耗时 |
|------|------|------|----------|--------|------|
| 1 | `schema` | 创建表结构 | `db/init_db.sql` | - | 1秒 |
| 2 | `stocks` | 全A股股票列表 | `db/rebuild_db.py --step 1` | 东方财富 | 2分钟 |
| 3 | `listing` | 补全上市日期 | `db/fetch_listing_dates.py` | 东方财富 | 3分钟 |
| 4 | `bonds` | 可转债行情+基础 | `db/daily_update.py --step bonds` | 东方财富 | 1分钟 |
| 5 | `putback` | 补全回售起始日 | `db/fetch_putback_dates.py` | 东方财富RPT_BOND_CB_CLAUSE | 1分钟 |
| 6 | `fundamentals` | 年度财务(2020-2024) | `db/import_fundamentals.py` | 东方财富 | 5分钟 |
| 7 | `klines` | 1年日K线历史 | `db/import_klines.py` | 腾讯财经 | ~85分钟 |
| 8 | `shareholders` | 股东人数历史 | `db/fetch_all_shareholders.py` | 东方财富 | 10分钟 |
| 9 | `revise` | 转债下修事件历史 | `db/fetch_revise_history.py --all` | 集思录 | 30分钟 |
| 10 | `analysis` | 计算分析指标 | `db/daily_update.py --step analysis` | 本地SQL | 5秒 |

### 单独运行某步骤

```bash
python .github/skills/db-manager/manage.py init --step klines    # 仅K线
python .github/skills/db-manager/manage.py init --step putback   # 仅回售起始日
python .github/skills/db-manager/manage.py init --step revise    # 仅下修历史
python .github/skills/db-manager/manage.py init --step analysis  # 仅分析指标
```

### 建库后验证

```bash
python .github/skills/db-manager/manage.py status
```

预期输出：
- `stocks`: ~5480只
- `klines`: ~125万条, 250个交易日
- `bonds`: ~370只活跃, 有价格/YTM/触发进度
- `fundamentals`: ~57000条, 2020-2024
- `shareholders`: ~41万条
- `revise_history`: ~494条, 329只债有下修
- `bonds.putback_start`: ~349/351活跃转债有值

---

## Part 2: 每日更新 (`daily`)

**频率**: 每个交易日盘后15:30运行一次

```bash
python .github/skills/db-manager/manage.py daily
```

### 覆盖内容

| Step | 数据 | 来源 | 目标表 |
|------|------|------|--------|
| 1 | 股票OHLCV+PE/PB/市值 | Sina Finance | `daily_market`(覆盖) + `klines`(追加) |
| 2 | 转债价格/转股价/YTM | 东方财富 | `bonds` |
| 3 | 触发进度/盈利状态 | 本地SQL计算 | `bonds` |

### 分步运行

```bash
python .github/skills/db-manager/manage.py daily --step stocks     # 仅股票
python .github/skills/db-manager/manage.py daily --step bonds      # 仅转债
python .github/skills/db-manager/manage.py daily --step analysis   # 仅分析
python .github/skills/db-manager/manage.py daily --dry-run         # 仅保存CSV
python .github/skills/db-manager/manage.py daily --date 2026-03-18 # 指定日期
```

### 关键机制

- `bonds.convert_price` 每日自动更新为最新转股价（含下修/送股等所有调整，来自东财f235实时推送）
- `klines` 每日追加，自然积累历史——不再需要全量拉取
- DB锁定时自动保存SQL到 `data/daily_update_{date}.sql`，可通过MCP工具执行

---

## Part 3: 每周维护 (`weekly`)

**频率**: 每周一次（周末或非交易日均可）

```bash
python .github/skills/db-manager/manage.py weekly
```

### 覆盖内容

| 步骤 | 数据 | 来源 | 目标表 | 耗时 |
|------|------|------|--------|------|
| `shareholders` | 股东人数增量 | 东方财富 | `shareholders` | ~5分钟 |
| `revise` | 转债下修事件 | 集思录(akshare) | `revise_history` | ~10分钟 |

### 分步运行

```bash
python .github/skills/db-manager/manage.py weekly --step shareholders  # 仅股东
python .github/skills/db-manager/manage.py weekly --step revise        # 仅下修
```

### 为什么不是每日？

- **股东人数**: 公告频率不固定（季报/半年报/年报），每周增量足够
- **下修历史**: 全市场每月约10-20次下修事件，每日转股价已通过daily自动更新，下修历史分析不需要实时性

---

## Part 4: 健康检查 (`status`)

```bash
python .github/skills/db-manager/manage.py status
```

输出各表的记录数、时间范围、新鲜度，并自动提示过期数据：
- daily_market 过期 >1天 → 提示运行 daily
- revise_history 过期 >14天 → 提示运行 weekly
- shareholders 过期 >7天 → 提示运行 weekly

---

## DB锁定处理

DuckDB 同一时间只允许一个写连接。常见场景：MCP server 持锁。

**自动处理**：`daily_update.py` 检测到锁会保存CSV + 回退SQL。

**手动处理**：
```bash
# 方法1: 找到并关闭占锁进程
lsof data/a-share.db

# 方法2: 通过MCP DuckDB工具执行保存的SQL
# 读取 data/daily_update_{date}.sql 逐段通过 mcp_duckdb_query 执行
```

---

## 底层脚本清单

### 核心（被 manage.py 调用）

| 脚本 | 用途 | 被谁调用 |
|------|------|----------|
| `db/init_db.sql` | Schema初始化 | `init --step schema` |
| `db/daily_update.py` | 每日统一更新 | `daily`, `init --step bonds/analysis` |
| `db/rebuild_db.py` | 股票列表导入 | `init --step stocks` |
| `db/fetch_listing_dates.py` | 上市日期补全 | `init --step listing` |
| `db/import_fundamentals.py` | 年度财务导入 | `init --step fundamentals` |
| `db/import_klines.py` | K线历史导入 | `init --step klines` |
| `db/fetch_all_shareholders.py` | 股东人数全量 | `init --step shareholders` |
| `db/fetch_revise_history.py` | 下修历史 | `init --step revise`, `weekly --step revise` |
| `db/fetch_putback_dates.py` | 回售起始日补全 | `init --step putback` |
| `db/update_shareholders.py` | 股东人数增量 | `weekly --step shareholders` |

### 备用/灾难恢复

| 脚本 | 场景 |
|------|------|
| `manage.py rebuild` | **推荐** — 从Cache CSV重建全库（~1分钟）|
| `.github/skills/db-manager/import_cache.py` | rebuild底层脚本，可独立运行 |
| `db/rebuild_full.py` | 从CSV备份重建全库（旧版，WAL损坏恢复）|
| `db/recover_db.py` | DB文件修复尝试 |
| `db/backfill_baostock.py` | 补填缺失日K线（用Baostock） |
| `db/backfill_klines.py` | 补填缺失日K线（用腾讯） |
| `db/update_bond_market.py` | 独立转债更新（被daily_update取代）|
| `db/update_bond_analysis.py` | 独立分析计算（被daily_update取代）|

---

## 数据源 API 速查

| 数据源 | URL | 用途 | 限流 |
|--------|-----|------|------|
| Sina Finance | `money.finance.sina.com.cn/quotes_service/api/json_v2.php` | 股票行情 | 100条/页, 无需认证 |
| 东方财富 datacenter | `datacenter-web.eastmoney.com/api/data/v1/get` | 转债/股东/财务 | 500条/页, 0.3s间隔 |
| 腾讯财经 | `web.ifzq.gtimg.cn/appstock/app/fqkline/get` | K线历史 | 0.3-0.8s间隔 |
| 集思录(via akshare) | `www.jisilu.cn/data/cbnew/adj_logs/` | 转债下修历史 | 0.3s间隔 |
| 东方财富RPT_BOND_CB_CLAUSE | `datacenter-web.eastmoney.com/api/data/v1/get` | 回售起始日/转股起始日 | 500条/页 |

### 东方财富关键 quoteColumns

转债数据使用 `quoteColumns` 获取实时推送字段（report columns 可能为NULL）：
- `f2~10~SECURITY_CODE~BOND_PRICE` — 转债价格
- `f2~01~CONVERT_STOCK_CODE~CONVERT_STOCK_PRICE` — 正股价格
- `f235~10~SECURITY_CODE~TRANSFER_PRICE` — **最新转股价**（含所有调整，⚠️ report字段TRANSFER_PRICE=NULL）

---

## 维护日历

| 频率 | 操作 | 命令 |
|------|------|------|
| **每日** | 行情+转债+分析 | `manage.py daily` |
| **每周** | 股东人数+下修历史 | `manage.py weekly` |
| **每季** | 财务数据更新 | `manage.py init --step fundamentals` |
| **按需** | 股票列表（新股上市） | `manage.py init --step stocks && manage.py init --step listing` |
| **按需** | 健康检查 | `manage.py status` |

---

## 验证SQL

更新后可用以下SQL验证数据完整性：

```sql
-- 1. daily_market 是否为最新日期
SELECT COUNT(*), MAX(trade_date) FROM daily_market;

-- 2. klines 是否追加了今日数据
SELECT COUNT(*) FROM klines WHERE trade_date = CURRENT_DATE;

-- 3. bonds 市场数据覆盖率
SELECT
  COUNT(*) AS total,
  COUNT(CASE WHEN bond_price IS NOT NULL THEN 1 END) AS has_price,
  COUNT(CASE WHEN revise_trigger_count IS NOT NULL THEN 1 END) AS has_triggers,
  COUNT(CASE WHEN is_profitable IS NOT NULL THEN 1 END) AS has_profit
FROM bonds WHERE maturity_date > CURRENT_DATE;

-- 4. 转股价是否为最新值（应非初始值）
SELECT bond_code, bond_name, convert_price, initial_convert_price
FROM bonds WHERE delist_date IS NULL
  AND convert_price = initial_convert_price
  AND bond_code IN (SELECT DISTINCT bond_code FROM revise_history)
LIMIT 10;
```

---

## Part 5: 缓存架构 (`rebuild` / `cache-status`)

### 核心理念

**CSV 是持久数据层，DB 随时可从 Cache 重建。**

每次 `init`/`daily`/`weekly` 执行后，产出的 CSV 自动复制到 `cache/` 目录。DB 损坏或需要迁移时，只需 `rebuild` 即可从 cache 完整恢复。

### Cache 目录结构

```
.github/skills/db-manager/cache/
├── .gitignore              # 排除大文件
├── stocks.csv              # 全A股列表 (~127KB)
├── listing_dates.csv       # 上市日期 (~231KB)
├── bonds_full.csv          # 转债完整数据 (~140KB, 含静态+行情+触发条件+票息)
├── bonds_market.csv        # 转债最新行情 (~69KB, 仅市场字段)
├── fundamentals.csv        # 年度财务 (~5.2MB)
├── klines.csv              # K线历史 (~64MB)
├── shareholders.csv        # 股东人数 (~56MB)
├── revise_history.csv      # 下修事件 (~28KB)
├── bond_putback_dates.csv  # 回售起始日 (~73KB)
└── daily_market/           # 日行情快照（按日期积累）
    ├── 2026-03-18.csv
    └── ...
```

### 自动缓存流程

| 命令 | 缓存内容 |
|------|----------|
| `init --step stocks` | `stocks.csv` |
| `init --step listing` | `listing_dates.csv` |
| `init --step bonds` | `bonds_full.csv`, `bonds_market.csv` |
| `init --step fundamentals` | `fundamentals.csv` |
| `init --step klines` | `klines.csv` |
| `init --step shareholders` | `shareholders.csv` |
| `init --step revise` | `revise_history.csv` |
| `daily` | `bonds_full.csv`, `bonds_market.csv`, `daily_market/YYYY-MM-DD.csv` |
| `weekly --step shareholders` | `shareholders.csv` |
| `weekly --step revise` | `revise_history.csv` |

### 从 Cache 重建

```bash
# 完整重建（删除旧DB后执行）
python .github/skills/db-manager/manage.py rebuild

# 仅重建转债表
python .github/skills/db-manager/manage.py rebuild --table bonds

# DB锁定时：生成SQL文件，通过MCP执行
python .github/skills/db-manager/manage.py rebuild --sql-only
```

### 查看缓存状态

```bash
python .github/skills/db-manager/manage.py cache-status
```

输出各缓存文件的大小、更新时间、完整度。

### import_cache.py 重建流程

1. 创建表结构（9张表 + 索引）
2. 导入 `stocks.csv` → stocks 表
3. 更新 `listing_dates.csv` → stocks.listing_date
4. 导入 `bonds_full.csv` → bonds 表（完整 INSERT OR REPLACE）
5. 更新 `bonds_market.csv` → bonds 行情字段（UPDATE）
6. 导入 `fundamentals.csv` → fundamentals 表
7. 导入 `klines.csv` → klines 表
8. 导入 `shareholders.csv` → shareholders 表
9. 导入 `revise_history.csv` → revise_history 表
10. 导入 `daily_market/*.csv` → daily_market + klines（追加）
11. 运行分析SQL（触发进度 + 盈利状态）

### bonds_full.csv 字段说明

`bonds_full.csv` 包含转债的**全部**静态和市场字段，足以完整重建 bonds 表：

| 字段组 | 字段 | 说明 |
|--------|------|------|
| 基本 | bond_code, bond_name, stock_code | 标识 |
| 发行 | issue_size, rating, maturity_date | 发行信息 |
| 转股 | convert_price, original_price | 最新/初始转股价 |
| 日期 | listing_date, delist_date | 上市/退市日期 |
| 强赎 | redeem_pct, redeem_days, redeem_window | 130/15/30 |
| 回售 | putback_pct, putback_days, putback_window | 70/30/30 |
| 下修 | revise_pct, revise_days, revise_window | 85/15/30 |
| 行情 | bond_price, convert_value, premium_rate, ytm, remaining_size | 最新市场数据 |
| 票息 | maturity_redemption_price, coupon_rate_1~10 | 用于YTM计算 |

---

## Part 6: 灾难恢复手册 (Recovery Playbook)

### 恢复方式优先级

| 优先级 | 方式 | 前提条件 | 耗时 | 命令 |
|--------|------|----------|------|------|
| 🥇 | rebuild 从 Cache 重建 | cache/ 目录完整 | ~1分钟 | `manage.py rebuild` |
| 🥈 | 修复 WAL + rebuild | DB主文件存在但WAL损坏 | ~2分钟 | 见下方步骤 |
| 🥉 | 从 backup 恢复 | backup/ 目录有完整备份 | ~5分钟 | 手动恢复 |
| 4️⃣ | 联网全量重建 | 有网络 + API可用 | ~2小时 | `manage.py init` |

### 场景1: WAL 损坏 (最常见)

**症状**: `manage.py status` 显示所有表 ❌ 空，但 `data/a-share.db` 文件很大

**原因**: DuckDB WAL (Write-Ahead Log) 文件损坏，通常因程序崩溃或MCP连接异常中断

**修复步骤**:
```bash
# 1. 确认DB主文件存在且有内容
ls -la data/a-share.db        # 应>100MB
ls -la data/a-share.db.wal    # WAL文件可能存在

# 2. 移除损坏的WAL（主文件数据不受影响）
mv data/a-share.db.wal data/a-share.db.wal.bak

# 3. 验证恢复
python .github/skills/db-manager/manage.py status

# 4. 如果表结构缺失（WAL中的ALTER TABLE丢失），从Cache重建
python .github/skills/db-manager/manage.py rebuild
```

**关键认知**: DuckDB 的 WAL 是增量日志。移除后回滚到最近的 checkpoint 状态，丢失最近未 checkpoint 的更新。Cache CSV 保存了最新数据，rebuild 可恢复到最新状态。

### 场景2: DB文件丢失/损坏

```bash
# 直接从Cache重建（推荐）
python .github/skills/db-manager/manage.py rebuild

# 验证
python .github/skills/db-manager/manage.py status
```

### 场景3: Cache也不全

```bash
# 查看缺什么
python .github/skills/db-manager/manage.py cache-status

# 方案A: 从 backup/ 恢复缺失的CSV
cp backup/all_stocks.csv .github/skills/db-manager/cache/stocks.csv

# 方案B: 仅重建有Cache的表 + 联网补全缺失的
python .github/skills/db-manager/manage.py rebuild          # 先用现有Cache
python .github/skills/db-manager/manage.py init --step X    # 补全缺失步骤
```

### 日常预防措施

1. **每次 daily/weekly 后 Cache 自动更新** — 无需手动备份
2. **Cache 在 git 中被 .gitignore** — 大文件不进版本库
3. **定期确认**: `manage.py cache-status` 检查完整度 (8/8)
4. **data/a-share.db.wal 文件异常增大** = DuckDB checkpoint 未执行 = 潜在风险

### 已知问题与修复记录

| 问题 | 根因 | 修复方式 | 日期 |
|------|------|----------|------|
| revise_history duplicate key | `import_to_db()` 用 `;` 拆分SQL时跳过以 `--` 注释开头的语句 | 添加 `_strip_comments()` | 2026-03-24 |
| bonds 缺少 maturity_redemption_price/coupon_rate 列 | WAL中的ALTER TABLE丢失 | import_cache.py schema 已包含所有列 | 2026-03-24 |
| revise_pct 全部为85 (硬编码) | RPT_BOND_CB_LIST 用 regex 解析条款文本有bug | 改用 RPT_BOND_CB_CLAUSE API + bonds_full.csv 缓存正确值 | 2026-03-24 |
| WAL replay failure | 程序崩溃/MCP锁异常 | 移除 .wal → rebuild | 2026-03-24 |
