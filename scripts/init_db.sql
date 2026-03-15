-- A股数据库初始化脚本
-- 用于创建项目所需的基础表结构

-- 加载 MCP 扩展
LOAD duckdb_mcp;

-- ============================================================
-- 股东人数表（完整字段，支持历史数据）
-- ============================================================
CREATE TABLE IF NOT EXISTS shareholders (
    code VARCHAR NOT NULL,           -- 股票代码
    name VARCHAR,                    -- 股票名称  
    shareholders INT,                -- 股东户数（本次）
    shareholders_prev INT,           -- 股东户数（上次）
    change INT,                      -- 增减
    change_ratio DECIMAL(10,4),      -- 增减比例(%)
    price DECIMAL(10,2),             -- 最新价
    change_pct DECIMAL(10,2),        -- 涨跌幅(%)
    stat_date DATE,                  -- 统计截止日
    announce_date DATE,              -- 公告日期
    avg_value DECIMAL(18,2),         -- 户均持股市值(元)
    avg_shares DECIMAL(18,2),        -- 户均持股数量(股)
    market_cap DECIMAL(18,2),        -- 总市值(元)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- 入库时间
    range_change_pct DECIMAL(10,4),  -- 区间涨跌幅(%)
    total_shares BIGINT,             -- 总股本(股)
    shares_change BIGINT,            -- 股本变动(股)
    shares_change_reason VARCHAR,    -- 股本变动原因
    PRIMARY KEY (code, stat_date)    -- 防止重复插入
);

-- 创建索引加速查询
CREATE INDEX IF NOT EXISTS idx_shareholders_announce ON shareholders(announce_date);
CREATE INDEX IF NOT EXISTS idx_shareholders_change ON shareholders(change_ratio);

-- ============================================================
-- 元数据表（记录数据更新情况）
-- ============================================================
CREATE TABLE IF NOT EXISTS data_updates (
    table_name VARCHAR NOT NULL,     -- 表名
    update_type VARCHAR,             -- 更新类型 (full/incremental)
    records_count INT,               -- 记录数
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes VARCHAR
);

-- 插入初始化记录
INSERT INTO data_updates (table_name, update_type, records_count, notes)
VALUES ('shareholders', 'init', 0, '数据库初始化');

SELECT '数据库初始化完成' AS status;
