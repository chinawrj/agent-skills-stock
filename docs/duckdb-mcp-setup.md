# DuckDB MCP Server 配置说明

## 概述

本项目已配置 DuckDB 作为 MCP Server，可以让 AI 助手（Copilot CLI、Claude Desktop）直接查询和操作数据库。

## 文件结构

```
a-share/
├── data/
│   └── a-share.db          # DuckDB 数据库文件
├── scripts/
│   ├── init_db.sql         # 数据库初始化脚本
│   └── start_mcp_server.sh # MCP Server 启动脚本
└── .vscode/
    └── mcp.json            # MCP Server 配置
```

## 安装依赖

```bash
# 安装 DuckDB CLI
brew install duckdb

# 安装 Python 包
pip install duckdb

# 安装 MCP 扩展（首次使用会自动安装）
duckdb -c "INSTALL duckdb_mcp FROM community;"
```

## 初始化数据库

```bash
cd /Users/rjwang/fun/a-share
duckdb data/a-share.db < scripts/init_db.sql
```

## MCP Server 配置

### VS Code / Copilot CLI

配置文件 `.vscode/mcp.json`:

```json
{
  "servers": {
    "duckdb": {
      "type": "stdio",
      "command": "duckdb",
      "args": [
        "data/a-share.db",
        "-cmd",
        "LOAD duckdb_mcp; SELECT mcp_server_start('stdio');"
      ]
    }
  }
}
```

### Claude Desktop

编辑 `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "a-share-db": {
      "command": "duckdb",
      "args": [
        "/Users/rjwang/fun/a-share/data/a-share.db",
        "-cmd",
        "LOAD duckdb_mcp; SELECT mcp_server_start('stdio');"
      ]
    }
  }
}
```

## 使用方法

### 命令行查询

```bash
# 进入数据库
duckdb data/a-share.db

# 查看所有表
SHOW TABLES;

# 查看表结构
DESCRIBE shareholders;

# 查询数据
SELECT * FROM shareholders ORDER BY announce_date DESC LIMIT 10;
```

### Python 操作

```python
import duckdb

# 连接数据库
conn = duckdb.connect('data/a-share.db')

# 插入数据
conn.execute("""
    INSERT OR REPLACE INTO shareholders 
    (code, name, shareholders, change_ratio, stat_date, announce_date)
    VALUES (?, ?, ?, ?, ?, ?)
""", ['300401', '花园生物', 26228, -12.35, '2026-01-31', '2026-02-03'])

# 查询数据
df = conn.execute("SELECT * FROM shareholders WHERE code = '300401'").df()
print(df)
```

## 表结构

### shareholders（股东人数表）

| 字段 | 类型 | 说明 |
|------|------|------|
| code | VARCHAR | 股票代码 (PK) |
| name | VARCHAR | 股票名称 |
| shareholders | INT | 股东户数（本次） |
| shareholders_prev | INT | 股东户数（上次） |
| change | INT | 增减 |
| change_ratio | DECIMAL | 增减比例(%) |
| price | DECIMAL | 最新价 |
| change_pct | DECIMAL | 涨跌幅(%) |
| stat_date | DATE | 统计截止日 (PK) |
| announce_date | DATE | 公告日期 |
| avg_value | DECIMAL | 户均持股市值 |
| avg_shares | DECIMAL | 户均持股数量 |
| market_cap | DECIMAL | 总市值 |
| created_at | TIMESTAMP | 入库时间 |

### data_updates（数据更新记录表）

| 字段 | 类型 | 说明 |
|------|------|------|
| table_name | VARCHAR | 表名 |
| update_type | VARCHAR | 更新类型 |
| records_count | INT | 记录数 |
| updated_at | TIMESTAMP | 更新时间 |
| notes | VARCHAR | 备注 |

## 扩展

后续可以通过 skill 添加更多数据采集和分析功能：

- 自动采集股东人数数据并入库
- 添加可转债、定增等其他表
- 数据分析和报告生成
