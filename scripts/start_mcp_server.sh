#!/bin/bash
# 启动 DuckDB MCP Server (stdio 模式)
# 用于 Copilot CLI 或 Claude Desktop 连接

cd "$(dirname "$0")/.."

# 数据库路径
DB_PATH="data/a-share.db"

# 启动 DuckDB MCP Server
exec duckdb "$DB_PATH" -cmd "LOAD duckdb_mcp; SELECT mcp_server_start('stdio');" -no-stdin
