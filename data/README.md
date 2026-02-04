# DuckDB 数据库

此目录存放项目的 DuckDB 数据库文件。

## 初始化数据库

```bash
cd /Users/rjwang/fun/a-share
duckdb data/a-share.db < scripts/init_db.sql
```

## 数据库文件

- `a-share.db` - 主数据库（已在 .gitignore 中忽略）

## 表结构

运行 `duckdb data/a-share.db -c "SHOW TABLES;"` 查看所有表。
