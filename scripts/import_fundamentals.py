#!/usr/bin/env python3
"""
全市场年度财务数据导入脚本

从东方财富 RPT_LICO_FN_CPD API 获取全A股年报财务数据，保存为 CSV 后导入 DuckDB。

数据源: datacenter-web.eastmoney.com
API: RPT_LICO_FN_CPD (历次财报)
覆盖: 2020-2024 年全部A股年度报告

用法:
    # 抽样测试 (只获取前2页, 约1000条)
    python scripts/import_fundamentals.py --sample 2

    # 全量导入 (115页, 约57000条)
    python scripts/import_fundamentals.py

    # 指定年份范围
    python scripts/import_fundamentals.py --years 2022,2023,2024

    # 仅下载CSV, 不打印导入SQL
    python scripts/import_fundamentals.py --no-sql

导入DuckDB (通过 MCP 执行输出的 SQL):
    脚本运行后会输出 INSERT OR REPLACE SQL, 复制到 MCP 执行即可。
    重复执行安全: INSERT OR REPLACE 自动覆盖旧数据。
"""

import argparse
import csv
import json
import os
import sys
import time

import requests

API_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://data.eastmoney.com/",
}
DEFAULT_YEARS = ["2020", "2021", "2022", "2023", "2024"]
OUTPUT_CSV = "data/fundamentals_annual.csv"
PAGE_SIZE = 500
DELAY_BETWEEN_PAGES = 0.3  # seconds


def fetch_page(page_num: int, years: list[str]) -> dict:
    """Fetch one page of annual financial data."""
    years_str = '","'.join(years)
    params = {
        "reportName": "RPT_LICO_FN_CPD",
        "columns": "SECURITY_CODE,SECURITY_NAME_ABBR,DATAYEAR,DATEMMDD,"
                   "BASIC_EPS,WEIGHTAVG_ROE,PARENT_NETPROFIT,TOTAL_OPERATE_INCOME,"
                   "YSTZ,SJLTZ",
        "filter": f'(DATEMMDD="年报")(DATAYEAR in ("{years_str}"))',
        "sortColumns": "DATAYEAR,SECURITY_CODE",
        "sortTypes": "-1,1",
        "pageSize": PAGE_SIZE,
        "pageNumber": page_num,
    }
    resp = requests.get(API_URL, params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def year_to_report_date(year: str) -> str:
    """Convert year string to report_date (annual = 12-31)."""
    return f"{year}-12-31"


def run(args):
    years = args.years.split(",") if args.years else DEFAULT_YEARS
    max_pages = args.sample if args.sample else 9999

    print(f"=== 全市场年度财务数据导入 ===")
    print(f"年份范围: {', '.join(years)}")
    print(f"输出文件: {OUTPUT_CSV}")
    if args.sample:
        print(f"抽样模式: 仅获取前 {args.sample} 页")
    print()

    # Step 1: Fetch first page to get total info
    print("获取第1页...")
    data = fetch_page(1, years)
    if not data.get("success") or not data.get("result"):
        print(f"ERROR: API 返回错误: {data.get('message')}")
        sys.exit(1)

    total_count = data["result"]["count"]
    total_pages = data["result"]["pages"]
    actual_pages = min(total_pages, max_pages)

    print(f"总记录数: {total_count}, 总页数: {total_pages}, 将获取: {actual_pages} 页")
    print()

    # Step 2: Process all pages
    all_rows = []
    rows_from_page = data["result"]["data"]
    all_rows.extend(rows_from_page)
    print(f"  页 1/{actual_pages}: {len(rows_from_page)} 条")

    for page in range(2, actual_pages + 1):
        time.sleep(DELAY_BETWEEN_PAGES)
        data = fetch_page(page, years)
        if not data.get("result") or not data["result"].get("data"):
            print(f"  页 {page}: 无数据, 跳过")
            continue
        rows_from_page = data["result"]["data"]
        all_rows.extend(rows_from_page)
        print(f"  页 {page}/{actual_pages}: {len(rows_from_page)} 条 (累计: {len(all_rows)})")

    print(f"\n共获取 {len(all_rows)} 条记录")

    # Step 3: Write CSV
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    csv_fields = ["code", "name", "report_date", "report_type", "eps", "roe",
                   "net_profit", "revenue", "profit_yoy", "revenue_yoy"]

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields)
        writer.writeheader()
        for row in all_rows:
            writer.writerow({
                "code": row["SECURITY_CODE"],
                "name": row["SECURITY_NAME_ABBR"],
                "report_date": year_to_report_date(row["DATAYEAR"]),
                "report_type": "年报",
                "eps": row.get("BASIC_EPS"),
                "roe": row.get("WEIGHTAVG_ROE"),
                "net_profit": row.get("PARENT_NETPROFIT"),
                "revenue": row.get("TOTAL_OPERATE_INCOME"),
                "profit_yoy": row.get("SJLTZ"),
                "revenue_yoy": row.get("YSTZ"),
            })

    file_size = os.path.getsize(OUTPUT_CSV) / 1024
    print(f"已保存: {OUTPUT_CSV} ({file_size:.0f} KB, {len(all_rows)} 行)")

    # Step 4: Print import SQL
    if not args.no_sql:
        abs_path = os.path.abspath(OUTPUT_CSV)
        sql = f"""INSERT OR REPLACE INTO fundamentals (code, name, report_date, report_type, eps, roe, net_profit, revenue, profit_yoy, revenue_yoy)
SELECT
    code, name,
    CAST(report_date AS DATE),
    report_type,
    CAST(eps AS DECIMAL(10,4)),
    CAST(roe AS DECIMAL(10,4)),
    CAST(net_profit AS DECIMAL(18,2)),
    CAST(revenue AS DECIMAL(18,2)),
    CAST(profit_yoy AS DECIMAL(10,4)),
    CAST(revenue_yoy AS DECIMAL(10,4))
FROM read_csv('{abs_path}', header=true, nullstr='');"""

        print(f"\n{'='*60}")
        print("导入 SQL (通过 MCP mcp_duckdb_query 执行):")
        print(f"{'='*60}")
        print(sql)
        print(f"{'='*60}")

    # Step 5: Quick data quality report
    print(f"\n--- 数据质量报告 ---")
    year_counts = {}
    null_counts = {"eps": 0, "roe": 0, "net_profit": 0}
    for row in all_rows:
        y = row["DATAYEAR"]
        year_counts[y] = year_counts.get(y, 0) + 1
        if row.get("BASIC_EPS") is None:
            null_counts["eps"] += 1
        if row.get("WEIGHTAVG_ROE") is None:
            null_counts["roe"] += 1
        if row.get("PARENT_NETPROFIT") is None:
            null_counts["net_profit"] += 1

    for y in sorted(year_counts):
        print(f"  {y}年: {year_counts[y]} 条")
    for field, cnt in null_counts.items():
        pct = cnt / len(all_rows) * 100 if all_rows else 0
        print(f"  {field} 为空: {cnt} ({pct:.1f}%)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="导入全市场年度财务数据")
    parser.add_argument("--years", help="年份列表, 逗号分隔 (默认: 2020-2024)")
    parser.add_argument("--sample", type=int, help="抽样: 仅获取前N页")
    parser.add_argument("--no-sql", action="store_true", help="不输出导入SQL")
    run(parser.parse_args())
