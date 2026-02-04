#!/usr/bin/env python3
"""
股东人数筛选工具 v2 - 基于 Playwright

直接通过浏览器访问东方财富，使用 JavaScript 调用底层API获取数据。
避免 akshare 依赖，更稳定可靠。

用法：
    # 基本筛选
    python screen_shareholders_v2.py
    
    # 指定条件
    python screen_shareholders_v2.py -m 10 --min-cap 30 --max-cap 150
    
    # AI友好模式（JSON输出）
    python screen_shareholders_v2.py --json 2>/dev/null
"""

import asyncio
import json
import sys
import os
import argparse
from datetime import datetime, timedelta

# 添加 scripts 目录到路径以导入共享模块
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../../../scripts'))

try:
    from browser_manager import get_browser_page, close_browser
except ImportError:
    # 如果导入失败，提供本地实现
    from playwright.async_api import async_playwright
    _browser = None
    _page = None
    _playwright = None
    
    async def get_browser_page():
        global _browser, _page, _playwright
        if _page is None:
            _playwright = await async_playwright().start()
            _browser = await _playwright.chromium.launch(headless=False)
            context = await _browser.new_context()
            _page = await context.new_page()
            await _page.goto("https://data.eastmoney.com/gdhs/", wait_until="domcontentloaded")
            await asyncio.sleep(1)
        return _page
    
    async def close_browser():
        global _browser, _page, _playwright
        if _browser:
            await _browser.close()
            _browser = None
            _page = None
        if _playwright:
            await _playwright.stop()
            _playwright = None


def log(msg: str = ""):
    """进度信息输出到stderr"""
    print(msg, file=sys.stderr, flush=True)


async def fetch_shareholder_data_via_browser(page_size: int = 500) -> list:
    """
    通过 Playwright 浏览器获取股东人数数据
    
    使用浏览器执行 fetch 请求，支持分页获取全量数据
    """
    log("启动浏览器...")
    
    page = await get_browser_page()
    
    # 通过 JavaScript 调用 API，支持分页
    log("获取股东人数数据...")
    js_code = f"""
    async () => {{
        const url = "https://datacenter-web.eastmoney.com/api/data/v1/get";
        const pageSize = {page_size};
        let allData = [];
        let pageNumber = 1;
        let totalPages = 1;
        
        do {{
            const params = new URLSearchParams({{
                sortColumns: "HOLD_NOTICE_DATE,SECURITY_CODE",
                sortTypes: "-1,-1",
                pageSize: pageSize.toString(),
                pageNumber: pageNumber.toString(),
                reportName: "RPT_HOLDERNUMLATEST",
                columns: "ALL",
                source: "WEB",
                client: "WEB"
            }});
            
            const resp = await fetch(url + "?" + params.toString());
            const data = await resp.json();
            
            if (!data.success) {{
                throw new Error(data.message);
            }}
            
            totalPages = data.result.pages;
            allData = allData.concat(data.result.data);
            pageNumber++;
        }} while (pageNumber <= totalPages);
        
        return allData;
    }}
    """
    
    data = await page.evaluate(js_code)
    log(f"获取到 {len(data)} 只股票数据（共{len(data)//500 + 1}页）")
    
    return data


def filter_stocks(data: list,
                  min_decrease_pct: float = 5.0,
                  max_announce_age_days: int = 3,
                  max_data_age_days: int = 10,
                  min_market_cap: float = None,
                  max_market_cap: float = None) -> list:
    """
    筛选股东人数减少的股票
    """
    log("=" * 60)
    log("开始筛选...")
    log(f"  全部股票: {len(data)} 只")
    
    results = []
    now = datetime.now()
    
    for row in data:
        name = row.get('SECURITY_NAME_ABBR', '')
        
        # 1. 排除ST
        if 'ST' in name or '退' in name:
            continue
        
        # 2. 公告日期筛选
        if max_announce_age_days > 0:
            announce_date_str = row.get('HOLD_NOTICE_DATE', '')
            if announce_date_str:
                announce_date = datetime.fromisoformat(announce_date_str.replace(' 00:00:00', ''))
                if (now - announce_date).days > max_announce_age_days:
                    continue
        
        # 3. 统计截止日筛选
        if max_data_age_days > 0:
            end_date_str = row.get('END_DATE', '')
            if end_date_str:
                end_date = datetime.fromisoformat(end_date_str.replace(' 00:00:00', ''))
                if (now - end_date).days > max_data_age_days:
                    continue
        
        # 4. 减少比例筛选
        ratio = row.get('HOLDER_NUM_RATIO', 0)
        if ratio >= -min_decrease_pct:
            continue
        
        # 5. 市值筛选
        market_cap = row.get('TOTAL_MARKET_CAP', 0)
        market_cap_yi = market_cap / 1e8 if market_cap else 0
        
        if min_market_cap is not None and market_cap_yi < min_market_cap:
            continue
        if max_market_cap is not None and market_cap_yi > max_market_cap:
            continue
        
        results.append(row)
    
    log(f"  筛选后: {len(results)} 只")
    
    return results


def display_results(data: list):
    """展示结果到stderr"""
    log("\n" + "=" * 60)
    log(f"【筛选结果】共 {len(data)} 只")
    log("=" * 60)
    
    if not data:
        log("  未找到符合条件的股票")
        return
    
    # 按减少比例排序
    data = sorted(data, key=lambda x: x.get('HOLDER_NUM_RATIO', 0))
    
    log(f"\n{'代码':<8} {'名称':<10} {'股东数':>8} {'增减':>8} {'增减比例':>8} {'市值(亿)':>8} {'公告日':<12}")
    log("-" * 75)
    
    for row in data[:30]:
        code = row.get('SECURITY_CODE', '')
        name = row.get('SECURITY_NAME_ABBR', '')[:8]
        holder_num = row.get('HOLDER_NUM', 0)
        change = row.get('HOLDER_NUM_CHANGE', 0)
        ratio = row.get('HOLDER_NUM_RATIO', 0)
        market_cap_yi = row.get('TOTAL_MARKET_CAP', 0) / 1e8
        announce = row.get('HOLD_NOTICE_DATE', '')[:10]
        
        log(f"{code:<8} {name:<10} {holder_num:>8,} {change:>+8,} {ratio:>+7.1f}% {market_cap_yi:>8.1f} {announce}")
    
    if len(data) > 30:
        log(f"\n  ... 还有 {len(data) - 30} 只未显示")


def save_results(data: list, filename: str):
    """保存结果到CSV"""
    if not data:
        return
    
    import csv
    
    # 按减少比例排序
    data = sorted(data, key=lambda x: x.get('HOLDER_NUM_RATIO', 0))
    
    # 保存到项目根目录
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(script_dir))))
    filepath = os.path.join(project_root, filename)
    
    with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(['代码', '名称', '股东户数', '上次股东户数', '增减', '增减比例(%)', 
                        '统计截止日', '公告日期', '户均持股市值', '总市值(亿)'])
        
        for row in data:
            writer.writerow([
                row.get('SECURITY_CODE', ''),
                row.get('SECURITY_NAME_ABBR', ''),
                row.get('HOLDER_NUM', ''),
                row.get('PRE_HOLDER_NUM', ''),
                row.get('HOLDER_NUM_CHANGE', ''),
                round(row.get('HOLDER_NUM_RATIO', 0), 2),
                row.get('END_DATE', '')[:10],
                row.get('HOLD_NOTICE_DATE', '')[:10],
                round(row.get('AVG_MARKET_CAP', 0), 0),
                round(row.get('TOTAL_MARKET_CAP', 0) / 1e8, 2)
            ])
    
    log(f"\n结果已保存到: {filepath}")


def to_json_output(data: list, params: dict) -> dict:
    """转换为JSON输出格式"""
    results = []
    for row in sorted(data, key=lambda x: x.get('HOLDER_NUM_RATIO', 0)):
        results.append({
            "code": row.get('SECURITY_CODE', ''),
            "name": row.get('SECURITY_NAME_ABBR', ''),
            "holder_num": row.get('HOLDER_NUM', 0),
            "pre_holder_num": row.get('PRE_HOLDER_NUM', 0),
            "change": row.get('HOLDER_NUM_CHANGE', 0),
            "change_pct": round(row.get('HOLDER_NUM_RATIO', 0), 2),
            "end_date": row.get('END_DATE', '')[:10],
            "announce_date": row.get('HOLD_NOTICE_DATE', '')[:10],
            "market_cap_yi": round(row.get('TOTAL_MARKET_CAP', 0) / 1e8, 2),
            "avg_hold_value": round(row.get('AVG_MARKET_CAP', 0), 0)
        })
    
    return {
        "run_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "params": params,
        "count": len(results),
        "results": results
    }


async def main_async(args):
    log("=" * 60)
    log("股东人数筛选工具 v2 (Playwright)")
    log(f"运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"筛选条件: 减少>{args.min_decrease}%, 公告<{args.max_announce_age}天, 统计截止<{args.max_age}天")
    if args.min_cap or args.max_cap:
        cap_info = []
        if args.min_cap:
            cap_info.append(f">{args.min_cap}亿")
        if args.max_cap:
            cap_info.append(f"<{args.max_cap}亿")
        log(f"市值范围: {' & '.join(cap_info)}")
    log("=" * 60)
    
    try:
        # 获取数据
        data = await fetch_shareholder_data_via_browser()
        
        # 筛选
        filtered = filter_stocks(
            data,
            min_decrease_pct=args.min_decrease,
            max_announce_age_days=args.max_announce_age,
            max_data_age_days=args.max_age,
            min_market_cap=args.min_cap,
            max_market_cap=args.max_cap
        )
        
        # 展示结果
        display_results(filtered)
        
        # 保存CSV
        if not args.no_save:
            save_results(filtered, args.output)
        
        # JSON输出
        if args.json:
            params = {
                "min_decrease": args.min_decrease,
                "max_announce_age": args.max_announce_age,
                "max_age": args.max_age,
                "min_cap": args.min_cap,
                "max_cap": args.max_cap
            }
            output = to_json_output(filtered, params)
            print(json.dumps(output, ensure_ascii=False, indent=2))
    finally:
        # 如果指定 --close，关闭浏览器
        if args.close:
            await close_browser()


def main():
    parser = argparse.ArgumentParser(description='股东人数筛选工具 v2 (Playwright)')
    parser.add_argument('-m', '--min-decrease', type=float, default=5.0,
                        help='最小减少比例%%(默认5%%)')
    parser.add_argument('--max-announce-age', type=int, default=3,
                        help='公告日期最大天数(默认3天)')
    parser.add_argument('--max-age', type=int, default=10,
                        help='统计截止日最大天数(默认10天)')
    parser.add_argument('--min-cap', type=float, default=None,
                        help='最小市值(亿)')
    parser.add_argument('--max-cap', type=float, default=None,
                        help='最大市值(亿)')
    parser.add_argument('--json', action='store_true',
                        help='输出JSON到stdout')
    parser.add_argument('--no-save', action='store_true',
                        help='不保存CSV')
    parser.add_argument('--close', action='store_true',
                        help='执行完毕后关闭浏览器（默认保持打开供复用）')
    parser.add_argument('-o', '--output', type=str, 
                        default='screened_shareholders_v2.csv',
                        help='输出文件名')
    
    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
