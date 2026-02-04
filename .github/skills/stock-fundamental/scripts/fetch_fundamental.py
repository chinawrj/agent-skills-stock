#!/usr/bin/env python3
"""
股票基本面数据获取工具 - 基于 Playwright DOM解析

通过Playwright访问东方财富个股详情页，解析DOM提取业绩数据。
比API调用更稳定，不易被封禁。

数据来源页面：https://data.eastmoney.com/stockdata/{code}.html

数据包含：
- 基本面：每股收益、每股净资产、ROE
- 业绩数据：净利润、营收、同比增长率

用法：
    # 查询单只股票
    python fetch_fundamental.py 301216
    
    # 查询多只股票
    python fetch_fundamental.py 301216 002801 300530
    
    # 从CSV文件读取代码列表并筛选盈利股
    python fetch_fundamental.py -f screened_shareholders_v2.csv --profit-years 2
    
    # JSON输出
    python fetch_fundamental.py 301216 --json
"""

import asyncio
import json
import sys
import os
import argparse
import csv
from datetime import datetime

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


def normalize_code(code: str) -> str:
    """标准化股票代码（6位数字）"""
    code = str(code).strip()
    if '.' in code:
        code = code.split('.')[-1]
    return code.zfill(6)


async def fetch_fundamental_data_via_dom(codes: list[str], close: bool = False) -> list[dict]:
    """
    通过 Playwright 解析东方财富页面DOM获取业绩数据
    
    访问每只股票的详情页：https://data.eastmoney.com/stockdata/{code}.html
    从页面DOM中提取业绩表格数据
    
    Args:
        codes: 股票代码列表
        close: 是否关闭浏览器（默认不关闭，供后续脚本复用）
    """
    log("获取浏览器页面...")
    results = []
    
    try:
        page = await get_browser_page()
        
        for i, code in enumerate(codes):
            log(f"  [{i+1}/{len(codes)}] 获取 {code} 的业绩数据...")
            
            try:
                url = f"https://data.eastmoney.com/stockdata/{code}.html"
                await page.goto(url, wait_until="domcontentloaded", timeout=15000)
                await asyncio.sleep(1)  # 等待JS渲染
                
                # 通过JS从DOM提取业绩表格数据
                js_code = """
                () => {
                    const result = {
                        code: '',
                        name: '',
                        industry: '',
                        reports: []
                    };
                    
                    // 获取股票名称（从标题提取）
                    const title = document.title || '';
                    const match = title.match(/^(.+?)股票/);
                    if (match) result.name = match[1];
                    
                    // 提取股票代码
                    const urlMatch = window.location.href.match(/stockdata\\/([0-9]+)\\.html/);
                    if (urlMatch) result.code = urlMatch[1];
                    
                    // 找到业绩表格 - 查找包含"基本每股收益"的表格
                    const tables = document.querySelectorAll('table');
                    let targetTable = null;
                    
                    for (const table of tables) {
                        if (table.innerText.includes('基本每股收益') && table.innerText.includes('ROE')) {
                            targetTable = table;
                            break;
                        }
                    }
                    
                    if (!targetTable) {
                        return result;
                    }
                    
                    // 提取表头（报告期日期）
                    const headers = [];
                    const headerRow = targetTable.querySelector('thead tr');
                    if (headerRow) {
                        headerRow.querySelectorAll('th').forEach((th, idx) => {
                            if (idx > 0) {  // 跳过第一列（指标名称）
                                headers.push(th.innerText.trim());
                            }
                        });
                    }
                    
                    // 提取数据行
                    const dataRows = {};
                    const rows = targetTable.querySelectorAll('tbody tr');
                    rows.forEach(row => {
                        const cells = row.querySelectorAll('td');
                        if (cells.length > 1) {
                            const label = cells[0].innerText.trim();
                            const values = [];
                            for (let i = 1; i < cells.length; i++) {
                                values.push(cells[i].innerText.trim());
                            }
                            dataRows[label] = values;
                        }
                    });
                    
                    // 组装报告数据
                    for (let i = 0; i < headers.length; i++) {
                        const report = {
                            report_date: headers[i] || '',
                            eps: null,
                            net_profit: null,
                            revenue: null,
                            roe: null,
                            profit_yoy: null,
                            revenue_yoy: null
                        };
                        
                        // 提取各项指标
                        if (dataRows['基本每股收益(元)'] && dataRows['基本每股收益(元)'][i]) {
                            const v = parseFloat(dataRows['基本每股收益(元)'][i]);
                            if (!isNaN(v)) report.eps = v;
                        }
                        if (dataRows['ROE(%)'] && dataRows['ROE(%)'][i]) {
                            const v = parseFloat(dataRows['ROE(%)'][i]);
                            if (!isNaN(v)) report.roe = v;
                        }
                        if (dataRows['净利润同比(%)'] && dataRows['净利润同比(%)'][i]) {
                            const v = parseFloat(dataRows['净利润同比(%)'][i]);
                            if (!isNaN(v)) report.profit_yoy = v;
                        }
                        if (dataRows['营收同比率(%)'] && dataRows['营收同比率(%)'][i]) {
                            const v = parseFloat(dataRows['营收同比率(%)'][i]);
                            if (!isNaN(v)) report.revenue_yoy = v;
                        }
                        
                        // 净利润（需要解析"万"、"亿"单位）
                        if (dataRows['净利润'] && dataRows['净利润'][i]) {
                            const text = dataRows['净利润'][i];
                            let value = parseFloat(text);
                            if (!isNaN(value)) {
                                if (text.includes('亿')) value *= 100000000;
                                else if (text.includes('万')) value *= 10000;
                                report.net_profit = value;
                            }
                        }
                        
                        // 总营收
                        if (dataRows['总营收'] && dataRows['总营收'][i]) {
                            const text = dataRows['总营收'][i];
                            let value = parseFloat(text);
                            if (!isNaN(value)) {
                                if (text.includes('亿')) value *= 100000000;
                                else if (text.includes('万')) value *= 10000;
                                report.revenue = value;
                            }
                        }
                        
                        result.reports.push(report);
                    }
                    
                    return result;
                }
                """
                
                data = await page.evaluate(js_code)
                if data and data.get('code'):
                    results.append(data)
                else:
                    log(f"    警告: {code} 未找到业绩数据")
                    
            except Exception as e:
                log(f"    错误: {code} - {str(e)}")
            
            # 防止请求过快
            await asyncio.sleep(0.3)
    
    finally:
        # 如果指定 --close，关闭浏览器
        if close:
            await close_browser()
    
    log(f"共获取 {len(results)} 只股票的业绩数据")
    return results


def is_profitable(data: dict, years: int = 2) -> bool:
    """
    判断股票是否盈利
    
    Args:
        data: 基本面数据
        years: 要求盈利的年数
        
    Returns:
        是否盈利
    """
    reports = data.get('reports', [])
    
    if not reports:
        return False
    
    # 识别年报：report_date 以 12-31 结尾
    annual_reports = [r for r in reports 
                      if r.get('report_date', '').endswith('12-31')]
    
    # 检查年报数量是否足够
    if len(annual_reports) < years:
        # 如果年报不够，用最近报告的净利润判断
        for r in reports[:years * 4]:  # 检查最近几期
            net_profit = r.get('net_profit')
            if net_profit is not None and net_profit < 0:
                return False
        return len(reports) > 0
    
    # 检查最近N年的年报净利润
    for i in range(min(years, len(annual_reports))):
        net_profit = annual_reports[i].get('net_profit')
        if net_profit is not None and net_profit < 0:
            return False
    
    return True


def display_results(results: list[dict], show_all: bool = False):
    """展示结果"""
    if not results:
        log("未获取到数据")
        return
    
    log("\n" + "=" * 80)
    log(f"【基本面数据】共 {len(results)} 只")
    log("=" * 80)
    
    # 表头
    log(f"\n{'代码':<8} {'名称':<10} {'行业':<10} {'最新ROE':>8} {'净利润(亿)':>10} {'盈利':>6}")
    log("-" * 60)
    
    display_list = results if show_all else results[:30]
    for data in display_list:
        code = data.get('code', '')
        name = data.get('name', '')[:8]
        industry = data.get('industry', '')[:8]
        
        # 获取最新一期数据
        reports = data.get('reports', [])
        if reports:
            latest = reports[0]
            roe = latest.get('roe')
            net_profit = latest.get('net_profit')
            roe_str = f"{roe:.2f}" if roe is not None else "-"
            profit_str = f"{net_profit/1e8:.2f}" if net_profit is not None else "-"
        else:
            roe_str = "-"
            profit_str = "-"
        
        profitable = "✅" if is_profitable(data) else "❌"
        
        log(f"{code:<8} {name:<10} {industry:<10} {roe_str:>8} {profit_str:>10} {profitable:>6}")
    
    if len(results) > 30 and not show_all:
        log(f"\n  ... 还有 {len(results) - 30} 只未显示")


def filter_profitable(results: list[dict], years: int = 2) -> list[dict]:
    """筛选盈利股票"""
    return [r for r in results if is_profitable(r, years)]


def save_results(results: list[dict], filename: str):
    """保存结果到CSV"""
    if not results:
        return
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(script_dir))))
    filepath = os.path.join(project_root, filename)
    
    with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(['代码', '名称', '行业', '最新报告期', '每股收益', 'ROE(%)', 
                        '净利润(元)', '营收(元)', '净利润同比(%)', '营收同比(%)', '盈利'])
        
        for data in results:
            reports = data.get('reports', [])
            if reports:
                latest = reports[0]
                writer.writerow([
                    data.get('code', ''),
                    data.get('name', ''),
                    data.get('industry', ''),
                    latest.get('report_date', ''),
                    latest.get('eps', ''),
                    latest.get('roe', ''),
                    latest.get('net_profit', ''),
                    latest.get('revenue', ''),
                    latest.get('profit_yoy', ''),
                    latest.get('revenue_yoy', ''),
                    '是' if is_profitable(data) else '否'
                ])
    
    log(f"\n结果已保存到: {filepath}")


def read_codes_from_csv(filepath: str) -> list[str]:
    """从CSV文件读取股票代码"""
    codes = []
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # 尝试常见的代码列名
            code = row.get('代码') or row.get('code') or row.get('股票代码') or row.get('证券代码')
            if code:
                codes.append(normalize_code(code))
    return codes


async def main_async(args):
    log("=" * 60)
    log("股票基本面数据获取工具 (Playwright DOM解析)")
    log(f"运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 60)
    
    # 获取股票代码列表
    codes = []
    if args.file:
        log(f"从文件读取: {args.file}")
        codes = read_codes_from_csv(args.file)
        log(f"  读取到 {len(codes)} 只股票")
    
    if args.codes:
        codes.extend([normalize_code(c) for c in args.codes])
    
    if not codes:
        log("错误: 请提供股票代码或CSV文件")
        return
    
    # 去重
    codes = list(dict.fromkeys(codes))
    
    # 限制数量
    if args.limit and len(codes) > args.limit:
        log(f"限制查询数量: {args.limit}")
        codes = codes[:args.limit]
    
    # 获取数据
    results = await fetch_fundamental_data_via_dom(codes, close=args.close)
    
    # 自动缓存到 DuckDB
    if results and not args.json:
        try:
            from fundamental_cache import save_to_cache
            saved = save_to_cache(results, silent=True)
            log(f"已自动缓存 {saved} 条记录到 DuckDB")
        except ImportError:
            pass  # 缓存模块不可用，跳过
        except Exception as e:
            log(f"缓存失败: {e}")
    
    # 筛选盈利
    if args.profit_years:
        log(f"\n筛选最近{args.profit_years}年盈利的股票...")
        profitable = filter_profitable(results, args.profit_years)
        log(f"  盈利: {len(profitable)} 只，亏损: {len(results) - len(profitable)} 只")
        
        if args.only_profitable:
            results = profitable
    
    # 展示结果
    if not args.json:
        display_results(results, show_all=args.all)
    
    # 保存CSV
    if args.output:
        save_results(results, args.output)
    
    # JSON输出
    if args.json:
        output = {
            "run_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "count": len(results),
            "results": results
        }
        print(json.dumps(output, ensure_ascii=False, indent=2))


def main():
    parser = argparse.ArgumentParser(description='股票基本面数据获取工具 (Playwright DOM解析)')
    parser.add_argument('codes', nargs='*', help='股票代码列表')
    parser.add_argument('-f', '--file', type=str, help='从CSV文件读取股票代码')
    parser.add_argument('--profit-years', type=int, default=None,
                        help='筛选最近N年盈利的股票')
    parser.add_argument('--only-profitable', action='store_true',
                        help='只输出盈利股票（需配合--profit-years使用）')
    parser.add_argument('--limit', type=int, default=None,
                        help='限制查询数量')
    parser.add_argument('-o', '--output', type=str, default=None,
                        help='输出CSV文件名')
    parser.add_argument('--json', action='store_true',
                        help='JSON格式输出到stdout')
    parser.add_argument('--all', action='store_true',
                        help='显示全部结果（默认只显示前30条）')
    parser.add_argument('--close', action='store_true',
                        help='执行完毕后关闭浏览器（默认保持打开供复用）')
    
    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
