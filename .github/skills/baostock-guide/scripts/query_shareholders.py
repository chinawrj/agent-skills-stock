#!/usr/bin/env python3
"""
股东人数查询工具

查询A股股票的股东人数变化历史，用于分析庄家吸筹/出货行为：
- 股东减少 + 股价上涨 → 庄家吸筹
- 股东增加 + 股价下跌 → 庄家出货/散户接盘

数据来源：东方财富（通过 Playwright 浏览器 + JS fetch）
"""

import asyncio
import pandas as pd
import sys
import os
import warnings
from datetime import datetime

warnings.filterwarnings('ignore')

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

from tqdm import tqdm


async def fetch_stock_list_via_browser() -> pd.DataFrame:
    """
    通过 Playwright 获取股票列表（用于名称搜索）
    """
    page = await get_browser_page()
    
    # 获取第一页数据即可用于搜索
    js_code = """
    async () => {
        const url = "https://datacenter-web.eastmoney.com/api/data/v1/get";
        const params = new URLSearchParams({
            sortColumns: "HOLD_NOTICE_DATE,SECURITY_CODE",
            sortTypes: "-1,-1",
            pageSize: "5000",
            pageNumber: "1",
            reportName: "RPT_HOLDERNUMLATEST",
            columns: "SECURITY_CODE,SECURITY_NAME_ABBR",
            source: "WEB",
            client: "WEB"
        });
        const resp = await fetch(url + "?" + params.toString());
        const data = await resp.json();
        return data.result ? data.result.data : [];
    }
    """
    data = await page.evaluate(js_code)
        
    rows = []
    for row in data:
        rows.append({
            '代码': row.get('SECURITY_CODE', ''),
            '名称': row.get('SECURITY_NAME_ABBR', '')
        })
    return pd.DataFrame(rows)


async def fetch_shareholder_detail_via_browser(symbol: str) -> pd.DataFrame:
    """
    通过 Playwright 获取单只股票的股东人数历史
    """
    page = await get_browser_page()
    
    # 获取股东人数历史
    js_code = f"""
    async () => {{
        const url = "https://datacenter-web.eastmoney.com/api/data/v1/get";
        const params = new URLSearchParams({{
            sortColumns: "END_DATE",
            sortTypes: "-1",
            pageSize: "50",
            pageNumber: "1",
            reportName: "RPT_HOLDERNUM_DET",
            columns: "ALL",
            filter: "(SECURITY_CODE=\\"{symbol}\\")",
            source: "WEB",
            client: "WEB"
        }});
        const resp = await fetch(url + "?" + params.toString());
        const data = await resp.json();
        return data.result ? data.result.data : [];
    }}
    """
    data = await page.evaluate(js_code)
        
    if not data:
        return pd.DataFrame()
    
    # 转换为 DataFrame
    rows = []
    for row in data:
        rows.append({
            '名称': row.get('SECURITY_NAME_ABBR', ''),
            '股东户数统计截止日': row.get('END_DATE', '')[:10] if row.get('END_DATE') else '',
            '股东户数-本次': row.get('HOLDER_NUM', 0),
            '股东户数-增减': row.get('HOLDER_NUM_CHANGE', 0),
            '股东户数-增减比例': row.get('HOLDER_NUM_RATIO', 0),
            '区间涨跌幅': row.get('INTERVAL_CHRATE', 0) or 0,
            '户均持股市值': row.get('AVG_MARKET_CAP', 0),
            '股东户数公告日期': row.get('HOLD_NOTICE_DATE', '')[:10] if row.get('HOLD_NOTICE_DATE') else ''
        })
    
    return pd.DataFrame(rows)


async def get_stock_code_async(keyword: str) -> tuple:
    """
    根据股票名称或代码获取标准代码（异步版本）
    
    Returns:
        (code, name) 元组
    """
    # 如果是6位数字代码，直接验证并获取名称
    if keyword.isdigit() and len(keyword) == 6:
        try:
            df = await fetch_shareholder_detail_via_browser(keyword)
            if not df.empty and '名称' in df.columns:
                return keyword, df.iloc[0]['名称']
            return keyword, keyword
        except:
            return keyword, keyword
    
    try:
        # 按名称搜索
        df = await fetch_stock_list_via_browser()
        
        # 按名称匹配（模糊匹配）
        match = df[df['名称'].str.contains(keyword, na=False)]
        if not match.empty:
            return match.iloc[0]['代码'], match.iloc[0]['名称']
        
        return None, None
    except Exception as e:
        print(f"获取股票代码失败: {e}")
        return None, None


def get_stock_code(keyword: str) -> tuple:
    """同步版本的 get_stock_code"""
    return asyncio.run(get_stock_code_async(keyword))


async def query_shareholders_async(symbol: str, limit: int = 16) -> pd.DataFrame:
    """
    查询股东人数历史数据（异步版本）
    """
    try:
        df = await fetch_shareholder_detail_via_browser(symbol)
        
        if df.empty:
            return pd.DataFrame()
        
        # 按日期降序排列（最新在前）
        df = df.sort_values('股东户数统计截止日', ascending=False)
        
        # 取最近N期
        df = df.head(limit).copy()
        
        return df
    except Exception as e:
        print(f"查询股东人数失败: {e}")
        return pd.DataFrame()


def query_shareholders(symbol: str, limit: int = 16) -> pd.DataFrame:
    """同步版本的 query_shareholders"""
    return asyncio.run(query_shareholders_async(symbol, limit))


def analyze_shareholders(df: pd.DataFrame) -> dict:
    """
    分析股东人数变化趋势
    
    Returns:
        分析结果字典
    """
    if df.empty or len(df) < 2:
        return {}
    
    # 最新一期数据
    latest = df.iloc[0]
    
    # 统计减少/增加期数
    decrease_count = (df['股东户数-增减'] < 0).sum()
    increase_count = (df['股东户数-增减'] > 0).sum()
    
    # 计算总变化
    oldest_count = df.iloc[-1]['股东户数-本次']
    newest_count = df.iloc[0]['股东户数-本次']
    total_change = newest_count - oldest_count
    total_change_pct = (total_change / oldest_count * 100) if oldest_count > 0 else 0
    
    # 判断吸筹/出货信号
    # 最近3期股东减少且股价不跌 → 吸筹信号
    recent_3 = df.head(3)
    recent_decrease = (recent_3['股东户数-增减'] < 0).sum()
    recent_price_up = (recent_3['区间涨跌幅'] >= 0).sum()
    
    is_accumulating = recent_decrease >= 2 and recent_price_up >= 2
    is_distributing = recent_decrease == 0 and recent_price_up <= 1
    
    return {
        'latest_count': int(newest_count),
        'latest_change': int(latest['股东户数-增减']) if pd.notna(latest['股东户数-增减']) else 0,
        'latest_change_pct': float(latest['股东户数-增减比例']) if pd.notna(latest['股东户数-增减比例']) else 0,
        'decrease_periods': int(decrease_count),
        'increase_periods': int(increase_count),
        'total_change': int(total_change),
        'total_change_pct': round(total_change_pct, 2),
        'is_accumulating': is_accumulating,  # 吸筹信号
        'is_distributing': is_distributing,   # 出货信号
    }


def display_shareholders(df: pd.DataFrame, stock_code: str, stock_name: str, analysis: dict):
    """格式化展示股东人数数据"""
    
    print("=" * 80)
    print(f"{stock_name} ({stock_code}) 股东人数变化")
    print(f"查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    if df.empty:
        print("  未找到股东人数数据")
        return
    
    # 格式化显示
    display_cols = ['股东户数统计截止日', '股东户数-本次', '股东户数-增减', 
                    '股东户数-增减比例', '区间涨跌幅', '户均持股市值', '股东户数公告日期']
    
    df_display = df[display_cols].copy()
    df_display.columns = ['统计截止日', '股东人数', '增减', '增减比例(%)', 
                          '区间涨跌(%)', '户均市值(元)', '公告日期']
    
    # 格式化数值
    df_display['股东人数'] = df_display['股东人数'].apply(lambda x: f"{x:,}")
    df_display['增减'] = df_display['增减'].apply(
        lambda x: f"{x:+,}" if pd.notna(x) else "-")
    df_display['增减比例(%)'] = df_display['增减比例(%)'].apply(
        lambda x: f"{x:+.2f}" if pd.notna(x) else "-")
    df_display['区间涨跌(%)'] = df_display['区间涨跌(%)'].apply(
        lambda x: f"{x:+.2f}" if pd.notna(x) else "-")
    df_display['户均市值(元)'] = df_display['户均市值(元)'].apply(
        lambda x: f"{x:,.0f}" if pd.notna(x) else "-")
    
    print()
    print(df_display.to_string(index=False))
    
    # 显示分析结果
    if analysis:
        print()
        print("-" * 80)
        print("【趋势分析】")
        print("-" * 80)
        print(f"  最新股东人数: {analysis['latest_count']:,}")
        print(f"  最新变化: {analysis['latest_change']:+,} ({analysis['latest_change_pct']:+.2f}%)")
        print(f"  统计期内: 减少{analysis['decrease_periods']}期, 增加{analysis['increase_periods']}期")
        print(f"  累计变化: {analysis['total_change']:+,} ({analysis['total_change_pct']:+.2f}%)")
        
        print()
        print("【信号判断】")
        if analysis['is_accumulating']:
            print("  🟢 吸筹信号：近期股东持续减少且股价稳定/上涨")
        elif analysis['is_distributing']:
            print("  🔴 出货信号：近期股东持续增加且股价下跌")
        else:
            print("  ⚪ 无明显信号")
    
    print()
    print("=" * 80)
    print("【分析说明】")
    print("  - 股东减少 + 股价上涨 → 庄家吸筹（筹码集中）")
    print("  - 股东增加 + 股价下跌 → 庄家出货（散户接盘）")
    print("=" * 80)


async def main_async(close: bool = False):
    """异步主函数"""
    if len(sys.argv) < 2:
        print("用法: python query_shareholders.py <股票代码或名称> [期数] [--close]")
        print("示例:")
        print("  python query_shareholders.py 300401        # 按代码查询")
        print("  python query_shareholders.py 花园生物      # 按名称查询")
        print("  python query_shareholders.py 300401 20     # 查询最近20期")
        print("  python query_shareholders.py 300401 --close  # 执行完关闭浏览器")
        sys.exit(1)
    
    keyword = sys.argv[1]
    limit = 16
    
    # 解析参数
    for arg in sys.argv[2:]:
        if arg == '--close':
            close = True
        elif arg.isdigit():
            limit = int(arg)
    
    try:
        # 获取股票代码
        print(f"正在查询: {keyword}")
        
        stock_code, stock_name = await get_stock_code_async(keyword)
        
        if not stock_code:
            print(f"未找到股票: {keyword}")
            sys.exit(1)
        
        print(f"匹配到: {stock_name} ({stock_code})")
        
        # 查询股东人数（复用同一浏览器）
        df = await query_shareholders_async(stock_code, limit)
        
        if df.empty:
            print(f"未找到 {stock_name} 的股东人数数据")
            sys.exit(1)
        
        # 分析
        analysis = analyze_shareholders(df)
        
        # 展示
        display_shareholders(df, stock_code, stock_name, analysis)
    finally:
        # 如果指定 --close，关闭浏览器
        if close:
            await close_browser()


def main():
    """主函数入口"""
    close = '--close' in sys.argv
    asyncio.run(main_async(close))


if __name__ == "__main__":
    main()
