#!/usr/bin/env python3
"""
获取最新公告股东人数的公司列表

基于 Playwright 浏览器 + JS fetch 方式获取数据，避免 API 被封禁。
"""

import argparse
import asyncio
import sys
import os
from datetime import datetime

import pandas as pd

# 添加 scripts 目录到路径以导入共享模块
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../../scripts'))

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


async def fetch_shareholder_data_via_browser(page_size: int = 500) -> list:
    """
    通过 Playwright 浏览器获取股东人数数据
    
    使用浏览器执行 fetch 请求，支持分页获取全量数据
    """
    from tqdm import tqdm
    
    page = await get_browser_page()
    
    # 先获取总页数
    js_get_pages = f"""
    async () => {{
        const url = "https://datacenter-web.eastmoney.com/api/data/v1/get";
        const params = new URLSearchParams({{
            sortColumns: "HOLD_NOTICE_DATE,SECURITY_CODE",
            sortTypes: "-1,-1",
            pageSize: "{page_size}",
            pageNumber: "1",
            reportName: "RPT_HOLDERNUMLATEST",
            columns: "ALL",
            source: "WEB",
            client: "WEB"
        }});
        const resp = await fetch(url + "?" + params.toString());
        const data = await resp.json();
        return data.result ? data.result.pages : 1;
    }}
    """
    total_pages = await page.evaluate(js_get_pages)
    
    # 分页获取数据
    all_data = []
    for page_num in tqdm(range(1, total_pages + 1), desc="", leave=False):
        js_code = f"""
        async () => {{
            const url = "https://datacenter-web.eastmoney.com/api/data/v1/get";
            const params = new URLSearchParams({{
                sortColumns: "HOLD_NOTICE_DATE,SECURITY_CODE",
                sortTypes: "-1,-1",
                pageSize: "{page_size}",
                pageNumber: "{page_num}",
                reportName: "RPT_HOLDERNUMLATEST",
                columns: "ALL",
                source: "WEB",
                client: "WEB"
            }});
            const resp = await fetch(url + "?" + params.toString());
            const data = await resp.json();
            return data.result ? data.result.data : [];
        }}
        """
        page_data = await page.evaluate(js_code)
        all_data.extend(page_data)
        
    return all_data


def convert_to_dataframe(data: list) -> pd.DataFrame:
    """将 API 数据转换为 DataFrame"""
    rows = []
    for row in data:
        rows.append({
            'code': row.get('SECURITY_CODE', ''),
            'name': row.get('SECURITY_NAME_ABBR', ''),
            'price': row.get('CLOSE_PRICE', 0) or 0,
            'change_pct': row.get('INTERVAL_CHRATE', 0) or 0,
            'shareholders': row.get('HOLDER_NUM', 0),
            'shareholders_prev': row.get('PRE_HOLDER_NUM', 0),
            'change': row.get('HOLDER_NUM_CHANGE', 0),
            'change_ratio': row.get('HOLDER_NUM_RATIO', 0),
            'range_change_pct': row.get('INTERVAL_CHRATE', 0) or 0,
            'stat_date': row.get('END_DATE', '')[:10] if row.get('END_DATE') else '',
            'stat_date_prev': row.get('PRE_END_DATE', '')[:10] if row.get('PRE_END_DATE') else '',
            'avg_value': row.get('AVG_MARKET_CAP', 0),
            'avg_shares': row.get('AVG_HOLD_NUM', 0),
            'market_cap': row.get('TOTAL_MARKET_CAP', 0),
            'total_shares': row.get('TOTAL_A_SHARES', 0),
            'announce_date': row.get('HOLD_NOTICE_DATE', '')[:10] if row.get('HOLD_NOTICE_DATE') else ''
        })
    return pd.DataFrame(rows)


async def fetch_latest_shareholders_async(num: int = 50, min_decrease: float = 0, 
                              decrease_only: bool = False, increase_only: bool = False,
                              save: bool = False) -> pd.DataFrame:
    """
    获取最新公告股东人数的公司（异步版本）
    
    Args:
        num: 显示数量
        min_decrease: 最小减少比例(%)，只筛选减少超过此比例的
        decrease_only: 只显示股东减少的
        increase_only: 只显示股东增加的
        save: 是否保存到CSV
    
    Returns:
        DataFrame with latest shareholder data
    """
    print("正在获取最新股东人数数据...")
    
    try:
        data = await fetch_shareholder_data_via_browser()
        df = convert_to_dataframe(data)
    except Exception as e:
        print(f"获取数据失败: {e}")
        sys.exit(1)
    
    if df.empty:
        print("获取数据失败")
        sys.exit(1)
    
    # 转换数值类型
    for col in ['change_ratio', 'change_pct']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 过滤条件
    if decrease_only:
        df = df[df['change_ratio'] < 0]
    elif increase_only:
        df = df[df['change_ratio'] > 0]
    
    if min_decrease > 0:
        df = df[df['change_ratio'] <= -min_decrease]
    
    # 按公告日期和增减比例排序
    df = df.sort_values(['announce_date', 'change_ratio'], ascending=[False, True])
    
    # 获取最新公告日期
    latest_date = df['announce_date'].iloc[0] if len(df) > 0 else datetime.now().strftime('%m-%d')
    
    print(f"\n{'='*80}")
    print(f"最新公告股东人数 ({latest_date})")
    print('='*80)
    
    # 分组显示
    if decrease_only:
        df_decrease = df.head(num)
        df_increase = pd.DataFrame()
    elif increase_only:
        df_decrease = pd.DataFrame()
        df_increase = df.head(num)
    else:
        df_decrease = df[df['change_ratio'] < 0].head(num // 2)
        df_increase = df[df['change_ratio'] > 0].head(num // 2)
    
    if len(df_decrease) > 0:
        print(f"\n【筹码集中】股东人数减少 TOP{len(df_decrease)}")
        print('-'*80)
        print(f"{'代码':<8} {'名称':<10} {'最新价':>8} {'涨跌幅':>8} {'股东人数':>10} {'增减':>10} {'增减比例':>8} {'截止日':>8}")
        for _, row in df_decrease.iterrows():
            change_str = f"{int(row['change']):,}" if pd.notna(row['change']) else '-'
            ratio_str = f"{row['change_ratio']:.2f}%" if pd.notna(row['change_ratio']) else '-'
            shareholders_str = f"{int(row['shareholders']):,}" if pd.notna(row['shareholders']) else '-'
            stat_date_str = row['stat_date'].strftime('%m-%d') if hasattr(row['stat_date'], 'strftime') else str(row['stat_date'])
            print(f"{row['code']:<8} {row['name']:<10} {row['price']:>8.2f} {row['change_pct']:>7.2f}% {shareholders_str:>10} {change_str:>10} {ratio_str:>8} {stat_date_str:>8}")
    
    if len(df_increase) > 0:
        print(f"\n【筹码分散】股东人数增加 TOP{len(df_increase)}")
        print('-'*80)
        print(f"{'代码':<8} {'名称':<10} {'最新价':>8} {'涨跌幅':>8} {'股东人数':>10} {'增减':>10} {'增减比例':>8} {'截止日':>8}")
        for _, row in df_increase.sort_values('change_ratio', ascending=False).iterrows():
            change_str = f"+{int(row['change']):,}" if pd.notna(row['change']) else '-'
            ratio_str = f"+{row['change_ratio']:.2f}%" if pd.notna(row['change_ratio']) else '-'
            shareholders_str = f"{int(row['shareholders']):,}" if pd.notna(row['shareholders']) else '-'
            stat_date_str = row['stat_date'].strftime('%m-%d') if hasattr(row['stat_date'], 'strftime') else str(row['stat_date'])
            print(f"{row['code']:<8} {row['name']:<10} {row['price']:>8.2f} {row['change_pct']:>7.2f}% {shareholders_str:>10} {change_str:>10} {ratio_str:>8} {stat_date_str:>8}")
    
    # 统计摘要
    total = len(df)
    decrease_count = len(df[df['change_ratio'] < 0])
    increase_count = len(df[df['change_ratio'] > 0])
    
    print(f"\n{'='*80}")
    print("【统计摘要】")
    print(f"  今日公告: {total} 只")
    print(f"  股东减少: {decrease_count} 只 ({decrease_count/total*100:.1f}%)" if total > 0 else "  股东减少: 0 只")
    print(f"  股东增加: {increase_count} 只 ({increase_count/total*100:.1f}%)" if total > 0 else "  股东增加: 0 只")
    
    if len(df_decrease) > 0:
        min_row = df_decrease.iloc[0]
        print(f"  减少最多: {min_row['name']} {min_row['change_ratio']:.2f}%")
    
    if len(df_increase) > 0:
        max_row = df_increase.sort_values('change_ratio', ascending=False).iloc[0]
        print(f"  增加最多: {max_row['name']} +{max_row['change_ratio']:.2f}%")
    
    # 保存CSV
    if save:
        filename = f"latest_shareholders_{datetime.now().strftime('%Y%m%d')}.csv"
        df.head(num).to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"\n已保存到: {filename}")
    
    return df.head(num)


def fetch_latest_shareholders(num: int = 50, min_decrease: float = 0, 
                              decrease_only: bool = False, increase_only: bool = False,
                              save: bool = False) -> pd.DataFrame:
    """同步版本的 fetch_latest_shareholders"""
    return asyncio.run(fetch_latest_shareholders_async(num, min_decrease, decrease_only, increase_only, save))


async def main_async(args):
    """异步主函数"""
    try:
        await fetch_latest_shareholders_async(
            num=args.num,
            min_decrease=args.min_decrease,
            decrease_only=args.decrease_only,
            increase_only=args.increase_only,
            save=args.save
        )
    finally:
        # 如果不是 --no-close 模式，关闭浏览器
        if not args.no_close:
            await close_browser()


def main():
    parser = argparse.ArgumentParser(description='获取最新公告股东人数的公司')
    parser.add_argument('-n', '--num', type=int, default=50, help='显示数量 (默认50)')
    parser.add_argument('-m', '--min-decrease', type=float, default=0, help='最小减少比例%% (默认0)')
    parser.add_argument('--decrease-only', action='store_true', help='只显示股东减少的')
    parser.add_argument('--increase-only', action='store_true', help='只显示股东增加的')
    parser.add_argument('--save', action='store_true', help='保存到CSV文件')
    parser.add_argument('--no-close', action='store_true', help='不关闭浏览器（供其他脚本复用）')
    
    args = parser.parse_args()
    
    if args.decrease_only and args.increase_only:
        print("错误: --decrease-only 和 --increase-only 不能同时使用")
        sys.exit(1)
    
    asyncio.run(main_async(args))


if __name__ == '__main__':
    main()
