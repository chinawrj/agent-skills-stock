#!/usr/bin/env python3
"""
股东人数连续下降筛选工具

两阶段筛选策略：
1. 初筛：批量获取最近一期股东减少的股票（~5秒）
2. 验证：逐只查询历史，确认连续N期下降（~0.5秒/只）

用于发现庄家吸筹信号：股东持续减少 = 筹码集中

数据来源：东方财富（通过 akshare）
"""

import akshare as ak
import pandas as pd
import sys
import os
import argparse
import warnings
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')


def get_listing_dates() -> dict:
    """
    获取所有A股的上市日期
    
    Returns:
        {股票代码: 上市日期} 字典
    """
    listing_dates = {}
    
    try:
        # 上交所
        for symbol in ["主板A股", "科创板"]:
            try:
                df = ak.stock_info_sh_name_code(symbol=symbol)
                for _, row in df.iterrows():
                    code = str(row['证券代码'])
                    date_str = row['上市日期']
                    if pd.notna(date_str):
                        listing_dates[code] = pd.to_datetime(date_str)
            except:
                pass
        
        # 深交所
        df = ak.stock_info_sz_name_code(symbol="A股列表")
        for _, row in df.iterrows():
            code = str(row['A股代码'])
            date_str = row['A股上市日期']
            if pd.notna(date_str):
                listing_dates[code] = pd.to_datetime(date_str)
    except Exception as e:
        print(f"  警告: 获取上市日期失败 - {e}")
    
    return listing_dates


def phase1_quick_filter(min_decrease_pct: float = 1.0, 
                        min_listing_days: int = 365,
                        min_market_cap: float = None,
                        max_market_cap: float = None,
                        max_data_age_days: int = 10) -> pd.DataFrame:
    """
    阶段1: 快速初筛
    
    Args:
        min_decrease_pct: 最小减少比例（%），默认1%
        min_listing_days: 最小上市天数，默认365天（过滤次新股）
        min_market_cap: 最小市值（亿元），None表示不限制
        max_market_cap: 最大市值（亿元），None表示不限制
        max_data_age_days: 数据最大时效（天），默认10天
    
    Returns:
        初筛通过的股票DataFrame
    """
    print("=" * 70)
    print("阶段1: 快速初筛 - 获取最近一期股东减少的股票")
    print("=" * 70)
    
    df = ak.stock_zh_a_gdhs(symbol="最新")
    print(f"  全部股票: {len(df)} 只")
    
    # 1. 排除ST股票（最快，字符串匹配）
    df = df[~df['名称'].str.contains('ST|退', na=False)]
    print(f"  排除ST后: {len(df)} 只")
    
    # 2. 数据新鲜度筛选（已有数据，零开销）
    if max_data_age_days > 0:
        cutoff_date = datetime.now() - timedelta(days=max_data_age_days)
        df['股东户数统计截止日-本次'] = pd.to_datetime(df['股东户数统计截止日-本次'])
        df = df[df['股东户数统计截止日-本次'] >= cutoff_date].copy()
        print(f"  数据在{max_data_age_days}天内: {len(df)} 只")
    
    # 3. 筛选最近一期减少的（已有数据，零开销）
    df = df[df['股东户数-增减'] < 0].copy()
    print(f"  最近一期减少: {len(df)} 只")
    
    # 4. 筛选减少比例（已有数据，零开销）
    if min_decrease_pct > 0:
        df = df[df['股东户数-增减比例'] < -min_decrease_pct].copy()
        print(f"  减少比例 > {min_decrease_pct}%: {len(df)} 只")
    
    # 5. 市值筛选（需要API调用，但此时候选量已大幅减少）
    if min_market_cap is not None or max_market_cap is not None:
        print(f"  获取市值数据...")
        try:
            df_realtime = ak.stock_zh_a_spot_em()
            df_realtime['市值(亿)'] = df_realtime['总市值'] / 1e8
            df = df.merge(
                df_realtime[['代码', '市值(亿)']],
                on='代码',
                how='left'
            )
            
            before_count = len(df)
            if min_market_cap is not None:
                df = df[df['市值(亿)'] >= min_market_cap]
            if max_market_cap is not None:
                df = df[df['市值(亿)'] <= max_market_cap]
            
            cap_range = []
            if min_market_cap is not None:
                cap_range.append(f">{min_market_cap}亿")
            if max_market_cap is not None:
                cap_range.append(f"<{max_market_cap}亿")
            print(f"  市值 {' & '.join(cap_range)}: {len(df)} 只 (排除{before_count - len(df)}只)")
        except Exception as e:
            print(f"  警告: 获取市值数据失败 - {e}")
    
    # 6. 过滤次新股（需要API调用，放最后）
    if min_listing_days > 0:
        print(f"  获取上市日期信息...")
        listing_dates = get_listing_dates()
        cutoff_date = datetime.now() - timedelta(days=min_listing_days)
        
        def is_old_enough(code):
            listing_date = listing_dates.get(str(code))
            if listing_date is None:
                return True  # 找不到上市日期的保留
            return listing_date <= cutoff_date
        
        df = df[df['代码'].apply(is_old_enough)]
        print(f"  排除上市<{min_listing_days}天: {len(df)} 只")
    
    return df


def check_consecutive_decrease(code: str, periods: int = 3) -> dict:
    """
    检查单只股票是否连续N期下降
    
    Args:
        code: 股票代码
        periods: 连续下降期数，默认3期
    
    Returns:
        符合条件返回详情dict，否则返回None
    """
    try:
        df = ak.stock_zh_a_gdhs_detail_em(symbol=code)
        if df.empty or len(df) < periods:
            return None
        
        # 按日期降序（最新在前）
        df = df.sort_values('股东户数统计截止日', ascending=False).head(periods)
        
        # 检查连续N期下降
        counts = df['股东户数-本次'].tolist()
        
        # 验证每一期都比上一期少
        is_consecutive = all(counts[i] < counts[i+1] for i in range(len(counts)-1))
        
        if is_consecutive:
            return {
                'code': code,
                'name': df.iloc[0]['名称'],
                'latest': int(counts[0]),
                'oldest': int(counts[-1]),
                'counts': counts,
                'dates': df['股东户数统计截止日'].tolist(),
                'total_drop_pct': round((counts[0] - counts[-1]) / counts[-1] * 100, 2)
            }
        return None
    except:
        return None


def phase2_verify(df_candidates: pd.DataFrame, periods: int = 3, 
                  max_stocks: int = None) -> list:
    """
    阶段2: 详细验证连续下降
    
    Args:
        df_candidates: 初筛通过的股票
        periods: 连续下降期数
        max_stocks: 最大验证数量（None表示全部）
    
    Returns:
        符合条件的股票列表
    """
    print("\n" + "=" * 70)
    print(f"阶段2: 详细验证 - 检查是否连续{periods}期下降")
    print("=" * 70)
    
    codes = df_candidates['代码'].tolist()
    if max_stocks:
        codes = codes[:max_stocks]
    
    total = len(codes)
    print(f"  待验证: {total} 只")
    print(f"  预计耗时: {total * 0.5:.0f} 秒")
    print()
    
    results = []
    for i, code in enumerate(codes):
        result = check_consecutive_decrease(code, periods)
        if result:
            results.append(result)
        
        if (i + 1) % 20 == 0 or (i + 1) == total:
            print(f"  进度: {i+1}/{total} ({(i+1)*100//total}%) | 已找到: {len(results)} 只")
    
    return results


def display_results(results: list, periods: int):
    """展示筛选结果"""
    print("\n" + "=" * 70)
    print(f"【连续{periods}期股东人数下降】共 {len(results)} 只")
    print("=" * 70)
    
    if not results:
        print("  未找到符合条件的股票")
        return
    
    # 按降幅排序
    results = sorted(results, key=lambda x: x['total_drop_pct'])
    
    print(f"\n{'代码':<8} {'名称':<8} {'最早人数':>10} → {'最新人数':>10} {'总降幅':>10}")
    print("-" * 70)
    
    for r in results[:30]:  # 显示前30只
        dates = r['dates']
        print(f"{r['code']:<8} {r['name']:<8} {r['oldest']:>10,} → {r['latest']:>10,} {r['total_drop_pct']:>+10.1f}%")
        # 显示日期范围
        print(f"         期间: {dates[-1]} → {dates[0]}")
    
    if len(results) > 30:
        print(f"\n  ... 还有 {len(results) - 30} 只未显示")
    
    # 统计信息
    print("\n" + "-" * 70)
    print("【统计汇总】")
    print(f"  符合条件: {len(results)} 只")
    avg_drop = sum(r['total_drop_pct'] for r in results) / len(results)
    print(f"  平均降幅: {avg_drop:.1f}%")
    max_drop = min(r['total_drop_pct'] for r in results)
    print(f"  最大降幅: {max_drop:.1f}%")


def save_results(results: list, periods: int):
    """保存结果到CSV"""
    if not results:
        return
    
    # 保存到项目根目录
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(script_dir))))
    
    filename = f"screened_shareholders_decrease_{periods}periods.csv"
    filepath = os.path.join(project_root, filename)
    
    # 构建DataFrame
    rows = []
    for r in results:
        row = {
            '代码': r['code'],
            '名称': r['name'],
            '最新股东数': r['latest'],
            '最早股东数': r['oldest'],
            '总降幅(%)': r['total_drop_pct'],
            '最新日期': r['dates'][0],
            '最早日期': r['dates'][-1],
        }
        # 添加每期数据
        for i, (count, date) in enumerate(zip(r['counts'], r['dates'])):
            row[f'第{i+1}期人数'] = count
            row[f'第{i+1}期日期'] = date
        rows.append(row)
    
    df = pd.DataFrame(rows)
    df = df.sort_values('总降幅(%)')
    df.to_csv(filepath, index=False, encoding='utf-8-sig')
    print(f"\n结果已保存到: {filepath}")


def main():
    parser = argparse.ArgumentParser(description='筛选股东人数连续下降的股票')
    parser.add_argument('-p', '--periods', type=int, default=3,
                        help='连续下降期数（默认3期）')
    parser.add_argument('-m', '--min-decrease', type=float, default=1.0,
                        help='初筛最小减少比例%%（默认1%%）')
    parser.add_argument('-l', '--min-listing-days', type=int, default=365,
                        help='最小上市天数，过滤次新股（默认365天，0表示不过滤）')
    parser.add_argument('-n', '--max-stocks', type=int, default=None,
                        help='最大验证数量（默认全部）')
    parser.add_argument('--min-cap', type=float, default=None,
                        help='最小市值（亿元），例如: --min-cap 40')
    parser.add_argument('--max-cap', type=float, default=None,
                        help='最大市值（亿元），例如: --max-cap 100')
    parser.add_argument('--max-age', type=int, default=10,
                        help='数据最大时效（天），默认10天，0表示不限制')
    parser.add_argument('--no-save', action='store_true',
                        help='不保存结果到CSV')
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("股东人数连续下降筛选工具")
    print(f"运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    cap_info = ""
    if args.min_cap is not None or args.max_cap is not None:
        cap_parts = []
        if args.min_cap is not None:
            cap_parts.append(f">{args.min_cap}亿")
        if args.max_cap is not None:
            cap_parts.append(f"<{args.max_cap}亿")
        cap_info = f", 市值{' & '.join(cap_parts)}"
    age_info = f", 数据<{args.max_age}天" if args.max_age > 0 else ""
    print(f"筛选条件: 连续{args.periods}期下降, 初筛减少>{args.min_decrease}%, 上市>{args.min_listing_days}天{cap_info}{age_info}")
    print("=" * 70)
    
    # 阶段1: 快速初筛
    df_candidates = phase1_quick_filter(args.min_decrease, args.min_listing_days,
                                        args.min_cap, args.max_cap, args.max_age)
    
    if df_candidates.empty:
        print("\n初筛无结果")
        return
    
    # 阶段2: 详细验证
    results = phase2_verify(df_candidates, args.periods, args.max_stocks)
    
    # 展示结果
    display_results(results, args.periods)
    
    # 保存结果
    if not args.no_save:
        save_results(results, args.periods)


if __name__ == "__main__":
    main()
