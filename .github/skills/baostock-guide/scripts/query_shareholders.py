#!/usr/bin/env python3
"""
股东人数查询工具

查询A股股票的股东人数变化历史，用于分析庄家吸筹/出货行为：
- 股东减少 + 股价上涨 → 庄家吸筹
- 股东增加 + 股价下跌 → 庄家出货/散户接盘

数据来源：东方财富（通过 akshare）
"""

import akshare as ak
import pandas as pd
import sys
import warnings
from datetime import datetime

warnings.filterwarnings('ignore')


def get_stock_code(keyword: str) -> tuple:
    """
    根据股票名称或代码获取标准代码
    
    Returns:
        (code, name) 元组
    """
    # 如果是6位数字代码，直接返回
    if keyword.isdigit() and len(keyword) == 6:
        # 通过股东人数接口验证代码有效性并获取名称
        try:
            df = ak.stock_zh_a_gdhs_detail_em(symbol=keyword)
            if not df.empty and '名称' in df.columns:
                return keyword, df.iloc[0]['名称']
            # 备用方案：从实时行情获取名称
            spot = ak.stock_zh_a_spot_em()
            match = spot[spot['代码'] == keyword]
            if not match.empty:
                return keyword, match.iloc[0]['名称']
            return keyword, keyword  # 返回代码本身作为名称
        except:
            return keyword, keyword
    
    try:
        # 按名称搜索需要获取股票列表
        df = ak.stock_zh_a_spot_em()
        
        # 按名称匹配（模糊匹配）
        match = df[df['名称'].str.contains(keyword, na=False)]
        if not match.empty:
            return match.iloc[0]['代码'], match.iloc[0]['名称']
        
        return None, None
    except Exception as e:
        print(f"获取股票代码失败: {e}")
        return None, None


def query_shareholders(symbol: str, limit: int = 16) -> pd.DataFrame:
    """
    查询股东人数历史数据
    
    Args:
        symbol: 股票代码（6位数字）
        limit: 返回最近几期数据，默认16期
    
    Returns:
        DataFrame 包含股东人数历史
    """
    try:
        df = ak.stock_zh_a_gdhs_detail_em(symbol=symbol)
        
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


def main():
    """主函数"""
    if len(sys.argv) < 2:
        print("用法: python query_shareholders.py <股票代码或名称> [期数]")
        print("示例:")
        print("  python query_shareholders.py 300401        # 按代码查询")
        print("  python query_shareholders.py 花园生物      # 按名称查询")
        print("  python query_shareholders.py 300401 20     # 查询最近20期")
        sys.exit(1)
    
    keyword = sys.argv[1]
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 16
    
    # 获取股票代码
    print(f"正在查询: {keyword}")
    
    stock_code, stock_name = get_stock_code(keyword)
    
    if not stock_code:
        print(f"未找到股票: {keyword}")
        sys.exit(1)
    
    print(f"匹配到: {stock_name} ({stock_code})")
    
    # 查询股东人数
    df = query_shareholders(stock_code, limit)
    
    if df.empty:
        print(f"未找到 {stock_name} 的股东人数数据")
        sys.exit(1)
    
    # 分析
    analysis = analyze_shareholders(df)
    
    # 展示
    display_shareholders(df, stock_code, stock_name, analysis)


if __name__ == "__main__":
    main()
