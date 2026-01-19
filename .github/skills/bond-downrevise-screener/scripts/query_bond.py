#!/usr/bin/env python3
"""
查询单只可转债详细信息

用法:
    python query_bond.py 希望转2
    python query_bond.py 127049
"""

import akshare as ak
import pandas as pd
from datetime import date
import sys
import warnings
warnings.filterwarnings('ignore')


def query_bond(keyword: str):
    """查询转债详细信息"""
    print(f"\n{'=' * 60}")
    print(f"查询: {keyword}")
    print('=' * 60)
    
    # 获取转债列表
    df = ak.bond_cov_comparison()
    
    # 按名称或代码搜索
    if keyword.isdigit():
        bond = df[df['转债代码'] == keyword]
    else:
        bond = df[df['转债名称'].str.contains(keyword)]
    
    if len(bond) == 0:
        print(f"未找到匹配的转债: {keyword}")
        return
    
    bond = bond.iloc[0]
    code = str(bond['转债代码'])
    name = bond['转债名称']
    
    # 计算强赎进度（100%=触发强赎）
    convert_value = bond['转股价值']
    redemption_progress = round(convert_value / 130 * 100, 1)
    
    print(f"\n【基本信息】")
    print(f"  转债代码: {code}")
    print(f"  转债名称: {name}")
    print(f"  最新价格: {bond['转债最新价']}")
    print(f"  正股名称: {bond['正股名称']}")
    print(f"  正股代码: {bond['正股代码']}")
    print(f"  转股价: {bond['转股价']}")
    print(f"  转股价值: {bond['转股价值']}")
    print(f"  转股溢价率: {bond['转股溢价率']}%")
    print(f"  强赎进度: {redemption_progress}%" + (" ⚠️ 接近强赎!" if redemption_progress >= 90 else "") + (" 🔴 已触发强赎!" if redemption_progress >= 100 else ""))
    
    # 获取到期时间
    try:
        df_ths = ak.bond_zh_cov_info_ths()
        ths = df_ths[df_ths['债券代码'] == code]
        if len(ths) > 0:
            expire = ths.iloc[0]['到期时间']
            days = (expire - date.today()).days
            print(f"  到期时间: {expire}")
            print(f"  剩余年限: {round(days/365, 2)} 年")
    except:
        pass
    
    # 获取发行规模
    try:
        df_cov = ak.bond_zh_cov()
        cov = df_cov[df_cov['债券代码'] == code]
        if len(cov) > 0:
            print(f"  发行规模: {cov.iloc[0]['发行规模']} 亿")
    except:
        pass
    
    # 获取下修历史
    print(f"\n【下修历史】")
    try:
        history = ak.bond_cb_adj_logs_jsl(symbol=code)
        if len(history) > 0:
            print(f"  下修次数: {len(history)}")
            for _, row in history.iterrows():
                print(f"  - {row['股东大会日']}: {row['下修前转股价']} → {row['下修后转股价']}")
        else:
            print("  无下修记录")
    except:
        print("  查询下修历史失败")
    
    # 获取正股财务数据
    print(f"\n【正股财务】")
    try:
        df_profit = ak.stock_yjbb_em(date='20250930')
        stock_code = str(bond['正股代码']).zfill(6)
        profit_data = df_profit[df_profit['股票代码'] == stock_code]
        if len(profit_data) > 0:
            row = profit_data.iloc[0]
            net_profit = row['净利润-净利润'] / 100000000
            revenue = row['营业总收入-营业总收入'] / 100000000
            print(f"  报告期: 2025年Q3")
            print(f"  营业收入: {round(revenue, 2)} 亿")
            print(f"  净利润: {round(net_profit, 2)} 亿")
            print(f"  盈利状态: {'盈利' if net_profit > 0 else '亏损'}")
            
            # 计算规模/净利
            try:
                scale = float(cov.iloc[0]['发行规模'])
                if net_profit != 0:
                    ratio = scale / net_profit * 100
                    print(f"  规模/净利: {round(ratio, 2)}%")
            except:
                pass
    except:
        print("  查询财务数据失败")
    
    print()


def main():
    if len(sys.argv) < 2:
        print("用法: python query_bond.py <转债名称或代码>")
        print("示例: python query_bond.py 希望转2")
        print("      python query_bond.py 127049")
        return
    
    keyword = sys.argv[1]
    query_bond(keyword)


if __name__ == "__main__":
    main()
