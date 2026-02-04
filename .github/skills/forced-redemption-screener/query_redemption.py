#!/usr/bin/env python3
"""
查询单只转债的强赎状态
"""

import akshare as ak
import pandas as pd
import sys


def parse_redeem_count(s):
    """解析强赎天计数"""
    if pd.isna(s) or '|' not in str(s):
        return None, None, None
    try:
        parts = str(s).split('|')
        left = parts[0].strip()
        period = int(parts[1].strip())
        current, required = left.split('/')
        return int(current), int(required), period
    except:
        return None, None, None


def query_redemption(keyword):
    """查询单只转债强赎状态"""
    df = ak.bond_cb_redeem_jsl()
    
    # 搜索匹配
    mask = df['名称'].str.contains(keyword, na=False) | df['代码'].str.contains(keyword, na=False)
    matches = df[mask]
    
    if len(matches) == 0:
        print(f"未找到匹配 '{keyword}' 的转债")
        return None
    
    if len(matches) > 1:
        print(f"找到多个匹配，显示第一个:")
    
    bond = matches.iloc[0]
    
    # 解析天数
    current, required, period = parse_redeem_count(bond['强赎天计数'])
    
    # 计算强赎价格比
    stock_price = float(bond['正股价']) if pd.notna(bond['正股价']) else 0
    redeem_price = float(bond['强赎触发价']) if pd.notna(bond['强赎触发价']) else 0
    ratio = (stock_price / redeem_price * 100) if redeem_price > 0 else 0
    
    # 判断状态
    if ratio >= 100:
        status_emoji = "🔴"
        status_text = "正股已超过强赎触发价"
    elif ratio >= 95:
        status_emoji = "⭐"
        status_text = "接近强赎触发价（买入区间）"
    elif ratio >= 90:
        status_emoji = "🟡"
        status_text = "距强赎触发价较近"
    else:
        status_emoji = "⚪"
        status_text = "距强赎触发价较远"
    
    print("=" * 60)
    print(f"查询: {keyword}")
    print("=" * 60)
    print()
    print("【基本信息】")
    print(f"  转债代码: {bond['代码']}")
    print(f"  转债名称: {bond['名称']}")
    print(f"  转债现价: {bond['现价']}")
    print(f"  正股名称: {bond['正股名称']}")
    print(f"  正股现价: {bond['正股价']}")
    print(f"  剩余规模: {bond['剩余规模']} 亿")
    print()
    print("【强赎状态】")
    print(f"  转股价: {bond['转股价']}")
    print(f"  强赎触发价: {bond['强赎触发价']} (转股价×130%)")
    print(f"  强赎价格比: {ratio:.1f}% {status_emoji} {status_text}")
    print()
    print("【强赎进度】")
    print(f"  强赎天计数: {bond['强赎天计数']}")
    if current is not None:
        print(f"  已满足天数: {current} / {required} (观察期{period}天)")
        remaining = required - current
        if remaining > 0:
            print(f"  距离触发: 还需 {remaining} 天满足条件")
        else:
            print(f"  ✅ 已满足天数要求！")
    print(f"  强赎条款: {bond['强赎条款'][:50]}...")
    print(f"  当前状态: {bond['强赎状态'] if pd.notna(bond['强赎状态']) and bond['强赎状态'] else '未触发'}")
    print()
    print("【时间信息】")
    print(f"  转股起始日: {bond['转股起始日']}")
    print(f"  最后交易日: {bond['最后交易日']}")
    print(f"  到期日: {bond['到期日']}")
    
    return bond


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("用法: python query_redemption.py <转债名称或代码>")
        print("示例: python query_redemption.py 银邦转债")
        print("      python query_redemption.py 123456")
        sys.exit(1)
    
    query_redemption(sys.argv[1])
