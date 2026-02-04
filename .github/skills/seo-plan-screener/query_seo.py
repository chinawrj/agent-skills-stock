#!/usr/bin/env python3
"""
查询单只股票的定增信息

用法：
  python query_seo.py 新希望     # 按名称查询
  python query_seo.py 000876    # 按代码查询
"""

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import sys
import warnings
warnings.filterwarnings('ignore')


def query_stock_seo(keyword: str, days: int = 90):
    """查询单只股票的定增信息"""
    print("=" * 60)
    print(f"查询: {keyword}")
    print("=" * 60)
    
    # 获取融资公告
    all_notices = []
    for i in range(days):
        date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
        try:
            df = ak.stock_notice_report(symbol="融资公告", date=date)
            if len(df) > 0:
                all_notices.append(df)
        except:
            pass
    
    if not all_notices:
        print("未获取到公告数据")
        return
    
    df_all = pd.concat(all_notices, ignore_index=True)
    
    # 搜索匹配
    keyword_clean = keyword.strip()
    if keyword_clean.isdigit():
        # 按代码搜索
        code = keyword_clean.zfill(6)
        matches = df_all[df_all['代码'].astype(str).str.contains(code)]
    else:
        # 按名称搜索
        matches = df_all[df_all['名称'].str.contains(keyword_clean, na=False)]
    
    # 筛选定增相关
    seo_keywords = ['定增', '定向增发', '非公开发行', '向特定对象', '发行股份']
    matches = matches[matches['公告标题'].str.contains('|'.join(seo_keywords), na=False)]
    
    if matches.empty:
        print(f"\n未找到 '{keyword}' 的定增相关公告")
        return
    
    # 获取股票基本信息
    stock_code = matches.iloc[0]['代码']
    stock_name = matches.iloc[0]['名称']
    
    print(f"\n【基本信息】")
    print(f"  股票代码: {stock_code}")
    print(f"  股票名称: {stock_name}")
    
    # 查询是否有可转债
    print(f"\n【可转债信息】")
    try:
        df_bonds = ak.bond_cov_comparison()
        df_bonds['正股代码'] = df_bonds['正股代码'].astype(str).str.zfill(6)
        bond = df_bonds[df_bonds['正股代码'] == str(stock_code).zfill(6)]
        
        if not bond.empty:
            bond = bond.iloc[0]
            print(f"  ✅ 有可转债")
            print(f"  转债名称: {bond.get('转债名称', '')}")
            print(f"  转债价格: {bond.get('转债最新价', '')}")
            print(f"  转股价: {bond.get('转股价', '')}")
            print(f"  正股价格: {bond.get('正股最新价', '')}")
            
            # 计算下修触发价
            convert_price = float(bond.get('转股价', 0))
            stock_price = float(bond.get('正股最新价', 0))
            if convert_price > 0:
                trigger_price = convert_price * 0.8
                distance = (stock_price - trigger_price) / trigger_price * 100
                print(f"  下修触发价: {trigger_price:.2f} (转股价×80%)")
                print(f"  距下修: {distance:.2f}%")
                
                if distance <= 0:
                    print(f"  ⚠️  已触发下修条件！")
                elif distance <= 10:
                    print(f"  ⚠️  接近下修触发价！")
        else:
            print(f"  ❌ 无可转债")
    except Exception as e:
        print(f"  查询转债失败: {e}")
    
    # 显示定增公告
    print(f"\n【定增相关公告】（最近{days}天）")
    for _, row in matches.iterrows():
        print(f"  [{row['公告日期']}] {row['公告标题']}")
        print(f"    链接: {row.get('网址', '')}")


def main():
    if len(sys.argv) < 2:
        print("用法: python query_seo.py <股票名称或代码>")
        print("示例: python query_seo.py 新希望")
        print("      python query_seo.py 000876")
        return
    
    keyword = sys.argv[1]
    days = int(sys.argv[2]) if len(sys.argv) > 2 else 90
    query_stock_seo(keyword, days)


if __name__ == "__main__":
    main()
