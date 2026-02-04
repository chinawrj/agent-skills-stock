#!/usr/bin/env python3
"""
定增预案筛选工具

功能：
1. 获取最近N天内公告定增预案的公司
2. 筛选同时有可转债的公司
3. 分析可能存在"定增压价+转债下修"策略的标的

数据源：AKShare (东方财富融资公告)
"""

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import os
import warnings
warnings.filterwarnings('ignore')


def get_seo_plans(days: int = 30) -> pd.DataFrame:
    """获取最近N天的定增预案公告"""
    print("=" * 80)
    print(f"步骤1: 获取最近{days}天的融资公告")
    print("=" * 80)
    
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
        print("  未获取到融资公告数据")
        return pd.DataFrame()
    
    df_all = pd.concat(all_notices, ignore_index=True)
    print(f"  共获取 {len(df_all)} 条融资公告")
    
    # 筛选定增相关公告
    seo_keywords = ['定增', '定向增发', '非公开发行', '向特定对象']
    df_seo = df_all[df_all['公告标题'].str.contains('|'.join(seo_keywords), na=False)]
    print(f"  定增相关公告: {len(df_seo)} 条")
    
    # 筛选预案阶段
    plan_keywords = ['预案', '草案', '方案', '议案', '计划', '拟']
    df_plan = df_seo[df_seo['公告标题'].str.contains('|'.join(plan_keywords), na=False)]
    
    # 去重，保留每家公司最新的公告
    df_plan = df_plan.drop_duplicates(subset=['代码'], keep='first')
    df_plan = df_plan.rename(columns={'代码': '股票代码', '名称': '股票名称'})
    
    print(f"  定增预案公司（去重后）: {len(df_plan)} 家")
    
    return df_plan


def get_active_bonds() -> pd.DataFrame:
    """获取当前存续的可转债列表"""
    print("\n" + "=" * 80)
    print("步骤2: 获取存续可转债列表")
    print("=" * 80)
    
    try:
        df = ak.bond_cov_comparison()
        df = df.rename(columns={
            '转债代码': '债券代码',
            '转债名称': '债券简称',
            '转债最新价': '转债价格',
            '正股代码': '正股代码',
            '正股名称': '正股简称',
            '正股最新价': '正股价格'
        })
        
        # 计算下修触发价（转股价×80%）
        if '转股价' in df.columns:
            df['下修触发价'] = (df['转股价'] * 0.8).round(2)
            df['距下修(%)'] = ((df['正股价格'] - df['下修触发价']) / df['下修触发价'] * 100).round(2)
        
        print(f"  存续可转债: {len(df)} 只")
        return df
    except Exception as e:
        print(f"  获取可转债失败: {e}")
        return pd.DataFrame()


def match_seo_with_bonds(df_seo: pd.DataFrame, df_bonds: pd.DataFrame) -> pd.DataFrame:
    """匹配定增公司与可转债"""
    print("\n" + "=" * 80)
    print("步骤3: 匹配定增公司与可转债")
    print("=" * 80)
    
    if df_bonds.empty:
        df_seo['有可转债'] = '否'
        return df_seo
    
    # 标准化股票代码
    df_seo['股票代码'] = df_seo['股票代码'].astype(str).str.zfill(6)
    df_bonds['正股代码'] = df_bonds['正股代码'].astype(str).str.zfill(6)
    
    # 创建正股代码到转债信息的映射
    bond_map = {}
    for _, row in df_bonds.iterrows():
        code = row['正股代码']
        if code not in bond_map:
            bond_map[code] = []
        bond_map[code].append({
            '债券简称': row.get('债券简称', ''),
            '转债价格': row.get('转债价格', ''),
            '转股价': row.get('转股价', ''),
            '正股价格': row.get('正股价格', ''),
            '下修触发价': row.get('下修触发价', ''),
            '距下修(%)': row.get('距下修(%)', ''),
            '转股溢价率': row.get('转股溢价率', '')
        })
    
    # 匹配
    def get_bond_info(stock_code):
        bonds = bond_map.get(stock_code, [])
        if bonds:
            return bonds[0]  # 取第一只转债
        return {}
    
    df_seo['有可转债'] = df_seo['股票代码'].apply(lambda x: '是' if x in bond_map else '否')
    df_seo['转债名称'] = df_seo['股票代码'].apply(lambda x: get_bond_info(x).get('债券简称', ''))
    df_seo['转债价格'] = df_seo['股票代码'].apply(lambda x: get_bond_info(x).get('转债价格', ''))
    df_seo['转股价'] = df_seo['股票代码'].apply(lambda x: get_bond_info(x).get('转股价', ''))
    df_seo['正股价格'] = df_seo['股票代码'].apply(lambda x: get_bond_info(x).get('正股价格', ''))
    df_seo['下修触发价'] = df_seo['股票代码'].apply(lambda x: get_bond_info(x).get('下修触发价', ''))
    df_seo['距下修(%)'] = df_seo['股票代码'].apply(lambda x: get_bond_info(x).get('距下修(%)', ''))
    
    with_bonds = len(df_seo[df_seo['有可转债'] == '是'])
    print(f"  有可转债的定增公司: {with_bonds} 家")
    
    return df_seo


def display_results(df: pd.DataFrame):
    """展示筛选结果"""
    print("\n" + "=" * 80)
    print("【定增预案筛选结果】")
    print("=" * 80)
    
    # 分组展示
    df_with_bonds = df[df['有可转债'] == '是'].copy()
    df_without_bonds = df[df['有可转债'] == '否'].copy()
    
    # 有可转债的公司（重点关注）
    print("\n" + "-" * 80)
    print("【重点关注】同时有定增预案和可转债的公司")
    print("投资逻辑: 可能存在'定增压价 → 触发下修 → 强赎促转股'策略")
    print("-" * 80)
    
    if len(df_with_bonds) > 0:
        # 按距下修百分比排序
        df_with_bonds['距下修(%)'] = pd.to_numeric(df_with_bonds['距下修(%)'], errors='coerce')
        df_with_bonds = df_with_bonds.sort_values('距下修(%)', ascending=True)
        
        cols = ['股票代码', '股票名称', '转债名称', '正股价格', '转股价', '下修触发价', '距下修(%)', '公告日期']
        print(df_with_bonds[cols].to_string(index=False))
        
        # 标注接近下修的
        near_downrevise = df_with_bonds[df_with_bonds['距下修(%)'] <= 10]
        if len(near_downrevise) > 0:
            print(f"\n⚠️  距下修触发价 ≤10% 的公司: {len(near_downrevise)} 家")
            for _, row in near_downrevise.iterrows():
                print(f"    {row['股票名称']}({row['股票代码']}): 正股{row['正股价格']}元, 下修触发价{row['下修触发价']}元, 距下修{row['距下修(%)']}%")
    else:
        print("无")
    
    # 无可转债的定增公司
    print("\n" + "-" * 80)
    print("【一般关注】仅有定增预案（无可转债）")
    print("-" * 80)
    
    if len(df_without_bonds) > 0:
        cols = ['股票代码', '股票名称', '公告标题', '公告日期']
        # 截断公告标题
        df_without_bonds['公告标题'] = df_without_bonds['公告标题'].str[:40] + '...'
        print(df_without_bonds[cols].head(20).to_string(index=False))
        if len(df_without_bonds) > 20:
            print(f"  ... 共 {len(df_without_bonds)} 家，仅显示前20家")
    else:
        print("无")
    
    # 统计
    print("\n" + "=" * 80)
    print("【统计汇总】")
    print("=" * 80)
    print(f"  定增预案公司总数: {len(df)} 家")
    print(f"  有可转债的公司: {len(df_with_bonds)} 家")
    print(f"  无可转债的公司: {len(df_without_bonds)} 家")


def main(days: int = 30):
    """主函数"""
    print("=" * 80)
    print("定增预案筛选工具")
    print(f"运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"筛选范围: 最近 {days} 天的融资公告")
    print("=" * 80)
    
    # 1. 获取定增预案
    df_seo = get_seo_plans(days)
    if df_seo.empty:
        print("\n未找到定增预案公告")
        return
    
    # 2. 获取存续转债
    df_bonds = get_active_bonds()
    
    # 3. 匹配
    df_result = match_seo_with_bonds(df_seo, df_bonds)
    
    # 4. 展示
    display_results(df_result)
    
    # 5. 保存结果
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    
    # 保存全部结果
    output_file = os.path.join(project_root, 'screened_seo_plans.csv')
    df_result.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n定增预案结果已保存到: {output_file}")
    
    # 保存有可转债的
    df_with_bonds = df_result[df_result['有可转债'] == '是']
    if len(df_with_bonds) > 0:
        bonds_file = os.path.join(project_root, 'screened_seo_with_bonds.csv')
        df_with_bonds.to_csv(bonds_file, index=False, encoding='utf-8-sig')
        print(f"有转债的定增公司已保存到: {bonds_file}")
    
    return df_result


if __name__ == "__main__":
    import sys
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    main(days)
