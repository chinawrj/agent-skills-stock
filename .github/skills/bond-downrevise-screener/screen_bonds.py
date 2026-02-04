#!/usr/bin/env python3
"""
可转债下修策略筛选工具

筛选逻辑：
1. 仅筛选当前存续的转债（排除已退市）
2. 近2年有下修历史
3. 正股盈利 + 有营收（>5亿）
4. 转债规模/净利比值大（偿债压力大）
5. 按最新价格排序（140元以下更有机会）

核心假设：
有实际业务 + 略有盈利 + 转债偿债压力大 + 有下修历史 → 大概率继续下修促转股
"""

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import time
import warnings
warnings.filterwarnings('ignore')


def get_active_bonds():
    """获取当前存续的可转债列表（关键：排除已退市）"""
    print("=" * 80)
    print("步骤1: 获取当前存续的可转债列表")
    print("=" * 80)
    
    # 使用 bond_zh_cov 获取转债列表（包含实时价格、转股价值等）
    print("  正在获取转债列表...")
    df_cov = ak.bond_zh_cov()
    df_cov['债券代码'] = df_cov['债券代码'].astype(str)
    
    # 筛选已上市且有现价的存续转债
    df_active = df_cov[
        (df_cov['上市时间'].notna()) & 
        (df_cov['债现价'].notna()) &
        (df_cov['债现价'] > 0)
    ].copy()
    
    # 重命名列以保持兼容
    df_active = df_active.rename(columns={
        '债现价': '现价',
        '转股价': '转股价',
        '转股溢价率': '转股溢价率'
    })
    
    # 计算强赎进度（强赎触发条件：转股价值>=130，进度100%=触发）
    if '转股价值' in df_active.columns:
        df_active['强赎进度(%)'] = (df_active['转股价值'] / 130 * 100).round(1)
    
    print(f"  已上市存续转债: {len(df_active)} 只")
    
    # 获取到期时间信息（添加延迟避免频率限制）
    time.sleep(1)
    print("  正在获取到期时间信息...")
    df_ths = ak.bond_zh_cov_info_ths()
    df_ths['债券代码'] = df_ths['债券代码'].astype(str)
    
    # 合并到期时间
    df_active = df_active.merge(
        df_ths[['债券代码', '到期时间']],
        on='债券代码',
        how='left'
    )
    
    print(f"  最终存续转债: {len(df_active)} 只")
    
    return df_active


def get_bond_downrevise_history(bond_code: str) -> pd.DataFrame:
    """获取单只转债的下修记录"""
    try:
        df = ak.bond_cb_adj_logs_jsl(symbol=bond_code)
        return df
    except:
        return pd.DataFrame()


def get_recent_downrevise_bonds(active_bonds: pd.DataFrame, years: int = 2):
    """查询近N年有下修记录的存续转债"""
    print("\n" + "=" * 80)
    print(f"步骤2: 查询近{years}年有下修记录的转债")
    print("=" * 80)
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=years * 365)
    print(f"  查询范围: {start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")
    
    results = []
    total = len(active_bonds)
    
    for idx, row in active_bonds.iterrows():
        bond_code = str(row['债券代码'])
        bond_name = row['债券简称']
        
        # 每50个进度显示一次
        if (idx + 1) % 50 == 0:
            print(f"  进度: {idx + 1}/{total}")
        
        history = get_bond_downrevise_history(bond_code)
        
        if history.empty:
            continue
        
        if '股东大会日' in history.columns:
            history['股东大会日'] = pd.to_datetime(history['股东大会日'])
            recent = history[history['股东大会日'] >= start_date]
            
            if not recent.empty:
                for _, rev_row in recent.iterrows():
                    results.append({
                        '转债代码': bond_code,
                        '转债名称': bond_name,
                        '正股代码': row.get('正股代码', ''),
                        '正股简称': row.get('正股简称', ''),
                        '股东大会日': rev_row['股东大会日'].strftime('%Y-%m-%d'),
                        '下修前转股价': rev_row.get('下修前转股价', ''),
                        '下修后转股价': rev_row.get('下修后转股价', ''),
                        '当前转股价': row.get('转股价', ''),
                        '最新价格': row.get('现价', ''),
                        '转股价值': row.get('转股价值', ''),
                        '转股溢价率': row.get('转股溢价率', ''),
                        '强赎进度(%)': row.get('强赎进度(%)', ''),
                        '发行规模(亿)': row.get('发行规模', ''),
                        '到期时间': row.get('到期时间', ''),
                    })
        
        # 增加请求间隔到0.2秒，降低频率
        time.sleep(0.2)
    
    print(f"\n  找到 {len(results)} 条下修记录")
    
    if not results:
        return pd.DataFrame()
    
    result_df = pd.DataFrame(results)
    return result_df


def get_stock_financial_data():
    """获取正股财务数据"""
    print("\n" + "=" * 80)
    print("步骤3: 获取正股财务数据")
    print("=" * 80)
    
    dates = ['20250930', '20250630', '20250331', '20241231', '20240930']
    
    for date in dates:
        try:
            df = ak.stock_yjbb_em(date=date)
            if len(df) > 1000:
                print(f"  获取到 {date[:4]}年{date[4:6]}月 业绩报表，共 {len(df)} 条")
                return df, date
            time.sleep(1)  # 添加延迟
        except:
            time.sleep(1)
            continue
    
    return None, None


def enrich_with_financial_data(df_bonds: pd.DataFrame, df_profit: pd.DataFrame):
    """补充财务数据"""
    if df_profit is None:
        return df_bonds
    
    df_profit['股票代码'] = df_profit['股票代码'].astype(str).str.zfill(6)
    
    # 创建映射
    profit_map = {}
    revenue_map = {}
    
    for _, row in df_profit.iterrows():
        code = row['股票代码']
        profit_map[code] = row['净利润-净利润']
        revenue_map[code] = row['营业总收入-营业总收入']
    
    def get_profit(stock_code):
        if pd.isna(stock_code):
            return None
        code = str(stock_code).zfill(6)
        value = profit_map.get(code)
        if value is not None:
            return round(value / 100000000, 2)
        return None
    
    def get_revenue(stock_code):
        if pd.isna(stock_code):
            return None
        code = str(stock_code).zfill(6)
        value = revenue_map.get(code)
        if value is not None:
            return round(value / 100000000, 2)
        return None
    
    df_bonds['正股净利润(亿)'] = df_bonds['正股代码'].apply(get_profit)
    df_bonds['正股营收(亿)'] = df_bonds['正股代码'].apply(get_revenue)
    
    # 计算盈利状态
    def get_status(profit):
        if pd.isna(profit):
            return '未知'
        return '盈利' if profit > 0 else '亏损'
    
    df_bonds['正股盈利状态'] = df_bonds['正股净利润(亿)'].apply(get_status)
    
    # 计算规模/净利比值
    def calc_ratio(row):
        scale = row['发行规模(亿)']
        profit = row['正股净利润(亿)']
        if pd.notna(scale) and pd.notna(profit) and profit != 0:
            return round(scale / profit * 100, 2)
        return None
    
    df_bonds['规模/净利(%)'] = df_bonds.apply(calc_ratio, axis=1)
    
    # 计算剩余年限
    def calc_remaining_years(expire_date):
        if pd.isna(expire_date):
            return None
        try:
            from datetime import datetime, date
            # 转为date对象
            if isinstance(expire_date, datetime):
                expire = expire_date.date()
            elif isinstance(expire_date, date):
                expire = expire_date
            else:
                expire = pd.to_datetime(expire_date).date()
            today = date.today()
            days = (expire - today).days
            return round(days / 365, 2)
        except:
            return None
    
    df_bonds['剩余年限'] = df_bonds['到期时间'].apply(calc_remaining_years)
    
    return df_bonds


def screen_bonds(df_bonds: pd.DataFrame):
    """筛选符合条件的转债"""
    print("\n" + "=" * 80)
    print("步骤4: 筛选符合策略条件的转债")
    print("=" * 80)
    
    # 去重，保留每只转债最新的下修记录
    df_unique = df_bonds.drop_duplicates(subset=['转债代码'], keep='first')
    
    # 统计下修次数
    revise_count = df_bonds.groupby('转债代码').size().reset_index(name='下修次数')
    df_unique = df_unique.merge(revise_count, on='转债代码')
    
    print(f"  存续且有下修历史的转债: {len(df_unique)} 只")
    
    # 筛选条件1：多次下修 + 盈利 + 有营收 + 规模压力大
    cond1 = (
        (df_unique['正股盈利状态'] == '盈利') &
        (df_unique['正股营收(亿)'] >= 5) &
        (df_unique['规模/净利(%)'] >= 300) &
        (df_unique['下修次数'] >= 2)
    )
    
    # 筛选条件2：单次下修 + 盈利好 + 规模压力适中
    cond2 = (
        (df_unique['正股盈利状态'] == '盈利') &
        (df_unique['正股净利润(亿)'] >= 1) &
        (df_unique['规模/净利(%)'] >= 500) &
        (df_unique['规模/净利(%)'] <= 1500) &
        (df_unique['下修次数'] == 1)
    )
    
    result_profit = df_unique[cond1 | cond2].copy()
    result_profit = result_profit.sort_values('最新价格', ascending=True)
    
    print(f"  符合盈利筛选条件的转债: {len(result_profit)} 只")
    
    # 筛选条件3：亏损但营收大（>10亿）+ 有下修历史
    cond_loss = (
        (df_unique['正股盈利状态'] == '亏损') &
        (df_unique['正股营收(亿)'] >= 10)
    )
    
    result_loss = df_unique[cond_loss].copy()
    result_loss = result_loss.sort_values('最新价格', ascending=True)
    
    print(f"  符合亏损筛选条件的转债: {len(result_loss)} 只")
    
    return result_profit, result_loss


def display_results(df_profit: pd.DataFrame, df_loss: pd.DataFrame = None):
    """展示筛选结果"""
    print("\n" + "=" * 80)
    print("【盈利组筛选结果】按最新价格排序（140元以下更有机会）")
    print("筛选逻辑: 存续 + 有营收(>5亿) + 盈利 + 规模/净利压力大 + 有下修历史")
    print("=" * 80)
    
    df = df_profit
    cols = ['转债名称', '最新价格', '发行规模(亿)', '剩余年限', '下修次数', '正股营收(亿)', '正股净利润(亿)', 
            '规模/净利(%)', '转股溢价率', '强赎进度(%)', '股东大会日']
    
    def show_group(title, data):
        print(f"\n{'-' * 80}")
        print(title)
        print(f"{'-' * 80}")
        if len(data) > 0:
            print(data[cols].to_string(index=False))
        else:
            print("无")
    
    # 按价格分组
    show_group("【强烈关注】价格 < 110元（低价+下修历史=安全边际高）",
               df[df['最新价格'] < 110])
    
    show_group("【重点关注】110元 <= 价格 < 120元",
               df[(df['最新价格'] >= 110) & (df['最新价格'] < 120)])
    
    show_group("【次重点关注】120元 <= 价格 < 130元",
               df[(df['最新价格'] >= 120) & (df['最新价格'] < 130)])
    
    show_group("【一般关注】130元 <= 价格 < 140元",
               df[(df['最新价格'] >= 130) & (df['最新价格'] < 140)])
    
    show_group("【边缘关注】140元 <= 价格 < 150元",
               df[(df['最新价格'] >= 140) & (df['最新价格'] < 150)])
    
    show_group("【观望】价格 >= 150元（价格偏高，下修收益空间有限）",
               df[df['最新价格'] >= 150])
    
    # 盈利组统计
    print(f"\n{'=' * 80}")
    print("【盈利组统计汇总】")
    print(f"{'=' * 80}")
    print(f"  符合条件的转债总数: {len(df)} 只")
    print(f"  价格 < 140元: {len(df[df['最新价格'] < 140])} 只")
    print(f"  多次下修(>=2次): {len(df[df['下修次数'] >= 2])} 只")
    
    # 亏损组展示
    if df_loss is not None and len(df_loss) > 0:
        print(f"\n{'=' * 80}")
        print("【亏损组筛选结果】营收>10亿的亏损转债（困境反转潜力）")
        print("筛选逻辑: 存续 + 亏损 + 营收>10亿 + 有下修历史")
        print(f"{'=' * 80}")
        
        cols_loss = ['转债名称', '最新价格', '发行规模(亿)', '剩余年限', '下修次数', '正股营收(亿)', '正股净利润(亿)', 
                     '转股溢价率', '强赎进度(%)', '股东大会日']
        
        show_group("【低价亏损】价格 < 120元（困境反转+下修双击）",
                   df_loss[df_loss['最新价格'] < 120])
        
        show_group("【中价亏损】120元 <= 价格 < 140元",
                   df_loss[(df_loss['最新价格'] >= 120) & (df_loss['最新价格'] < 140)])
        
        show_group("【高价亏损】价格 >= 140元",
                   df_loss[df_loss['最新价格'] >= 140])
        
        print(f"\n{'=' * 80}")
        print("【亏损组统计汇总】")
        print(f"{'=' * 80}")
        print(f"  亏损组转债总数: {len(df_loss)} 只")
        print(f"  价格 < 140元: {len(df_loss[df_loss['最新价格'] < 140])} 只")


def main():
    print("=" * 80)
    print("可转债下修策略筛选工具")
    print(f"运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # 1. 获取存续转债列表（关键：排除已退市）
    active_bonds = get_active_bonds()
    
    # 2. 查询近3年下修记录
    df_downrevise = get_recent_downrevise_bonds(active_bonds, years=3)
    
    if df_downrevise.empty:
        print("\n未找到符合条件的转债")
        return
    
    # 3. 获取财务数据
    df_profit, report_date = get_stock_financial_data()
    
    # 4. 补充财务数据
    print("\n" + "=" * 80)
    print("步骤4: 补充财务数据")
    print("=" * 80)
    df_enriched = enrich_with_financial_data(df_downrevise, df_profit)
    
    # 5. 筛选
    df_profit, df_loss = screen_bonds(df_enriched)
    
    # 6. 展示结果
    display_results(df_profit, df_loss)
    
    # 7. 保存结果到项目根目录
    import os
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    
    output_file = os.path.join(project_root, 'screened_downrevise_bonds.csv')
    df_profit.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n盈利组结果已保存到: {output_file}")
    
    if len(df_loss) > 0:
        loss_file = os.path.join(project_root, 'screened_loss_bonds.csv')
        df_loss.to_csv(loss_file, index=False, encoding='utf-8-sig')
        print(f"亏损组结果已保存到: {loss_file}")
    
    return df_profit, df_loss


if __name__ == "__main__":
    main()
