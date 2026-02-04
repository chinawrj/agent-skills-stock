#!/usr/bin/env python3
"""
可转债强赎博弈筛选脚本
筛选正股价在强赎触发价附近（90%-105%）的转债
"""

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import time
import os


def parse_redeem_count(s):
    """解析强赎天计数，如 '12/15 | 30' -> (12, 15, 30)"""
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


CACHE_DIR = os.path.join(os.path.dirname(__file__), '.cache')
FINANCIAL_CACHE_FILE = os.path.join(CACHE_DIR, 'financial_data.csv')
DOWNREVISE_CACHE_FILE = os.path.join(CACHE_DIR, 'downrevise_history.csv')


def get_stock_financial_data():
    """获取正股财务数据（净利润），带本地缓存"""
    # 检查缓存是否存在且是今天的
    if os.path.exists(FINANCIAL_CACHE_FILE):
        cache_mtime = datetime.fromtimestamp(os.path.getmtime(FINANCIAL_CACHE_FILE))
        if cache_mtime.date() == datetime.now().date():
            print(f"  使用本地缓存的财务数据")
            return pd.read_csv(FINANCIAL_CACHE_FILE, dtype={'股票代码': str})
    
    # 缓存不存在或已过期，重新获取
    dates = ['20250930', '20250630', '20250331', '20241231', '20240930']
    
    for date in dates:
        try:
            df = ak.stock_yjbb_em(date=date)
            if len(df) > 1000:
                print(f"  获取到 {date[:4]}年{date[4:6]}月 业绩报表")
                # 保存缓存
                os.makedirs(CACHE_DIR, exist_ok=True)
                df.to_csv(FINANCIAL_CACHE_FILE, index=False)
                return df
            time.sleep(0.5)
        except:
            time.sleep(0.5)
            continue
    
    return None


def get_downrevise_history(bond_codes):
    """
    获取转债下修历史，带本地缓存
    返回字典：{转债代码: 下修次数}
    """
    # 检查缓存是否存在且是今天的
    if os.path.exists(DOWNREVISE_CACHE_FILE):
        cache_mtime = datetime.fromtimestamp(os.path.getmtime(DOWNREVISE_CACHE_FILE))
        if cache_mtime.date() == datetime.now().date():
            print(f"  使用本地缓存的下修历史")
            df = pd.read_csv(DOWNREVISE_CACHE_FILE, dtype={'代码': str})
            return dict(zip(df['代码'], df['下修次数']))
    
    # 缓存不存在或已过期，重新获取
    print(f"  查询下修历史（共{len(bond_codes)}只，约需1-2分钟）...")
    
    # 近3年
    end_date = datetime.now()
    start_date = end_date - timedelta(days=3 * 365)
    
    result = {}
    for i, code in enumerate(bond_codes):
        if (i + 1) % 50 == 0:
            print(f"    进度: {i+1}/{len(bond_codes)}")
        try:
            df = ak.bond_cb_adj_logs_jsl(symbol=str(code))
            if not df.empty and '股东大会日' in df.columns:
                df['股东大会日'] = pd.to_datetime(df['股东大会日'])
                recent = df[df['股东大会日'] >= start_date]
                result[str(code)] = len(recent)
            else:
                result[str(code)] = 0
            time.sleep(0.1)
        except:
            result[str(code)] = 0
    
    # 保存缓存
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_df = pd.DataFrame({'代码': list(result.keys()), '下修次数': list(result.values())})
    cache_df.to_csv(DOWNREVISE_CACHE_FILE, index=False)
    
    return result


def calc_half_distance_score(current, required):
    """
    计算距离半程的得分
    参考值 = 需要天数/2，距离参考值越近得分越高
    得分范围: 0-100
    """
    if pd.isna(current) or pd.isna(required) or required <= 0:
        return 0
    half = required / 2
    distance = abs(current - half)
    max_distance = required / 2  # 最大距离就是半程本身
    score = max(0, 100 - (distance / max_distance * 100))
    return round(score, 1)


def calc_potential_profit_score(price_ratio):
    """
    计算潜在收益得分
    强赎价格比 < 100% 时有上涨空间
    得分范围: 0-100
    """
    if pd.isna(price_ratio):
        return 0
    if price_ratio >= 100:
        return 0  # 已在强赎价上方，无潜在收益
    elif price_ratio >= 95:
        # 95%-100%: 每1%差距得20分，最高100分
        return min(100, (100 - price_ratio) * 20)
    else:
        # 90%-95%: 每1%差距得10分（风险较大，收益递减）
        return min(100, (100 - price_ratio) * 10)


def calc_composite_score(half_score, profit_score, profit_weight=0.67):
    """
    计算综合得分
    默认潜在收益权重是半程博弈的2倍 (0.67 vs 0.33)
    
    参数:
        half_score: 半程博弈得分
        profit_score: 潜在收益得分
        profit_weight: 潜在收益权重，默认0.67（2倍于半程得分的0.33）
    """
    half_weight = 1 - profit_weight
    return round(half_score * half_weight + profit_score * profit_weight, 1)


def screen_redemption_bonds(min_ratio=90, max_ratio=105, profit_weight=0.67):
    """
    筛选强赎博弈标的
    
    参数:
        min_ratio: 最小强赎价格比（%）
        max_ratio: 最大强赎价格比（%）
        profit_weight: 潜在收益权重，默认0.75（3倍于半程得分）
    """
    print("=" * 80)
    print("可转债强赎博弈筛选工具")
    print(f"运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # 获取强赎数据
    print("\n正在获取强赎数据...")
    df = ak.bond_cb_redeem_jsl()
    print(f"  获取到 {len(df)} 只转债")
    
    # 获取财务数据
    print("正在获取财务数据...")
    df_profit = get_stock_financial_data()
    
    # 构建净利润映射
    profit_map = {}
    if df_profit is not None:
        df_profit['股票代码'] = df_profit['股票代码'].astype(str).str.zfill(6)
        for _, row in df_profit.iterrows():
            code = row['股票代码']
            profit_map[code] = row['净利润-净利润'] / 100000000  # 转为亿
    
    # 解析强赎天计数
    df[['已满足天数', '需要天数', '观察期']] = df['强赎天计数'].apply(
        lambda x: pd.Series(parse_redeem_count(x))
    )
    
    # 数值转换
    df['正股价'] = pd.to_numeric(df['正股价'], errors='coerce')
    df['强赎触发价'] = pd.to_numeric(df['强赎触发价'], errors='coerce')
    df['现价'] = pd.to_numeric(df['现价'], errors='coerce')
    df['剩余规模'] = pd.to_numeric(df['剩余规模'], errors='coerce')
    df['正股代码'] = df['正股代码'].astype(str).str.zfill(6)
    
    # 计算剩余年限
    df['到期日'] = pd.to_datetime(df['到期日'], errors='coerce')
    df['剩余年限'] = df['到期日'].apply(
        lambda x: round((x - datetime.now()).days / 365, 2) if pd.notna(x) else None
    )
    
    # 获取净利润
    df['正股净利(亿)'] = df['正股代码'].apply(lambda x: profit_map.get(x))
    
    # 获取下修历史
    print("正在获取下修历史...")
    bond_codes = df['代码'].tolist()
    downrevise_map = get_downrevise_history(bond_codes)
    df['下修次数'] = df['代码'].apply(lambda x: downrevise_map.get(str(x), 0))
    df['有下修'] = df['下修次数'].apply(lambda x: '是' if x > 0 else '否')
    
    # 计算规模/净利
    def calc_scale_profit_ratio(row):
        if pd.isna(row['剩余规模']) or pd.isna(row['正股净利(亿)']) or row['正股净利(亿)'] <= 0:
            return None
        return round(row['剩余规模'] / row['正股净利(亿)'] * 100, 1)
    
    df['规模/净利(%)'] = df.apply(calc_scale_profit_ratio, axis=1)
    
    # 计算强赎价格比
    df['强赎价格比'] = (df['正股价'] / df['强赎触发价'] * 100).round(1)
    
    # 计算半程距离得分
    df['半程得分'] = df.apply(
        lambda row: calc_half_distance_score(row['已满足天数'], row['需要天数']), 
        axis=1
    )
    
    # 计算潜在收益得分
    df['收益得分'] = df['强赎价格比'].apply(calc_potential_profit_score)
    
    # 计算综合得分
    df['综合得分'] = df.apply(
        lambda row: calc_composite_score(row['半程得分'], row['收益得分'], profit_weight),
        axis=1
    )
    
    # 筛选条件：强赎价格比在指定区间 + 排除已公告强赎
    mask = (
        df['强赎价格比'].notna() &
        (df['强赎价格比'] >= min_ratio) &
        (df['强赎价格比'] <= max_ratio) &
        (~df['强赎状态'].str.contains('已公告|已满足', na=False))
    )
    
    result = df[mask][[
        '代码', '名称', '现价', '正股名称', '正股价', '强赎触发价', 
        '强赎价格比', '强赎天计数', '已满足天数', '需要天数', 
        '半程得分', '收益得分', '综合得分', '剩余规模', '剩余年限', '正股净利(亿)', '规模/净利(%)', '有下修', '下修次数', '强赎状态'
    ]].copy()
    
    # 按综合得分排序
    result = result.sort_values('综合得分', ascending=False)
    
    print(f"\n筛选条件: 强赎价格比 {min_ratio}%-{max_ratio}%，排除已公告强赎")
    print(f"评分权重: 潜在收益={profit_weight:.0%}, 半程博弈={1-profit_weight:.0%}")
    print(f"符合条件: {len(result)} 只")
    
    # 分档输出 - 按综合得分分组
    print("\n" + "=" * 80)
    
    # 高分组 (>=60)
    group1 = result[result['综合得分'] >= 60]
    if len(group1) > 0:
        print("【⭐ 最佳博弈】综合得分 >= 60")
        print("-" * 80)
        print(group1[['名称', '现价', '强赎价格比', '强赎天计数', '综合得分', '剩余规模', '剩余年限', '规模/净利(%)', '有下修']].to_string(index=False))
        print()
    
    # 中分组 (30-60)
    group2 = result[(result['综合得分'] >= 30) & (result['综合得分'] < 60)]
    if len(group2) > 0:
        print("【🟡 可关注】综合得分 30-60")
        print("-" * 80)
        print(group2[['名称', '现价', '强赎价格比', '强赎天计数', '综合得分', '剩余规模', '剩余年限', '规模/净利(%)', '有下修']].to_string(index=False))
        print()
    
    # 低分组 (<30)
    group3 = result[result['综合得分'] < 30]
    if len(group3) > 0:
        print("【⚪ 观望】综合得分 < 30")
        print("-" * 80)
        print(group3[['名称', '现价', '强赎价格比', '强赎天计数', '综合得分', '剩余规模', '剩余年限', '规模/净利(%)', '有下修']].to_string(index=False))
        print()
    
    # 保存结果
    output_file = 'screened_redemption_bonds.csv'
    result.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n结果已保存到: {output_file}")
    
    return result


if __name__ == '__main__':
    screen_redemption_bonds()
