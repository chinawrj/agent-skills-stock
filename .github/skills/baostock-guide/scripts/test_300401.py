#!/usr/bin/env python3
"""测试花园生物(300401)是否符合庄股筛选条件"""

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta

code = '300401'
end_date = datetime.now()
start_date = end_date - timedelta(days=730)  # 2年

print('获取周线数据...')
df_w = ak.stock_zh_a_hist(
    symbol=code, 
    period='weekly', 
    start_date=start_date.strftime('%Y%m%d'), 
    end_date=end_date.strftime('%Y%m%d'), 
    adjust='qfq'
)
# 重命名列（akshare返回中文列名）
df_w = df_w.rename(columns={'日期': 'date', '开盘': 'open', '收盘': 'close', '最高': 'high', '最低': 'low', '成交量': 'volume', '换手率': 'turnover'})
df_w['date'] = pd.to_datetime(df_w['date'])
print(f'周线数据: {len(df_w)}行')

print('获取日线数据...')
df_d = ak.stock_zh_a_hist(
    symbol=code, 
    period='daily', 
    start_date=start_date.strftime('%Y%m%d'),  # 也用2年数据
    end_date=end_date.strftime('%Y%m%d'), 
    adjust='qfq'
)
df_d = df_d.rename(columns={'日期': 'date', '开盘': 'open', '收盘': 'close', '最高': 'high', '最低': 'low', '成交量': 'volume', '换手率': 'turnover'})
df_d['date'] = pd.to_datetime(df_d['date'])
print(f'日线数据: {len(df_d)}行')

# 计算日线量比
df_d['vol_ma20'] = df_d['volume'].rolling(window=20).mean()
df_d['vol_ratio'] = df_d['volume'] / df_d['vol_ma20']

# 找阶段性高点（周线上比前后3周的high都高）
peaks = []
for i in range(3, len(df_w) - 3):
    current_high = df_w.iloc[i]['high']
    is_peak = True
    for j in range(i-3, i+4):
        if j != i and df_w.iloc[j]['high'] >= current_high:
            is_peak = False
            break
    
    if is_peak:
        peak_date = df_w.iloc[i]['date']
        start = peak_date - timedelta(days=7)
        end = peak_date + timedelta(days=7)
        nearby_daily = df_d[(df_d['date'] >= start) & (df_d['date'] <= end)]
        surge_count = (nearby_daily['vol_ratio'] >= 1.5).sum() if len(nearby_daily) > 0 else 0
        max_vol = nearby_daily['vol_ratio'].max() if len(nearby_daily) > 0 else 0
        
        if surge_count >= 2:
            peaks.append({
                'week_idx': i,
                'date': peak_date,
                'price': current_high,
                'surge_days': surge_count,
                'max_vol': round(max_vol, 2) if pd.notna(max_vol) else 0
            })

print(f'\n找到 {len(peaks)} 个放量高点:')
for p in peaks:
    print(f"  {p['date'].strftime('%Y-%m-%d')} 价格:{p['price']:.2f} 放量{p['surge_days']}天 最大量比:{p['max_vol']}")

# 计算间隔
if len(peaks) >= 2:
    intervals = []
    for i in range(1, len(peaks)):
        interval = peaks[i]['week_idx'] - peaks[i-1]['week_idx']
        intervals.append(interval)
        status = '✓ 合理' if 8 <= interval <= 22 else '✗ 不合理'
        print(f"  间隔{i}: {intervals[-1]}周 {status}")
    
    valid = sum(1 for x in intervals if 8 <= x <= 22)
    avg = sum(intervals) / len(intervals)
    print(f'\n合理间隔数: {valid}, 平均间隔: {avg:.1f}周')
    
    # 严格条件检验
    is_zhuang = len(peaks) >= 3 and valid >= 2 and 8 <= avg <= 22
    print(f'符合庄股模式: {is_zhuang}')
    print(f'  - 放量高点数>=3: {len(peaks) >= 3} ({len(peaks)}个)')
    print(f'  - 合理间隔数>=2: {valid >= 2} ({valid}个)')
    print(f'  - 8<=平均间隔<=22: {8 <= avg <= 22} ({avg:.1f}周)')
else:
    print('高点数不足，无法计算间隔')
