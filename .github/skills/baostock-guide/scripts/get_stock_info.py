#!/usr/bin/env python3
"""
股票基础信息获取工具

用于庄股筛选的数据准备，获取以下信息：
1. 股票基础信息（代码、名称、行业、上市日期）
2. 市值数据（总市值、流通市值）
3. 周线K线数据（用于计算波幅、换手率等）

支持按市值区间筛选（如30-100亿适合庄股筛选）
"""

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import time
import warnings
import os
warnings.filterwarnings('ignore')


def get_all_stocks():
    """获取所有A股股票列表"""
    print("=" * 80)
    print("步骤1: 获取A股股票列表")
    print("=" * 80)
    
    try:
        # 使用akshare获取A股列表
        df = ak.stock_zh_a_spot_em()
        df = df.rename(columns={
            '代码': '股票代码',
            '名称': '股票名称',
            '最新价': '现价',
            '总市值': '总市值',
            '流通市值': '流通市值',
            '换手率': '换手率',
            '涨跌幅': '涨跌幅',
            '成交量': '成交量',
            '成交额': '成交额',
            '市盈率-动态': 'PE(动)',
        })
        
        # 转换市值为亿元
        if '总市值' in df.columns:
            df['总市值(亿)'] = (df['总市值'] / 100000000).round(2)
        if '流通市值' in df.columns:
            df['流通市值(亿)'] = (df['流通市值'] / 100000000).round(2)
        
        # 根据市值和市盈率计算净利润（亿）= 总市值 / PE
        if 'PE(动)' in df.columns and '总市值(亿)' in df.columns:
            df['净利润(亿)'] = df.apply(
                lambda x: round(x['总市值(亿)'] / x['PE(动)'], 2) 
                if pd.notna(x['PE(动)']) and x['PE(动)'] > 0 else None, 
                axis=1
            )
        
        print(f"  获取到 {len(df)} 只股票")
        return df
    except Exception as e:
        print(f"  获取股票列表失败: {e}")
        return pd.DataFrame()


def get_stock_industry():
    """获取股票行业分类"""
    print("\n" + "=" * 80)
    print("步骤2: 获取行业分类信息")
    print("=" * 80)
    
    try:
        # 获取申万行业分类
        df = ak.stock_board_industry_name_em()
        print(f"  获取到 {len(df)} 个行业板块")
        
        # 获取每个行业的成分股
        industry_map = {}
        for idx, row in df.iterrows():
            industry_name = row['板块名称']
            try:
                stocks = ak.stock_board_industry_cons_em(symbol=industry_name)
                for _, stock in stocks.iterrows():
                    code = stock['代码']
                    industry_map[code] = industry_name
                if (idx + 1) % 20 == 0:
                    print(f"  进度: {idx + 1}/{len(df)}")
                time.sleep(0.1)
            except:
                continue
        
        print(f"  成功映射 {len(industry_map)} 只股票的行业")
        return industry_map
    except Exception as e:
        print(f"  获取行业分类失败: {e}")
        return {}


def filter_by_market_cap(df: pd.DataFrame, min_cap: float = 30, max_cap: float = 100):
    """按市值区间筛选股票"""
    print("\n" + "=" * 80)
    print(f"步骤3: 按市值筛选 ({min_cap}-{max_cap}亿)")
    print("=" * 80)
    
    if '总市值(亿)' not in df.columns:
        print("  缺少市值数据")
        return df
    
    # 排除ST股票
    df_filtered = df[~df['股票名称'].str.contains('ST|退', na=False)].copy()
    print(f"  排除ST后: {len(df_filtered)} 只")
    
    # 按市值筛选
    df_filtered = df_filtered[
        (df_filtered['总市值(亿)'] >= min_cap) & 
        (df_filtered['总市值(亿)'] <= max_cap)
    ]
    
    print(f"  市值{min_cap}-{max_cap}亿: {len(df_filtered)} 只")
    
    return df_filtered


def get_weekly_kline(stock_code: str, days: int = 365):
    """获取单只股票的周线数据"""
    try:
        # 转换股票代码格式
        if stock_code.startswith('6'):
            bs_code = f"sh.{stock_code}"
        else:
            bs_code = f"sz.{stock_code}"
        
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        # 使用akshare获取周线数据
        df = ak.stock_zh_a_hist(
            symbol=stock_code, 
            period="weekly",
            start_date=start_date.replace('-', ''),
            end_date=end_date.replace('-', ''),
            adjust="qfq"  # 前复权
        )
        
        if df.empty:
            return pd.DataFrame()
        
        df = df.rename(columns={
            '日期': 'date',
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'amount',
            '换手率': 'turn',
        })
        
        return df
    except Exception as e:
        return pd.DataFrame()


def get_daily_kline(stock_code: str, days: int = 730):
    """获取单只股票的日线数据（用于分析放量特征，默认两年，与周线数据匹配）"""
    try:
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
        
        df = ak.stock_zh_a_hist(
            symbol=stock_code, 
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq"
        )
        
        if df.empty:
            return pd.DataFrame()
        
        df = df.rename(columns={
            '日期': 'date',
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'amount',
            '换手率': 'turn',
        })
        
        return df
    except Exception as e:
        return pd.DataFrame()


def analyze_volume_surge(df_daily: pd.DataFrame):
    """
    分析日线放量特征
    
    庄股特征：高位阶段会出现持续剧烈放量（庄家出货）
    
    检测逻辑：
    1. 计算成交量的20日均量
    2. 找出成交量 >= 2倍均量的"放量日"
    3. 检测是否有连续3天以上的放量（持续剧烈放量）
    4. 检测放量时是否处于相对高位（价格 >= 近期80%分位）
    """
    if df_daily.empty or len(df_daily) < 30:
        return None
    
    try:
        df = df_daily.copy()
        
        # 计算20日均量
        df['vol_ma20'] = df['volume'].rolling(window=20).mean()
        
        # 计算量比（当日成交量 / 20日均量）
        df['vol_ratio'] = df['volume'] / df['vol_ma20']
        
        # 标记放量日（量比 >= 2）
        df['is_surge'] = df['vol_ratio'] >= 2.0
        
        # 统计放量日数量
        surge_days = df['is_surge'].sum()
        
        # 检测连续放量（连续3天以上量比>=1.5）
        df['mild_surge'] = df['vol_ratio'] >= 1.5
        df['surge_streak'] = df['mild_surge'].astype(int)
        
        # 计算最长连续放量天数
        max_streak = 0
        current_streak = 0
        for val in df['surge_streak']:
            if val == 1:
                current_streak += 1
                max_streak = max(max_streak, current_streak)
            else:
                current_streak = 0
        
        # 计算价格的80%分位（判断高位）
        price_80pct = df['close'].quantile(0.8)
        
        # 检测高位放量：放量日中有多少处于高位
        df['is_high'] = df['close'] >= price_80pct
        high_surge_days = ((df['is_surge']) & (df['is_high'])).sum()
        
        # 计算最大量比
        max_vol_ratio = df['vol_ratio'].max()
        
        # 计算平均量比（放量日）
        surge_vol_ratios = df[df['is_surge']]['vol_ratio']
        avg_surge_ratio = surge_vol_ratios.mean() if len(surge_vol_ratios) > 0 else 0
        
        return {
            'surge_days': int(surge_days),           # 放量日总数
            'max_streak': int(max_streak),           # 最长连续放量天数
            'high_surge_days': int(high_surge_days), # 高位放量日数
            'max_vol_ratio': round(max_vol_ratio, 2) if pd.notna(max_vol_ratio) else 0,  # 最大量比
            'avg_surge_ratio': round(avg_surge_ratio, 2),  # 平均放量倍数
        }
    except Exception as e:
        return None


def analyze_surge_peaks(df_weekly: pd.DataFrame, df_daily: pd.DataFrame):
    """
    检测放量阶段性高点
    
    庄股核心特征：一年内出现3次以上的放量阶段性高点，每次间隔2-5个月
    
    检测逻辑：
    1. 在周线上找阶段性高点（比前后3周都高）
    2. 检查该高点前后一周内日线是否有放量（量比>=1.5的天数>=2）
    3. 统计符合条件的放量高点数量
    4. 检查高点之间的间隔是否在8-22周（2-5个月）范围内
    """
    if df_weekly.empty or len(df_weekly) < 20 or df_daily.empty:
        return None
    
    try:
        df_w = df_weekly.copy().reset_index(drop=True)
        df_d = df_daily.copy()
        
        # 确保日期格式一致
        df_w['date'] = pd.to_datetime(df_w['date'])
        df_d['date'] = pd.to_datetime(df_d['date'])
        
        # 计算日线量比
        df_d['vol_ma20'] = df_d['volume'].rolling(window=20).mean()
        df_d['vol_ratio'] = df_d['volume'] / df_d['vol_ma20']
        
        # 找阶段性高点（周线上比前后3周的high都高）
        peaks = []
        for i in range(3, len(df_w) - 3):
            current_high = df_w.iloc[i]['high']
            # 检查是否是局部最高（前后3周）
            is_peak = True
            for j in range(i-3, i+4):
                if j != i and df_w.iloc[j]['high'] >= current_high:
                    is_peak = False
                    break
            
            if is_peak:
                peak_date = df_w.iloc[i]['date']
                peak_price = current_high
                
                # 检查该周前后5个交易日内的日线放量情况
                start_date = peak_date - timedelta(days=7)
                end_date = peak_date + timedelta(days=7)
                
                nearby_daily = df_d[(df_d['date'] >= start_date) & (df_d['date'] <= end_date)]
                
                # 统计放量天数（量比>=1.5）
                surge_count = (nearby_daily['vol_ratio'] >= 1.5).sum() if len(nearby_daily) > 0 else 0
                max_vol_ratio = nearby_daily['vol_ratio'].max() if len(nearby_daily) > 0 else 0
                
                # 如果附近有至少2天放量，认为是放量高点
                if surge_count >= 2:
                    peaks.append({
                        'week_idx': i,
                        'date': peak_date,
                        'price': peak_price,
                        'surge_days': int(surge_count),
                        'max_vol_ratio': round(max_vol_ratio, 2) if pd.notna(max_vol_ratio) else 0
                    })
        
        # 统计放量高点数量
        surge_peak_count = len(peaks)
        
        # 检查高点间隔是否合理（8-22周，约2-5个月）
        valid_intervals = 0
        intervals = []
        if len(peaks) >= 2:
            for i in range(1, len(peaks)):
                interval = peaks[i]['week_idx'] - peaks[i-1]['week_idx']
                intervals.append(interval)
                if 8 <= interval <= 22:  # 2-5个月
                    valid_intervals += 1
        
        # 计算平均间隔
        avg_interval = sum(intervals) / len(intervals) if intervals else 0
        
        # 判断是否符合庄股模式：
        # 严格条件：至少3个放量高点，且至少2个间隔在2-5个月范围内
        is_zhuang_pattern = (
            surge_peak_count >= 3 and      # 至少3个放量高点
            valid_intervals >= 2 and       # 至少2个间隔在合理范围内（8-22周）
            8 <= avg_interval <= 22        # 平均间隔也要在合理范围内
        )
        
        return {
            'surge_peak_count': surge_peak_count,  # 放量高点数量
            'valid_intervals': valid_intervals,     # 合理间隔数量
            'avg_interval_weeks': round(avg_interval, 1),  # 平均间隔周数
            'is_zhuang_pattern': is_zhuang_pattern,  # 是否符合庄股模式
            'peaks_detail': peaks,  # 高点详情（用于AI二次确认）
        }
    except Exception as e:
        return None


def analyze_wave_pattern(df_kline: pd.DataFrame):
    """分析波段特征"""
    if df_kline.empty or len(df_kline) < 10:
        return None
    
    try:
        # 计算周涨跌幅
        df_kline['pct_change'] = df_kline['close'].pct_change() * 100
        
        # 计算年度涨跌幅（如果数据足够）
        if len(df_kline) >= 52:
            year_start = df_kline.iloc[-52]['close']
            year_end = df_kline.iloc[-1]['close']
            yearly_change = (year_end - year_start) / year_start * 100
        else:
            yearly_change = (df_kline.iloc[-1]['close'] - df_kline.iloc[0]['close']) / df_kline.iloc[0]['close'] * 100
        
        # 计算单周最大涨跌幅
        max_up = df_kline['pct_change'].max()
        max_down = df_kline['pct_change'].min()
        
        # 计算平均换手率和峰值换手率
        if 'turn' in df_kline.columns:
            avg_turn = df_kline['turn'].mean()
            max_turn = df_kline['turn'].max()
        else:
            avg_turn = 0
            max_turn = 0
        
        # 计算价格区间
        price_high = df_kline['high'].max()
        price_low = df_kline['low'].min()
        price_range = (price_high - price_low) / price_low * 100
        
        # 统计波段（简单方法：统计趋势反转次数）
        df_kline['trend'] = df_kline['pct_change'].apply(lambda x: 1 if x > 0 else -1)
        df_kline['trend_change'] = df_kline['trend'].diff().abs()
        wave_count = df_kline['trend_change'].sum() / 2
        
        return {
            'yearly_change': round(yearly_change, 2),
            'max_up': round(max_up, 2),
            'max_down': round(max_down, 2),
            'avg_turn': round(avg_turn, 2),
            'max_turn': round(max_turn, 2),
            'price_high': round(price_high, 2),
            'price_low': round(price_low, 2),
            'price_range': round(price_range, 2),
            'wave_count': int(wave_count),
        }
    except Exception as e:
        return None


def screen_zhuang_stocks(df_stocks: pd.DataFrame, sample_size: int = None):
    """
    筛选具有庄股特征的股票
    
    庄股特征：
    1. 年度涨跌幅在±15%以内（箱体震荡）
    2. 单周最大涨跌幅 >= 10%（波幅大）
    3. 周换手率峰值 >= 10%（资金活跃）
    4. 波段次数 >= 6（多轮波段）
    """
    print("\n" + "=" * 80)
    print("步骤4: 分析波段特征，筛选庄股")
    print("=" * 80)
    
    if sample_size and len(df_stocks) > sample_size:
        df_sample = df_stocks.sample(n=sample_size, random_state=42)
        print(f"  抽样分析 {sample_size} 只股票")
    else:
        df_sample = df_stocks
        print(f"  分析 {len(df_sample)} 只股票")
    
    results = []
    found_count = 0  # 已发现的庄股数量
    total = len(df_sample)
    
    for idx, row in df_sample.iterrows():
        stock_code = row['股票代码']
        stock_name = row['股票名称']
        
        # 显示扫描进度
        progress = len(results) + 1
        print(f"\r  扫描进度: {progress}/{total} ({progress*100//total}%) | 已发现庄股: {found_count} 只 | 当前: {stock_code} {stock_name}    ", end='', flush=True)
        
        # 获取周线数据
        df_kline = get_weekly_kline(stock_code)
        
        if df_kline.empty:
            continue
        
        # 分析波段特征
        pattern = analyze_wave_pattern(df_kline)
        
        if pattern is None:
            continue
        
        # 获取日线数据，分析放量特征
        df_daily = get_daily_kline(stock_code)
        vol_pattern = analyze_volume_surge(df_daily)
        
        # 分析放量阶段性高点（核心庄股特征）
        surge_peaks = analyze_surge_peaks(df_kline, df_daily)
        
        result_item = {
            '股票代码': stock_code,
            '股票名称': stock_name,
            '现价': row.get('现价', 0),
            '总市值(亿)': row.get('总市值(亿)', 0),
            '流通市值(亿)': row.get('流通市值(亿)', 0),
            'PE(动)': row.get('PE(动)', None),
            '净利润(亿)': row.get('净利润(亿)', None),
            '年度涨跌幅(%)': pattern['yearly_change'],
            '单周最大涨幅(%)': pattern['max_up'],
            '单周最大跌幅(%)': pattern['max_down'],
            '平均换手率(%)': pattern['avg_turn'],
            '峰值换手率(%)': pattern['max_turn'],
            '价格区间高': pattern['price_high'],
            '价格区间低': pattern['price_low'],
            '价格波幅(%)': pattern['price_range'],
            '波段次数': pattern['wave_count'],
        }
        
        # 添加放量特征
        if vol_pattern:
            result_item['放量日数'] = vol_pattern['surge_days']
            result_item['最长连续放量'] = vol_pattern['max_streak']
            result_item['高位放量日'] = vol_pattern['high_surge_days']
            result_item['最大量比'] = vol_pattern['max_vol_ratio']
            result_item['平均放量倍数'] = vol_pattern['avg_surge_ratio']
        else:
            result_item['放量日数'] = 0
            result_item['最长连续放量'] = 0
            result_item['高位放量日'] = 0
            result_item['最大量比'] = 0
            result_item['平均放量倍数'] = 0
        
        # 添加放量高点特征
        if surge_peaks:
            result_item['放量高点数'] = surge_peaks['surge_peak_count']
            result_item['合理间隔数'] = surge_peaks['valid_intervals']
            result_item['平均间隔周'] = surge_peaks['avg_interval_weeks']
            result_item['庄股模式'] = '是' if surge_peaks['is_zhuang_pattern'] else '否'
            result_item['高点详情'] = surge_peaks['peaks_detail']
        else:
            result_item['放量高点数'] = 0
            result_item['合理间隔数'] = 0
            result_item['平均间隔周'] = 0
            result_item['庄股模式'] = '否'
            result_item['高点详情'] = []
        
        results.append(result_item)
        
        # 实时检测是否符合庄股特征（严格条件）
        # 核心条件：至少3个放量阶段性高点，间隔2-5个月
        is_zhuang = (
            abs(result_item['年度涨跌幅(%)']) <= 25 and  # 箱体震荡
            (result_item['单周最大涨幅(%)'] >= 8 or abs(result_item['单周最大跌幅(%)']) >= 8) and  # 波幅大
            result_item['峰值换手率(%)'] >= 8 and  # 换手活跃
            result_item['庄股模式'] == '是'  # 核心：符合放量高点模式
        )
        
        if is_zhuang:
            found_count += 1
            print(f"\n  🎯 [{found_count}] 发现庄股: {stock_code} {stock_name}")
            print(f"     市值:{result_item['总市值(亿)']}亿 | PE:{result_item['PE(动)']} | 净利润:{result_item['净利润(亿)']}亿")
            print(f"     年涨跌:{result_item['年度涨跌幅(%)']}% | 波幅:{result_item['价格波幅(%)']}% | 峰值换手:{result_item['峰值换手率(%)']}%")
            print(f"     🔥 放量高点:{result_item['放量高点数']}个 | 平均间隔:{result_item['平均间隔周']}周 | 合理间隔:{result_item['合理间隔数']}个")
            # 输出高点详情供AI二次确认
            if result_item['高点详情']:
                peaks_str = ' -> '.join([f"{p['date'].strftime('%m/%d')}({p['surge_days']}天放量)" for p in result_item['高点详情'][:5]])
                print(f"     📈 高点时间线: {peaks_str}")
        
        time.sleep(0.1)  # 避免请求过快
    
    print(f"\n  成功分析 {len(results)} 只股票")
    
    if not results:
        return pd.DataFrame()
    
    df_result = pd.DataFrame(results)
    return df_result


def filter_zhuang_pattern(df_analyzed: pd.DataFrame):
    """筛选符合庄股特征的股票"""
    print("\n" + "=" * 80)
    print("步骤5: 筛选符合庄股特征的股票")
    print("=" * 80)
    
    # 核心筛选条件（严格版）
    cond = (
        # 箱体震荡：年度涨跌幅在±25%以内
        (df_analyzed['年度涨跌幅(%)'].abs() <= 25) &
        # 波幅大：单周最大涨幅或跌幅 >= 8%
        ((df_analyzed['单周最大涨幅(%)'] >= 8) | (df_analyzed['单周最大跌幅(%)'].abs() >= 8)) &
        # 换手活跃：峰值换手率 >= 8%
        (df_analyzed['峰值换手率(%)'] >= 8) &
        # 核心条件：符合放量高点模式（>=3个放量高点，间隔合理）
        (df_analyzed['庄股模式'] == '是')
    )
    
    df_zhuang = df_analyzed[cond].copy()
    # 按放量高点数和平均间隔排序
    df_zhuang = df_zhuang.sort_values(['放量高点数', '平均间隔周'], ascending=[False, False])
    
    print(f"  符合庄股特征的股票: {len(df_zhuang)} 只")
    
    return df_zhuang


def display_results(df_zhuang: pd.DataFrame):
    """展示筛选结果"""
    print("\n" + "=" * 80)
    print("【庄股筛选结果】按放量阶段性高点排序")
    print("筛选逻辑: 箱体震荡(±25%) + 波幅大(8%+) + 换手高(8%+) + >=3个放量高点(间隔2-5月)")
    print("=" * 80)
    
    if df_zhuang.empty:
        print("  未找到符合条件的股票")
        return
    
    cols = ['股票代码', '股票名称', '现价', '总市值(亿)', 'PE(动)', '净利润(亿)',
            '年度涨跌幅(%)', '峰值换手率(%)', '价格波幅(%)',
            '放量高点数', '平均间隔周', '合理间隔数', '高位放量日', '最大量比']
    
    # 按市值分组显示
    def show_group(title, data):
        print(f"\n{'-' * 80}")
        print(title)
        print(f"{'-' * 80}")
        if len(data) > 0:
            print(data[cols].to_string(index=False))
        else:
            print("无")
    
    show_group("【小市值】30-50亿（控盘容易）",
               df_zhuang[(df_zhuang['总市值(亿)'] >= 30) & (df_zhuang['总市值(亿)'] < 50)])
    
    show_group("【中小市值】50-70亿",
               df_zhuang[(df_zhuang['总市值(亿)'] >= 50) & (df_zhuang['总市值(亿)'] < 70)])
    
    show_group("【中市值】70-100亿",
               df_zhuang[(df_zhuang['总市值(亿)'] >= 70) & (df_zhuang['总市值(亿)'] <= 100)])
    
    # 统计汇总
    print(f"\n{'=' * 80}")
    print("【统计汇总】")
    print(f"{'=' * 80}")
    print(f"  符合条件的股票总数: {len(df_zhuang)} 只")
    print(f"  平均峰值换手率: {df_zhuang['峰值换手率(%)'].mean():.2f}%")
    print(f"  平均价格波幅: {df_zhuang['价格波幅(%)'].mean():.2f}%")
    print(f"  平均放量高点数: {df_zhuang['放量高点数'].mean():.1f}个")
    print(f"  平均高点间隔: {df_zhuang['平均间隔周'].mean():.1f}周")
    print(f"  平均高位放量日: {df_zhuang['高位放量日'].mean():.1f}天")


def main(min_cap: float = 30, max_cap: float = 100, sample_size: int = None):
    """
    主函数
    
    Args:
        min_cap: 最小市值（亿），默认30亿
        max_cap: 最大市值（亿），默认100亿
        sample_size: 抽样数量，None表示全量分析
    """
    print("=" * 80)
    print("庄股筛选工具 - 股票基础信息获取")
    print(f"运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"市值范围: {min_cap}-{max_cap}亿")
    print("=" * 80)
    
    # 1. 获取股票列表
    df_all = get_all_stocks()
    
    if df_all.empty:
        print("\n获取股票列表失败")
        return
    
    # 2. 按市值筛选
    df_filtered = filter_by_market_cap(df_all, min_cap, max_cap)
    
    if df_filtered.empty:
        print("\n没有符合市值条件的股票")
        return
    
    # 3. 分析波段特征
    df_analyzed = screen_zhuang_stocks(df_filtered, sample_size)
    
    if df_analyzed.empty:
        print("\n分析失败，无有效数据")
        return
    
    # 4. 筛选庄股特征
    df_zhuang = filter_zhuang_pattern(df_analyzed)
    
    # 5. 展示结果
    display_results(df_zhuang)
    
    # 6. 保存结果
    # 脚本位于 .github/skills/baostock-guide/scripts/ 下，需要往上4层到项目根目录
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(script_dir))))
    
    # 保存全量分析结果
    analyzed_file = os.path.join(project_root, 'stock_wave_analysis.csv')
    df_analyzed.to_csv(analyzed_file, index=False, encoding='utf-8-sig')
    print(f"\n全量分析结果已保存到: {analyzed_file}")
    
    # 保存庄股筛选结果
    if len(df_zhuang) > 0:
        zhuang_file = os.path.join(project_root, 'screened_zhuang_stocks.csv')
        df_zhuang.to_csv(zhuang_file, index=False, encoding='utf-8-sig')
        print(f"庄股筛选结果已保存到: {zhuang_file}")
    
    return df_analyzed, df_zhuang


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='庄股筛选工具')
    parser.add_argument('--min-cap', type=float, default=30, help='最小市值（亿）')
    parser.add_argument('--max-cap', type=float, default=100, help='最大市值（亿）')
    parser.add_argument('--sample', type=int, default=None, help='抽样数量（默认全量）')
    
    args = parser.parse_args()
    
    main(min_cap=args.min_cap, max_cap=args.max_cap, sample_size=args.sample)
