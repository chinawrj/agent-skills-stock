#!/usr/bin/env python3
"""
股东人数数据管理模块
提供股东人数数据的缓存读取、在线获取和数据库更新功能
"""

import os
import sys
from datetime import datetime, date
from typing import Optional, List, Dict, Any

import duckdb
import pandas as pd

# 数据库路径
DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'data', 'a-share.db')


def get_db_connection() -> duckdb.DuckDBPyConnection:
    """获取数据库连接"""
    db_path = os.path.abspath(DB_PATH)
    return duckdb.connect(db_path)


def get_cached_latest(code: str, limit: int = 10) -> pd.DataFrame:
    """
    从缓存获取单只股票最新的股东人数数据
    
    Args:
        code: 股票代码（如 "300401"）
        limit: 返回的记录数量
    
    Returns:
        DataFrame 包含最新的股东人数历史记录
    """
    conn = get_db_connection()
    try:
        df = conn.execute("""
            SELECT code, name, stat_date, announce_date, 
                   shareholders, shareholders_prev, change, change_ratio,
                   range_change_pct, avg_value, avg_shares, 
                   market_cap, total_shares, shares_change, shares_change_reason
            FROM shareholders 
            WHERE code = ?
            ORDER BY stat_date DESC
            LIMIT ?
        """, [code, limit]).df()
        return df
    finally:
        conn.close()


def get_cached_all(code: str) -> pd.DataFrame:
    """
    从缓存获取单只股票所有历史股东人数数据
    
    Args:
        code: 股票代码
    
    Returns:
        DataFrame 包含所有历史记录
    """
    conn = get_db_connection()
    try:
        df = conn.execute("""
            SELECT * FROM shareholders 
            WHERE code = ?
            ORDER BY stat_date DESC
        """, [code]).df()
        return df
    finally:
        conn.close()


def get_cached_count(code: str) -> int:
    """获取缓存中某只股票的记录数"""
    conn = get_db_connection()
    try:
        result = conn.execute(
            "SELECT COUNT(*) FROM shareholders WHERE code = ?", [code]
        ).fetchone()
        return result[0] if result else 0
    finally:
        conn.close()


def fetch_online_history(code: str) -> pd.DataFrame:
    """
    从东方财富在线获取单只股票的完整股东人数历史
    
    Args:
        code: 股票代码
    
    Returns:
        DataFrame 包含在线获取的历史数据
    """
    import akshare as ak
    
    df = ak.stock_zh_a_gdhs_detail_em(symbol=code)
    
    # 重命名列
    df = df.rename(columns={
        '代码': 'code',
        '名称': 'name',
        '股东户数统计截止日': 'stat_date',
        '股东户数公告日期': 'announce_date',
        '股东户数-本次': 'shareholders',
        '股东户数-上次': 'shareholders_prev',
        '股东户数-增减': 'change',
        '股东户数-增减比例': 'change_ratio',
        '区间涨跌幅': 'range_change_pct',
        '户均持股市值': 'avg_value',
        '户均持股数量': 'avg_shares',
        '总市值': 'market_cap',
        '总股本': 'total_shares',
        '股本变动': 'shares_change',
        '股本变动原因': 'shares_change_reason'
    })
    
    # 转换日期格式
    df['stat_date'] = pd.to_datetime(df['stat_date']).dt.date
    df['announce_date'] = pd.to_datetime(df['announce_date']).dt.date
    
    return df


def _safe_get(row, key, default=None):
    """安全获取值，处理 NaN"""
    val = row.get(key, default)
    if pd.isna(val):
        return default
    return val


def save_to_cache(df: pd.DataFrame, silent: bool = True) -> int:
    """
    将数据保存到缓存数据库（使用 INSERT OR REPLACE）
    
    Args:
        df: 包含股东人数数据的 DataFrame
        silent: 是否静默模式（不打印输出）
    
    Returns:
        插入/更新的记录数
    """
    if df.empty:
        return 0
    
    conn = get_db_connection()
    try:
        # 准备数据，处理 NaN 值
        records = []
        for _, row in df.iterrows():
            records.append((
                row['code'],
                _safe_get(row, 'name'),
                _safe_get(row, 'shareholders'),
                _safe_get(row, 'shareholders_prev'),
                _safe_get(row, 'change'),
                _safe_get(row, 'change_ratio'),
                None,  # price
                None,  # change_pct
                row['stat_date'],
                _safe_get(row, 'announce_date'),
                _safe_get(row, 'avg_value'),
                _safe_get(row, 'avg_shares'),
                _safe_get(row, 'market_cap'),
                _safe_get(row, 'range_change_pct'),
                _safe_get(row, 'total_shares'),
                _safe_get(row, 'shares_change'),
                _safe_get(row, 'shares_change_reason')
            ))
        
        # 使用 INSERT OR REPLACE
        conn.executemany("""
            INSERT OR REPLACE INTO shareholders 
            (code, name, shareholders, shareholders_prev, change, change_ratio,
             price, change_pct, stat_date, announce_date, avg_value, avg_shares,
             market_cap, range_change_pct, total_shares, shares_change, shares_change_reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, records)
        
        count = len(records)
        
        if not silent:
            print(f"已保存 {count} 条记录到缓存")
        
        return count
    finally:
        conn.close()


def get_merged_latest(code: str, auto_cache: bool = True, silent: bool = True) -> pd.DataFrame:
    """
    获取融合的最新股东人数数据（缓存 + 在线）
    
    会自动从在线获取最新数据并更新缓存
    
    Args:
        code: 股票代码
        auto_cache: 是否自动缓存在线数据
        silent: 是否静默模式
    
    Returns:
        DataFrame 包含最新的股东人数数据
    """
    # 获取在线数据
    if not silent:
        print(f"正在从东方财富获取 {code} 的股东人数历史...")
    
    try:
        online_df = fetch_online_history(code)
    except Exception as e:
        if not silent:
            print(f"在线获取失败: {e}，使用缓存数据")
        return get_cached_all(code)
    
    if online_df.empty:
        if not silent:
            print("在线数据为空，使用缓存数据")
        return get_cached_all(code)
    
    # 自动缓存
    if auto_cache:
        save_to_cache(online_df, silent=silent)
        if not silent:
            print(f"已自动缓存 {len(online_df)} 条记录")
    
    return online_df


def update_stock_history(code: str, silent: bool = True) -> Dict[str, Any]:
    """
    静默更新单只股票的历史股东数据
    
    Args:
        code: 股票代码
        silent: 是否静默模式
    
    Returns:
        更新结果字典
    """
    result = {
        'code': code,
        'success': False,
        'cached_before': 0,
        'online_fetched': 0,
        'cached_after': 0,
        'new_records': 0,
        'error': None
    }
    
    try:
        # 获取缓存中的记录数
        result['cached_before'] = get_cached_count(code)
        
        # 从在线获取
        online_df = fetch_online_history(code)
        result['online_fetched'] = len(online_df)
        
        if online_df.empty:
            result['error'] = '在线数据为空'
            return result
        
        # 保存到缓存
        save_to_cache(online_df, silent=True)
        
        # 获取更新后的记录数
        result['cached_after'] = get_cached_count(code)
        result['new_records'] = result['cached_after'] - result['cached_before']
        result['success'] = True
        
        if not silent:
            name = online_df['name'].iloc[0] if 'name' in online_df.columns else code
            print(f"✓ {name}({code}): 在线{result['online_fetched']}条, "
                  f"缓存{result['cached_before']}→{result['cached_after']}条, "
                  f"新增{result['new_records']}条")
        
        return result
        
    except Exception as e:
        result['error'] = str(e)
        if not silent:
            print(f"✗ {code}: 更新失败 - {e}")
        return result


def batch_update(codes: List[str], silent: bool = False) -> List[Dict[str, Any]]:
    """
    批量更新多只股票的股东人数历史
    
    Args:
        codes: 股票代码列表
        silent: 是否静默模式
    
    Returns:
        更新结果列表
    """
    results = []
    total = len(codes)
    
    for i, code in enumerate(codes):
        if not silent:
            print(f"[{i+1}/{total}] 更新 {code}...")
        
        result = update_stock_history(code, silent=silent)
        results.append(result)
    
    # 汇总统计
    if not silent:
        success = sum(1 for r in results if r['success'])
        total_new = sum(r['new_records'] for r in results)
        print(f"\n更新完成: {success}/{total} 成功, 共新增 {total_new} 条记录")
    
    return results


def get_cache_summary() -> pd.DataFrame:
    """获取缓存数据汇总"""
    conn = get_db_connection()
    try:
        df = conn.execute("""
            SELECT 
                code,
                name,
                COUNT(*) as record_count,
                MIN(stat_date) as earliest_date,
                MAX(stat_date) as latest_date,
                MAX(created_at) as last_updated
            FROM shareholders
            GROUP BY code, name
            ORDER BY latest_date DESC
        """).df()
        return df
    finally:
        conn.close()


# 命令行接口
if __name__ == '__main__':
    import argparse
    import sys
    
    # 检查是否是子命令
    subcommands = {'get', 'update', 'summary'}
    
    if len(sys.argv) > 1 and sys.argv[1] in subcommands:
        # 子命令模式
        parser = argparse.ArgumentParser(description='股东人数数据管理')
        subparsers = parser.add_subparsers(dest='command')
        
        get_parser = subparsers.add_parser('get', help='从远端获取最新数据并缓存')
        get_parser.add_argument('code', help='股票代码')
        get_parser.add_argument('--no-cache', action='store_true', help='不自动缓存')
        
        update_parser = subparsers.add_parser('update', help='从远端更新股东人数历史')
        update_parser.add_argument('codes', nargs='+', help='股票代码（支持多个）')
        update_parser.add_argument('-s', '--silent', action='store_true', help='静默模式')
        
        summary_parser = subparsers.add_parser('summary', help='查看缓存数据汇总')
        
        args = parser.parse_args()
        
        if args.command == 'get':
            df = get_merged_latest(args.code, auto_cache=not args.no_cache, silent=False)
            print(f"\n共 {len(df)} 条记录")
            print(df.head(10).to_string())
        elif args.command == 'update':
            batch_update(args.codes, silent=args.silent)
        elif args.command == 'summary':
            df = get_cache_summary()
            if df.empty:
                print("缓存为空")
            else:
                print(df.to_string())
    else:
        # 默认模式：从缓存查询
        parser = argparse.ArgumentParser(description='股东人数数据管理（默认从缓存查询）')
        parser.add_argument('code', nargs='?', help='股票代码')
        parser.add_argument('-n', '--limit', type=int, default=10, help='返回记录数')
        parser.add_argument('-a', '--all', action='store_true', help='返回所有历史记录')
        
        args = parser.parse_args()
        
        if args.code:
            if args.all:
                df = get_cached_all(args.code)
            else:
                df = get_cached_latest(args.code, args.limit)
            if df.empty:
                print(f"缓存中没有 {args.code} 的数据")
            else:
                print(df.to_string())
        else:
            df = get_cache_summary()
            if df.empty:
                print("缓存为空")
            else:
                print(df.to_string())
